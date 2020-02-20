"""NCI NDR Analysis.

Design doc is available here:

https://docs.google.com/document/d/
1Iw8YxrXPSbSp5TemRo-mbfvxDiTpdCKqRrW1terp2gE/edit

"""
import argparse
import datetime
import json
import logging
import os
import multiprocessing
import pathlib
import queue
import sqlite3
import subprocess
import sys
import threading
import time
import uuid

from osgeo import gdal
from osgeo import osr
import flask
import numpy
import pygeoprocessing
import requests
import retrying
import taskgraph

gdal.SetCacheMax(2**29)


WATERSHEDS_URL = (
    'https://nci-ecoshards.s3-us-west-1.amazonaws.com/'
    'watersheds_globe_HydroSHEDS_15arcseconds_'
    'blake2b_14ac9c77d2076d51b0258fd94d9378d4.zip')

WORKSPACE_DIR = 'nci_stitcher_workspace'
ECOSHARD_DIR = os.path.join(WORKSPACE_DIR, 'ecoshards')
CHURN_DIR = os.path.join(WORKSPACE_DIR, 'churn')
STATUS_DATABASE_PATH = os.path.join(CHURN_DIR, 'status_database.sqlite3')
TILE_DIR = os.path.join(CHURN_DIR, 'tiles')
DATABASE_TOKEN_PATH = os.path.join(
    CHURN_DIR, '%s.CREATED' % os.path.basename(STATUS_DATABASE_PATH))
GRID_STEP_SIZE = 2


logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
logging.getLogger('taskgraph').setLevel(logging.INFO)

DETECTOR_POLL_TIME = 30.0
SCHEDULED_MAP = {}
GLOBAL_LOCK = threading.Lock()
RESULT_QUEUE = multiprocessing.Queue()
RESCHEDULE_QUEUE = queue.Queue()
WGS84_SR = osr.SpatialReference()
WGS84_SR.ImportFromEPSG(4326)
WGS84_WKT = WGS84_SR.ExportToWkt()

WORKER_TAG_ID = 'compute-server'
# this form must be of 's3://[bucket id]/[subdir]' any change should be updated
# in the worker when it uploads the zip file
BUCKET_URI_PREFIX = 's3://nci-ecoshards/ndr_stitches/tiles'
GLOBAL_STITCH_WGS84_CELL_SIZE = 0.002
GLOBAL_STITCH_NODATA = -1e38

APP = flask.Flask(__name__)


class WorkerStateSet(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.host_ready_event = threading.Event()
        self.ready_host_set = set()
        self.running_host_set = set()

    def add_host(self, host):
        """Add a host if it's not already in the set."""
        with self.lock:
            for internal_set in [self.ready_host_set, self.running_host_set]:
                if host in internal_set:
                    return False
            self.ready_host_set.add(host)
            LOGGER.debug('just added %s so setting the flag', host)
            self.host_ready_event.set()
            return True

    def get_ready_host(self):
        """Blocking call to fetch a ready host."""
        # this blocks until there is something in the ready host set
        self.host_ready_event.wait()
        with self.lock:
            ready_host = next(iter(self.ready_host_set))
            LOGGER.debug('this host is ready: %s', ready_host)
            self.ready_host_set.remove(ready_host)
            self.running_host_set.add(ready_host)
            if not self.ready_host_set:
                LOGGER.debug('no more ready hosts, clear the flag')
                self.host_ready_event.clear()
            LOGGER.debug('returning ready host: %s', ready_host)
            return ready_host

    def get_counts(self):
        with self.lock:
            return len(self.running_host_set), len(self.ready_host_set)

    def remove_host(self, host):
        """Remove a host from the ready or running set."""
        with self.lock:
            for internal_set in [self.ready_host_set, self.running_host_set]:
                if host in internal_set:
                    internal_set.remove(host)
                    return True
            LOGGER.warn('%s not in set' % host)
            return False

    def set_ready_host(self, host):
        """Indicate a running host is now ready for use."""
        with self.lock:
            if host in self.running_host_set:
                self.running_host_set.remove(host)
            self.ready_host_set.add(host)
            self.host_ready_event.set()

    def update_host_set(self, active_host_set):
        """Remove hosts not in `active_host_set`.

            Returns:
                set of removed hosts.

        """
        with self.lock:
            new_hosts = (
                active_host_set - self.ready_host_set - self.running_host_set)
            if new_hosts:
                LOGGER.debug('update_host_set: new hosts: %s', new_hosts)
            # remove hosts that aren't in the active host set
            removed_hosts = set()
            for working_host in [self.ready_host_set, self.running_host_set]:
                dead_hosts = working_host - active_host_set
                removed_hosts |= dead_hosts
                if dead_hosts:
                    LOGGER.debug('dead hosts: %s', dead_hosts)
                working_host -= dead_hosts

            # add the active hosts to the ready host set
            self.ready_host_set |= new_hosts
            if self.ready_host_set:
                self.host_ready_event.set()
        return removed_hosts


GLOBAL_WORKER_STATE_SET = WorkerStateSet()


def new_host_monitor(worker_list=None):
    """Watch for AWS worker instances on the network.

    Parameters:
        worker_list (list): if not not this is a list of ip:port strings that
            can be used to connect to workers. Used for running locally/debug.

    Returns:
        never

    """
    if worker_list:
        GLOBAL_WORKER_STATE_SET.update_host_set(set(worker_list))
        return
    while True:
        try:
            raw_output = subprocess.check_output(
                'aws2 ec2 describe-instances', shell=True)
            out_json = json.loads(raw_output)
            working_host_set = set()
            for reservation in out_json['Reservations']:
                for instance in reservation['Instances']:
                    try:
                        if 'Tags' not in instance:
                            continue
                        for tag in instance['Tags']:
                            if tag['Value'] == WORKER_TAG_ID and (
                                    instance['State']['Name'] == (
                                        'running')):
                                working_host_set.add(
                                    '%s:8888' % instance['PrivateIpAddress'])
                                break
                    except Exception:
                        LOGGER.exception('something bad happened')
            dead_hosts = GLOBAL_WORKER_STATE_SET.update_host_set(
                working_host_set)
            if dead_hosts:
                with GLOBAL_LOCK:
                    session_list_to_remove = []
                    for session_id, value in SCHEDULED_MAP.items():
                        if value['host'] in dead_hosts:
                            LOGGER.debug(
                                'found a dead host executing something: %s',
                                value['host'])
                            session_list_to_remove.append(session_id)
                    for session_id in session_list_to_remove:
                        RESCHEDULE_QUEUE.put(
                            SCHEDULED_MAP[session_id][
                                'watershed_fid_tuple_list'])
                        del SCHEDULED_MAP[session_id]
            time.sleep(DETECTOR_POLL_TIME)
        except Exception:
            LOGGER.exception('exception in `new_host_monitor`')


@APP.route('/api/v1/processing_status', methods=['GET'])
def processing_status():
    """Download necessary data and initalize empty rasters if needed."""
    try:
        ro_uri = 'file://%s?mode=ro' % os.path.abspath(STATUS_DATABASE_PATH)
        connection = sqlite3.connect(ro_uri, uri=True)
        cursor = connection.cursor()
        LOGGER.debug('querying prescheduled')
        cursor.execute('SELECT count(1) from job_status')
        total_count = int(cursor.fetchone()[0])
        cursor.execute(
            'SELECT count(1) FROM job_status '
            'WHERE (stiched=1)')
        stitched_count = int(cursor.fetchone()[0])
        connection.commit()
        connection.close()
        active_count, ready_count = (
            GLOBAL_WORKER_STATE_SET.get_counts())

        uptime = time.time() - START_TIME
        hours = uptime // 3600
        minutes = (uptime - hours*3600) // 60
        seconds = uptime % 60
        uptime_str = '%dh:%.2dm:%2.ds' % (
            hours, minutes, seconds)
        result_string = (
            'percent stitched: %.2f%% (%d)<br>'
            'total to stitch: %d<br>'
            'total left to stitch: %d<br>'
            'uptime: %s<br>'
            'active workers: %d<br>'
            'ready workers: %d<br>' % (
                100.0*stitched_count/total_count,
                stitched_count,
                total_count,
                total_count-stitched_count,
                uptime_str,
                active_count, ready_count))
        return result_string
    except Exception as e:
        return 'error: %s' % str(e)


GLOBAL_STATUS = {}
SCENARIO_ID_LIST = [
    'baseline_potter', 'baseline_napp_rate', 'ag_expansion',
    'ag_intensification', 'restoration_potter', 'restoration_napp_rate']

GLOBAL_STITCH_MAP = {
    'n_export': (
        'workspace_worker/[BASENAME]_[FID]/n_export.tif',
        gdal.GDT_Float32, -1),
    'modified_load_n': (
        'workspace_worker/[BASENAME]_[FID]/intermediate_outputs/'
        'modified_load_n.tif',
        gdal.GDT_Float32, -1),
}


def create_status_database(database_path, complete_token_path):
    """Create a runtime status database if it doesn't exist.

    Parameters:
        database_path (str): path to database to create.
        complete_token_path (str): path to a text file that will be created
            by this function written with the timestamp when it finishes.

    Returns:
        None.

    """
    LOGGER.debug('launching create_status_database')
    create_database_sql = (
        """
        CREATE TABLE job_status (
            grid_id INTEGER NOT NULL,
            scenario_id TEXT NOT NULL,
            raster_id TEXT NOT NULL,
            lng_min FLOAT NOT NULL,
            lat_min FLOAT NOT NULL,
            lng_max FLOAT NOT NULL,
            lat_max FLOAT NOT NULL,
            stiched INT NOT NULL);
        """)
    if os.path.exists(database_path):
        os.remove(database_path)
    connection = sqlite3.connect(database_path)
    cursor = connection.cursor()
    cursor.executescript(create_database_sql)

    with open(complete_token_path, 'w') as token_file:
        token_file.write(str(datetime.datetime.now()))
    scenario_output_lat_lng_list = []
    grid_id = 0
    for scenario_id in SCENARIO_ID_LIST:
        GLOBAL_STATUS[scenario_id] = {}
        for raster_id in GLOBAL_STITCH_MAP:
            for lat_max in reversed(range(90, -90, -GRID_STEP_SIZE)):
                lat_min = lat_max - GRID_STEP_SIZE
                for lng_min in range(-180, 180, GRID_STEP_SIZE):
                    lng_max = lng_min + GRID_STEP_SIZE
                    scenario_output_lat_lng_list.append(
                        (grid_id, scenario_id, raster_id, lng_min, lat_min,
                         lng_max, lat_max))
                    grid_id += 1
    insert_query = (
        'INSERT INTO job_status('
        'grid_id, scenario_id, raster_id, lng_min, lat_min, lng_max, lat_max, '
        'stiched) '
        'VALUES (?, ?, ?, ?, ?, ?, ?, 0)')
    cursor.executemany(insert_query, scenario_output_lat_lng_list)
    with open(complete_token_path, 'w') as complete_token_file:
        complete_token_file.write(str(datetime.datetime.now()))
    connection.commit()
    connection.close()


@retrying.retry(wait_exponential_multiplier=1000, wait_exponential_max=10000)
def schedule_worker():
    """Monitors STATUS_DATABASE_PATH and schedules work.

    Returns:
        None.

    """
    try:
        LOGGER.debug('launching schedule_worker')
        ro_uri = pathlib.Path(os.path.abspath(
            STATUS_DATABASE_PATH)).as_uri() + '?mode=ro'
        LOGGER.debug('opening %s', ro_uri)
        connection = sqlite3.connect(ro_uri, uri=True)
        cursor = connection.cursor()
        LOGGER.debug('querying unstitched')
        cursor.execute(
            'SELECT grid_id, scenario_id, raster_id, '
            'lng_min, lat_min, lng_max, lat_max '
            'FROM job_status WHERE stiched=0 AND '
            'lng_min>=10 AND lat_min>=0 AND lng_max<=16 AND lat_max<=4')
        payload_list = list(cursor.fetchall())
        connection.commit()
        connection.close()

        for job_tuple in payload_list:
            job_payload = {
                'grid_id': job_tuple[0],
                'scenario_id': job_tuple[1],
                'raster_id': job_tuple[2],
                'lng_min': job_tuple[3],
                'lat_min': job_tuple[4],
                'lng_max': job_tuple[5],
                'lat_max': job_tuple[6],
            }
            LOGGER.debug('scheduling %s', job_payload)
            send_job(job_payload)

    except Exception:
        LOGGER.exception('exception in scheduler')
        raise


@APP.route('/api/v1/processing_complete', methods=['POST'])
def processing_complete():
    """Invoked when processing is complete for given watershed.

    Body of the post includs a url to the stored .zip file of the archive.

    Returns
        None.

    """
    try:
        payload = flask.request.get_json()
        LOGGER.debug('this was the payload: %s', payload)
        session_id = payload['session_id']
        host = SCHEDULED_MAP[session_id]['host']
        del SCHEDULED_MAP[session_id]
        RESULT_QUEUE.put(payload)
        GLOBAL_WORKER_STATE_SET.set_ready_host(host)
        return 'complete', 202
    except Exception:
        LOGGER.exception(
            'error on processing completed for host %s. session_ids: %s',
            flask.request.remote_addr, str(SCHEDULED_MAP))


@retrying.retry(wait_exponential_multiplier=1000, wait_exponential_max=5000)
def global_stitcher(result_queue):
    """Worker to stitch global raster.

    Parameters:
        result_queue (multiprocessing.Queue): this queue will dump payloads
            that are ready to stitch.

    """
    LOGGER.debug('starting global stitcher')
    while True:
        try:
            payload = result_queue.get()
            LOGGER.debug('stitching this payload: %s' % payload)
            geotiff_s3_uri = payload['geotiff_s3_uri']
            local_tile_raster_path = os.path.join(
                TILE_DIR, os.path.basename(geotiff_s3_uri))
            subprocess.run(
                ["/usr/local/bin/aws2 s3 cp %s %s" % (
                    geotiff_s3_uri, local_tile_raster_path)], shell=True,
                check=True)
            raster_id = payload['raster_id']
            scenario_id = payload['scenario_id']
            global_stitch_raster_path = \
                GLOBAL_STITCH_PATH_MAP[(raster_id, scenario_id)]

            # get ul of tile and figure out where it goes in global
            local_tile_info = pygeoprocessing.get_raster_info(
                local_tile_raster_path)
            global_stitch_info = pygeoprocessing.get_raster_info(
                global_stitch_raster_path)
            global_inv_gt = gdal.InvGeoTransform(
                global_stitch_info['geotransform'])
            local_gt = local_tile_info['geotransform']
            global_i, global_j = gdal.ApplyGeoTransform(
                global_inv_gt, local_gt[0], local_gt[3])

            local_tile_raster = gdal.OpenEx(
                local_tile_raster_path, gdal.OF_RASTER)
            local_array = local_tile_raster.ReadAsArray()
            local_tile_raster = None
            global_raster = gdal.OpenEx(
                global_stitch_raster_path, gdal.OF_RASTER | gdal.GA_Update)
            global_band = global_raster.GetRasterBand(1)
            global_array = global_band.ReadAsArray(
                xoff=global_i, yoff=global_j,
                win_xsize=local_array.shape[1], win_ysize=local_array.shape[0])
            valid_mask = ~numpy.isclose(
                local_array, local_tile_info['nodata'][0])
            global_array[valid_mask] = local_array[valid_mask]
            global_band.WriteArray(global_array, xoff=global_i, yoff=global_j)
            global_band.FlushCache()
            global_band = None
            global_raster = None
            try:
                os.remove(local_tile_raster_path)
            except OSError:
                LOGGER.exception('unable to remove %s', local_tile_raster_path)

            while True:
                try:
                    connection = sqlite3.connect(STATUS_DATABASE_PATH)
                    cursor = connection.cursor()
                    LOGGER.debug(
                        'setting grid id %s to stitched', payload['grid_id'])
                    cursor.execute(
                        'UPDATE job_status '
                        'SET stitched=1 '
                        'WHERE grid_id=?',
                        (payload['grid_id'],))
                    break
                except Exception:
                    LOGGER.exception('error on connection')
                    time.sleep(0.1)
                finally:
                    connection.commit()
                    cursor.close()
                    connection.close()
                    LOGGER.debug('%s inserted', payload['grid_id'])

        except Exception:
            LOGGER.exception('error on global stitcher')
            raise


@retrying.retry(wait_exponential_multiplier=1000, wait_exponential_max=5000)
def send_job(job_payload):
    """Send a job tuple to the worker pool.

    Parameters:
        job_payload (dict): a dictionary with information to send to the worker
            process. This description is general so it's easy to change the
            data without changing the pipeline.

    Returns:
        None.

    """
    try:
        LOGGER.debug('scheduling %s', job_payload)
        with APP.app_context():
            LOGGER.debug('about to get url')
            callback_url = flask.url_for(
                'processing_complete', _external=True)
        LOGGER.debug('get available worker')
        worker_ip_port = GLOBAL_WORKER_STATE_SET.get_ready_host()
        LOGGER.debug('this is the worker: %s', worker_ip_port)
        session_id = str(uuid.uuid4())
        LOGGER.debug('this is the session id: %s', session_id)
        data_payload = {
            'job_payload': job_payload,
            'callback_url': callback_url,
            'bucket_uri_prefix': BUCKET_URI_PREFIX,
            'session_id': session_id,
            'wgs84_pixel_size': GLOBAL_STITCH_WGS84_CELL_SIZE,
        }

        LOGGER.debug('payload: %s', data_payload)
        LOGGER.debug('got this worker: %s', worker_ip_port)
        worker_rest_url = (
            'http://%s/api/v1/stitch_grid_cell' % worker_ip_port)
        LOGGER.debug(
            'sending job %s to %s', data_payload, worker_rest_url)
        response = requests.post(
            worker_rest_url, json=data_payload)
        if response.ok:
            LOGGER.debug('%s scheduled', job_payload)
            SCHEDULED_MAP[session_id] = {
                'status_url': response.json()['status_url'],
                'job_payload': job_payload,
                'last_time_accessed': time.time(),
                'host': worker_ip_port
            }
        else:
            raise RuntimeError(str(response))
    except Exception as e:
        LOGGER.debug('in the exception: %s', e)
        LOGGER.exception(
            'something bad happened, on %s for %s',
            worker_ip_port, job_payload)
        LOGGER.debug('removing %s from worker set', worker_ip_port)
        GLOBAL_WORKER_STATE_SET.remove_host(worker_ip_port)
        raise
    finally:
        LOGGER.debug('in the finally')


def make_empty_wgs84_raster(
        cell_size, nodata_value, target_datatype, target_raster_path,
        token_data, target_token_complete_path):
    """Make a big empty raster in WGS84 projection.

    Parameters:
        cell_size (float): this is the desired cell size in WSG84 degree
            units.
        nodata_value (float): desired nodata avlue of target raster
        target_datatype (gdal enumerated type): desired target datatype.
        target_raster_path (str): this is the target raster that will cover
            [-180, 180), [90, -90) with cell size units with y direction being
            negative.
        token_data (str): data to write to the token to make a unique function
            signature.
        target_token_complete_path (str): this file is created if the
            mosaic to target is successful. Useful for taskgraph task
            scheduling.

    Returns:
        None.

    """
    LOGGER.info('creating empty raster for %s', target_raster_path)
    gtiff_driver = gdal.GetDriverByName('GTiff')
    try:
        os.makedirs(os.path.dirname(target_raster_path))
    except OSError:
        pass

    n_cols = int(abs(360.0 / cell_size[0]))
    n_rows = int(abs(180.0 / cell_size[1]))
    geotransform = (-180.0, cell_size[0], 0.0, 90.0, 0, cell_size[1])

    target_raster = gtiff_driver.Create(
        target_raster_path, n_cols, n_rows, 1, target_datatype,
        options=(
            'TILED=YES', 'BIGTIFF=YES', 'BLOCKXSIZE=256', 'BLOCKYSIZE=256',
            'COMPRESS=LZW', 'SPARSE_OK=TRUE'))
    wgs84_sr = osr.SpatialReference()
    wgs84_sr.ImportFromEPSG(4326)
    target_raster.SetProjection(wgs84_sr.ExportToWkt())
    target_raster.SetGeoTransform(geotransform)
    target_band = target_raster.GetRasterBand(1)
    target_band.SetNoDataValue(nodata_value)
    target_band.Fill(nodata_value)
    target_band = None
    target_raster = None

    target_raster = gdal.OpenEx(target_raster_path, gdal.OF_RASTER)
    if target_raster:
        with open(target_token_complete_path, 'w') as target_token_file:
            target_token_file.write(token_data+str(datetime.datetime.now()))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='NCI NDR Stitching.')
    parser.add_argument(
        '--app_port', type=int, default=8080,
        help='port to listen on for callback complete')
    parser.add_argument(
        '--external_ip', type=str, default='localhost',
        help='define external IP that can be used to connect to this app')
    parser.add_argument(
        '--worker_list', type=str, nargs='+', default=None,
        help='ip:port strings for local workers.')
    args = parser.parse_args()

    for dir_path in [
            WORKSPACE_DIR, ECOSHARD_DIR, CHURN_DIR, TILE_DIR]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    task_graph = taskgraph.TaskGraph(CHURN_DIR, -1)
    task_graph.add_task(
        func=create_status_database,
        args=(STATUS_DATABASE_PATH, DATABASE_TOKEN_PATH),
        target_path_list=[DATABASE_TOKEN_PATH],
        ignore_path_list=[STATUS_DATABASE_PATH],
        task_name='create status database')

    GLOBAL_STITCH_PATH_MAP = {}
    for scenario_id in SCENARIO_ID_LIST:
        GLOBAL_STATUS[scenario_id] = {}
        for raster_id in GLOBAL_STITCH_MAP:
            global_stitch_raster_path = os.path.join(
                TILE_DIR, '%s_%s_global.tif' % (raster_id, scenario_id))
            GLOBAL_STITCH_PATH_MAP[(raster_id, scenario_id)] = \
                global_stitch_raster_path
            target_token_complete_path = os.path.join(
                CHURN_DIR, '%s.COMPLETE' % os.path.basename(
                    global_stitch_raster_path))
            task_graph.add_task(
                func=make_empty_wgs84_raster,
                args=(
                    (GLOBAL_STITCH_WGS84_CELL_SIZE,
                     -GLOBAL_STITCH_WGS84_CELL_SIZE), GLOBAL_STITCH_NODATA,
                    gdal.GDT_Float32, global_stitch_raster_path,
                    os.path.splitext(os.path.basename(
                        target_token_complete_path))[0],
                    target_token_complete_path),
                target_path_list=[target_token_complete_path],
                ignore_path_list=[global_stitch_raster_path],
                task_name='create empty global raster for %s' % (
                    os.path.basename(global_stitch_raster_path)))

    LOGGER.debug('start threading')
    new_host_monitor_thread = threading.Thread(
        target=new_host_monitor,
        args=(args.worker_list,))
    new_host_monitor_thread.start()

    scheduling_thread = threading.Thread(
        target=schedule_worker)
    scheduling_thread.start()

    LOGGER.debug('making stitching process')
    stitcher_process = threading.Thread(
        target=global_stitcher,
        args=(RESULT_QUEUE,))
    stitcher_process.start()

    START_TIME = time.time()
    LOGGER.debug('start the APP')
    APP.config.update(SERVER_NAME='%s:%d' % (args.external_ip, args.app_port))
    # Note: never run in debug mode because it starts two processes
    APP.run(
        host='0.0.0.0',
        port=args.app_port)
