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
import pathlib
import queue
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
import uuid
import zipfile

from osgeo import gdal
from osgeo import osr
import flask
import ecoshard
import numpy
import pygeoprocessing
import requests
import retrying
import shapely.strtree
import shapely.wkb
import taskgraph

gdal.SetCacheMax(2**29)

WATERSHEDS_URL = (
    'https://nci-ecoshards.s3-us-west-1.amazonaws.com/'
    'watersheds_globe_HydroSHEDS_15arcseconds_'
    'blake2b_14ac9c77d2076d51b0258fd94d9378d4.zip')

COUNTRY_BORDERS_URL = (
    'https://nci-ecoshards.s3-us-west-1.amazonaws.com/'
    'world_borders_md5_c8dd971a8a853b2f3e1d3801b9747d5f.gpkg')

WORKSPACE_DIR = 'workspace_manager'
STITCH_DIR = os.path.join(WORKSPACE_DIR, 'stitch_workspace')
ECOSHARD_DIR = os.path.join(WORKSPACE_DIR, 'ecoshards')
CHURN_DIR = os.path.join(WORKSPACE_DIR, 'churn')
STATUS_DATABASE_PATH = os.path.join(CHURN_DIR, 'status_database.sqlite3')
WATERSHED_STATS_DATABASE_PATH = os.path.join(
    CHURN_DIR, 'watershed_stats.sqlite3')
logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.FileHandler('log.txt'))
HOST_FILE_PATH = 'host_file.txt'
DETECTOR_POLL_TIME = 30.0
SCHEDULED_MAP = {}
GLOBAL_LOCK = threading.Lock()
GLOBAL_READY_HOST_SET = set()  # hosts that are ready to do work
GLOBAL_RUNNING_HOST_SET = set()  # hosts that are active
GLOBAL_FAILED_HOST_SET = set()  # hosts that failed to connect or other error
RESULT_QUEUE = queue.Queue()
RESCHEDULE_QUEUE = queue.Queue()
TIME_PER_AREA = 1e8
TIME_PER_WORKER = 10 * 60
WGS84_SR = osr.SpatialReference()
WGS84_SR.ImportFromEPSG(4326)
WGS84_WKT = WGS84_SR.ExportToWkt()

WORKER_TAG_ID = 'compute-server'
BUCKET_URI_PREFIX = 's3://nci-ecoshards/watershed_workspaces'
GLOBAL_STITCH_WGS84_CELL_SIZE = 0.002
GLOBAL_STITCH_MAP = {
    'n_export': (
        'workspace_worker/[BASENAME]_[FID]/n_export.tif',
        gdal.GDT_Float32, -1),
    'modified_load': (
        'workspace_worker/[BASENAME]_[FID]/intermediate_outputs/'
        'modified_load_n.tif',
        gdal.GDT_Float32, -1),
}

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


def initialize():
    """Entry point."""
    for dir_path in [WORKSPACE_DIR, ECOSHARD_DIR, CHURN_DIR]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    task_graph = taskgraph.TaskGraph(WORKSPACE_DIR, 0)
    # download countries
    country_borders_path = os.path.join(
        ECOSHARD_DIR, os.path.basename(COUNTRY_BORDERS_URL))
    country_fetch_task = task_graph.add_task(
        func=ecoshard.download_url,
        args=(COUNTRY_BORDERS_URL, country_borders_path),
        target_path_list=[country_borders_path],
        task_name='download country borders')

    # download watersheds
    watersheds_zip_path = os.path.join(
        ECOSHARD_DIR, os.path.basename(WATERSHEDS_URL))
    LOGGER.debug(
        'scheduing download of watersheds: %s', WATERSHEDS_URL)
    watersheds_zip_fetch_task = task_graph.add_task(
        func=ecoshard.download_url,
        args=(WATERSHEDS_URL, watersheds_zip_path),
        target_path_list=[watersheds_zip_path],
        task_name='download watersheds zip')
    watersheds_unzip_dir = os.path.join(
        CHURN_DIR, os.path.basename(watersheds_zip_path.replace('.zip', '')))
    unzip_token_path = os.path.join(
        CHURN_DIR, '%s.UNZIPTOKEN' % os.path.basename(watersheds_unzip_dir))
    LOGGER.debug(
        'scheduing unzip of: %s', watersheds_zip_path)
    unzip_watersheds_task = task_graph.add_task(
        func=unzip_file,
        args=(watersheds_zip_path, watersheds_unzip_dir, unzip_token_path),
        target_path_list=[unzip_token_path],
        dependent_task_list=[watersheds_zip_fetch_task],
        task_name='unzip %s' % watersheds_zip_path)

    database_complete_token_path = os.path.join(
        CHURN_DIR, 'create_status_database.COMPLETE')

    create_status_database_task = task_graph.add_task(
        func=create_status_database,
        args=(
            STATUS_DATABASE_PATH, watersheds_unzip_dir, country_borders_path,
            database_complete_token_path),
        target_path_list=[database_complete_token_path],
        ignore_path_list=[STATUS_DATABASE_PATH],
        dependent_task_list=[country_fetch_task, unzip_watersheds_task],
        task_name='create status database')

    task_graph.join()
    task_graph.close()


def unzip_file(zip_path, target_directory, token_file):
    """Unzip contents of `zip_path` into `target_directory`."""
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(target_directory)
    with open(token_file, 'w') as token_file:
        token_file.write(str(datetime.datetime.now()))


@retrying.retry()
def create_index(database_path):
    """Create an index on the database if it doesn't exist."""
    try:
        create_index_sql = (
            """
            CREATE UNIQUE INDEX IF NOT EXISTS watershed_fid_index
            ON job_status (watershed_basename, fid);
            """)
        connection = sqlite3.connect(database_path)
        cursor = connection.cursor()
        cursor.executescript(create_index_sql)
    except Exception:
        LOGGER.exception('exceptionon create_index')
    finally:
        connection.commit()
        connection.close()


def create_status_database(
        database_path, watersheds_dir_path, country_borders_path,
        complete_token_path):
    """Create the initial database that monitors execution status.

    Parameters:
        database_path (str): path to SQLite database that's created by this
            call.
        watersheds_dir_path (str): path to a directory containing .shp files
            that correspond to global watersheds.
        country_borders_path (str): path to a vector containing polygon country
            shapes, used to identify which watersheds are in which countries.
        complete_token_path (str): path to a file that's written if the
            entire initialization process has completed successfully.

    Returns:
        None.

    """
    LOGGER.debug('launching create_status_database')
    create_database_sql = (
        """
        CREATE TABLE job_status (
            watershed_basename TEXT NOT NULL,
            fid INT NOT NULL,
            watershed_area_deg REAL NOT NULL,
            job_status TEXT NOT NULL,
            country_list TEXT NOT NULL,
            workspace_url TEXT,
            stiched_n_export INT NOT NULL,
            stiched_modified_load INT NOT NULL);
        """)
    if os.path.exists(database_path):
        os.remove(database_path)
    connection = sqlite3.connect(database_path)
    cursor = connection.cursor()
    cursor.executescript(create_database_sql)

    world_borders_vector = gdal.OpenEx(
        country_borders_path, gdal.OF_VECTOR)
    world_borders_layer = world_borders_vector.GetLayer()
    world_border_polygon_list = []
    for feature in world_borders_layer:
        geom = shapely.wkb.loads(
            feature.GetGeometryRef().ExportToWkb())
        geom.prep_geom = shapely.prepared.prep(geom)
        geom.country_name = feature.GetField('NAME')
        world_border_polygon_list.append(geom)

    str_tree = shapely.strtree.STRtree(world_border_polygon_list)
    insert_query = (
        'INSERT INTO job_status('
        'watershed_basename, fid, watershed_area_deg, job_status, '
        'country_list, workspace_url, stiched_n_export, stiched_modified_load) '
        'VALUES (?, ?, ?, ?, ?, ?, ?, ?)')

    for watershed_shape_path in [str(p) for p in pathlib.Path(
            watersheds_dir_path).rglob('*.shp')]:
        LOGGER.debug('watershed shape path: %s', watershed_shape_path)
        watershed_vector = gdal.OpenEx(watershed_shape_path, gdal.OF_VECTOR)
        watershed_layer = watershed_vector.GetLayer()
        LOGGER.debug('processing watershed %s', watershed_shape_path)
        watershed_basename = os.path.splitext(
            os.path.basename(watershed_shape_path))[0]
        job_status_list = []
        last_time = time.time()
        for index, watershed_feature in enumerate(watershed_layer):
            if time.time() - last_time > 5.0:
                last_time = time.time()
                LOGGER.debug(
                    '%.2f%% (%d) complete',
                    100. * index/float(watershed_layer.GetFeatureCount()),
                    index+1)
            fid = watershed_feature.GetFID()
            watershed_geom = shapely.wkb.loads(
                watershed_feature.GetGeometryRef().ExportToWkb())
            country_name_list = []
            for intersect_geom in str_tree.query(watershed_geom):
                if intersect_geom.prep_geom.intersects(watershed_geom):
                    country_name_list.append(intersect_geom.country_name)
            if not country_name_list:
                # watershed is not in any country, so lets not run it
                continue
            country_names = ','.join(country_name_list)
            job_status_list.append(
                (watershed_basename, fid, watershed_geom.area, 'PRESCHEDULED',
                 country_names, None, 0, 0))
            if (index+1) % 10000 == 0:
                LOGGER.debug(
                    'every 10000 inserting %s watersheds into DB',
                    watershed_basename)
                cursor.executemany(insert_query, job_status_list)
                job_status_list = []
        if job_status_list:
            LOGGER.debug('inserting %s watersheds into DB', watershed_basename)
            cursor.executemany(insert_query, job_status_list)
    LOGGER.debug('all done with watersheds')
    connection.commit()
    connection.close()

    with open(complete_token_path, 'w') as token_file:
        token_file.write(str(datetime.datetime.now()))


@APP.route('/api/v1/processing_status', methods=['GET'])
def processing_status():
    """Return the state of processing."""
    try:
        ro_uri = 'file://%s?mode=ro' % os.path.abspath(STATUS_DATABASE_PATH)
        connection = sqlite3.connect(ro_uri, uri=True)
        cursor = connection.cursor()
        LOGGER.debug('querying prescheduled')
        cursor.execute('SELECT count(1) from job_status')
        total_count = int(cursor.fetchone()[0])
        cursor.execute(
            'SELECT count(1) from job_status '
            'where job_status=\'PRESCHEDULED\'')
        prescheduled_count = int(cursor.fetchone()[0])
        connection.commit()
        connection.close()
        processed_count = total_count - prescheduled_count
        active_count, ready_count = (
            GLOBAL_WORKER_STATE_SET.get_counts())

        uptime = time.time() - START_TIME
        hours = uptime // 3600
        minutes = (uptime - hours*60) / 60
        seconds = uptime % 60
        uptime_str = '%dh:%.2dm:%2.ds' % (
            hours, minutes, seconds)
        count_this_session = processed_count - START_COUNT
        processing_rate = count_this_session / uptime
        if processing_rate > 0:
            approx_time_left = prescheduled_count / processing_rate
        else:
            approx_time_left = 999999999
        hours = approx_time_left // 3600
        minutes = (approx_time_left - hours*60) / 60
        seconds = approx_time_left % 60
        approx_time_left_str = '%dh:%.2dm:%2.ds' % (
            hours, minutes, seconds)
        result_string = (
            'percent complete: %.2f%% (%s)<br>'
            'total to process: %s<br>'
            'approx time left: %s<br>'
            'processing %.2f watersheds every second<br>'
            'uptime: %s<br>'
            'active workers: %d<br>'
            'ready workers: %d<br>' % (
                processed_count/total_count*100, processed_count,
                total_count,
                approx_time_left_str,
                processing_rate,
                uptime_str,
                active_count, ready_count))
        return result_string
    except Exception as e:
        return 'error: %s' % str(e)


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
        watershed_fid_url_list = payload['watershed_fid_url_list']
        time_per_area = payload['time_per_area']
        session_id = payload['session_id']
        global TIME_PER_AREA
        with GLOBAL_LOCK:
            TIME_PER_AREA = (TIME_PER_AREA + time_per_area) / 2.0
            host = SCHEDULED_MAP[session_id]['host']
            del SCHEDULED_MAP[session_id]
        RESULT_QUEUE.put(watershed_fid_url_list)
        GLOBAL_WORKER_STATE_SET.set_ready_host(host)
        return 'complete', 202
    except Exception:
        LOGGER.exception(
            'error on processing completed for host %s. session_ids: %s',
            flask.request.remote_addr, str(SCHEDULED_MAP))


def job_status_updater():
    """Monitor result queue and add to database as needed."""
    while True:
        try:
            LOGGER.debug('waiting for result')
            watershed_fid_url_list = RESULT_QUEUE.get()
            url_watershed_fid_list = [
                (url, watershed, fid)
                for watershed, fid, url in watershed_fid_url_list]
            while True:
                try:
                    connection = sqlite3.connect(STATUS_DATABASE_PATH)
                    cursor = connection.cursor()
                    cursor.executemany(
                        'UPDATE job_status '
                        'SET workspace_url=?, job_status=\'DEBUG\' '
                        'WHERE watershed_basename=? AND fid=?',
                        url_watershed_fid_list)
                    break
                except Exception:
                    LOGGER.exception('error on connection')
                    time.sleep(0.1)
                finally:
                    connection.commit()
                    cursor.close()
                    connection.close()
            LOGGER.debug('inserting this: %s', url_watershed_fid_list)
            LOGGER.debug('%d inserted', len(url_watershed_fid_list))
        except Exception:
            LOGGER.exception('unhandled exception')


@retrying.retry()
def schedule_worker():
    """Monitors STATUS_DATABASE_PATH and schedules work.

    Returns:
        None.

    """
    try:
        LOGGER.debug('launching schedule_worker')
        ro_uri = 'file://%s?mode=ro' % os.path.abspath(STATUS_DATABASE_PATH)
        LOGGER.debug('opening %s', ro_uri)
        connection = sqlite3.connect(ro_uri, uri=True)
        cursor = connection.cursor()
        LOGGER.debug('querying prescheduled')
        cursor.execute(
            'SELECT watershed_basename, fid, watershed_area_deg '
            'FROM job_status '
            'WHERE job_status=\'PRESCHEDULED\'')
        watershed_fid_tuple_list = []
        total_expected_runtime = 0.0
        payload_list = list(cursor.fetchall())
        connection.commit()
        connection.close()
        for payload in payload_list:
            watershed_basename, fid, watershed_area_deg = payload
            total_expected_runtime += TIME_PER_AREA * watershed_area_deg
            watershed_fid_tuple_list.append(
                (watershed_basename, fid, watershed_area_deg))
            if total_expected_runtime > TIME_PER_WORKER:
                LOGGER.debug(
                    'sending job with %d elements %.2f min time',
                    len(watershed_fid_tuple_list), total_expected_runtime/60)
                send_job(watershed_fid_tuple_list)
                watershed_fid_tuple_list = []
                total_expected_runtime = 0.0
    except Exception:
        LOGGER.exception('exception in scheduler')


def reschedule_worker():
    """Reschedule any jobs that come through the schedule queue."""
    while True:
        try:
            watershed_fid_tuple_list = RESCHEDULE_QUEUE.get()
            LOGGER.debug('rescheduling %s', watershed_fid_tuple_list)
            send_job(watershed_fid_tuple_list)
        except Exception:
            LOGGER.exception('something bad happened in reschedule_worker')


@retrying.retry()
def send_job(watershed_fid_tuple_list):
    """Send watershed/fid to the global execution pool.

    Parameters:
        watershed_fid_tuple_list (list): list of
            (watershed_basename, fid, watershed_area)

    Returns:
        None.

    """
    try:
        LOGGER.debug('scheduling %s', watershed_fid_tuple_list)
        with APP.app_context():
            callback_url = flask.url_for(
                'processing_complete', _external=True)
        worker_ip_port = GLOBAL_WORKER_STATE_SET.get_ready_host()
        session_id = str(uuid.uuid4())
        data_payload = {
            'watershed_fid_tuple_list': watershed_fid_tuple_list,
            'callback_url': callback_url,
            'bucket_uri_prefix': BUCKET_URI_PREFIX,
            'session_id': session_id,
        }

        LOGGER.debug('payload: %s', data_payload)
        LOGGER.debug('got this worker: %s', worker_ip_port)
        worker_rest_url = (
            'http://%s/api/v1/run_ndr' % worker_ip_port)
        LOGGER.debug(
            'sending job %s to %s', data_payload, worker_rest_url)
        response = requests.post(
            worker_rest_url, json=data_payload)
        if response.ok:
            with GLOBAL_LOCK:
                LOGGER.debug('%s scheduled', watershed_fid_tuple_list)
                SCHEDULED_MAP[session_id] = {
                    'status_url': response.json()['status_url'],
                    'watershed_fid_tuple_list': watershed_fid_tuple_list,
                    'last_time_accessed': time.time(),
                    'host': worker_ip_port
                }
        else:
            raise RuntimeError(str(response))
    except Exception:
        LOGGER.exception(
            'something bad happened, on %s for %s',
            worker_ip_port, watershed_fid_tuple_list)
        GLOBAL_WORKER_STATE_SET.remove_host(worker_ip_port)
        raise


def new_host_monitor():
    """Watch for AWS worker instances on the network.

    Returns:
        never

    """
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
            with GLOBAL_LOCK:
                for host in dead_hosts:
                    watershed_fid_tuple_list = (
                        SCHEDULED_MAP[host]['watershed_fid_tuple_list'])
                    del SCHEDULED_MAP[host]
                    RESCHEDULE_QUEUE.put(watershed_fid_tuple_list)
            time.sleep(DETECTOR_POLL_TIME)
        except Exception:
            LOGGER.exception('exception in `new_host_monitor`')


def worker_status_monitor():
    """Monitor the status of watershed workers and reschedule if down."""
    while True:
        try:
            time.sleep(DETECTOR_POLL_TIME)
            current_time = time.time()
            failed_job_list = []
            with GLOBAL_LOCK:
                hosts_to_remove = set()
                for session_id, value in SCHEDULED_MAP.items():
                    host = value['host']
                    if current_time - value['last_time_accessed']:
                        response = requests.get(value['status_url'])
                        if response.ok:
                            value['last_time_accessed'] = time.time()
                        else:
                            failed_job_list.put(
                                value['watershed_fid_tuple_list'])
                            hosts_to_remove.add((session_id, host))
                for session_id, host in hosts_to_remove:
                    GLOBAL_WORKER_STATE_SET.remove_host(host)
                    del SCHEDULED_MAP[session_id]
            for watershed_fid_tuple_list in failed_job_list:
                LOGGER.debug('rescheduling %s', str(watershed_fid_tuple_list))
                RESCHEDULE_QUEUE.put(watershed_fid_tuple_list)
        except Exception:
            LOGGER.exception('exception in worker status monitor')


def make_empty_wgs84_raster(
        cell_size, nodata_value, target_datatype, target_raster_path,
        target_token_complete_path):
    """Make a big empty raster in WGS84 projection.

    Parameters:
        cell_size (float): this is the desired cell size in WSG84 degree
            units.
        nodata_value (float): desired nodata avlue of target raster
        target_datatype (gdal enumerated type): desired target datatype.
        target_raster_path (str): this is the target raster that will cover
            [-180, 180), [90, -90) with cell size units with y direction being
            negative.
        target_token_complete_path (str): this file is created if the
            mosaic to target is successful. Useful for taskgraph task
            scheduling.

    Returns:
        None.

    """
    gtiff_driver = gdal.GetDriverByName('GTiff')
    try:
        os.makedirs(os.path.dirname(target_raster_path))
    except OSError:
        pass

    n_cols = int(360.0 / cell_size)
    n_rows = int(180.0 / cell_size)

    geotransform = (-180.0, cell_size, 0.0, 90.0, 0, -cell_size)

    target_raster = gtiff_driver.Create(
        target_raster_path, n_cols, n_rows, 1, target_datatype,
        options=(
            'TILED=YES', 'BIGTIFF=YES', 'BLOCKXSIZE=256', 'BLOCKYSIZE=256',
            'COMPRESS=LZW', 'SPARSE_OK=TRUE'))
    target_raster.SetProjection(WGS84_WKT)
    target_raster.SetGeoTransform(geotransform)
    target_band = target_raster.GetRasterBand(1)
    target_band.SetNoDataValue(nodata_value)
    target_band = None
    target_raster = None

    target_raster = gdal.OpenEx(target_raster_path, gdal.OF_RASTER)
    if target_raster:
        with open(target_token_complete_path, 'w') as target_token_file:
            target_token_file.write('complete!')


@retrying.retry()
def execute_sql_on_database(sql_statement, database_path, query=False):
    """Execute sql_statement on given database path.

    Returns:
        if query, return a "fetchall" result of the query, else None.

    """
    try:
        connection = sqlite3.connect(database_path)
        cursor = connection.cursor()
        cursor.executescript(sql_statement)
        if query:
            result = list(cursor.fetchall())
        else:
            result = None
        return result
    except Exception:
        LOGGER.exception('exception on execute sql statement')
        raise
    finally:
        connection.commit()
        connection.close()


@retrying.retry()
def stitch_into(master_raster_path, base_raster_path, nodata_value):
    """Stitch `base`into `master` by only overwriting non-nodata values."""
    try:
        wgs84_base_raster_path = (
            '%s_wgs84%s' % os.path.splitext(base_raster_path))
        pygeoprocessing.warp_raster(
            base_raster_path,
            (GLOBAL_STITCH_WGS84_CELL_SIZE, -GLOBAL_STITCH_WGS84_CELL_SIZE),
            wgs84_base_raster_path, 'near', target_sr_wkt=WGS84_WKT)

        master_raster = gdal.OpenEx(
            master_raster_path, gdal.OF_RASTER | gdal.GA_Update)
        master_band = master_raster.GetRasterBand(1)
        master_raster_info = pygeoprocessing.get_raster_info(
            master_raster_path)
        master_nodata = master_raster_info['nodata'][0]
        base_raster_info = pygeoprocessing.get_raster_info(
            wgs84_base_raster_path)
        master_gt = master_raster_info['geotransform']
        base_gt = base_raster_info['geotransform']
        base_nodata = base_raster_info['nodata']

        target_x_off = int((base_gt[0] - master_gt[0]) / master_gt[1])
        target_y_off = int((base_gt[3] - master_gt[3]) / master_gt[5])
        LOGGER.debug(
            'stitch into: %f %f %s', target_x_off, target_y_off,
            base_raster_info)
        for offset_dict, base_block in pygeoprocessing.iterblocks(
                (wgs84_base_raster_path, 1)):
            master_block = master_band.ReadAsArray(
                xoff=offset_dict['xoff']+target_x_off,
                yoff=offset_dict['yoff']+target_y_off,
                win_xsize=offset_dict['win_xsize'],
                win_ysize=offset_dict['win_ysize'])
            valid_mask = (
                numpy.isclose(master_block, master_nodata) &
                ~numpy.isclose(base_block, base_nodata))
            master_block[valid_mask] = base_block[valid_mask]
            master_band.WriteArray(
                master_block,
                xoff=offset_dict['xoff']+target_x_off,
                yoff=offset_dict['yoff']+target_y_off)
        master_band.FlushCache()
        master_band = None
        master_raster = None

    except Exception:
        LOGGER.exception('error on stitch into')
    finally:
        pass # os.remove(wgs84_base_raster_path)


def stitch_worker():
    """Mange the stitching of a raster."""
    try:
        task_graph = taskgraph.TaskGraph(STITCH_DIR, -1)
        raster_id_path_map = {}
        for raster_id, (path_prefix, gdal_type, nodata_value) in (
                GLOBAL_STITCH_MAP.items()):
            stitch_raster_path = os.path.join(STITCH_DIR, '%s.tif' % raster_id)
            raster_id_path_map[raster_id] = stitch_raster_path
            stitch_raster_token_path = '%s.CREATED' % (
                os.path.splitext(stitch_raster_path)[0])
            task_graph.add_task(
                func=make_empty_wgs84_raster,
                args=(
                    GLOBAL_STITCH_WGS84_CELL_SIZE, nodata_value, gdal_type,
                    stitch_raster_path, stitch_raster_token_path),
                target_path_list=[stitch_raster_token_path],
                task_name='make base %s' % raster_id)
        task_graph.join()
    except Exception:
        LOGGER.exception('ERROR on stiched worker %s', traceback.format_exc())

    # This section can be uncommented for debugging in the case of wanting to
    # reset all the stitched rasters
    # connection = sqlite3.connect(STATUS_DATABASE_PATH)
    # cursor = connection.cursor()
    # update_stiched_record = (
    #     'UPDATE job_status '
    #     'SET stiched_n_export=0, stiched_modified_load=0 '
    #     'WHERE stiched_n_export=1 OR stiched_modified_load=1')
    # cursor.execute(update_stiched_record)
    # connection.commit()
    # connection.close()
    # cursor = None

    while True:
        # update the stitch with the latest.
        time.sleep(10)
        try:
            LOGGER.debug('searching for a new stitch')
            select_not_processed = (
                'SELECT watershed_basename, fid, workspace_url '
                'FROM job_status '
                'WHERE (stiched_n_export = 0 OR stiched_modified_load = 0) '
                'AND workspace_url IS NOT NULL')
            connection = sqlite3.connect(STATUS_DATABASE_PATH)
            cursor = connection.cursor()
            cursor.execute(select_not_processed)
            update_ws_fid_list = list(cursor.fetchall())
            connection.commit()
            connection.close()
            cursor = None
            LOGGER.debug('query string: %s', select_not_processed)
            LOGGER.debug('length of update list: %s', len(update_ws_fid_list))
            for watershed_basename, fid, workspace_url in (
                    update_ws_fid_list):
                workspace_zip_path = os.path.join(
                    STITCH_DIR, os.path.basename(workspace_url))
                # TODO: this is just a hack because there are // in some results
                workspace_url = workspace_url.replace('//', '/').replace(
                    'https:/', 'https://')
                LOGGER.debug('download url: %s', workspace_url)
                ecoshard.download_url(workspace_url, workspace_zip_path)
                for raster_id, (path_prefix, gdal_type, nodata_value) in (
                        GLOBAL_STITCH_MAP.items()):
                    LOGGER.debug('processing raster %s', raster_id)
                    zipped_path = path_prefix.replace(
                        '[BASENAME]', watershed_basename).replace(
                        '[FID]', str(fid))
                    local_zip_dir = os.path.join(STITCH_DIR, '%s_%s' % (
                        watershed_basename, fid))
                    with zipfile.ZipFile(workspace_zip_path, 'r') as zip_ref:
                        zip_ref.extract(zipped_path, local_zip_dir)
                    LOGGER.debug(
                        'stitching %s %s in %s', watershed_basename, fid,
                        raster_id)
                    local_path = os.path.join(local_zip_dir, zipped_path)
                    stitch_into(
                        raster_id_path_map[raster_id], local_path,
                        nodata_value)
                os.remove(workspace_zip_path)
                shutil.rmtree(local_zip_dir)

                while True:
                    try:
                        connection = sqlite3.connect(STATUS_DATABASE_PATH)
                        cursor = connection.cursor()
                        update_stiched_record = (
                            'UPDATE job_status '
                            'SET stiched_n_export=1, stiched_modified_load=1 '
                            'WHERE watershed_basename=? AND fid=?')
                        LOGGER.debug(
                            'attempting update %s', update_stiched_record)
                        cursor.execute(
                             update_stiched_record, (watershed_basename, fid))
                        LOGGER.debug(
                            'updated record! %s %s', watershed_basename, fid)
                        break
                    except Exception:
                        LOGGER.exception(
                            'exception when updating stiched status')
                    finally:
                        connection.commit()
                        connection.close()
        except Exception:
            LOGGER.exception('exception in stich worker')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='NCI NDR Analysis.')
    parser.add_argument(
        '--app_port', type=int, default=8080,
        help='port to listen on for callback complete')
    parser.add_argument(
        '--external_ip', type=str, default='localhost',
        help='define external IP that can be used to connect to this app')
    args = parser.parse_args()

    initialize()
    create_index(STATUS_DATABASE_PATH)

    worker_status_monitor_thread = threading.Thread(
        target=worker_status_monitor)
    worker_status_monitor_thread.start()

    schedule_worker_thread = threading.Thread(
        target=schedule_worker)
    schedule_worker_thread.start()

    new_host_monitor_thread = threading.Thread(
        target=new_host_monitor)
    new_host_monitor_thread.start()

    job_status_updater_thread = threading.Thread(
        target=job_status_updater)
    job_status_updater_thread.start()

    reschedule_worker_thread = threading.Thread(
        target=reschedule_worker)
    reschedule_worker_thread.start()

    stitch_worker_process = threading.Thread(
        target=stitch_worker)
    stitch_worker_process.start()

    START_TIME = time.time()
    ro_uri = 'file://%s?mode=ro' % os.path.abspath(STATUS_DATABASE_PATH)
    connection = sqlite3.connect(ro_uri, uri=True)
    cursor = connection.cursor()
    LOGGER.debug('querying prescheduled')
    cursor.execute('SELECT count(1) from job_status')
    total_count = int(cursor.fetchone()[0])
    cursor.execute(
        'SELECT count(1) from job_status '
        'where job_status=\'PRESCHEDULED\'')
    START_COUNT = total_count - int(cursor.fetchone()[0])
    connection.commit()
    connection.close()

    APP.config.update(SERVER_NAME='%s:%d' % (args.external_ip, args.app_port))
    APP.run(
        host='0.0.0.0',
        port=args.app_port)
