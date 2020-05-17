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
import re
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
import zipfile

from osgeo import gdal
from osgeo import osr
import flask
import ecoshard
import requests
import retrying
import shapely.strtree
import shapely.wkb
import taskgraph

gdal.SetCacheMax(2**29)

SCENARIO_ID_LIST = [
    'extensification_bmps_irrigated'
    'extensification_bmps_rainfed'
    'extensification_current_practices'
    'extensification_intensified_irrigated'
    'extensification_intensified_rainfed'
    'fixedarea_currentpractices'
    'fixedarea_bmps_irrigated'
    'fixedarea_bmps_rainfed'
    'fixedarea_intensified_irrigated'
    'fixedarea_intensified_rainfed'
    'global_potential_vegetation']

# This is old from pre "we need to rerun everything"
# SCENARIO_ID_LIST = [
#     'baseline_potter', 'baseline_napp_rate', 'ag_expansion',
#     'ag_intensification', 'restoration_potter', 'restoration_napp_rate']

WATERSHEDS_URL = (
    'https://nci-ecoshards.s3-us-west-1.amazonaws.com/'
    'watersheds_globe_HydroSHEDS_15arcseconds_'
    'blake2b_14ac9c77d2076d51b0258fd94d9378d4.zip')

COUNTRY_BORDERS_URL = (
    'https://nci-ecoshards.s3-us-west-1.amazonaws.com/'
    'world_borders_md5_c8dd971a8a853b2f3e1d3801b9747d5f.gpkg')

WORKSPACE_DIR = 'workspace_manager'
ECOSHARD_DIR = os.path.join(WORKSPACE_DIR, 'ecoshards')
CHURN_DIR = os.path.join(WORKSPACE_DIR, 'churn')
STATUS_DATABASE_PATH = os.path.join(CHURN_DIR, 'status_database.sqlite3')
logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
logging.getLogger('taskgraph').setLevel(logging.INFO)
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
# this form must be of 's3://[bucket id]/[subdir]' any change should be updated
# in the worker when it uploads the zip file
BUCKET_URI_PREFIX = 's3://nci-ecoshards/ndr_scenarios'
GLOBAL_STITCH_WGS84_CELL_SIZE = 0.002
DEFAULT_MAX_TO_SEND_TO_WORKER = 100
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
    """Object to manage which workers are found, active, dead."""
    def __init__(self):
        """Create new object, no parameters."""
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
        """Return number of running hosts and ready hosts."""
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

    task_graph = taskgraph.TaskGraph(WORKSPACE_DIR, -1)
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

    _ = task_graph.add_task(
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


def create_status_database(
        database_path, watersheds_dir_path, country_borders_path,
        complete_token_path):
    """Create the initial database that monitors execution status.

    Args:
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
            scenario_id TEXT NOT NULL,
            job_status TEXT NOT NULL,
            country_list TEXT NOT NULL,
            workspace_urls_json TEXT);
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
        '''
        INSERT INTO job_status(
        watershed_basename, fid, watershed_area_deg, scenario_id, job_status,
        country_list, workspace_urls_json)
        VALUES (?, ?, ?, ?, ?, ?)
        ''')

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
            for scenario_id in SCENARIO_ID_LIST:
                job_status_list.append(
                    (watershed_basename, fid, watershed_geom.area, scenario_id,
                     'PRESCHEDULED', country_names, None))
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
        minutes = (uptime - hours*3600) // 60
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
        minutes = (approx_time_left - hours*3600) / 60
        seconds = approx_time_left % 60
        approx_time_left_str = '%dh:%.2dm:%2.ds' % (
            hours, minutes, seconds)
        LOGGER.debug('vars: %s' % str(
            (processed_count/total_count*100, processed_count,
                total_count, total_count - processed_count,
                approx_time_left_str,
                processing_rate,
                uptime_str,
                active_count, ready_count)))

        result_string = (
            'percent complete: %.2f%% (%d)<br>'
            'total to process: %d<br>'
            'total left to process: %s<br>'
            'approx time left: %s<br>'
            'processing %.2f watersheds every second<br>'
            'uptime: %s<br>'
            'active workers: %d<br>'
            'ready workers: %d<br>' % (
                processed_count/total_count*100,
                processed_count,
                total_count,
                total_count - processed_count,
                approx_time_left_str,
                processing_rate,
                uptime_str,
                active_count,
                ready_count))
        LOGGER.debug(result_string)
        with GLOBAL_LOCK:
            result_string += 'what\'s running:<br>'
            for session_id, payload in sorted(SCHEDULED_MAP.items()):
                LOGGER.debug('%s %s', session_id, len(
                    payload['watershed_fid_tuple_list']))
                result_string += '* %s running %d watersheds (%s)<br>' % (
                    payload['host'], len(payload['watershed_fid_tuple_list']),
                    session_id)
                for ws_fid_tuple in payload['watershed_fid_tuple_list']:
                    result_string += '&emsp;&emsp;-%s<br>' % str(ws_fid_tuple)
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
        LOGGER.debug('this is the SCHEDULED_MAP: %s', SCHEDULED_MAP)
        watershed_fid_url_json_list = payload['watershed_fid_url_json_list']
        time_per_area = payload['time_per_area']
        session_id = payload['session_id']
        global TIME_PER_AREA
        with GLOBAL_LOCK:
            TIME_PER_AREA = (TIME_PER_AREA + time_per_area) / 2.0
            host = SCHEDULED_MAP[session_id]['host']
            del SCHEDULED_MAP[session_id]
        RESULT_QUEUE.put(watershed_fid_url_json_list)
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
            url_json_watershed_fid_list = [
                (url_json_map, watershed, fid)
                for watershed, fid, url_json_map in watershed_fid_url_list]
            while True:
                try:
                    connection = sqlite3.connect(STATUS_DATABASE_PATH)
                    cursor = connection.cursor()
                    LOGGER.debug(
                        'inserting this: %s', url_json_watershed_fid_list)
                    cursor.executemany(
                        'UPDATE job_status '
                        'SET workspace_urls_json=?, job_status=\'DEBUG\' '
                        'WHERE watershed_basename=? AND fid=?',
                        url_json_watershed_fid_list)
                    break
                except Exception:
                    LOGGER.exception('error on connection')
                    time.sleep(0.1)
                finally:
                    connection.commit()
                    cursor.close()
                    connection.close()
                    LOGGER.debug(
                        '%d inserted', len(url_json_watershed_fid_list))
        except Exception:
            LOGGER.exception('unhandled exception')


@retrying.retry(wait_exponential_multiplier=1000, wait_exponential_max=10000)
def schedule_worker(immediate_watershed_fid_list, max_to_send_to_worker):
    """Monitors STATUS_DATABASE_PATH and schedules work.

    Args:
        immediate_watershed_fid_list (tuple): If not `None` this funciton will
            execute NDR only on the `watershed_basename_fid` strings in this
            list. If `None` this function will loop through the uncompleted
            watershed/fid tuples in the database until complete.
        max_to_send_to_worker (int): maximum number of watersheds to send to
            each worker. Otherwise tries to schedule up to 10 minutes of
            computation per worker based on watershed size.

    Returns:
        None.

    """
    try:
        LOGGER.debug('launching schedule_worker')
        if not immediate_watershed_fid_list:
            LOGGER.debug('no predefined watershed fid, use database')
            ro_uri = 'file://%s?mode=ro' % os.path.abspath(
                STATUS_DATABASE_PATH)
            LOGGER.debug('opening %s', ro_uri)
            connection = sqlite3.connect(ro_uri, uri=True)
            cursor = connection.cursor()
            LOGGER.debug('querying prescheduled')
            cursor.execute(
                '''
                SELECT watershed_basename, fid, watershed_area_deg, scenario_id
                FROM job_status
                WHERE job_status='PRESCHEDULED'
                ''')
            payload_list = list(cursor.fetchall())
            connection.commit()
            connection.close()
        else:
            # hard-coding a 1.0 because we don't really know how big the
            # watershed is if it's passed as an immediate.
            payload_list = []
            for immediate in immediate_watershed_fid_list:
                (watershed_basename, fid, scenario_id) = re.match(
                    '(.*)_(\d+)_(.*)', immediate).groups()
                payload_list.append(
                    (watershed_basename, fid, 1.0, scenario_id))
            LOGGER.debug(
                'running in immediate mode for these watersheds: %s',
                payload_list)
            sys.exit()

        watershed_fid_tuple_list = []
        total_expected_runtime = 0.0

        for payload in payload_list:
            watershed_basename, fid, watershed_area_deg, scenario_id = payload
            fid = int(fid)  # I encountered a string, this prevents that.
            total_expected_runtime += TIME_PER_AREA * watershed_area_deg
            watershed_fid_tuple_list.append(
                (watershed_basename, fid, watershed_area_deg, scenario_id))
            if total_expected_runtime > TIME_PER_WORKER or (
                    len(watershed_fid_tuple_list) > max_to_send_to_worker):
                LOGGER.debug(
                    'sending job with %d elements %.2f min time',
                    len(watershed_fid_tuple_list), total_expected_runtime/60)
                send_job(watershed_fid_tuple_list)
                watershed_fid_tuple_list = []
                total_expected_runtime = 0.0
    except Exception:
        LOGGER.exception('exception in scheduler')
        raise


def reschedule_worker():
    """Reschedule any jobs that come through the schedule queue."""
    while True:
        try:
            watershed_fid_tuple_list = RESCHEDULE_QUEUE.get()
            LOGGER.debug('rescheduling %s', watershed_fid_tuple_list)
            send_job(watershed_fid_tuple_list)
        except Exception:
            LOGGER.exception('something bad happened in reschedule_worker')


@retrying.retry(wait_exponential_multiplier=1000, wait_exponential_max=5000)
def send_job(watershed_fid_tuple_list):
    """Send watershed/fid to the global execution pool.

    Args:
        watershed_fid_tuple_list (list): list of
            (watershed_fid_tuple_list, callback_url, bucket_uri_prefix,
            session_id) tuples

    Returns:
        None.

    """
    try:
        LOGGER.debug('scheduling %s', watershed_fid_tuple_list)
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
    except Exception as e:
        LOGGER.debug('in the exception: %s', e)
        LOGGER.exception(
            'something bad happened, on %s for %s',
            worker_ip_port, watershed_fid_tuple_list)
        LOGGER.debug('removing %s from worker set', worker_ip_port)
        GLOBAL_WORKER_STATE_SET.remove_host(worker_ip_port)
        raise
    finally:
        LOGGER.debug('in the finally')


def new_host_monitor():
    """Watch for AWS worker instances on the network.

    Returns:
        never

    """
    while True:
        try:
            raw_output = subprocess.check_output(
                'aws ec2 describe-instances', shell=True)
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
                        try:
                            LOGGER.debug('about to test status')
                            response = requests.get(value['status_url'])
                            LOGGER.debug('got status')
                            if response.ok:
                                value['last_time_accessed'] = time.time()
                            else:
                                raise RuntimeError('response not okay')
                        except (ConnectionError, Exception):
                            LOGGER.debug(
                                'failed job: %s on %s',
                                value['watershed_fid_tuple_list'],
                                str((session_id, host)))
                            failed_job_list.append(
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

    Args:
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


@retrying.retry(
    wait_exponential_multiplier=1000, wait_exponential_max=10000)
def download_url(source_url, target_path):
    """Wrapper for ecoshard.download_url."""
    try:
        ecoshard.download_url(source_url, target_path)
    except Exception:
        LOGGER.exception("couldn't download %s->, trying again" % (
            source_url, target_path))
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='NCI NDR Analysis.')
    parser.add_argument(
        '--watershed_fid_scenario_immedates', type=str, nargs='+',
        default=None, help=(
            'list of `(watershed)_(fid)_(scenario_id)` identifiers to run '
            'instead of database'))
    parser.add_argument(
        '--app_port', type=int, default=8080,
        help='port to listen on for callback complete')
    parser.add_argument(
        '--external_ip', type=str, default='localhost',
        help='define external IP that can be used to connect to this app')
    parser.add_argument(
        '--max_to_send_to_worker', type=int,
        default=DEFAULT_MAX_TO_SEND_TO_WORKER,
        help='maximum number of jobs to send to each worker, default: %s.' % (
            DEFAULT_MAX_TO_SEND_TO_WORKER))
    args = parser.parse_args()

    LOGGER.debug('initalizing')
    initialize()

    LOGGER.debug('start threading')
    worker_status_monitor_thread = threading.Thread(
        target=worker_status_monitor)
    worker_status_monitor_thread.start()

    schedule_worker_thread = threading.Thread(
        target=schedule_worker,
        args=(
            args.watershed_fid_scenario_immedates, args.max_to_send_to_worker))
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

    LOGGER.debug('start the APP')
    APP.config.update(SERVER_NAME='%s:%d' % (args.external_ip, args.app_port))
    # Note: never run in debug mode because it starts two processes
    APP.run(
        host='0.0.0.0',
        port=args.app_port)
