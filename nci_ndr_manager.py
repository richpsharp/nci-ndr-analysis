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
import sqlite3
import subprocess
import sys
import threading
import time
import zipfile

import flask
from osgeo import gdal
gdal.SetCacheMax(2**30)
import ecoshard
import requests
import retrying
import shapely.strtree
import shapely.wkb
import taskgraph

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
LOGGER.addHandler(logging.FileHandler('log.txt'))
HOST_FILE_PATH = 'host_file.txt'
DETECTOR_POLL_TIME = 15.0
SCHEDULED_MAP = {}
GLOBAL_LOCK = threading.Lock()
GLOBAL_READY_HOST_SET = set()  # hosts that are ready to do work
GLOBAL_RUNNING_HOST_SET = set()  # hosts that are active
GLOBAL_FAILED_HOST_SET = set()  # hosts that failed to connect or other error
WORKER_TAG_ID = 'compute-server'
RESULT_QUEUE = queue.Queue()
TIME_PER_AREA = 1e8
TIME_PER_WORKER = 10 * 60

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
            workspace_url TEXT);
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
        'country_list, workspace_url) VALUES (?, ?, ?, ?, ?, ?)')

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
                 country_names, None))
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

    cursor.close()
    connection.commit()

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
        processed_count = total_count - prescheduled_count
        active_count, ready_count = (
            GLOBAL_WORKER_STATE_SET.get_counts())
        result_string = (
            'total to process: %s<br>'
            'percent complete: %.2f%% (%s)<br>'
            'active workers: %d<br>'
            'ready workers: %d<br>' % (
                total_count, processed_count/total_count*100,
                processed_count, active_count, ready_count))
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
    payload = flask.request.get_json()
    LOGGER.debug('this was the payload: %s', payload)
    watershed_fid_url_list = payload['watershed_fid_url_list']
    time_per_area = payload['time_per_area']
    global TIME_PER_AREA
    with GLOBAL_LOCK:
        TIME_PER_AREA = (TIME_PER_AREA + time_per_area) / 2.0
    for watershed_basename, fid, workspace_url in watershed_fid_url_list:
        RESULT_QUEUE.put((workspace_url, watershed_basename, fid))
        with GLOBAL_LOCK:
            worker_ip_port = SCHEDULED_MAP[
                (watershed_basename, fid)]['worker_ip_port']
            # re-register the worker/port
            del SCHEDULED_MAP[(watershed_basename, fid)]
    GLOBAL_WORKER_STATE_SET.set_ready_host(worker_ip_port)
    return '%s:%d complete' % (watershed_basename, fid), 202


def job_monitor():
    """Monitor result queue and add to database as needed."""
    while True:
        try:
            LOGGER.debug('waiting for result')
            payload = RESULT_QUEUE.get()
            workspace_url, watershed_basename, fid = payload
            payload_list = [payload]
            while True:
                try:
                    payload = RESULT_QUEUE.get(False)
                    workspace_url, watershed_basename, fid = payload
                    payload_list.append(payload)
                    if len(payload_list) > 100:
                        break
                except queue.Empty:
                    break
            LOGGER.debug('%d inserted', len(payload_list))
            while True:
                try:
                    connection = sqlite3.connect(STATUS_DATABASE_PATH)
                    cursor = connection.cursor()
                    cursor.executemany(
                        'UPDATE job_status '
                        'SET workspace_url=?, job_status=\'DEBUG\' '
                        'WHERE watershed_basename=? AND fid=?',
                        payload_list)
                    connection.commit()
                    cursor.close()
                    break
                except Exception:
                    LOGGER.exception('error on connection')
                    time.sleep(0.1)
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
        total_area = 0.0
        for payload in cursor.fetchall():
            watershed_basename, fid, watershed_area_deg = payload
            total_expected_runtime += TIME_PER_AREA * watershed_area_deg
            watershed_fid_tuple_list.append(
                (watershed_basename, fid, watershed_area_deg))
            total_area += watershed_area_deg
            if total_expected_runtime > TIME_PER_WORKER:
                LOGGER.debug(
                    'sending job with %d elements %.2f min time',
                    len(watershed_fid_tuple_list), total_expected_runtime/60)
                send_job(watershed_fid_tuple_list, total_area)
                watershed_fid_tuple_list = []
                total_expected_runtime = 0.0
                total_area = 0.0
        cursor.close()
        connection.commit()
    except Exception:
        LOGGER.exception('exception in scheduler')
        cursor.close()
        connection.commit()


@retrying.retry()
def send_job(watershed_fid_tuple_list, total_area):
    """Send watershed/fid to the global execution pool."""
    try:
        LOGGER.debug('scheduling %s', watershed_fid_tuple_list)
        with APP.app_context():
            callback_url = flask.url_for(
                'processing_complete', _external=True)
        data_payload = {
            'watershed_fid_tuple_list': watershed_fid_tuple_list,
            'callback_url': callback_url,
            'total_area': total_area,
        }

        LOGGER.debug('payload: %s', data_payload)
        worker_ip_port = GLOBAL_WORKER_STATE_SET.get_ready_host()
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
                for watershed_basename, fid, watershed_area_deg in (
                        watershed_fid_tuple_list):
                    SCHEDULED_MAP[(watershed_basename, fid)] = {
                        'status_url': response.json()['status_url'],
                        'worker_ip_port': worker_ip_port,
                        'last_time_accessed': time.time(),
                        'total_area': watershed_area_deg
                    }
        else:
            raise RuntimeError(str(response))
    except Exception:
        LOGGER.exception(
            'something bad happened, on %s for %s',
            worker_ip_port, watershed_fid_tuple_list)
        GLOBAL_WORKER_STATE_SET.remove_host(worker_ip_port)
        raise


def host_file_monitor():
    """Watch for AWS worker instances on the network.

    Returns:
        never

    """
    while True:
        try:
            raw_output = subprocess.check_output(
                'aws2 ec2 describe-instances', shell=True)
            out_json = json.loads(raw_output)
            for reservation in out_json['Reservations']:
                for instance in reservation['Instances']:
                    try:
                        if 'Tags' not in instance:
                            continue
                        for tag in instance['Tags']:
                            if tag['Value'] == WORKER_TAG_ID and (
                                    instance['State']['Name'] == (
                                        'running')):
                                GLOBAL_WORKER_STATE_SET.add_host(
                                    '%s:8888' % instance['PrivateIpAddress'])
                                break
                    except Exception:
                        LOGGER.exception('something bad happened')
            time.sleep(DETECTOR_POLL_TIME)
        except Exception:
            LOGGER.exception('exception in `host_file_monitor`')


def worker_status_monitor():
    """Monitor the status of watershed workers and reschedule if down."""
    while True:
        time.sleep(30)
        current_time = time.time()
        failed_job_list = []
        with GLOBAL_LOCK:
            total_area = 0.0
            checked_hosts = set()
            hosts_to_remove = set()
            for watershed_fid_tuple, value in SCHEDULED_MAP.items():
                if value['worker_ip_port'] in checked_hosts:
                    continue
                checked_hosts.add(value['worker_ip_port'])
                if current_time - value['last_time_accessed']:
                    response = requests.get(value['status_url'])
                    if response.ok:
                        value['last_time_accessed'] = time.time()
                    else:
                        failed_job_list.put(watershed_fid_tuple)
                        total_area += value['total_area']
                        # it failed so we should remove it from the potential
                        # host list because we don't know why. If it's still up
                        # it will be added back by another worker
                        hosts_to_remove.add(value['worker_ip_port'])
            for host in hosts_to_remove:
                GLOBAL_WORKER_STATE_SET.remove_host(host)
        for watershed_fid_tuple in failed_job_list:
            LOGGER.debug('rescheduling %s', str(watershed_fid_tuple))
            with GLOBAL_LOCK:
                del SCHEDULED_MAP[watershed_fid_tuple]
            send_job([watershed_fid_tuple], total_area)


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

    worker_status_monitor_thread = threading.Thread(
        target=worker_status_monitor)
    worker_status_monitor_thread.start()

    schedule_worker_thread = threading.Thread(
        target=schedule_worker)
    schedule_worker_thread.start()

    host_file_monitor_thread = threading.Thread(
        target=host_file_monitor)
    host_file_monitor_thread.start()

    job_monitor_thread = threading.Thread(
        target=job_monitor)
    job_monitor_thread.start()

    APP.config.update(SERVER_NAME='%s:%d' % (args.external_ip, args.app_port))
    APP.run(
        host='0.0.0.0',
        port=args.app_port)
