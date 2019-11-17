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
WORKER_QUEUE = queue.Queue()
HOST_FILE_PATH = 'host_file.txt'
DETECTOR_POLL_TIME = 15.0
SCHEDULED_MAP = {}
GLOBAL_LOCK = threading.Lock()
GLOBAL_HOST_SET = set()
WORKER_TAG_ID = 'compute-server'

APP = flask.Flask(__name__)


def main(external_ip):
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

    schedule_worker_thread = threading.Thread(
        target=schedule_worker,
        args=(external_ip, WORKER_QUEUE,))
    schedule_worker_thread.start()


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


@APP.route('/api/v1/processing_complete', methods=['POST'])
def processing_complete():
    """Invoked when processing is complete for given watershed.

    Body of the post includs a url to the stored .zip file of the archive.

    Returns
        None.

    """
    payload = flask.request.get_json()
    LOGGER.debug('this was the payload: %s', payload)
    watershed_basename = payload['watershed_basename']
    fid = payload['fid']
    workspace_url = payload['workspace_url']
    with GLOBAL_LOCK:
        payload = SCHEDULED_MAP[(watershed_basename, fid)]
        status_url = payload['status_url']
        # re-register the worker/port
        WORKER_QUEUE.put(payload['worker_ip_port'])
    response = requests.get(status_url)
    LOGGER.debug(
        'updating %s:%d complete, status: %s', watershed_basename, fid,
        response)
    connection = None
    cursor = None
    while True:
        try:
            connection = sqlite3.connect(STATUS_DATABASE_PATH)
            cursor = connection.cursor()
            cursor.execute(
                'UPDATE job_status '
                'SET workspace_url=?, job_status=\'DEBUG\' '
                'WHERE watershed_basename=? AND fid=?',
                (workspace_url, watershed_basename, fid))
            cursor.close()
            connection.commit()
            break
        except Exception:
            LOGGER.exception(
                'exception when inserting %s:%d, trying again',
                watershed_basename, fid)
            if connection:
                connection.commit()
            if cursor:
                cursor.close()
    LOGGER.debug('%s:%d complete', watershed_basename, fid)
    return '%s:%d complete' % (watershed_basename, fid), 202


@retrying.retry()
def schedule_worker(external_ip, worker_queue):
    """Monitors STATUS_DATABASE_PATH and schedules work.

    Parameters:
        external_ip (str): IP address used as a base to define build urls.
        worker_queue (queue): queue ip/port strings that can be used to connect
            to workers via RESTful API.

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
            'SELECT watershed_basename, fid '
            'FROM job_status '
            'WHERE job_status=\'PRESCHEDULED\'')
        for payload in cursor.fetchall():
            watershed_basename, fid = payload
            LOGGER.debug('scheduling %s %d', watershed_basename, fid)
            with GLOBAL_LOCK:
                if (watershed_basename, fid) in SCHEDULED_MAP:
                    LOGGER.warning(
                        '%s already in schedule', (watershed_basename, fid))
            worker_ip_port = worker_queue.get()
            with APP.app_context():
                callback_url = flask.url_for(
                    'processing_complete', _external=True)

            data_payload = {
                'watershed_basename': watershed_basename,
                'fid': fid,
                'bucket_id': 'NOBUCKET',
                'callback_url': callback_url,
            }

            # send job
            worker_rest_url = (
                'http://%s/api/v1/run_ndr' % worker_ip_port)
            LOGGER.debug('sending job %s to %s', data_payload, worker_rest_url)
            response = requests.post(worker_rest_url, json=data_payload)
            if response.ok:
                with GLOBAL_LOCK:
                    SCHEDULED_MAP[(watershed_basename, fid)] = {
                        'status_url': response.json()['status_url'],
                        'worker_ip_port': worker_ip_port,
                    }
            else:
                LOGGER.error(
                    'something bad happened when scheduling worker: %s',
                    str(response))
                if worker_ip_port in GLOBAL_HOST_SET:
                    worker_queue.put(worker_ip_port)
        cursor.close()
        connection.commit()

    except Exception:
        LOGGER.exception('exception in scheduler')
        if cursor:
            cursor.close()
        if connection:
            connection.commit()
        raise


def host_file_monitor(host_file_path, worker_host_queue):
    """Watch host_file_path & update worker_host_queue.

    Parameters:
        host_file_path (str): path to a file that contains lines
            of http://[host]:[port]<?label> that can be used to send inference
            work to. <label> can be used to use the same machine more than
            once.
        worker_host_queue (queue.Queue): new hosts are queued here
            so they can be pulled by other workers later.

    Returns:
        never

    """
    while True:
        try:
            raw_output = subprocess.check_output(
                'aws2 ec2 describe-instances', shell=True)
            out_json = json.loads(raw_output)
            host_set = set()
            for reservation in out_json['Reservations']:
                for instance in reservation['Instances']:
                    for tag in instance['Tags']:
                        if tag['Value'] == WORKER_TAG_ID and (
                                instance['State']['Name'] == (
                                    'running')):
                            host_set.add(
                                '%s:8888' % instance['PrivateIpAddress'])
                            break
            with GLOBAL_LOCK:
                global GLOBAL_HOST_SET
                old_host_set = GLOBAL_HOST_SET
                GLOBAL_HOST_SET = host_set
                new_hosts = GLOBAL_HOST_SET.difference(old_host_set)
                LOGGER.debug('here are the new hosts: %s', new_hosts)
                for new_host in new_hosts:
                    worker_host_queue.put(new_host)
            time.sleep(DETECTOR_POLL_TIME)
        except Exception:
            LOGGER.exception('exception in `host_file_monitor`')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='NCI NDR Analysis.')
    parser.add_argument(
        '--app_port', type=int, default=8080,
        help='port to listen on for callback complete')
    parser.add_argument(
        '--external_ip', type=str, default='localhost',
        help='define external IP that can be used to connect to this app')
    args = parser.parse_args()
    main(args.external_ip)
    if not os.path.exists(HOST_FILE_PATH):
        with open(HOST_FILE_PATH, 'w') as host_file:
            host_file.write('')
    host_file_monitor_thread = threading.Thread(
        target=host_file_monitor,
        args=(HOST_FILE_PATH, WORKER_QUEUE))
    host_file_monitor_thread.start()

    APP.config.update(SERVER_NAME='%s:%d' % (args.external_ip, args.app_port))
    APP.run(
        host='0.0.0.0',
        port=args.app_port)
