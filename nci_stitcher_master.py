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
import numpy
import pygeoprocessing
import requests
import retrying
import shapely.strtree
import shapely.wkb
import taskgraph

gdal.SetCacheMax(2**29)


SCENARIO_ID_LIST = [
    'baseline_potter', 'baseline_napp_rate', 'ag_expansion',
    'ag_intensification', 'restoration_potter', 'restoration_napp_rate']

WATERSHEDS_URL = (
    'https://nci-ecoshards.s3-us-west-1.amazonaws.com/'
    'watersheds_globe_HydroSHEDS_15arcseconds_'
    'blake2b_14ac9c77d2076d51b0258fd94d9378d4.zip')

COUNTRY_BORDERS_URL = (
    'https://nci-ecoshards.s3-us-west-1.amazonaws.com/'
    'world_borders_md5_c8dd971a8a853b2f3e1d3801b9747d5f.gpkg')

WORKSPACE_DIR = 'nci_stitcher_workspace'
ECOSHARD_DIR = os.path.join(WORKSPACE_DIR, 'ecoshards')
CHURN_DIR = os.path.join(WORKSPACE_DIR, 'churn')
STATUS_DATABASE_PATH = os.path.join(CHURN_DIR, 'status_database.sqlite3')
DATABSE_TOKEN_PATH = os.path.join(
    CHURN_DIR, '%s.CREATED' % os.path.basename(STATUS_DATABASE_PATH))

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
    return 'hi'

GLOBAL_STATUS = {}
SCENARIO_ID_LIST = [
    'baseline_potter', 'baseline_napp_rate', 'ag_expansion',
    'ag_intensification', 'restoration_potter', 'restoration_napp_rate']

GLOBAL_STITCH_MAP = {
    'n_export': (
        'workspace_worker/[BASENAME]_[FID]/n_export.tif',
        gdal.GDT_Float32, -1),
    'modified_load': (
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
            scenario_id TEXT NOT NULL,
            raster_id TEXT NOT NULL,
            ul_grid_lng FLOAT NOT NULL,
            ul_grid_lat FLOAT NOT NULL,
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
    for scenario_id in SCENARIO_ID_LIST:
        GLOBAL_STATUS[scenario_id] = {}
        for raster_id in GLOBAL_STITCH_MAP:
            for lat in reversed(range(90, -90, -1)):
                for lng in range(-180, 180):
                    scenario_output_lat_lng_list.append(
                        (scenario_id, raster_id, lat, lng))
    insert_query = (
        'INSERT INTO job_status('
        'scenario_id, raster_id, ul_grid_lng, ul_grid_lat, stiched)'
        'VALUES (?, ?, ?, ?, 0)')
    cursor.executemany(insert_query, scenario_output_lat_lng_list)
    with open(complete_token_path, 'w') as complete_token_file:
        complete_token_file.write(str(datetime.datetime.now()))
    connection.commit()
    connection.close()


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
            WORKSPACE_DIR, ECOSHARD_DIR, CHURN_DIR]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    task_graph = taskgraph.TaskGraph(CHURN_DIR, -1)
    task_graph.add_task(
        func=create_status_database,
        args=(STATUS_DATABASE_PATH, DATABSE_TOKEN_PATH),
        target_path_list=[DATABSE_TOKEN_PATH],
        task_name='create status database')

    LOGGER.debug('start threading')
    new_host_monitor_thread = threading.Thread(
        target=new_host_monitor,
        args=(args.worker_list,))
    new_host_monitor_thread.start()

    START_TIME = time.time()
    LOGGER.debug('start the APP')
    APP.config.update(SERVER_NAME='%s:%d' % (args.external_ip, args.app_port))
    # Note: never run in debug mode because it starts two processes
    APP.run(
        host='0.0.0.0',
        port=args.app_port)
