"""NCI NDR Stitcher Worker.

This script will create 1 degree stitches of NDR results.
"""
import argparse
import datetime
import glob
import json
import logging
import multiprocessing
import os
import queue
import re
import shutil
import subprocess
import sys
import threading
import time
import traceback
import zipfile

from osgeo import gdal
from osgeo import ogr
from osgeo import osr
import ecoshard
import flask
import numpy
import pygeoprocessing
import requests
import retrying
import taskgraph

# set a 512MB limit for the cache
gdal.SetCacheMax(2**29)


WORKSPACE_DIR = 'workspace_worker'
CHURN_DIR = os.path.join(WORKSPACE_DIR, 'churn')

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
logging.getLogger('taskgraph').setLevel(logging.INFO)

GLOBAL_LOCK = threading.Lock()
WORK_QUEUE = queue.Queue()
JOB_STATUS = {}
APP = flask.Flask(__name__)
PATH_MAP = {}
TARGET_PIXEL_SIZE = (90, -90)


def unzip_file(zip_path, target_directory, token_file):
    """Unzip contents of `zip_path` into `target_directory`."""
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(target_directory)
    with open(token_file, 'w') as token_file:
        token_file.write(str(datetime.datetime.now()))


@APP.route('/api/v1/stitch_grid_cell', methods=['POST'])
def stitch_grid_cell():
    """Create a new stitch job w/ the given arguments.

    Parameters expected in post data:
        'job_tuple' (str):  this is enough information to perform the desired
            stitch job.
        'callback_url' (str): url to callback on successful run
        'bucket_uri_prefix' (str): the amazon s3 bucket to access needed data.
        'session_id' (str): globally unique ID that can be used to identify
            this job.

    Returns:
        (status_url, 201) if successful. `status_url` can be GET to monitor
            the status of the run.
        ('error text', 500) if too busy or some other exception occured.

    """
    try:
        payload = flask.request.get_json()
        LOGGER.debug('got post: %s', str(payload))
        session_id = payload['session_id']
        status_url = flask.url_for(
            'get_status', _external=True, session_id=session_id)
        with GLOBAL_LOCK:
            JOB_STATUS[session_id] = 'SCHEDULED'
        WORK_QUEUE.put(payload)
        return {'status_url': status_url}, 201
    except Exception:
        LOGGER.exception('an execption occured')
        return traceback.format_exc(), 500


@APP.route('/api/v1/get_status/<session_id>', methods=['GET'])
def get_status(session_id):
    """Report the status of the execution state of `session_id`."""
    try:
        status = JOB_STATUS[session_id]
        if 'ERROR' in status:
            raise RuntimeError(status)
        return status, 200
    except Exception as e:
        return str(e), 500


@retrying.retry(wait_exponential_multiplier=1000, wait_exponential_max=5000)
def stitcher_worker():
    """Run the NDR model.

    Runs NDR with the given watershed/fid and uses data previously synchronized
    when the module started.

    Paramters:
        work_queue (queue): gets dictionary of payloads.
    Returns:
        None.

    """
    while True:
        try:
            payload = WORK_QUEUE.get()
            JOB_STATUS[payload['session_id']] = 'RUNNING'

            start_time = time.time()
            total_time = time.time() - start_time

            data_payload = {
                'total_time': total_time,
                'session_id': payload['session_id'],
            }

            LOGGER.warning('TODO: no work being done yet but it would go here')

            LOGGER.debug(
                'about to callback to this url: %s', payload['callback_url'])
            LOGGER.debug('with this payload: %s', data_payload)
            response = requests.post(
                payload['callback_url'], json=data_payload)
            if not response.ok:
                raise RuntimeError(
                    'something bad happened when scheduling worker: %s',
                    str(response))
            with GLOBAL_LOCK:
                JOB_STATUS[payload['session_id']] = 'COMPLETE'
        except Exception as e:
            LOGGER.exception('something bad happened')
            with GLOBAL_LOCK:
                JOB_STATUS[payload['session_id']] = 'ERROR: %s' % str(e)
            raise


if __name__ == '__main__':
    for dir_path in [WORKSPACE_DIR, CHURN_DIR]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    task_graph = taskgraph.TaskGraph(WORKSPACE_DIR, -1)

    parser = argparse.ArgumentParser(description='NCI NDR Stitcher Worker.')
    parser.add_argument(
        '--app_port', type=int, default=8888,
        help='port to listen on for posts')
    args = parser.parse_args()
    stitcher_worker_thread = threading.Thread(
        target=stitcher_worker, args=())
    LOGGER.debug('starting stitcher worker')
    stitcher_worker_thread.start()

    LOGGER.debug('starting app')
    APP.run(host='0.0.0.0', port=args.app_port)
