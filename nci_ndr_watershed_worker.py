"""NCI NDR Watershed Worker.

This script will process requests to run NDR on a particular watershed through
a RESTful API.


"""
import argparse
import datetime
import logging
import os
import queue
import sys
import threading
import uuid
import zipfile

import ecoshard
import flask
import requests
import retrying
import taskgraph

DEM_URL = (
    'https://storage.googleapis.com/nci-ecoshards/'
    'global_dem_3s_blake2b_0532bf0a1bedbe5a98d1dc449a33ef0c.zip')

WATERSHEDS_URL = (
    'https://storage.googleapis.com/nci-ecoshards/'
    'watersheds_globe_HydroSHEDS_15arcseconds_'
    'blake2b_14ac9c77d2076d51b0258fd94d9378d4.zip')

PRECIP_URL = (
    'https://storage.googleapis.com/nci-ecoshards/'
    'worldclim_2015_md5_16356b3770460a390de7e761a27dbfa1.tif')

LULC_URL = (
    'https://storage.googleapis.com/nci-ecoshards/'
    'lulc_gc_esa_classes_md5_15b6d376e67f9e26a7188727278e630e.tif')

FERTILIZER_URL = (
    'https://storage.googleapis.com/nci-ecoshards/'
    'nfertilizer_global_kg_ha_yr_md5_88dae2a76a120dedeab153a334f929cc.tif')

BIOPHYSICAL_URL = (
    'https://storage.googleapis.com/nci-ecoshards/'
    'NDR_representative_table_md5_958bdeb45eb93e54d924ccd16b6cafee.csv')

GLOBAL_NDR_ARGS = {
    'threshold_flow_accumulation': 1000,
    'k_param': 2.0,
    'calc_n': True,
}

WORKSPACE_DIR = 'workspace_worker'
ECOSHARD_DIR = os.path.join(WORKSPACE_DIR, 'ecoshards')
CHURN_DIR = os.path.join(WORKSPACE_DIR, 'churn')

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)

GLOBAL_LOCK = threading.Lock()
WORK_QUEUE = queue.Queue()
JOB_STATUS = {}
APP = flask.Flask(__name__)


def main(n_workers):
    """Entry point."""
    for dir_path in [WORKSPACE_DIR, ECOSHARD_DIR, CHURN_DIR]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    task_graph = taskgraph.TaskGraph(WORKSPACE_DIR, n_workers)

    # used to create dynamic paths
    path_map = {}
    download_task_map = {}
    # download all the base data
    for path_key_prefix, url in zip(
            ('dem', 'watersheds', 'precip', 'lulc', 'fertilizer',
             'biophysical_table'),
            (DEM_URL, WATERSHEDS_URL, PRECIP_URL, LULC_URL, FERTILIZER_URL,
             BIOPHYSICAL_URL)):
        if url.endswith('zip'):
            path_key = '%s_zip_path' % path_key_prefix
        else:
            path_key = '%s_path' % path_key_prefix
        path_map[path_key] = os.path.join(ECOSHARD_DIR, os.path.basename(url))
        LOGGER.debug(
            'scheduing download of %s: %s', path_key, path_map[path_key])
        download_task_map[path_key] = task_graph.add_task(
            func=ecoshard.download_url,
            args=(url, path_map[path_key]),
            target_path_list=[path_map[path_key]],
            task_name='download %s' % path_key)

    for path_zip_key in [k for k in path_map if 'zip' in k]:
        # unzip it
        path_key = path_zip_key.replace('_zip', '')
        path_map[path_key] = path_map[path_zip_key].replace('.zip', '')
        unzip_token_path = os.path.join(
            CHURN_DIR, '%s.UNZIPTOKEN' % os.path.basename(path_map[path_key]))
        LOGGER.debug(
            'scheduing unzip of %s: %s', path_key, path_map[path_key])
        download_task_map[path_key] = task_graph.add_task(
            func=unzip_file,
            args=(path_map[path_zip_key], path_map[path_key],
                  unzip_token_path),
            target_path_list=[unzip_token_path],
            dependent_task_list=[download_task_map[path_zip_key]],
            task_name='unzip %s' % path_zip_key)

    task_graph.join()


def unzip_file(zip_path, target_directory, token_file):
    """Unzip contents of `zip_path` into `target_directory`."""
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(target_directory)
    with open(token_file, 'w') as token_file:
        token_file.write(str(datetime.datetime.now()))


@APP.route('/api/v1/run_ndr', methods=['POST'])
def run_ndr():
    """Create a new NDR calculation job w/ the given arguments.

    Parameters expected in post data:
        watershed_path (str): relative path to global watershed shapefile.
            This is defined across docker images and so the server and
            workers will have the same relative path.
        fid (int): this is the FID in the `watershed_path` vector that
            corresponds to the watershed to process.
        bucket_id (str): when the model run is complete, the workspace will
            zip itself up and push its contents to this Google Bucket ID.
        callback_url (str): this is the url to use to POST to when the
            watershed is complete. The body of the post will contain the
            url to the bucket OR the traceback of the exception that
            occured.

    Returns:
        (status_url, 201) if successful. `status_url` can be GET to monitor
            the status of the run.
        ('error text', 500) if too busy or some other exception occured.

    """
    try:
        payload = flask.request.get_json()
        LOGGER.debug('got post: %s', str(payload))
        watershed_path = payload['watershed_path']
        fid = payload['fid']
        bucket_id = payload['bucket_id']
        callback_url = payload['callback_url']
        session_id = str(uuid.uuid4())
        status_url = flask.url_for(
            'get_status', _external=True, session_id=session_id)
        WORK_QUEUE.put(
            (watershed_path, fid, bucket_id, callback_url, session_id))
        return {'status_url': status_url}, 201
    except Exception as e:
        return str(e), 500


def get_status(session_id):
    """Report the status of the execution state of `session_id`."""
    try:
        with GLOBAL_LOCK:
            status = JOB_STATUS[session_id]
            return status, 200
    except Exception as e:
        return str(e), 500


@retrying.retry()
def ndr_worker(work_queue):
    """Run the NDR model.

    Runs NDR with the given watershed/fid and uses data previously synchronized
    when the module started.

    Paramters:
        watershed_path (str): relative path to watershed vector.
        fid (int): feature ID for the watershed.
        bucket_id (str): Google Bucket ID to copy completed workspace to.
        job_id (str): unique string that can be used to update status of
            `ACTIVE_JOBS`.

    Returns:
        None.

    """
    while True:
        payload = work_queue.get()
        LOGGER.debug(
            'would run right now if implemented %s', payload)
        watershed_path, fid, bucket_id, callback_url, session_id = payload
        data_payload = {
            'workspace_url': 'TEST_URL'
        }
        response = requests.post(callback_url, data=data_payload)
        if not response.ok:
            LOGGER.error(
                'something bad happened when scheduling worker: %s',
                str(response))

        # create local workspace
        # extract the watershed to workspace/data
        # clip/extract/project the DEM, precip, lulc, fert. to local workspace
        # construct the args dict
        # call NDR
        # zip up the workspace
        # copy workspace to bucket
        # delete original workspace
        # update global status
        # post to callback url
        # terminate
        # ('dem', 'watersheds', 'precip', 'lulc', 'fertilizer',
        #          'biophysical_table'),

        # args = {
        #     'workspace_dir':
        #     'dem_path':
        #     'lulc_path':
        #     'runoff_proxy_path':
        #     'watersheds_path':
        #     'biophysical_table_path': path_map['biophysical_path']
        #     'calc_p': False,
        #     'calc_n': GLOBAL_NDR_ARGS['calc_n'].
        #     'results_suffix': '',
        #     'threshold_flow_accumulation': (
        #         GLOBAL_NDR_ARGS['threshold_flow_accumulation']),
        #     'n_workers': -1
        # }


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='NCI NDR Watershed Worker.')
    parser.add_argument(
        'n_workers', type=int, default=-1,
        help='number of taskgraph workers to create')
    parser.add_argument(
        'app_port', type=int, default=8888,
        help='port to listen on for posts')
    args = parser.parse_args()
    main(args.n_workers)
    for _ in range(args.n_workers):
        ndr_worker_thread = threading.Thread(
            target=ndr_worker, args=(WORK_QUEUE,))
        ndr_worker_thread.start()
    APP.run(host='0.0.0.0', port=args.app_port)
