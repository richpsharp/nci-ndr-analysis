"""NCI NDR Analysis.

Design doc is available here:

https://docs.google.com/document/d/
1Iw8YxrXPSbSp5TemRo-mbfvxDiTpdCKqRrW1terp2gE/edit

"""
import argparse
import datetime
import glob
import logging
import os
import queue
import sys
import zipfile

import flask
from osgeo import gdal
import ecoshard
import requests
import taskgraph

WATERSHEDS_URL = (
    'https://storage.googleapis.com/nci-ecoshards/'
    'watersheds_globe_HydroSHEDS_15arcseconds_'
    'blake2b_14ac9c77d2076d51b0258fd94d9378d4.zip')

WORKSPACE_DIR = 'workspace_manager'
ECOSHARD_DIR = os.path.join(WORKSPACE_DIR, 'ecoshards')
CHURN_DIR = os.path.join(WORKSPACE_DIR, 'churn')

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)

WORKER_QUEUE = queue.Queue()


def main(n_workers):
    """Entry point."""
    for dir_path in [WORKSPACE_DIR, ECOSHARD_DIR, CHURN_DIR]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    task_graph = taskgraph.TaskGraph(WORKSPACE_DIR, n_workers)

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
    _ = task_graph.add_task(
        func=unzip_file,
        args=(watersheds_zip_path, watersheds_unzip_dir, unzip_token_path),
        target_path_list=[unzip_token_path],
        dependent_task_list=[watersheds_zip_fetch_task],
        task_name='unzip %s' % watersheds_zip_path)
    task_graph.join()
    task_graph.close()

    watersheds_root_dir = os.path.join(
        watersheds_unzip_dir, 'watersheds_globe_HydroSHEDS_15arcseconds')
    for watershed_shape_path in glob.glob(
            os.path.join(watersheds_root_dir, '*.shp')):
        watershed_vector = gdal.OpenEx(watershed_shape_path)
        watershed_layer = watershed_vector.GetLayer()
        LOGGER.debug('processing watershed %s', watershed_shape_path)
        watershed_basename = os.path.splitext(
            os.path.basename(watershed_shape_path))[0]
        for watershed_feature in watershed_layer:
            # TODO: ensure feature has not been already processed
            # TODO: don't run on ahead until there's a free worker
            fid = watershed_feature.GetFID()
            callback_url = flask.url_for(
                'processing_complete', _external=True,
                watershed_basename=watershed_basename, fid=fid)
            data_payload = {
                'watershed_path': watershed_shape_path,
                'fid': fid,
                'bucket_id': 'NOBUCKET',
                'callback_url': callback_url,
            }
            while True:
                try:
                    LOGGER.debug(
                        'fetching a worker for %s:%s', watershed_shape_path,
                        fid)
                    worker_ip_port = WORKER_QUEUE.get()
                    worker_rest_url = (
                        'http://%s/api/v1/run_ndr' % worker_ip_port)
                    response = requests.post(
                        worker_rest_url, data=data_payload)
                    if response.ok:
                        WORKER_QUEUE.put(worker_ip_port)
                        break
                except Exception:
                    LOGGER.exception('something bad happened')

        LOGGER.debug('all done %d', fid)
    LOGGER.debug('all done with all')


def unzip_file(zip_path, target_directory, token_file):
    """Unzip contents of `zip_path` into `target_directory`."""
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(target_directory)
    with open(token_file, 'w') as token_file:
        token_file.write(str(datetime.datetime.now()))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='NCI NDR Analysis.')
    parser.add_argument(
        'n_workers', type=int, default=-1,
        help='number of taskgraph workers to create')

    args = parser.parse_args()
    WORKER_QUEUE.put('localhost:8888')
    main(args.n_workers)
    # TODO: for debugging
