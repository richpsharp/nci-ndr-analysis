"""Sample NDR watersheds from a database and report areas around points."""
import logging
import os
import sys
import zipfile

import ecoshard
import taskgraph

WORKSPACE_DIR = 'ndr_point_sampler_workspace'
ECOSHARD_DIR = os.path.join(WORKSPACE_DIR, 'ecoshard')
NDR_WATERSHED_DATABASE_URL = (
    'https://storage.googleapis.com/ipbes-ndr-ecoshard-data/'
    'ndr_global_run_database_md5_fa32958c7024e8e93d067ecfd0c4d419.sqlite3')
WATERSHEDS_URL = (
    'https://nci-ecoshards.s3-us-west-1.amazonaws.com/'
    'watersheds_globe_HydroSHEDS_15arcseconds_'
    'blake2b_14ac9c77d2076d51b0258fd94d9378d4.zip')

NDR_WATERSHED_DATABASE_PATH = os.path.join(
    ECOSHARD_DIR, 'ndr_global_run_database.sqlite3')
WATERSHEDS_DIR = os.path.join(ECOSHARD_DIR)

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(pathname)s.%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
logging.getLogger('taskgraph').setLevel(logging.INFO)


def download_and_unzip(url, target_dir, target_token_path):
    """Download `url` to `target_dir` and touch `target_token_path`."""
    zipfile_path = os.path.join(target_dir, os.path.basename(url))
    LOGGER.debug('url %s, zipfile_path: %s', url, zipfile_path)
    ecoshard.download_url(url, zipfile_path)

    with zipfile.ZipFile(zipfile_path, 'r') as zip_ref:
        zip_ref.extractall(target_dir)

    with open(target_token_path, 'w') as touchfile:
        touchfile.write(f'unzipped {zipfile_path}')


if __name__ == '__main__':
    for dir_path in [WORKSPACE_DIR, ECOSHARD_DIR]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    task_graph = taskgraph.TaskGraph(WORKSPACE_DIR, -1)

    download_database_task = task_graph.add_task(
        func=ecoshard.download_url,
        args=(NDR_WATERSHED_DATABASE_URL, NDR_WATERSHED_DATABASE_PATH),
        target_path_list=[NDR_WATERSHED_DATABASE_PATH],
        task_name='download ndr watershed database')

    watersheds_done_token_path = os.path.join(
        WORKSPACE_DIR, 'watersheds.UNZIPPED')
    download_watersheds_task = task_graph.add_task(
        func=download_and_unzip,
        args=(WATERSHEDS_URL, WATERSHEDS_DIR, watersheds_done_token_path),
        target_path_list=[watersheds_done_token_path],
        task_name='download and unzip watersheds')

    task_graph.join()
    task_graph.close()
