"""NCI NDR Watershed Worker.

This script will process requests to run NDR on a particular watershed through
a RESTful API.


"""
import argparse
import datetime
import logging
import os
import sys
import zipfile

import ecoshard
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
             'biophysical'),
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
            args=(path_map[path_zip_key], path_map[path_key], unzip_token_path),
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='NCI NDR Watershed Worker.')
    parser.add_argument(
        'n_workers', type=int, default=-1,
        help='number of taskgraph workers to create')

    args = parser.parse_args()
    main(args.n_workers)
