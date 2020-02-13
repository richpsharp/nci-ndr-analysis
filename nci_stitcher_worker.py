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
        WORK_QUEUE.put(
            (payload['job_tuple'],
             payload['callback_url'],
             payload['bucket_uri_prefix'],
             session_id,))
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
def stitcher_worker(work_queue, single_run_joinable_queue, error_queue):
    """Run the NDR model.

    Runs NDR with the given watershed/fid and uses data previously synchronized
    when the module started.

    Paramters:
        work_queue (queue): gets tuples of
            (watershed_basename, watershed_fid, callback_url,
             session_id)
    Returns:
        None.

    """
    while True:
        try:
            (job_tuple, callback_url, bucket_uri_prefix, session_id) = \
                WORK_QUEUE.get()
            JOB_STATUS[session_id] = 'RUNNING'

            start_time = time.time()
            total_time = time.time() - start_time
            data_payload = {
                'total_time': total_time,
                'session_id': session_id,
            }
            LOGGER.debug(
                'about to callback to this url: %s', callback_url)
            LOGGER.debug('with this payload: %s', data_payload)
            response = requests.post(callback_url, json=data_payload)
            if not response.ok:
                raise RuntimeError(
                    'something bad happened when scheduling worker: %s',
                    str(response))
            with GLOBAL_LOCK:
                JOB_STATUS[session_id] = 'COMPLETE'
        except Exception as e:
            LOGGER.exception('something bad happened')
            with GLOBAL_LOCK:
                JOB_STATUS[session_id] = 'ERROR: %s' % str(e)
            raise


def reproject_geometry_to_target(
        vector_path, feature_id, target_sr_wkt, target_path):
    """Reproject a single OGR DataSource feature.

    Transforms the features of the base vector to the desired output
    projection in a new ESRI Shapefile.

    Parameters:
        vector_path (str): path to vector
        feature_id (int): feature ID to reproject.
        target_sr_wkt (str): the desired output projection in Well Known Text
            (by layer.GetSpatialRef().ExportToWkt())
        feature_id (int): the feature to reproject and copy.
        target_path (str): the filepath to the transformed shapefile

    Returns:
        None

    """
    vector = gdal.OpenEx(vector_path, gdal.OF_VECTOR)
    layer = vector.GetLayer()
    feature = layer.GetFeature(feature_id)
    geom = feature.GetGeometryRef()
    geom_wkb = geom.ExportToWkb()
    base_sr_wkt = geom.GetSpatialReference().ExportToWkt()
    geom = None
    feature = None
    layer = None
    vector = None

    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    # if this file already exists, then remove it
    if os.path.isfile(target_path):
        LOGGER.warn(
            "reproject_vector: %s already exists, removing and overwriting",
            target_path)
        os.remove(target_path)

    target_sr = osr.SpatialReference(target_sr_wkt)

    # create a new shapefile from the orginal_datasource
    target_driver = gdal.GetDriverByName('GPKG')
    target_vector = target_driver.Create(
        target_path, 0, 0, 0, gdal.GDT_Unknown)
    layer_name = os.path.splitext(os.path.basename(target_path))[0]
    base_geom = ogr.CreateGeometryFromWkb(geom_wkb)
    target_layer = target_vector.CreateLayer(
        layer_name, target_sr, base_geom.GetGeometryType())

    # Create a coordinate transformation
    base_sr = osr.SpatialReference(base_sr_wkt)
    coord_trans = osr.CoordinateTransformation(base_sr, target_sr)

    # Transform geometry into format desired for the new projection
    error_code = base_geom.Transform(coord_trans)
    if error_code != 0:  # error
        # this could be caused by an out of range transformation
        # whatever the case, don't put the transformed poly into the
        # output set
        raise ValueError(
            "Unable to reproject geometry on %s." % target_path)

    # Copy original_datasource's feature and set as new shapes feature
    target_feature = ogr.Feature(target_layer.GetLayerDefn())
    target_feature.SetGeometry(base_geom)
    target_layer.CreateFeature(target_feature)
    target_layer.SyncToDisk()
    target_feature = None
    target_layer = None
    target_vector = None


def get_utm_epsg_srs(vector_path, fid):
    """Calculate the EPSG SRS of the watershed at the given feature.

    Parameters:
        vector_path (str): path to a vector in a wgs84 projection.
        fid (int): valid feature id in vector that will be used to
            calculate the UTM EPSG code.

    Returns:
        EPSG code of the centroid of the feature indicated by `fid`.

    """
    vector = gdal.OpenEx(vector_path, gdal.OF_VECTOR)
    layer = vector.GetLayer()
    feature = layer.GetFeature(fid)
    geometry = feature.GetGeometryRef()
    centroid_geom = geometry.Centroid()
    utm_code = (numpy.floor((centroid_geom.GetX() + 180)/6) % 60) + 1
    lat_code = 6 if centroid_geom.GetY() > 0 else 7
    epsg_code = int('32%d%02d' % (lat_code, utm_code))
    epsg_srs = osr.SpatialReference()
    epsg_srs.ImportFromEPSG(epsg_code)
    geometry = None
    layer = None
    vector = None
    return epsg_srs


def zipdir(dir_path, target_file):
    """Zip a directory to a file.

    Recurisvely zips the contents of `dir_path` to `target_file`.

    Parameters:
        dir_path (str): path to a directory.
        target_file (str): path to target zipfile.

    Returns:
        None.

    """
    with zipfile.ZipFile(
            target_file, 'w', zipfile.ZIP_DEFLATED) as zipfh:
        for root, dirs, files in os.walk(dir_path):
            for file in files:
                zipfh.write(os.path.join(root, file))


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
    single_run_work_queue = multiprocessing.JoinableQueue()
    error_queue = multiprocessing.Queue()
    stitcher_worker_thread = threading.Thread(
        target=stitcher_worker, args=(
            WORK_QUEUE, single_run_work_queue, error_queue))
    LOGGER.debug('starting stitcher worker')
    stitcher_worker_thread.start()

    for _ in range(multiprocessing.cpu_count()):
        ndr_stitcher_worker_process = threading.Thread(
            target=ndr_stitcher_worker, args=(
                single_run_work_queue, error_queue))
        LOGGER.debug('starting single worker process')
        ndr_stitcher_worker_process.start()

    LOGGER.debug('starting app')
    APP.run(host='0.0.0.0', port=args.app_port)
