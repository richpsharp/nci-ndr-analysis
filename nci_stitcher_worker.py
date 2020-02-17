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
import rtree
import shapely.wkb
import shapely.prepared
import taskgraph
import taskgraph_downloader_pnn

# set a 512MB limit for the cache
gdal.SetCacheMax(2**29)


WORKSPACE_DIR = 'workspace_worker'
CHURN_DIR = os.path.join(WORKSPACE_DIR, 'churn')
ECOSHARD_DIR = os.path.join(WORKSPACE_DIR, 'ecoshard')

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
logging.getLogger('taskgraph').setLevel(logging.INFO)

WORK_QUEUE = queue.Queue()
JOB_STATUS = {}
APP = flask.Flask(__name__)
PATH_MAP = {}
TARGET_PIXEL_SIZE = (90, -90)

WATERSHEDS_URL = (
    'https://nci-ecoshards.s3-us-west-1.amazonaws.com/'
    'watersheds_globe_HydroSHEDS_15arcseconds_'
    'blake2b_14ac9c77d2076d51b0258fd94d9378d4.zip')


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
def stitcher_worker(watershed_r_tree):
    """Run the NDR model.

    Runs NDR with the given watershed/fid and uses data previously synchronized
    when the module started.

    Paramters:
        watershed_r_tree (rtree.index.Index): rtree that contains an object
            with keys:
                shapely_obj: a shapely object that is the geometry of the
                    watershed
                BASIN_ID: the basin ID from the original vector feature,
                    used to determine the download url.
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

            job_payload = payload['job_payload']

            # make a new empty raster
            lng_min = job_payload['lng_min']
            lat_min = job_payload['lat_min']
            lng_max = job_payload['lng_max']
            lat_max = job_payload['lat_max']
            n_rows = int((lat_max - lat_min) / payload['wgs84_pixel_size'])
            n_cols = int((lng_max - lng_min) / payload['wgs84_pixel_size'])

            geotransform = [lng_min, payload['wgs84_pixel_size'], 0.0,
                            lat_max, 0, -payload['wgs84_pixel_size']]
            wgs84_srs = osr.SpatialReference()
            wgs84_srs.ImportFromEPSG(4326)

            stitch_raster_path = os.path.join(
                WORKSPACE_DIR, '%f_%f_%f_%f.tif' % (
                    lng_min, lat_min, lng_max, lat_max))
            gtiff_driver = gdal.GetDriverByName('GTiff')
            stitch_raster = gtiff_driver.Create(
                stitch_raster_path, n_cols, n_rows, 1, gdal.GDT_Float32,
                options=[
                    'TILED=YES', 'BIGTIFF=YES', 'COMPRESS=LZW',
                    'SPARSE_OK=TRUE'])
            stitch_raster.SetProjection(wgs84_srs.ExportToWkt())
            stitch_raster.SetGeoTransform(geotransform)
            stitch_band = stitch_raster.GetRasterBand(1)

            # find all the watersheds that overlap this grid cell
            for obj in watershed_r_tree.intersection(
                    (lng_min, lat_min, lng_max, lat_max), objects=True):
                LOGGER.debug(obj)

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
            JOB_STATUS[payload['session_id']] = 'COMPLETE'
        except Exception as e:
            LOGGER.exception('something bad happened')
            JOB_STATUS[payload['session_id']] = 'ERROR: %s' % str(e)
            raise


@retrying.retry(wait_exponential_multiplier=1000, wait_exponential_max=10000)
def stitch_into(master_raster_path, base_raster_path, nodata_value):
    """Stitch `base`into `master` by only overwriting non-nodata values."""
    try:
        global_raster_info = pygeoprocessing.get_raster_info(
            master_raster_path)
        global_raster = gdal.OpenEx(
            master_raster_path, gdal.OF_RASTER | gdal.GA_Update)
        global_band = global_raster.GetRasterBand(1)
        global_inv_gt = gdal.InvGeoTransform(
            global_raster_info['geotransform'])
        warp_dir = os.path.dirname(base_raster_path)
        warp_raster_path = os.path.join(
            warp_dir, os.path.basename(base_raster_path))
        pygeoprocessing.warp_raster(
            base_raster_path, global_raster_info['pixel_size'],
            warp_raster_path, 'near',
            target_sr_wkt=global_raster_info['projection'])
        warp_info = pygeoprocessing.get_raster_info(warp_raster_path)
        warp_bb = warp_info['bounding_box']

        # recall that y goes down as j goes up, so min y is max j
        global_i_min, global_j_max = [
            int(round(x)) for x in gdal.ApplyGeoTransform(
                global_inv_gt, warp_bb[0], warp_bb[1])]
        global_i_max, global_j_min = [
            int(round(x)) for x in gdal.ApplyGeoTransform(
                global_inv_gt, warp_bb[2], warp_bb[3])]

        if (global_i_min >= global_raster.RasterXSize or
                global_j_min >= global_raster.RasterYSize or
                global_i_max < 0 or global_j_max < 0):
            LOGGER.debug(global_raster_info)
            raise ValueError(
                '%f %f %f %f out of bounds (%d, %d)',
                global_i_min, global_j_min,
                global_i_max, global_j_max,
                global_raster.RasterXSize,
                global_raster.RasterYSize)

        # clamp to fit in the global i/j rasters
        stitch_i = 0
        stitch_j = 0
        if global_i_min < 0:
            stitch_i = -global_i_min
            global_i_min = 0
        if global_j_min < 0:
            stitch_j = -global_j_min
            global_j_min = 0
        global_i_max = min(global_raster.RasterXSize, global_i_max)
        global_j_max = min(global_raster.RasterYSize, global_j_max)
        stitch_x_size = global_i_max - global_i_min
        stitch_y_size = global_j_max - global_j_min

        stitch_raster = gdal.OpenEx(warp_raster_path, gdal.OF_RASTER)

        if stitch_i + stitch_x_size > stitch_raster.RasterXSize:
            stitch_x_size = stitch_raster.RasterXSize - stitch_i
        if stitch_j + stitch_y_size > stitch_raster.RasterYSize:
            stitch_y_size = stitch_raster.RasterYSize - stitch_j

        global_array = global_band.ReadAsArray(
            global_i_min, global_j_min,
            global_i_max-global_i_min,
            global_j_max-global_j_min)

        stitch_nodata = warp_info['nodata'][0]

        stitch_array = stitch_raster.ReadAsArray(
            stitch_i, stitch_j, stitch_x_size, stitch_y_size)
        valid_stitch = (
            ~numpy.isclose(stitch_array, stitch_nodata))
        if global_array.size != stitch_array.size:
            raise ValueError(
                "global not equal to stitch:\n"
                "%d %d %d %d\n%d %d %d %d",
                global_i_min, global_j_min,
                global_i_max-global_i_min,
                global_j_max-global_j_min,
                stitch_i, stitch_j, stitch_x_size, stitch_y_size)

        global_array[valid_stitch] = stitch_array[valid_stitch]
        global_band.WriteArray(
            global_array, xoff=global_i_min, yoff=global_j_min)
        global_band = None
    except Exception:
        LOGGER.exception('error on stitch into')
    finally:
        pass  # os.remove(wgs84_base_raster_path)


def build_watershed_index(watershed_path_list, index_base_file_path):
    """Build an RTree index of watershed geometry.

    Parameters:
        watershed_path_list (str): list of paths to .shp files.
        index_base_file_path (str): basename to use to stream index to a file.

    Returns:
        rtree.Index object containing sorted watershed objects.

    """
    watershed_list = []
    obj_id = 0
    for watershed_path in watershed_path_list:
        watershed_vector = gdal.OpenEx(watershed_path, gdal.OF_VECTOR)
        watershed_layer = watershed_vector.GetLayer()
        LOGGER.debug(watershed_path)
        feature_count = watershed_layer.GetFeatureCount()
        for watershed_index, watershed_feature in enumerate(watershed_layer):
            if watershed_index % 10000 == 0:
                LOGGER.debug(
                    '%.2f%% complete on %s',
                    100*watershed_index/(feature_count-1), watershed_path)
            watershed_geom = watershed_feature.GetGeometryRef()
            watershed_shapely = shapely.wkb.loads(watershed_geom.ExportToWkb())
            watershed_geom = None
            obj = {
                'watershed_path': watershed_path,
                'fid': watershed_feature.GetFID(),
            }
            watershed_list.append((obj_id, watershed_shapely.bounds, obj))
            obj_id += 1
            watershed_shapely = None
    LOGGER.info('build the index')
    rtree.index.Index(index_base_file_path, watershed_list)
    LOGGER.info('index all done')


if __name__ == '__main__':
    for dir_path in [WORKSPACE_DIR, CHURN_DIR, ECOSHARD_DIR]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    task_graph = taskgraph.TaskGraph(WORKSPACE_DIR, -1)

    parser = argparse.ArgumentParser(description='NCI NDR Stitcher Worker.')
    parser.add_argument(
        '--app_port', type=int, default=8888,
        help='port to listen on for posts')

    # download those watershed shapefiles
    tdd_downloader = taskgraph_downloader_pnn.TaskGraphDownloader(
        ECOSHARD_DIR, task_graph)

    LOGGER.debug('download watersheds')
    tdd_downloader.download_ecoshard(
        WATERSHEDS_URL, 'watersheds', decompress='unzip',
        local_path='watersheds_globe_HydroSHEDS_15arcseconds')

    LOGGER.debug('build watershed')
    watershed_index_basename_path = os.path.join(
        WORKSPACE_DIR, 'watershed_index')
    build_watershed_index_task = task_graph.add_task(
        func=build_watershed_index,
        args=(
            list(glob.glob(os.path.join(
                tdd_downloader.get_path('watersheds'), '*.shp'))),
            watershed_index_basename_path),
        ignore_path_list=[
            watershed_index_basename_path + x for x in ['.dat', '.idx']],
        target_path_list=[
            watershed_index_basename_path + x for x in ['.dat', '.idx']],
        task_name='make index')

    build_watershed_index_task.join()
    LOGGER.debug('load watershed index')
    watershed_index = rtree.index.Index('watershed_index')

    args = parser.parse_args()
    stitcher_worker_thread = threading.Thread(
        target=stitcher_worker, args=(watershed_index,))
    LOGGER.debug('starting stitcher worker')
    stitcher_worker_thread.start()

    LOGGER.debug('starting app')
    APP.run(host='0.0.0.0', port=args.app_port)
