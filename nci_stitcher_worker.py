"""NCI NDR Stitcher Worker.

This script will create 1 degree stitches of NDR results.
"""
import argparse
import datetime
import glob
import logging
import os
import pathlib
import queue
import subprocess
import sys
import threading
import time
import traceback
import zipfile

from osgeo import gdal
from osgeo import osr
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
WARP_DIR = os.path.join(WORKSPACE_DIR, 'warped_rasters')
GLOBAL_NODATA_VAL = -1

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

RASTER_PATH_BASE_LIST = [
    'n_export.tif', 'intermediate_outputs/modified_load_n.tif']

AWS_BASE_URL = (
    'https://nci-ecoshards.s3-us-west-1.amazonaws.com/watershed_workspaces/')

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
    path_to_watershed_vector_map = {}
    while True:
        try:
            payload = WORK_QUEUE.get()
            JOB_STATUS[payload['session_id']] = 'RUNNING'

            start_time = time.time()

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

            raster_id = job_payload['raster_id']
            scenario_id = job_payload['scenario_id']
            global_raster_path = os.path.join(
                WORKSPACE_DIR, '%f_%f_%f_%f_%s_%s.tif' % (
                    lng_min, lat_min, lng_max, lat_max, raster_id,
                    scenario_id))
            gtiff_driver = gdal.GetDriverByName('GTiff')
            global_raster = gtiff_driver.Create(
                global_raster_path, n_cols, n_rows, 1, gdal.GDT_Float32,
                options=['COMPRESS=LZW', 'SPARSE_OK=TRUE'])
            global_raster.SetProjection(wgs84_srs.ExportToWkt())
            global_raster.SetGeoTransform(geotransform)
            global_band = global_raster.GetRasterBand(1)
            global_band.SetNoDataValue(GLOBAL_NODATA_VAL)
            global_band.FlushCache()
            global_raster.FlushCache()
            global_raster_info = pygeoprocessing.get_raster_info(
                global_raster_path)
            # find all the watersheds that overlap this grid cell
            bounding_box = shapely.geometry.box(
                lng_min, lat_min, lng_max, lat_max)
            for item in watershed_r_tree.intersection(
                    bounding_box.bounds, objects=True):
                vector_path = item.object['vector_path']
                if vector_path not in path_to_watershed_vector_map:
                    path_to_watershed_vector_map[vector_path] = (
                        gdal.OpenEx(vector_path, gdal.OF_VECTOR))
                watershed_basename = (
                    os.path.basename(os.path.splitext(vector_path)[0]))
                layer = path_to_watershed_vector_map[vector_path].GetLayer()
                watershed_feature = layer.GetFeature(item.object['fid'])
                geom = watershed_feature.GetGeometryRef()
                geom_shapely = shapely.wkb.loads(geom.ExportToWkb())
                if not geom_shapely.intersects(bounding_box):
                    continue
                basin_id = watershed_feature.GetField('BASIN_ID')
                watershed_id = '%s_%d' % (watershed_basename, basin_id-1)
                # test if resource exists
                watershed_url = os.path.join(
                    AWS_BASE_URL, '%s.zip' % watershed_id)
                download_watershed(watershed_url, watershed_id, tdd_downloader)
                global_inv_gt = gdal.InvGeoTransform(
                    global_raster_info['geotransform'])
                LOGGER.debug('looking for %s.tif', raster_id)
                watershed_raster_path = str(next(
                    pathlib.Path(tdd_downloader.get_path(watershed_id)).rglob(
                        '%s.tif' % raster_id)))
                stitch_raster_info = pygeoprocessing.get_raster_info(
                    watershed_raster_path)
                warp_raster_path = os.path.join(
                    WARP_DIR, '%s_%s' % (
                        watershed_id, os.path.basename(watershed_raster_path)))
                warp_task = task_graph.add_task(
                    func=pygeoprocessing.warp_raster,
                    args=(
                        watershed_raster_path,
                        global_raster_info['pixel_size'], warp_raster_path,
                        'near'),
                    kwargs={'target_sr_wkt': global_raster_info['projection']},
                    target_path_list=[warp_raster_path],
                    task_name='warp %s' % watershed_raster_path)
                warp_task.join()
                warp_info = pygeoprocessing.get_raster_info(warp_raster_path)
                warp_bb = warp_info['bounding_box']

                # recall that y goes down as j goes up, so min y is max j
                global_i_min, global_j_max = [
                    int(round(x)) for x in gdal.ApplyGeoTransform(
                        global_inv_gt, warp_bb[0], warp_bb[1])]
                global_i_max, global_j_min = [
                    int(round(x)) for x in gdal.ApplyGeoTransform(
                        global_inv_gt, warp_bb[2], warp_bb[3])]

                global_xsize, global_ysize = global_raster_info['raster_size']

                if (global_i_min >= global_xsize or
                        global_j_min >= global_ysize or
                        global_i_max < 0 or global_j_max < 0):
                    LOGGER.debug(stitch_raster_info)
                    raise ValueError(
                        '%f %f %f %f out of bounds (%d, %d)',
                        global_i_min, global_j_min,
                        global_i_max, global_j_max,
                        global_xsize, global_ysize)

                # clamp to fit in the global i/j rasters
                stitch_i = 0
                stitch_j = 0
                if global_i_min < 0:
                    stitch_i = -global_i_min
                    global_i_min = 0
                if global_j_min < 0:
                    stitch_j = -global_j_min
                    global_j_min = 0
                global_i_max = min(global_xsize, global_i_max)
                global_j_max = min(global_ysize, global_j_max)
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

                stitch_nodata = stitch_raster_info['nodata'][0]
                stitch_array = stitch_raster.ReadAsArray(
                    stitch_i, stitch_j, stitch_x_size, stitch_y_size)
                stitch_raster.FlushCache()
                stitch_raster = None
                valid_stitch = ~numpy.isclose(stitch_array, stitch_nodata)
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
                global_band.FlushCache()
                global_raster.FlushCache()

                try:
                    tdd_downloader.remove_files(watershed_id)
                except OSError:
                    LOGGER.warn(
                        "couldn't remove %s" % tdd_downloader.get_path(
                            watershed_id))

            global_band = None
            global_raster = None

            # TODO: upload the .tif
            geotiff_s3_uri = (
                "%s/%s/%s" %
                (payload['bucket_uri_prefix'], scenario_id,
                 os.path.basename(global_raster_path)))
            subprocess.run(
                ["/usr/local/bin/aws2 s3 cp %s %s" % (
                    global_raster_path, geotiff_s3_uri)], shell=True,
                check=True)
            total_time = time.time() - start_time
            data_payload = {
                'total_time': total_time,
                'session_id': payload['session_id'],
                'grid_id': job_payload['grid_id'],
                'geotiff_s3_uri': geotiff_s3_uri,
                'raster_id': raster_id,
                'scenario_id': scenario_id,
            }
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


@retrying.retry(
    stop_max_attempt_number=5, wait_exponential_multiplier=1000,
    wait_exponential_max=10000)
def download_watershed(watershed_url, watershed_id, tdd_downloader):
    """Download the watershed workspace/zip file if possible.

    Parameters:
        watershed_url (str): url to .zip watershed workspace.
        watershed_id (str): id of watershed feature.

    Returns:
        None.

    """
    with requests.get(watershed_url, stream=True) as response:
        try:
            response.raise_for_status()
            # check to make sure it's not already downloaded
            if not tdd_downloader.exists(watershed_id):
                tdd_downloader.download_ecoshard(
                    watershed_url, watershed_id, decompress='unzip',
                    local_path=os.path.join('workspace_worker', watershed_id))
                task_graph.join()
        except requests.exceptions.HTTPError:
            # probably not a workspace we processed
            LOGGER.exception('exception in download_watershed')
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


def build_watershed_index(
        watershed_path_list, index_base_file_path):
    """Build an RTree index of watershed geometry.

    Parameters:
        watershed_path_list (str): list of paths to .shp files.
        index_base_file_path (str): basename to use to stream index to a file.

    Returns:
        watershed rtree whose objects contain:
            'vector_path': path to watershed shapefile
            'fid': watershed feature id.

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
                'vector_path': watershed_path,
                'fid': watershed_feature.GetFID(),
            }
            watershed_list.append((obj_id, watershed_shapely.bounds, obj))
            obj_id += 1
            watershed_shapely = None
        break
    LOGGER.info('build the index')
    watershed_rtree = rtree.index.Index(watershed_list)
    LOGGER.info('index all done')
    return watershed_rtree


if __name__ == '__main__':
    for dir_path in [WORKSPACE_DIR, CHURN_DIR, ECOSHARD_DIR, WARP_DIR]:
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
    watershed_path_list = list(glob.glob(os.path.join(
        tdd_downloader.get_path('watersheds'), '*.shp')))
    LOGGER.debug('watershed path list: %s', watershed_path_list)
    index_token_path = os.path.join(CHURN_DIR, 'index.COMPLETE')

    watershed_r_tree = build_watershed_index(
        watershed_path_list, watershed_index_basename_path)

    args = parser.parse_args()
    stitcher_worker_thread = threading.Thread(
        target=stitcher_worker, args=(watershed_r_tree,))
    LOGGER.debug('starting stitcher worker')
    stitcher_worker_thread.start()

    LOGGER.debug('starting app')
    APP.run(host='0.0.0.0', port=args.app_port)
