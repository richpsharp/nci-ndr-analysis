"""NCI NDR Watershed Worker.

This script will process requests to run NDR on a particular watershed through
a RESTful API.


"""
import argparse
import datetime
import glob
import logging
import multiprocessing
import os
import queue
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
import inspring.ndr.ndr
import numpy
import pygeoprocessing
import requests
import retrying
import taskgraph

# set a 512MB limit for the cache
gdal.SetCacheMax(2**29)


DEM_URL = (
    'https://nci-ecoshards.s3-us-west-1.amazonaws.com/'
    'global_dem_3s_blake2b_0532bf0a1bedbe5a98d1dc449a33ef0c.zip')

WATERSHEDS_URL = (
    'https://nci-ecoshards.s3-us-west-1.amazonaws.com/'
    'watersheds_globe_HydroSHEDS_15arcseconds_'
    'blake2b_14ac9c77d2076d51b0258fd94d9378d4.zip')

PRECIP_URL = (
    'https://nci-ecoshards.s3-us-west-1.amazonaws.com/'
    'worldclim_2015_md5_16356b3770460a390de7e761a27dbfa1.tif')

SCENARIO_ID_LULC_FERT_URL_PAIRS = [
    ('current', 'https://nci-ecoshards.s3-us-west-1.amazonaws.com/'
     'lulc_gc_esa_classes_md5_15b6d376e67f9e26a7188727278e630e.tif',
     'https://nci-ecoshards.s3-us-west-1.amazonaws.com/'
     'nfertilizer_global_kg_ha_yr_md5_88dae2a76a120dedeab153a334f929cc.tif'),
    ('scenario_a',
     'https://storage.googleapis.com/critical-natural-capital-ecoshards/'
     'ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_'
     'md5_1254d25f937e6d9bdee5779d377c5aa4.tif',
     'https://storage.googleapis.com/nci-ecoshards/'
     'nfertilizer_global_Potter_md5_88dae2a76a120dedeab153a334f929cc.tif'),
    ('scenario_b',
     'https://storage.googleapis.com/nci-ecoshards/'
     'scenarios_intensified_ag_irrigated_'
     'md5_f954fdd1729718beda90d8ab8182b17c.tif',
     'https://storage.googleapis.com/nci-ecoshards/'
     'NitrogenApplication_Rate_md5_caee837fa0e881be0c36c1eba1dea44e.tif'),
    ('scenario_c',
     'https://storage.googleapis.com/nci-ecoshards/'
     'scenarios_intensified_ag_irrigated_'
     'md5_f954fdd1729718beda90d8ab8182b17c.tif',
     'https://storage.googleapis.com/nci-ecoshards/'
     'Intensified_NitrogenApplication_Rate_'
     'md5_7639f5b9604da28e683bfc138239df66.tif')
    # ('scenario_d',
    #  'FILL_IN_LULC',
    #  'https://storage.googleapis.com/nci-ecoshards/'
    #  'nfertilizer_global_Potter_md5_88dae2a76a120dedeab153a334f929cc.tif')
    ]

BIOPHYSICAL_URL = (
    'https://storage.googleapis.com/nci-ecoshards/'
    'nci-NDR-biophysical_table_ESA_ARIES_RS2_'
    'md5_18a0595bbc98ea5ad6a359fd3ee368ac.csv')

GLOBAL_NDR_ARGS = {
    'threshold_flow_accumulation': 1000,
    'k_param': 2.0,
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
LOGGER.addHandler(logging.FileHandler('log.txt'))

GLOBAL_LOCK = threading.Lock()
WORK_QUEUE = queue.Queue()
JOB_STATUS = {}
APP = flask.Flask(__name__)
PATH_MAP = {}
TARGET_PIXEL_SIZE = (90, -90)


def main():
    """Entry point."""
    for dir_path in [WORKSPACE_DIR, ECOSHARD_DIR, CHURN_DIR]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    task_graph = taskgraph.TaskGraph(WORKSPACE_DIR, -1)

    download_task_map = {}
    # download all the base data
    for path_key_prefix, url in zip(
            ('dem', 'watersheds', 'precip', 'biophysical_table'),
            (DEM_URL, WATERSHEDS_URL, PRECIP_URL,
             BIOPHYSICAL_URL)):
        if url.endswith('zip'):
            path_key = '%s_zip_path' % path_key_prefix
        else:
            path_key = '%s_path' % path_key_prefix
        PATH_MAP[path_key] = os.path.join(ECOSHARD_DIR, os.path.basename(url))
        LOGGER.debug(
            'scheduing download of %s: %s', path_key, PATH_MAP[path_key])
        download_task_map[path_key] = task_graph.add_task(
            func=ecoshard.download_url,
            args=(url, PATH_MAP[path_key]),
            target_path_list=[PATH_MAP[path_key]],
            task_name='download %s' % path_key)

    for scenario_id, lulc_url, fert_url in SCENARIO_ID_LULC_FERT_URL_PAIRS:
        PATH_MAP[scenario_id] = {
            'lulc_path': os.path.join(
                ECOSHARD_DIR, os.path.basename(lulc_url)),
            'fertilizer_path': os.path.join(
                ECOSHARD_DIR, os.path.basename(fert_url)),
        }
        for url, path_key in [
                (lulc_url, 'lulc_path'), (fert_url, 'fertilizer_path')]:
            LOGGER.debug(
                'scheduing download of %s: %s', scenario_id,
                PATH_MAP[scenario_id][path_key])
            download_task_map[path_key] = task_graph.add_task(
                func=ecoshard.download_url,
                args=(url, PATH_MAP[scenario_id][path_key]),
                target_path_list=[PATH_MAP[scenario_id][path_key]],
                task_name='download %s' % path_key)

    for path_zip_key in [k for k in PATH_MAP if 'zip' in k]:
        # unzip it
        path_key = path_zip_key.replace('_zip', '')
        PATH_MAP[path_key] = PATH_MAP[path_zip_key].replace('.zip', '')
        unzip_token_path = os.path.join(
            CHURN_DIR, '%s.UNZIPTOKEN' % os.path.basename(PATH_MAP[path_key]))
        LOGGER.debug(
            'scheduing unzip of %s: %s', path_key, PATH_MAP[path_key])
        download_task_map[path_key] = task_graph.add_task(
            func=unzip_file,
            args=(PATH_MAP[path_zip_key], PATH_MAP[path_key],
                  unzip_token_path),
            target_path_list=[unzip_token_path],
            dependent_task_list=[download_task_map[path_zip_key]],
            task_name='unzip %s' % path_zip_key)

    task_graph.join()
    task_graph.close()
    del task_graph


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
        session_id = payload['session_id']
        status_url = flask.url_for(
            'get_status', _external=True, session_id=session_id)
        with GLOBAL_LOCK:
            JOB_STATUS[session_id] = 'SCHEDULED'
        WORK_QUEUE.put(
            (payload['watershed_fid_tuple_list'],
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
        with GLOBAL_LOCK:
            status = JOB_STATUS[session_id]
            if 'ERROR' in status:
                raise RuntimeError(status)
            return status, 200
    except Exception as e:
        return str(e), 500


def ndr_single_worker(joinable_work_queue, error_queue):
    """Monitor joinable work queue and call single run as needed."""
    while True:
        try:
            watershed_basename, watershed_fid, bucket_uri_prefix = (
                joinable_work_queue.get())
            for scenario_id in []:
                single_run_ndr(
                    watershed_basename, watershed_fid, bucket_uri_prefix,
                    scenario_id, error_queue)
        except Exception:
            LOGGER.exception('exception in ndr worker')
            error_queue.put(traceback.format_exc())
        finally:
            joinable_work_queue.task_done()


def single_run_ndr(
        watershed_basename, watershed_fid, bucket_uri_prefix, scenario_id,
        error_queue):
    """Run a single instance of NDR."""
    try:
        LOGGER.debug(
            'running %s %d', watershed_basename, watershed_fid)
        # create local workspace
        ws_prefix = '%s_%d' % (watershed_basename, watershed_fid)
        local_workspace = os.path.join(WORKSPACE_DIR, ws_prefix)
        try:
            os.makedirs(local_workspace)
        except OSError:
            LOGGER.exception('unable to create %s', local_workspace)

        # extract the watershed to workspace/data
        watershed_root_path = os.path.join(
            ECOSHARD_DIR,
            'watersheds_globe_HydroSHEDS_15arcseconds_'
            'blake2b_14ac9c77d2076d51b0258fd94d9378d4',
            'watersheds_globe_HydroSHEDS_15arcseconds',
            '%s.shp' % watershed_basename)
        epsg_srs = get_utm_epsg_srs(watershed_root_path, watershed_fid)
        local_watershed_path = os.path.join(
            local_workspace, '%s.gpkg' % ws_prefix)

        # the dem is in lat/lng and is also a big set of tiles. Make a
        # VRT which is the bounds of the lat/lng of the watershed and
        # use that as the dem path argument
        watershed_vector = gdal.OpenEx(
            watershed_root_path, gdal.OF_VECTOR)
        watershed_layer = watershed_vector.GetLayer()
        watershed_feature = watershed_layer.GetFeature(watershed_fid)
        watershed_geom = watershed_feature.GetGeometryRef()
        x1, x2, y1, y2 = watershed_geom.GetEnvelope()
        watershed_geom = None
        watershed_feature = None
        watershed_layer = None
        watershed_vector = None

        watershed_bounding_box = [
            min(x1, x2),
            min(y1, y2),
            max(x1, x2),
            max(y1, y2)]

        vrt_options = gdal.BuildVRTOptions(
            outputBounds=(
                min(x1, x2)-0.1,
                min(y1, y2)-0.1,
                max(x1, x2)+0.1,
                max(y1, y2)+0.1)
        )
        dem_dir_path = os.path.join(
            PATH_MAP['dem_path'], 'global_dem_3s')
        dem_vrt_path = os.path.join(
            dem_dir_path, '%s_%s_vrt.vrt' % (
                watershed_basename, watershed_fid))
        gdal.BuildVRT(
            dem_vrt_path, glob.glob(
                os.path.join(dem_dir_path, '*.tif')),
            options=vrt_options)

        wgs84_sr = osr.SpatialReference()
        wgs84_sr.ImportFromEPSG(4326)
        target_bounding_box = pygeoprocessing.transform_bounding_box(
            watershed_bounding_box, wgs84_sr.ExportToWkt(),
            epsg_srs.ExportToWkt())

        reproject_geometry_to_target(
            watershed_root_path, watershed_fid, epsg_srs.ExportToWkt(),
            local_watershed_path)

        args = {
            'workspace_dir': local_workspace,
            'dem_path': dem_vrt_path,
            'lulc_path': PATH_MAP[scenario_id]['lulc_path'],
            'runoff_proxy_path': PATH_MAP['precip_path'],
            'ag_load_path': PATH_MAP[scenario_id]['fertilizer_path'],
            'watersheds_path': local_watershed_path,
            'biophysical_table_path': (
                PATH_MAP['biophysical_table_path']),
            'calc_n': True,
            'calc_p': False,
            'results_suffix': '',
            'threshold_flow_accumulation': (
                GLOBAL_NDR_ARGS['threshold_flow_accumulation']),
            'k_param': GLOBAL_NDR_ARGS['k_param'],
            'n_workers': -1,
            'target_sr_wkt': epsg_srs.ExportToWkt(),
            'target_pixel_size': TARGET_PIXEL_SIZE,
            'target_bounding_box': target_bounding_box
        }
        inspring.ndr.ndr.execute(args)
        zipfile_path = '%s.zip' % ws_prefix
        LOGGER.debug(
            "zipping %s to %s", args['workspace_dir'],
            zipfile_path)
        zipdir(args['workspace_dir'], zipfile_path)
        zipfile_s3_uri = (
            "%s/%s/%s" %
            (bucket_uri_prefix, scenario_id, os.path.basename(zipfile_path)))
        subprocess.run(
            ["/usr/local/bin/aws2 s3 cp %s %s" % (
                zipfile_path, zipfile_s3_uri)], shell=True,
            check=True)
        shutil.rmtree(args['workspace_dir'])
        os.remove(dem_vrt_path)
        workspace_url = (
            'https://nci-ecoshards.s3-us-west-1.amazonaws.com/'
            'watershed_workspaces/%s/%s' %
            (scenario_id, os.path.basename(zipfile_path)))
        os.remove(zipfile_path)
        try:
            head_request = requests.head(workspace_url)
            if not head_request:
                raise RuntimeError(
                    "something bad happened when checking if url "
                    "workspace was live: %s", str(head_request))
        except ConnectionError:
            LOGGER.exception(
                'a connection error when checking live url '
                'workspace')
            raise
    except Exception:
        LOGGER.exception('something bad happened when running ndr')
        error_queue.put(traceback.format_exc())
        raise


@retrying.retry()
def ndr_worker(work_queue, single_run_joinable_queue, error_queue):
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
            payload = work_queue.get()
            (watershed_fid_tuple_list, callback_url, bucket_uri_prefix,
                session_id) = payload
            with GLOBAL_LOCK:
                JOB_STATUS[session_id] = 'RUNNING'
            watershed_fid_url_list = []
            start_time = time.time()
            total_area = 0.0
            for watershed_basename, watershed_fid, watershed_area in (
                    watershed_fid_tuple_list):
                single_run_joinable_queue.put(
                    (watershed_basename, watershed_fid, bucket_uri_prefix))
                total_area += watershed_area
                zipfile_url = "%s/%s_%d.zip" % (
                    'https://nci-ecoshards.s3-us-west-1.amazonaws.com/'
                    'watershed_workspaces/', watershed_basename, watershed_fid)
                watershed_fid_url_list.append(
                    (watershed_basename, watershed_fid, zipfile_url))
            single_run_joinable_queue.join()
            error_message = ''
            while True:
                try:
                    error_message += error_queue.get(False)
                except queue.Empty:
                    break
            if error_message:
                raise RuntimeError(error_message)
            data_payload = {
                'watershed_fid_url_list': watershed_fid_url_list,
                'time_per_area': (time.time()-start_time) / total_area,
                'session_id': session_id,
            }
            LOGGER.debug(
                'about to callback to this url: %s', callback_url)
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
    parser = argparse.ArgumentParser(description='NCI NDR Watershed Worker.')
    parser.add_argument(
        'app_port', type=int, default=8888,
        help='port to listen on for posts')
    args = parser.parse_args()
    main()
    single_run_work_queue = multiprocessing.JoinableQueue()
    error_queue = multiprocessing.Queue()
    ndr_worker_thread = threading.Thread(
        target=ndr_worker, args=(
            WORK_QUEUE, single_run_work_queue, error_queue))
    LOGGER.debug('starting ndr worker')
    ndr_worker_thread.start()

    for _ in range(multiprocessing.cpu_count()):
        ndr_single_worker_process = threading.Thread(
            target=ndr_single_worker, args=(
                single_run_work_queue, error_queue))
        LOGGER.debug('starting single worker process')
        ndr_single_worker_process.start()

    LOGGER.debug('starting app')
    APP.run(host='0.0.0.0', port=args.app_port)
