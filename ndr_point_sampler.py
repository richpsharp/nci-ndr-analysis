"""Sample NDR watersheds from a database and report areas around points."""
import argparse
import glob
import logging
import os
import pickle
import sys
import zipfile

from osgeo import gdal
import shapely.geometry
import shapely.strtree
import shapely.wkb
import ecoshard
import taskgraph

WORKSPACE_DIR = 'ndr_point_sampler_workspace'
ECOSHARD_DIR = os.path.join(WORKSPACE_DIR, 'ecoshard')
CHURN_DIR = os.path.join(WORKSPACE_DIR, 'churn')
NDR_WATERSHED_DATABASE_URL = (
    'https://storage.googleapis.com/ipbes-ndr-ecoshard-data/'
    'ndr_global_run_database_md5_fa32958c7024e8e93d067ecfd0c4d419.sqlite3')
WATERSHEDS_URL = (
    'https://nci-ecoshards.s3-us-west-1.amazonaws.com/'
    'watersheds_globe_HydroSHEDS_15arcseconds_'
    'blake2b_14ac9c77d2076d51b0258fd94d9378d4.zip')

NDR_WATERSHED_DATABASE_PATH = os.path.join(
    ECOSHARD_DIR, 'ndr_global_run_database.sqlite3')
WATERSHEDS_DIR = os.path.join(
    ECOSHARD_DIR, 'watersheds_globe_HydroSHEDS_15arcseconds')
R_TREE_PICKLE_PATH = os.path.join(CHURN_DIR, 'watershed_r_tree.pickle')

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


def build_watershed_r_tree(watershed_dir_path):
    """Create a Shapely STR-packed R-tree off all geometry.

    Parameters:
        watershed_dir_path (str): directory to a directory of shapefiles. The
            contents of these shapefiles will be indexed into the R-tree.
        r_tree_pickle_path (str): path to an R-tree that can be unpickled
            containing an index of the watershed polygons.

    Returns:
        Watershed r-tree.

    """
    shapely_geometry_list = []
    for path in glob.glob(os.path.join(watershed_dir_path, '*.shp')):
        watershed_id = os.path.basename(os.path.splitext(path)[0])
        LOGGER.debug(path)
        vector = gdal.OpenEx(path, gdal.OF_VECTOR)
        layer = vector.GetLayer()
        for watershed_feature in layer:
            watershed_geom = watershed_feature.GetGeometryRef()
            shapely_geom = shapely.wkb.loads(watershed_geom.ExportToWkb())
            shapely_geom.basename = watershed_id
            shapely_geom.fid = watershed_feature.GetFID()
            shapely_geometry_list.append(shapely_geom)
    LOGGER.debug('building r-tree')
    r_tree = shapely.strtree.STRtree(shapely_geometry_list)
    return r_tree


def sample_points(
        point_vector_path, watershed_r_tree_pickle_path,
        subraster_to_sample_path,
        database_path, target_sample_point_path):
    """Sample watershed subrasters across `point_vector_path`.

    Create a new point vector path containing the same geometry from
    `point_vector_path`. The target path will contain fields

    Parameters:
        point_vector_path (str): path to a point vector to sample.
        watershed_r_tree_pickle_path (str): path to a shapely STRTree object
            that when queried provides objcts with "fid" and "basename" fields
            indicating the basename/fid pair to find in `database_path`.
        subraster_to_sample_path (str): a partial path relative to a watershed
            workspace that will be sampled at the given radius and placed in
            the `target_fieldname` field of `target_sample_point_path.
        database_path (str): path to a database containing the table
            `job_status` with fields `watershed_basename`, `fid`, and
            `workspace_url`. The fields `watershed_basename` and `fid`
            correspond with the `basename` and `fid` members in the objects
            queried in the r-tree.
        target_sample_point_path (str): created by this call and contains
            the same geometry as `point_vector_path` as well as its `OBJECTID`.
            A new parameter `target_fieldname` will be added and will correspond
            to the sum of the `subraster_to_sample_path` raster sample.

    Returns:
        None.

    """
    LOGGER.debug('build r tree')
    r_tree = build_watershed_r_tree(WATERSHEDS_DIR)
    LOGGER.debug('sample points')
    point_vector = gdal.OpenEx(point_vector_path, gdal.OF_VECTOR)
    point_layer = point_vector.GetLayer()
    for point_feature in point_layer:
        point_geom = point_feature.GetGeometryRef()
        point_shapely = shapely.wkb.loads(point_geom.ExportToWkb())
        watershed_list = r_tree.query(point_shapely)
        if watershed_list:
            watershed_basename = watershed_list[0].basename
            fid = watershed_list[0].fid
            LOGGER.debug('%s: %d', watershed_basename, fid)
        else:
            LOGGER.debug('no watershed found')


if __name__ == '__main__':
    for dir_path in [WORKSPACE_DIR, ECOSHARD_DIR, CHURN_DIR]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    parser = argparse.ArgumentParser(description='NDR Point Sampler.')
    parser.add_argument('point_path', type=str, help='path to point shapefile')
    args = parser.parse_args()

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
        args=(WATERSHEDS_URL, ECOSHARD_DIR, watersheds_done_token_path),
        target_path_list=[watersheds_done_token_path],
        task_name='download and unzip watersheds')

    subraster_to_sample_path = None
    target_sample_point_path = os.path.join(
        WORKSPACE_DIR, os.path.splitext(os.path.basename(args.point_path))[0])
    sample_points_task = task_graph.add_task(
        func=sample_points,
        args=(
            args.point_path, R_TREE_PICKLE_PATH,
            subraster_to_sample_path,
            NDR_WATERSHED_DATABASE_PATH, target_sample_point_path),
        target_path_list=[target_sample_point_path],
        task_name='sample points')

    task_graph.join()
    task_graph.close()
