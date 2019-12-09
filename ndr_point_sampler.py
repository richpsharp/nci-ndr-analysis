"""Sample NDR watersheds from a database and report areas around points."""
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


def build_watershed_r_tree(watershed_dir_path, r_tree_pickle_path):
    """Create a Shapely STR-packed R-tree off all geometry.

    Parameters:
        watershed_dir_path (str): directory to a directory of shapefiles. The
            contents of these shapefiles will be indexed into the R-tree.
        r_tree_pickle_path (str): path to an R-tree that can be unpickled
            containing an index of the watershed polygons.

    Returns:
        None.

    """
    shapely_geometry_list = []
    for path in glob.glob(watershed_dir_path, '*.shp'):
        watershed_id = os.path.basename(os.path.splitext(path)[0])
        LOGGER.debug(path)
        vector = gdal.OpenEx(path, gdal.OF_VECTOR)
        layer = vector.GetLayer()
        for watershed_feature in layer:
            watershed_geom = shapely.wkb.loads(
                watershed_feature.GetGeometryRef())
            shapely_geom = shapely.wkb.loads(watershed_geom.ExportToWkb())
            shapely_geom.basename = watershed_id
            shapely_geom.fid = watershed_feature.GetFID()
            shapely_geometry_list.append(shapely_geom)
    LOGGER.debug('building r-tree')
    r_tree = shapely.strtree.STRtree(shapely_geometry_list)
    LOGGER.debug('pickling r-tree')
    with open(r_tree_pickle_path, 'wb') as r_tree_pickle_file:
        pickle.dump(r_tree, r_tree_pickle_file)
    LOGGER.debug('pickle complete')

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
        args=(WATERSHEDS_URL, ECOSHARD_DIR, watersheds_done_token_path),
        target_path_list=[watersheds_done_token_path],
        task_name='download and unzip watersheds')

    build_r_tree_task = task_graph.add_task(
        func=build_watershed_r_tree,
        args=(WATERSHEDS_DIR, R_TREE_PICKLE_PATH),
        target_path_list=[R_TREE_PICKLE_PATH],
        dependent_task_list=[download_watersheds_task],
        task_name='pickle r tree')

    task_graph.join()
    task_graph.close()
