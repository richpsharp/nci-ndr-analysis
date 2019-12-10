"""Sample NDR watersheds from a database and report areas around points."""
import argparse
import glob
import logging
import os
import sqlite3
import sys
import zipfile

from osgeo import gdal
from osgeo import ogr
from osgeo import osr
import pygeoprocessing
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
WATERSHED_WORKSPACE_DIR = os.path.join(
    ECOSHARD_DIR, 'watershed_workspaces')
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
        break
    LOGGER.debug('building r-tree')
    r_tree = shapely.strtree.STRtree(shapely_geometry_list)
    return r_tree


def create_local_buffer_region(
        raster_to_sample_path, sample_point_lat_lng_wkt, buffer_vector_path):
    """Creates a buffer geometry from a point and samples the raster.

    Parameters:
        raster_to_sample_path (str): path to a raster to sum the values on.
        buffer_vector_path (str): path to a target vector created by this call
            centered on `sample_point_lat_lng_wkt` in a local coordinate system.

    """
    sample_point = ogr.CreateGeometryFromWkt(sample_point_lat_lng_wkt)
    wgs84_srs = osr.SpatialReference()
    wgs84_srs.ImportFromEPSG(4326)

    target_srs = osr.SpatialReference()
    target_srs.ImportFromWkt(
        pygeoprocessing.get_raster_info(raster_to_sample_path)['projection'])

    coord_trans = osr.CoordinateTransformation(wgs84_srs, target_srs)
    sample_point.Transform(coord_trans)
    buffer_geom = sample_point.Buffer(10000)

    target_driver = gdal.GetDriverByName('GPKG')
    target_vector = target_driver.Create(
        buffer_vector_path, 0, 0, 0, gdal.GDT_Unknown)
    layer_name = os.path.splitext(os.path.basename(
        buffer_vector_path))[0]
    target_layer = target_vector.CreateLayer(
        layer_name, target_srs, ogr.wkbPolygon)
    target_layer.CreateField(ogr.FieldDefn('sum', ogr.OFTReal))
    feature_defn = target_layer.GetLayerDefn()
    buffer_feature = ogr.Feature(feature_defn)
    buffer_feature.SetGeometry(buffer_geom)
    target_layer.CreateFeature(buffer_feature)
    target_layer.SyncToDisk()

    buffer_stats = pygeoprocessing.zonal_statistics(
        (raster_to_sample_path, 1), buffer_vector_path,
        polygons_might_overlap=False, working_dir=CHURN_DIR)
    LOGGER.debug(buffer_stats)
    buffer_feature.SetField('sum', buffer_stats[1]['sum'])
    target_layer.SetFeature(buffer_feature)
    target_layer.SyncToDisk()
    target_layer = None
    target_vector = None


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

    target_driver = gdal.GetDriverByName('GPKG')
    target_vector = target_driver.Create(
        target_sample_point_path, 0, 0, 0, gdal.GDT_Unknown)
    layer_name = os.path.splitext(os.path.basename(
        target_sample_point_path))[0]
    target_layer = target_vector.CreateLayer(
        layer_name, point_layer.GetSpatialRef(), ogr.wkbPoint)

    target_layer.CreateField(ogr.FieldDefn('OBJECTID', ogr.OFTInteger))
    target_layer.CreateField(ogr.FieldDefn('basinid', ogr.OFTInteger))
    target_layer.CreateField(ogr.FieldDefn('N_export', ogr.OFTReal))
    feature_defn = target_layer.GetLayerDefn()

    connection = sqlite3.connect(database_path)
    cursor = connection.cursor()

    for point_feature in point_layer:
        point_geom = point_feature.GetGeometryRef()
        point_shapely = shapely.wkb.loads(point_geom.ExportToWkb())
        watershed_list = r_tree.query(point_shapely)
        if not watershed_list:
            LOGGER.debug('no watershed found')
            continue
        for watershed in watershed_list:
            if watershed.intersects(point_shapely):
                watershed_basename = watershed.basename
                fid = watershed.fid
                break
        cursor.execute(
            'SELECT workspace_url from job_status '
            'where watershed_basename=? and fid=?', (
                watershed_basename, fid))
        workspace_url = cursor.fetchone()[0]
        feature = ogr.Feature(feature_defn)
        feature.SetGeometry(point_geom.Clone())
        feature.SetField('OBJECTID', point_feature.GetField('OBJECTID'))
        feature.SetField('basinid', fid)
        if workspace_url is None:
            LOGGER.error(
                '%s %d: has no workspace', watershed_basename, fid)
            feature.SetField('N_export', -9999)
        else:
            workspace_url = workspace_url.replace(
                'watershed_workspaces//', 'watershed_workspaces/')
            LOGGER.info('%s %d: %s', watershed_basename, fid, workspace_url)
            raster_to_sample_path = os.path.join(
                WATERSHED_WORKSPACE_DIR, 'workspace_worker',
                '%s_%d' % (watershed_basename, fid), 'n_export.tif')

            watershed_workspace_token_path = os.path.join(
                CHURN_DIR, '%s_%d.UNZIPPED' % (watershed_basename, fid))
            download_workspace_task = task_graph.add_task(
                func=download_and_unzip,
                args=(workspace_url, WATERSHED_WORKSPACE_DIR,
                      watershed_workspace_token_path),
                target_path_list=[watershed_workspace_token_path],
                task_name='download and unzip watersheds')

            buffer_vector_path = os.path.join(
                os.path.dirname(raster_to_sample_path), 'buffer.gpkg')
            create_local_buffer_region_task = task_graph.add_task(
                func=create_local_buffer_region,
                args=(raster_to_sample_path, point_geom.ExportToWkt(),
                      buffer_vector_path),
                dependent_task_list=[download_workspace_task],
                target_path_list=[buffer_vector_path])
            create_local_buffer_region_task.join()
            break
        target_layer.CreateFeature(feature)
        feature = None

    connection.commit()
    connection.close()


if __name__ == '__main__':
    for dir_path in [
            WORKSPACE_DIR, ECOSHARD_DIR, CHURN_DIR, WATERSHED_WORKSPACE_DIR]:
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
    target_sample_point_path = '%s.gpkg' % os.path.join(
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
