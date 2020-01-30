"""Stitch raster from pre-calculated watersheds."""
import glob
import logging
import os
import sys

from osgeo import gdal
from osgeo import osr
import numpy
import pygeoprocessing
import shapely.geometry
import shapely.strtree
import shapely.wkt
import taskgraph
import taskgraph_downloader_pnn

WORKSPACE_DIR = 'nci_stitcher_workspace'
ECOSHARD_DIR = os.path.join(WORKSPACE_DIR, 'ecoshard')
WARP_DIR = os.path.join(WORKSPACE_DIR, 'warped_rasters')

AWS_BASE_URL = (
    'https://nci-ecoshards.s3-us-west-1.amazonaws.com/watershed_workspaces/')

WATERSHEDS_URL = (
    'https://nci-ecoshards.s3-us-west-1.amazonaws.com/'
    'watersheds_globe_HydroSHEDS_15arcseconds_'
    'blake2b_14ac9c77d2076d51b0258fd94d9378d4.zip')

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.FileHandler('%s_log.txt' % __name__))
WGS84_CELL_SIZE = (0.002, -0.002)
GLOBAL_NODATA_VAL = -1


def build_strtree(vector_path_pattern):
    """Build an rtree that generates geom and preped geometry.

    Parameters:
        vector_path_pattern (str): path pattern to a path to vector of geometry
            to build into r tree.

    Returns:
        strtree.STRtree object that will return shapely geometry objects
            with a .prep field that is prepared geomtry for fast testing,
            a .geom field that is the base gdal geometry, and a field_val_map
            field that contains the 'fieldname'->value pairs from the original
            vector. The main object will also have a `field_name_type_list`
            field which contains original fieldname/field type pairs

    """
    geometry_list = []
    for vector_path in glob.glob(vector_path_pattern):
        basename = os.path.splitext(os.path.basename(vector_path))[0]
        vector = gdal.OpenEx(vector_path, gdal.OF_VECTOR)
        layer = vector.GetLayer()
        layer_defn = layer.GetLayerDefn()
        field_name_type_list = []
        for index in range(layer_defn.GetFieldCount()):
            field_name = layer_defn.GetFieldDefn(index).GetName()
            field_type = layer_defn.GetFieldDefn(index).GetType()
            field_name_type_list.append((field_name, field_type))

        LOGGER.debug('loop through features for rtree')
        for index, feature in enumerate(layer):
            if index % 10000 == 0:
                LOGGER.debug(
                    '%.2f%% complete in %s',
                    100.0 * index/layer.GetFeatureCount(),
                    vector_path)
            feature_geom = feature.GetGeometryRef()
            feature_geom_shapely = shapely.wkb.loads(feature_geom.ExportToWkb())
            #feature_geom_shapely.prep = shapely.prepared.prep(feature_geom_shapely)
            feature_geom_shapely.field_val_map = {}
            for field_name, _ in field_name_type_list:
                feature_geom_shapely.field_val_map[field_name] = (
                    feature.GetField(field_name))
            feature_geom_shapely.field_val_map['BASENAME'] = basename
            geometry_list.append(feature_geom_shapely)
    LOGGER.debug('constructing the tree')
    r_tree = shapely.strtree.STRtree(geometry_list)
    LOGGER.debug('all done')
    r_tree.field_name_type_list = field_name_type_list
    return r_tree


def make_empty_wgs84_raster(
        cell_size, nodata_value, target_datatype, target_raster_path,
        target_token_complete_path, center_point=None, buffer_range=None):
    """Make a big empty raster in WGS84 projection.

    Parameters:
        cell_size (float): this is the desired cell size in WSG84 degree
            units.
        nodata_value (float): desired nodata avlue of target raster
        target_datatype (gdal enumerated type): desired target datatype.
        target_raster_path (str): this is the target raster that will cover
            [-180, 180), [90, -90) with cell size units with y direction being
            negative.
        target_token_complete_path (str): this file is created if the
            mosaic to target is successful. Useful for taskgraph task
            scheduling.
        center_point (tuple): if not None, this is the center point to start
            an AOI around.
        buffer_range (float): if `center_point` is not null contains the degree
            buffer to build around `center_point` for a square AOI.

    Returns:
        None.

    """
    gtiff_driver = gdal.GetDriverByName('GTiff')
    try:
        os.makedirs(os.path.dirname(target_raster_path))
    except OSError:
        pass

    if not center_point:
        n_cols = int(abs(360.0 / cell_size[0]))
        n_rows = int(abs(180.0 / cell_size[1]))
        geotransform = (-180.0, cell_size[0], 0.0, 90.0, 0, cell_size[1])
    else:
        n_cols = int(abs(2*buffer_range / cell_size[0]))
        n_rows = int(abs(2*buffer_range / cell_size[1]))
        geotransform = (
            center_point[0]-buffer_range, cell_size[0],
            0.0, center_point[1]+buffer_range, 0, cell_size[1])

    target_raster = gtiff_driver.Create(
        target_raster_path, n_cols, n_rows, 1, target_datatype,
        options=(
            'TILED=YES', 'BIGTIFF=YES', 'BLOCKXSIZE=256', 'BLOCKYSIZE=256',
            'COMPRESS=LZW', 'SPARSE_OK=TRUE'))
    wgs84_sr = osr.SpatialReference()
    wgs84_sr.ImportFromEPSG(4326)
    target_raster.SetProjection(wgs84_sr.ExportToWkt())
    target_raster.SetGeoTransform(geotransform)
    target_band = target_raster.GetRasterBand(1)
    target_band.SetNoDataValue(nodata_value)
    target_band.Fill(nodata_value)
    target_band = None
    target_raster = None

    target_raster = gdal.OpenEx(target_raster_path, gdal.OF_RASTER)
    if target_raster:
        with open(target_token_complete_path, 'w') as target_token_file:
            target_token_file.write('complete!')


if __name__ == '__main__':
    try:
        os.makedirs(WARP_DIR)
    except OSError:
        pass

    task_graph = taskgraph.TaskGraph(WORKSPACE_DIR, -1)
    tdd_downloader = taskgraph_downloader_pnn.TaskGraphDownloader(
        ECOSHARD_DIR, task_graph)

    tdd_downloader.download_ecoshard(
        WATERSHEDS_URL, 'watersheds', decompress='unzip',
        local_path='watersheds_globe_HydroSHEDS_15arcseconds')

    raster_path_base_list = [
        'n_export.tif',
        'intermediate_outputs/modified_load_n.tif',
        'intermediate_outputs/stream.tif',
        ]
    query_point = shapely.geometry.Point(-117, 38)
    buffer_range = 1.0
    global_raster_info_map = {}
    for raster_path_pattern in raster_path_base_list:
        global_raster_path = os.path.join(
            WORKSPACE_DIR, '%s_stitch%s' % os.path.splitext(os.path.basename(
                raster_path_pattern)))
        target_token_complete_path = '%s.INITALIZED' % os.path.splitext(
            global_raster_path)[0]
        make_empty_task = task_graph.add_task(
            func=make_empty_wgs84_raster,
            args=(
                WGS84_CELL_SIZE, GLOBAL_NODATA_VAL, gdal.GDT_Float32,
                global_raster_path, target_token_complete_path),
            kwargs={
                'center_point': (query_point.x, query_point.y),
                'buffer_range': buffer_range,
            },
            target_path_list=[target_token_complete_path],
            task_name='make empty %s' % os.path.basename(raster_path_pattern))
        make_empty_task.join()
        global_raster_info_map[raster_path_pattern] = (
            gdal.OpenEx(global_raster_path, gdal.OF_RASTER | gdal.GA_Update),
            pygeoprocessing.get_raster_info(global_raster_path))

    watershed_strtree = build_strtree(
        os.path.join(tdd_downloader.get_path('watersheds'), 'na*.shp'))
    aoi = shapely.geometry.box(
        query_point.x - buffer_range,
        query_point.y - buffer_range,
        query_point.x + buffer_range,
        query_point.y + buffer_range)
    query_point.buffer(buffer_range)
    for watershed_object in watershed_strtree.query(aoi):
        basin_id = watershed_object.field_val_map['BASIN_ID']
        basename = watershed_object.field_val_map['BASENAME']
        watershed_id = '%s_%d' % (basename, basin_id-1)
        LOGGER.debug(watershed_id)
        tdd_downloader.download_ecoshard(
            os.path.join(AWS_BASE_URL, '%s.zip' % watershed_id),
            watershed_id, decompress='unzip',
            local_path='workspace_worker/%s' % watershed_id)

        for raster_subpath in raster_path_base_list:
            global_raster, global_raster_info = global_raster_info_map[
                raster_subpath]
            global_band = global_raster.GetRasterBand(1)
            global_inv_gt = gdal.InvGeoTransform(
                global_raster_info['geotransform'])
            stitch_raster_path = os.path.join(
                tdd_downloader.get_path(watershed_id), raster_subpath)
            stitch_raster_info = pygeoprocessing.get_raster_info(
                stitch_raster_path)
            warp_raster_path = os.path.join(
                WARP_DIR, '%s_%s' % (
                    watershed_id, os.path.basename(stitch_raster_path)))

            warp_task = task_graph.add_task(
                func=pygeoprocessing.warp_raster,
                args=(
                    stitch_raster_path, global_raster_info['pixel_size'],
                    warp_raster_path, 'near'),
                kwargs={'target_sr_wkt': global_raster_info['projection']},
                target_path_list=[warp_raster_path],
                task_name='warp %s' % stitch_raster_path)
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
            if (global_i_min >= global_raster.RasterXSize or
                    global_j_min >= global_raster.RasterYSize or
                    global_i_max < 0 or global_j_max < 0):
                LOGGER.debug(stitch_raster_info)
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

            stitch_nodata = stitch_raster_info['nodata'][0]
            global_nodata = global_raster_info['nodata'][0]

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

    task_graph.join()
    task_graph.close()
