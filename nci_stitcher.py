"""Stitch raster from pre-calculated watersheds."""
import logging
import os
import sys

from osgeo import gdal
from osgeo import osr
import taskgraph

WORKSPACE_DIR = 'nci_stitcher_workspace'

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.FileHandler('%s_log.txt' % __name__))
WGS84_CELL_SIZE = (0.008333, -0.008333)
GLOBAL_NODATA_VAL = -1

def make_empty_wgs84_raster(
        cell_size, nodata_value, target_datatype, target_raster_path,
        target_token_complete_path):
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

    Returns:
        None.

    """
    gtiff_driver = gdal.GetDriverByName('GTiff')
    try:
        os.makedirs(os.path.dirname(target_raster_path))
    except OSError:
        pass

    n_cols = int(360.0 / cell_size)
    n_rows = int(180.0 / cell_size)

    geotransform = (-180.0, cell_size, 0.0, 90.0, 0, -cell_size)

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
    target_band = None
    target_raster = None

    target_raster = gdal.OpenEx(target_raster_path, gdal.OF_RASTER)
    if target_raster:
        with open(target_token_complete_path, 'w') as target_token_file:
            target_token_file.write('complete!')


if __name__ == '__main__':
    task_graph = taskgraph.TaskGraph(WORKSPACE_DIR, -1)
    raster_path_base_list = [
        '[BASENAME]/workspace_worker/[BASENAME]_[FID}/n_export.tif',
        '[BASENAME]/workspace_worker/[BASENAME]_[FID}/intermediate_outputs/modified_load_n.tif',
        '[BASENAME]/workspace_worker/[BASENAME]_[FID}/intermediate_outputs/stream.tif',
        ]
    for raster_path_pattern in raster_path_base_list:
        global_raster_path = os.path.join(WORKSPACE_DIR, os.path.basename(
            raster_path_pattern))
        target_token_complete_path = '%s.INITALIZED' % os.path.splitext(
            global_raster_path)[0]
        task_graph.add_task(
            func=make_empty_wgs84_raster,
            args=(
                WGS84_CELL_SIZE, GLOBAL_NODATA_VAL, gdal.GDT_Float32,
                global_raster_path, target_token_complete_path),
            target_path_list=[target_token_complete_path],
            task_name='make empty %s' % os.path.basename(raster_path_pattern))



    task_graph.join()
    task_graph.close()