"""NCI Special Scenario Generator for Peter's masks."""
import logging
import os
import subprocess
import sys

from osgeo import gdal
from osgeo import ogr
import ecoshard
import pandas
import pygeoprocessing
import taskgraph

WORKSPACE_DIR = 'nci_peter_mask_workspaces'
ECOSHARD_DIR = os.path.join(WORKSPACE_DIR, 'ecoshard')
CHURN_DIR = os.path.join(WORKSPACE_DIR, 'churn')

SLOPE_THRESHOLD_PATH = os.path.join('data', 'jamie_slope_thresholds.csv')

LOGGER = logging.getLogger(__name__)

LULC_WATER = 210

GLOBAL_SLOPE_URI = 'gs://ecoshard-root/topo_variables/global_slope_3s.tif'

GLOBAL_STREAMS_URI = (
    'gs://shared-with-users/'
    'global_streams_from_ndr_md5_d41aa48e92005fe79287ae4a66efb412.tif')

BASE_LULC_RASTER_URI = (
    'gs://critical-natural-capital-ecoshards/'
    'ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_'
    'md5_1254d25f937e6d9bdee5779d377c5aa4.tif')

GLOBAL_COUNTRY_URI = (
    'gs://critical-natural-capital-ecoshards/realized_service_ecoshards/'
    'by_country/'
    'countries_singlepart_md5_b7aaa5bc55cefc1f28d9655629c2c702.gpkg')

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(processName)s %(levelname)s '
        '%(name)s [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)


def gs_copy(uri_path, target_path):
    """Use gsutil to copy local file."""
    subprocess.run([
        f'gsutil cp "{uri_path}" "{target_path}"'],
        shell=True, check=True)


def threshold_value_op(
        base_array, threshold_value, base_nodata, target_nodata):
    result = numpy.empty(base_array.shape, dtype=numpy.uint8)
    result[:] = target_nodata
    valid_mask = ~numpy.isclose(base_array, base_nodata)
    result[valid_mask] = base_array[valid_mask] <= threshold_value
    return result


def threshold_by_value(
        base_raster_path, threshold_value, target_raster_path):
    """If base <= threshold, then 1, otherwise 0."""
    base_raster_info = pygeoprocessing.get_raster_info(base_raster_path)
    threshold_raster_info = pygeoprocessing.get_raster_info(
        threshold_raster_path)
    target_nodata = 255

    pygeoprocessing.raster_calculator(
        [(base_raster_path, 1),
         (threshold_value, 'raw'),
         (base_raster_info['nodata'][0], 'raw'),
         (target_nodata, 'raw')], threshold_value_op, gdal.GDT_Byte,
        target_nodata)


def threshold_array_op(
        base_array, threshold_array, base_nodata, threshold_nodata,
        target_nodata):
    result = numpy.empty(base_array.shape, dtype=numpy.uint8)
    result[:] = target_nodata
    valid_mask = (
        ~numpy.isclose(base_array, base_nodata) &
        ~numpy.isclose(threshold_array, threshold_nodata))
    result[valid_mask] = base_array[valid_mask] <= threshold_array[valid_mask]
    return result


def threshold_by_raster(
        base_raster_path, threshold_raster_path, target_raster_path):
    """If base <= threshold, then 1, otherwise 0."""
    base_raster_info = pygeoprocessing.get_raster_info(base_raster_path)
    threshold_raster_info = pygeoprocessing.get_raster_info(
        threshold_raster_path)
    target_nodata = 255

    pygeoprocessing.raster_calculator(
        [(base_raster_path, 1), (threshold_raster_path, 1),
         (base_raster_info['nodata'][0], 'raw'),
         (threshold_raster_info['nodata'][0], 'raw'),
         (target_nodata, 'raw')], threshold_array_op, gdal.GDT_Byte,
         target_nodata)


def mask_op(base_array, value, nodata, target_nodata):
    result = numpy.empty(base_array.shape, dtype=numpy.uint8)
    result[:] = target_nodata
    valid_mask = base_array != nodata
    result[(base_array == value) & valid_mask] = 1
    result[(base_array != value) & valid_mask] = 0
    return result


def erode_one_pixel(base_raster_path, target_raster_path):
    """Take base and erode out one pixel."""
    kernel_raster_path = os.path.join(
        os.path.dirname(target_raster_path), 'kernel.tif')
    gtiff_driver = gdal.GetDriverByName('GTiff')
    kernel_raster = gtiff_driver.Create(
        kernel_raster_path, 3, 3, 1, gdal.GDT_Float32)

    # Make some kind of geotransform, it doesn't matter what but
    # will make GIS libraries behave better if it's all defined
    kernel_raster.SetGeoTransform([0, 1, 0, 0, 0, -1])
    srs = osr.SpatialReference()
    srs.SetWellKnownGeogCS('WGS84')
    kernel_raster.SetProjection(srs.ExportToWkt())

    kernel_band = kernel_raster.GetRasterBand(1)
    kernel_band.SetNoDataValue(-1)
    kernel_band.Fill(1)
    kernel_band = None
    kernel_raster = None

    pygeoprocessing.convolve_2d(
        (base_raster_path, 1), (kernel_raster_path, 1), target_path,
        target_datatype=gdal.GDT_Byte,
        target_nodata=255)


def mask_raster(base_raster_path, code_to_mask, target_raster_path):
    """Make 0/1 mask if base matches code."""
    base_info = pygeoprocessing.get_raster_info(base_raster_path)
    target_nodata = 255
    pygeoprocessing.raster_calculator(
        [(base_raster_path, 1), (code_to_mask, 'raw'),
         (base_info['nodata'][0], 'raw'), (target_nodata, 'raw')],
        mask_op, gdal.GDT_Byte, target_nodata)


def modify_vector(
        base_vector_path, index_field, index_to_value_map, target_field,
        target_vector_path):
    """Copy base to target and add a new field with values from map.

    Args:
        base_vector_path (str): path to base vector
        index_field (str): field that indexes a feature into value map
        index_to_value_map (str): maps feature to the value we'll add
        target_field (str): new float field to add to the layer
        target_vector_path (str): path to new target vector.

    Returns:
        None.

    """
    os.copy(base_vector_path, target_vector_path)
    vector = gdal.OpenEx(target_vector_path, gdal.OF_VECTOR | gdal.GA_Update)
    layer = vector.GetLayer()
    layer.CreateField(ogr.FieldDefn(target_field, ogr.OFTReal))
    for feature in layer:
        value = index_to_value_map[feature.GetField(index_field)]
        feature.SetField(target_field, value)
        layer.SetFeature(feature)


def main():
    """Entry point."""
    for dir_path in [WORKSPACE_DIR, ECOSHARD_DIR, CHURN_DIR]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    task_graph = taskgraph.TaskGraph(WORKSPACE_DIR, -1)

    slope_raster_path = os.path.join(
        ECOSHARD_DIR, os.path.basename(GLOBAL_SLOPE_URI))
    stream_raster_path = os.path.join(
        ECOSHARD_DIR, os.path.basename(GLOBAL_STREAMS_URI))
    base_lulc_raster_path = os.path.join(
        ECOSHARD_DIR, os.path.basename(BASE_LULC_RASTER_URI))
    global_country_vector_path = os.path.join(
        ECOSHARD_DIR, os.path.basename(GLOBAL_COUNTRY_URI))

    for raster_path, ecoshard_uri in [
            (slope_raster_path, GLOBAL_SLOPE_URI),
            (stream_raster_path, GLOBAL_STREAMS_URI),
            (base_lulc_raster_path, BASE_LULC_RASTER_URI),
            (global_country_vector_path, GLOBAL_COUNTRY_URI)]:
        task_graph.add_task(
            func=gs_copy,
            args=(ecoshard_uri, raster_path),
            target_path_list=[raster_path],
            task_name=f'download {os.path.basename(raster_path)}')
    task_graph.join()

    slope_threshold_df = pandas.read_csv(SLOPE_THRESHOLD_PATH)
    slope_threshold_map = {
        iso3: float(val) for iso3, val in zip(
            slope_threshold_df['gdam'], slope_threshold_df['slope_limit'])
    }
    print(slope_threshold_map)

    slope_threshold_raster_path = os.path.join(
        CHURN_DIR, 'slope_threshold_jamie.tif')
    create_empty_threshold_raster_task = task_graph.add_task(
        func=pygeoprocessing.new_raster_from_base,
        args=(base_lulc_raster_path, slope_threshold_raster_path,
              gdal.GDT_Byte, [255]),
        hash_target_files=False,
        target_path_list=[slope_threshold_raster_path],
        task_name='new slope slope_threshold_raster_path')

    max_slope_vector_path = os.path.join(CHURN_DIR, 'max_slope.gpkg')
    max_slope_task = task_graph.add_task(
        func=modify_vector,
        args=(global_country_vector_path, 'iso3',
              slope_threshold_map, 'max_slope', max_slope_vector_path),
        target_path_list=[max_slope_vector_path],
        task_name='adding max slopes')

    rasterize_slope_threshold_task = task_graph.add_task(
        func=pygeoprocessing.rasterize,
        args=(max_slope_vector_path, slope_threshold_raster_path),
        kwargs={'option_list': ['ATTRIBUTE="max_slope"']},
        ignore_path_list=[slope_threshold_raster_path],
        hash_target_files=False,
        dependent_task_list=[
            max_slope_task, create_empty_threshold_raster_task],
        target_path_list=[slope_threshold_raster_path],
        task_name='rasterize max slope')

    # make mask of water bodies
    water_body_mask_raster_path = os.path.join(
        CHURN_DIR, 'water_body_mask.tif')
    water_body_mask_task = task_graph.add_task(
        func=mask_raster,
        args=(base_lulc_raster_path, LULC_WATER, water_body_mask_raster_path),
        target_path_list=[water_body_mask_raster_path],
        task_name='mask water bodies')

    extended_water_body_raster_path = os.path.join(
        CHURN_DIR, 'extended_water_body.tif')
    task_graph.add_task(
        func=erode_one_pixel,
        args=(water_body_mask_raster_path, extended_water_body_raster_path),
        target_path_list=[extended_water_body_raster_path],
        task_name='extend water body mask')

    lulc_info = pygeoprocessing.get_raster_info(base_lulc_raster_path)

    # create resampled stream raster
    low_res_stream_raster_path = os.path.join(CHURN_DIR, 'low_res_streams.tif')
    max_slope_task = task_graph.add_task(
        func=pygeoprocessing.warp_raster,
        args=(
            stream_raster_path, lulc_info['pixel_size'],
            low_res_stream_raster_path, 'max'),
        target_path_list=[low_res_stream_raster_path],
        task_name='low res stream')

    # create "max" resampled slope raster
    max_slope_raster_path = os.path.join(CHURN_DIR, 'max_slope.tif')
    max_slope_task = task_graph.add_task(
        func=pygeoprocessing.warp_raster,
        args=(
            slope_raster_path, lulc_info['pixel_size'], max_slope_raster_path,
            'max'),
        target_path_list=[max_slope_raster_path],
        task_name='max slope')

    # create "average" resampled slope raster
    average_slope_raster_path = os.path.join(CHURN_DIR, 'average_slope.tif')
    average_slope_task = task_graph.add_task(
        func=pygeoprocessing.warp_raster,
        args=(
            slope_raster_path, lulc_info['pixel_size'],
            average_slope_raster_path, 'average'),
        target_path_list=[average_slope_raster_path],
        task_name='average slope')

    # use "slope threshold raster path"
    ag_expansion_slope_max_exclusion_mask_path = os.path.join(
        WORKSPACE_DIR, 'ag_expansion_slope_max_exclusion_mask.tif')
    ag_expansion_slope_avg_exclusion_mask_path = os.path.join(
        WORKSPACE_DIR, 'ag_expansion_slope_avg_exclusion_mask.tif')

    for slope_raster_path, target_raster_path in [
            (max_slope_raster_path,
             ag_expansion_slope_max_exclusion_mask_path),
            (average_slope_raster_path,
             ag_expansion_slope_avg_exclusion_mask_path)]:
        task_graph.add_task(
            func=threshold_by_raster,
            args=(
                slope_raster_path, slope_threshold_raster_path,
                target_raster_path),
            target_raster_path_list=[target_raster_path],
            dependent_task_list=[max_slope_task, average_slope_task],
            task_name=(
                f'threshold raster {os.path.basename(target_raster_path)}'))

    # use 10% slope cutoff
    ag_intensification_slope_max_exclusion_mask_path = os.path.join(
        WORKSPACE_DIR, 'ag_intensification_slope_max_exclusion_mask.tif')
    ag_intensification_slope_avg_exclusion_mask_path = os.path.join(
        WORKSPACE_DIR, 'ag_intensification_slope_avg_exclusion_mask.tif')

    for slope_raster_path, target_raster_path in [
            (max_slope_raster_path,
             ag_intensification_slope_max_exclusion_mask_path),
            (average_slope_raster_path,
             ag_intensification_slope_avg_exclusion_mask_path)]:
        task_graph.add_task(
            func=threshold_by_value,
            args=(
                slope_raster_path, 10.0, target_raster_path),
            target_raster_path_list=[target_raster_path],
            dependent_task_list=[max_slope_task, average_slope_task],
            task_name=\
                f'threshold raster {os.path.basename(target_raster_path)}')

    task_graph.close()
    task_graph.join()


if __name__ == '__main__':
    main()
