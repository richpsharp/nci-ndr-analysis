"""NCI Special Scenario Generator for Peter's masks."""
import logging
import glob
import os
import multiprocessing
import shutil
import subprocess
import sys

from osgeo import gdal
from osgeo import ogr
from osgeo import osr
import numpy
import pandas
import pygeoprocessing
import taskgraph
import zipfile

# set a 512MB limit for the cache
gdal.SetCacheMax(2**29)

WORKSPACE_DIR = 'nci_peter_mask_workspaces'
ECOSHARD_DIR = os.path.join(WORKSPACE_DIR, 'ecoshard')
CHURN_DIR = os.path.join(WORKSPACE_DIR, 'churn')

SLOPE_THRESHOLD_PATH = os.path.join('data', 'jamie_slope_thresholds.csv')

LOGGER = logging.getLogger(__name__)

LULC_WATER = 210

GLOBAL_DEM_URI = (
    'gs://global-invest-sdr-data/'
    'global_dem_3s_md5_22d0c3809af491fa09d03002bdf09748.zip')

GLOBAL_STREAMS_URI = (
    'gs://shared-with-users/'
    'global_streams_from_ndr_md5_d41aa48e92005fe79287ae4a66efb412.tif')

BASE_LULC_RASTER_URI = (
    'gs://critical-natural-capital-ecoshards/'
    'ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_'
    'md5_1254d25f937e6d9bdee5779d377c5aa4.tif')

GLOBAL_COUNTRY_URI = (
    'gs://critical-natural-capital-ecoshards/realized_service_ecoshards/'
    'countries_iso3_md5_6fb2431e911401992e6e56ddf0a9bcda.gpkg')

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(processName)s %(levelname)s '
        '%(name)s [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
logging.getLogger('taskgraph').setLevel(logging.INFO)


def build_vrt(vrt_raster_path, raster_path_list):
    """Build a VRT and capture the result so it doesn't raise an exception."""
    _ = gdal.BuildVRT(vrt_raster_path, raster_path_list)
    return None


def mult_op(base_array, base_nodata, factor_array, target_nodata):
    """Multipy base by a constant factor array."""
    result = numpy.empty(base_array.shape, dtype=numpy.float32)
    result[:] = target_nodata
    valid_mask = ~numpy.isclose(base_array, base_nodata)
    result[valid_mask] = base_array[valid_mask] * factor_array[valid_mask]
    return result


def gs_copy(uri_path, target_path):
    """Use gsutil to copy local file."""
    subprocess.run([
        f'gsutil cp "{uri_path}" "{target_path}"'],
        shell=True, check=True)


def mask_by_array_op(base_array, mask_array, base_nodata, target_nodata):
    """Mask base by a mask array where it's 1 base goes to 0."""
    result = base_array.copy()
    result[mask_array == 1] = 0
    return result


def mask_out_raster(base_raster_path, mask_raster_path, target_raster_path):
    """Pass."""
    raster_info = pygeoprocessing.get_raster_info(base_raster_path)
    pygeoprocessing.raster_calculator(
        [(base_raster_path, 1), (mask_raster_path, 1),
         (raster_info['nodata'][0], 'raw'), (255, 'raw')],
        mask_by_array_op, target_raster_path, gdal.GDT_Byte, 255)


def threshold_value_op(
        base_array, threshold_value, base_nodata, target_nodata):
    """If base > threshold, set to 0."""
    result = numpy.empty(base_array.shape, dtype=numpy.uint8)
    result[:] = target_nodata
    valid_mask = ~numpy.isclose(base_array, base_nodata)
    result[valid_mask] = base_array[valid_mask] <= threshold_value
    return result


def threshold_by_value(
        base_raster_path, threshold_value, target_raster_path):
    """If base <= threshold, then 1, otherwise 0."""
    base_raster_info = pygeoprocessing.get_raster_info(base_raster_path)
    target_nodata = 255

    pygeoprocessing.raster_calculator(
        [(base_raster_path, 1),
         (threshold_value, 'raw'),
         (base_raster_info['nodata'][0], 'raw'),
         (target_nodata, 'raw')], threshold_value_op, target_raster_path,
        gdal.GDT_Byte, target_nodata)


def threshold_array_op(
        base_array, threshold_array, base_nodata, threshold_nodata,
        target_nodata):
    """Threshold base array by the threshold array."""
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

    align_id = (
        f'{os.path.splitext(os.path.basename(base_raster_path))[0]}_'
        f'{os.path.splitext(os.path.basename(threshold_raster_path))[0]}')

    # align rasters
    align_raster_path_list = [
        os.path.join(
            CHURN_DIR,
            f'aligned_{align_id}_'
            f'{os.path.basename(os.path.splitext(path)[0])}.tif')
        for path in [base_raster_path, threshold_raster_path]]

    pygeoprocessing.align_and_resize_raster_stack(
        [base_raster_path, threshold_raster_path], align_raster_path_list,
        ['near']*2, base_raster_info['pixel_size'], 'intersection')

    pygeoprocessing.raster_calculator(
        [(align_raster_path_list[0], 1), (align_raster_path_list[1], 1),
         (base_raster_info['nodata'][0], 'raw'),
         (threshold_raster_info['nodata'][0], 'raw'),
         (target_nodata, 'raw')], threshold_array_op, target_raster_path,
        gdal.GDT_Byte, target_nodata)


def mask_op(base_array, value, nodata, target_nodata):
    """Mask array by specific value."""
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
        (base_raster_path, 1), (kernel_raster_path, 1), target_raster_path,
        target_datatype=gdal.GDT_Byte,
        target_nodata=255)


def mask_raster(base_raster_path, code_to_mask, target_raster_path):
    """Make 0/1 mask if base matches code."""
    base_info = pygeoprocessing.get_raster_info(base_raster_path)
    target_nodata = 255
    pygeoprocessing.raster_calculator(
        [(base_raster_path, 1), (code_to_mask, 'raw'),
         (base_info['nodata'][0], 'raw'), (target_nodata, 'raw')],
        mask_op, target_raster_path, gdal.GDT_Byte, target_nodata)


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
    shutil.copy(base_vector_path, target_vector_path)
    vector = gdal.OpenEx(target_vector_path, gdal.OF_VECTOR | gdal.GA_Update)
    layer = vector.GetLayer()
    layer.CreateField(ogr.FieldDefn(target_field, ogr.OFTReal))
    layer.SyncToDisk()

    for feature in layer:
        field_value = feature.GetField(index_field)
        if field_value in index_to_value_map:
            value = index_to_value_map[field_value]
            feature.SetField(target_field, value)
            layer.SetFeature(feature)
        else:
            feature.SetField(target_field, 10.0)
        layer.SetFeature(feature)
        feature = None
    layer = None
    vector = None


def unzip(zipfile_path, target_dir):
    """Unzip file to target dir."""
    with zipfile.ZipFile(zipfile_path, 'r') as zip_ref:
        zip_ref.extractall(target_dir)


def degrees_per_meter(max_lat, min_lat, n_pixels):
    """Calculate degrees/meter for a range of latitudes.

    Create a 1D array ranging from "max_lat" to "min_lat" where each element
    contains an average degrees per meter that maps to that lat value.

    Adapted from: https://gis.stackexchange.com/a/127327/2397

    Args:
        max_lat (float): max lat for first element
        min_lat (float): min lat non-inclusive for last element
        n_pixels (int): number of elements in target array

    Returns:
        Area of square pixel of side length `pixel_size_in_degrees` centered at
        `center_lat` in m^2.

    """
    m1 = 111132.92
    m2 = -559.82
    m3 = 1.175
    m4 = -0.0023
    p1 = 111412.84
    p2 = -93.5
    p3 = 0.118

    pixel_length_array = numpy.empty(n_pixels)
    for index, lat_deg in enumerate(numpy.linspace(
            max_lat, min_lat, num=n_pixels, endpoint=False)):
        lat = lat_deg * numpy.pi / 180

        lat_mpd = (
            m1+(m2*numpy.cos(2*lat))+(m3*numpy.cos(4*lat)) +
            (m4*numpy.cos(6*lat)))
        lng_mpd = (
            (p1*numpy.cos(lat))+(p2*numpy.cos(3*lat)) + (p3*numpy.cos(5*lat)))

        pixel_length_array[index] = numpy.sqrt(lat_mpd*lng_mpd)

    return pixel_length_array


def main():
    """Entry point."""
    dem_zip_dir = os.path.join(CHURN_DIR, 'dem')
    for dir_path in [WORKSPACE_DIR, ECOSHARD_DIR, CHURN_DIR, dem_zip_dir]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    task_graph = taskgraph.TaskGraph(
        WORKSPACE_DIR, -1) #multiprocessing.cpu_count(), 5.0)

    stream_raster_path = os.path.join(
        ECOSHARD_DIR, os.path.basename(GLOBAL_STREAMS_URI))
    base_lulc_raster_path = os.path.join(
        ECOSHARD_DIR, os.path.basename(BASE_LULC_RASTER_URI))
    global_country_vector_path = os.path.join(
        ECOSHARD_DIR, os.path.basename(GLOBAL_COUNTRY_URI))

    dem_zip_path = os.path.join(
        ECOSHARD_DIR, os.path.basename(GLOBAL_DEM_URI))
    dem_dir_path = os.path.join(dem_zip_dir, 'global_dem_3s')

    for raster_path, ecoshard_uri in [
            (stream_raster_path, GLOBAL_STREAMS_URI),
            (base_lulc_raster_path, BASE_LULC_RASTER_URI),
            (global_country_vector_path, GLOBAL_COUNTRY_URI),
            (dem_zip_path, GLOBAL_DEM_URI)]:
        task_graph.add_task(
            func=gs_copy,
            args=(ecoshard_uri, raster_path),
            target_path_list=[raster_path],
            task_name=f'download {os.path.basename(raster_path)}')

    task_graph.join()

    # extract dem:
    unzip_dem_task = task_graph.add_task(
        func=unzip,
        args=(dem_zip_path, dem_zip_dir),
        task_name='unzip dem')

    # create VRT
    dem_vrt_raster_path = os.path.join(dem_dir_path, 'dem.vrt')
    build_vrt_task = task_graph.add_task(
        func=build_vrt,
        args=(dem_vrt_raster_path, glob.glob(
            os.path.join(dem_dir_path, '*.tif'))),
        target_path_list=[dem_vrt_raster_path],
        dependent_task_list=[unzip_dem_task],
        task_name='build vrt')

    build_vrt_task.join()
    vrt_info = pygeoprocessing.get_raster_info(dem_vrt_raster_path)

    # do a clip first
    clipped_vrt_raster_path = os.path.join(CHURN_DIR, 'clipped_dem.tif')
    pygeoprocessing.warp_raster(
        dem_vrt_raster_path, vrt_info['pixel_size'],
        clipped_vrt_raster_path, 'near', target_bb=[-122, 32, -126, 36],
        n_threads=multiprocessing.cpu_count())
    dem_vrt_raster_path = clipped_vrt_raster_path
    vrt_info = pygeoprocessing.get_raster_info(dem_vrt_raster_path)
    vrt_bb = vrt_info['bounding_box']
    n_cols, n_rows = vrt_info['raster_size']

    # create meters to degree array
    dpm_task = task_graph.add_task(
        func=degrees_per_meter,
        args=(vrt_bb[3], vrt_bb[1], n_rows),
        task_name='degrees_per_meter array')

    dem_in_degrees_raster_path = os.path.join(
        CHURN_DIR, 'dem_in_degrees.tif')
    dem_vrt_nodata = -9999
    lng_m_to_d_array = dpm_task.get()
    dem_to_degrees = task_graph.add_task(
        func=pygeoprocessing.raster_calculator,
        args=(
            [(dem_vrt_raster_path, 1), (dem_vrt_nodata, 'raw'),
             lng_m_to_d_array[:, None], (vrt_info['nodata'][0], 'raw')],
            mult_op, dem_in_degrees_raster_path, gdal.GDT_Float32,
            dem_vrt_nodata),
        target_path_list=[dem_in_degrees_raster_path],
        task_name='convert dem to degrees')

    slope_raster_path = os.path.join(CHURN_DIR, 'slope.tif')
    slope_task = task_graph.add_task(
        func=pygeoprocessing.calculate_slope,
        args=((dem_in_degrees_raster_path, 1), slope_raster_path),
        dependent_task_list=[dem_to_degrees],
        target_path_list=[slope_raster_path],
        task_name='calc slope')

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
              gdal.GDT_Float32, [-1]),
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
        kwargs={'option_list': ['ATTRIBUTE=max_slope']},
        dependent_task_list=[
            max_slope_task, create_empty_threshold_raster_task],
        target_path_list=[slope_threshold_raster_path],
        task_name='rasterize max slope')

    rasterize_slope_threshold_task.join()

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
        dependent_task_list=[water_body_mask_task],
        target_path_list=[extended_water_body_raster_path],
        task_name='extend water body mask')

    lulc_info = pygeoprocessing.get_raster_info(base_lulc_raster_path)

    # create resampled stream raster
    low_res_stream_raster_path = os.path.join(CHURN_DIR, 'low_res_streams.tif')
    low_res_stream_task = task_graph.add_task(
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
        dependent_task_list=[slope_task],
        target_path_list=[max_slope_raster_path],
        task_name='max slope')

    # create "average" resampled slope raster
    average_slope_raster_path = os.path.join(CHURN_DIR, 'average_slope.tif')
    average_slope_task = task_graph.add_task(
        func=pygeoprocessing.warp_raster,
        args=(
            slope_raster_path, lulc_info['pixel_size'],
            average_slope_raster_path, 'average'),
        dependent_task_list=[slope_task],
        target_path_list=[average_slope_raster_path],
        task_name='average slope')

    # use "slope threshold raster path"
    ag_expansion_slope_max_exclusion_mask_path = os.path.join(
        WORKSPACE_DIR, 'ag_expansion_slope_max_exclusion_mask.tif')
    ag_expansion_slope_avg_exclusion_mask_path = os.path.join(
        WORKSPACE_DIR, 'ag_expansion_slope_avg_exclusion_mask.tif')

    for _slope_raster_path, _target_raster_path in [
            (max_slope_raster_path,
             ag_expansion_slope_max_exclusion_mask_path),
            (average_slope_raster_path,
             ag_expansion_slope_avg_exclusion_mask_path)]:
        task_graph.add_task(
            func=threshold_by_raster,
            args=(
                _slope_raster_path, slope_threshold_raster_path,
                _target_raster_path),
            target_path_list=[_target_raster_path],
            dependent_task_list=[
                rasterize_slope_threshold_task, max_slope_task,
                average_slope_task],
            task_name=(
                f'threshold raster {os.path.basename(_target_raster_path)}'))

    # use 10% slope cutoff
    ag_intensification_slope_max_exclusion_mask_path = os.path.join(
        WORKSPACE_DIR, 'ag_intensification_slope_max_exclusion_mask.tif')
    ag_intensification_slope_avg_exclusion_mask_path = os.path.join(
        WORKSPACE_DIR, 'ag_intensification_slope_avg_exclusion_mask.tif')

    for _slope_raster_path, target_raster_path in [
            (max_slope_raster_path,
             ag_intensification_slope_max_exclusion_mask_path),
            (average_slope_raster_path,
             ag_intensification_slope_avg_exclusion_mask_path)]:
        task_graph.add_task(
            func=threshold_by_value,
            args=(
                _slope_raster_path, 10.0, target_raster_path),
            target_path_list=[target_raster_path],
            dependent_task_list=[max_slope_task, average_slope_task],
            task_name=(
                f'threshold raster {os.path.basename(target_raster_path)}'))

    # set anywhere landcode is water body back to zero
    riparian_buffer_raster_path = os.path.join(
        WORKSPACE_DIR, 'riparian_buffer_mask.tif')

    task_graph.add_task(
        func=mask_out_raster,
        args=(
            low_res_stream_raster_path, water_body_mask_raster_path,
            riparian_buffer_raster_path),
        dependent_task_list=[low_res_stream_task, water_body_mask_task],
        target_path_list=[riparian_buffer_raster_path],
        task_name='riparian buffer mask out')

    task_graph.close()
    task_graph.join()


if __name__ == '__main__':
    main()
