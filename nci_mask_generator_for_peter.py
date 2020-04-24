"""NCI Special Scenario Generator for Peter's masks."""
import logging
import os
import sys

from osgeo import gdal
import ecoshard
import pandas
import pygeoprocessing
import taskgraph

WORKSPACE_DIR = 'nci_peter_mask_workspaces'
ECOSHARD_DIR = os.path.join(WORKSPACE_DIR, 'ecoshard')
CHURN_DIR = os.path.join(WORKSPACE_DIR, 'churn')

SLOPE_THRESHOLD_PATH = os.path.join('data', 'jamie_slope_thresholds.csv')

LOGGER = logging.getLogger(__name__)

GLOBAL_SLOPE_URL = (
    'https://storage.googleapis.com/ecoshard-root/topo_variables/'
    'global_slope_3s.tif')
    # 'https://storage.googleapis.com/shared-with-users/topo_variables/'
    # 'global_slope_3s.tif')

GLOBAL_STREAMS_URL = (
    'https://storage.googleapis.com/shared-with-users/'
    'global_streams_from_ndr_md5_d41aa48e92005fe79287ae4a66efb412.tif')

BASE_LULC_RASTER_URL = (
    'https://storage.googleapis.com/critical-natural-capital-ecoshards/'
    'ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7_'
    'md5_1254d25f937e6d9bdee5779d377c5aa4.tif')

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(processName)s %(levelname)s '
        '%(name)s [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)


def main():
    """Entry point."""
    for dir_path in [WORKSPACE_DIR, ECOSHARD_DIR, CHURN_DIR]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    task_graph = taskgraph.TaskGraph(WORKSPACE_DIR, -1)

    slope_raster_path = os.path.join(
        ECOSHARD_DIR, os.path.basename(GLOBAL_SLOPE_URL))
    stream_raster_path = os.path.join(
        ECOSHARD_DIR, os.path.basename(GLOBAL_STREAMS_URL))
    base_lulc_raster_path = os.path.join(
        ECOSHARD_DIR, os.path.basename(BASE_LULC_RASTER_URL))

    for raster_path, ecoshard_url in [
            (slope_raster_path, GLOBAL_SLOPE_URL),
            (stream_raster_path, GLOBAL_STREAMS_URL),
            (base_lulc_raster_path, BASE_LULC_RASTER_URL)]:
        task_graph.add_task(
            func=ecoshard.download_url,
            args=(ecoshard_url, raster_path),
            target_path_list=[raster_path],
            task_name=f'download {os.path.basename(raster_path)}')

    slope_threshold_df = pandas.read_csv(SLOPE_THRESHOLD_PATH)
    slope_threshold_map = {
        iso3: float(val) for iso3, val in zip(
            slope_threshold_df['gdam'], slope_threshold_df['slope_limit'])
    }
    print(slope_threshold_map)

    slope_threshold_raster_path = os.path.join(
        CHURN_DIR, 'slope_threshold_jamie.tif')
    task_graph.add_task(
        func=pygeoprocessing.new_raster_from_base,
        args=(base_lulc_raster_path, slope_threshold_raster_path,
              gdal.GDT_Byte, [255]),
        target_path_list=[slope_threshold_raster_path],
        task_name='new slope slope_threshold_raster_path')

    task_graph.close()
    task_graph.join()


if __name__ == '__main__':
    main()
