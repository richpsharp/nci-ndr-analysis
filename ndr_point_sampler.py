"""Sample NDR watersheds from a database and report areas around points."""
import logging
import os
import sys

import ecoshard
import taskgraph

WORKSPACE_DIR = 'ndr_point_sampler_workspace'

NDR_WATERSHED_DATABASE_URL = (
    'https://storage.googleapis.com/ipbes-ndr-ecoshard-data/'
    'ndr_global_run_database_md5_fa32958c7024e8e93d067ecfd0c4d419.sqlite3')
NDR_WATERSHED_DATABASE_PATH = os.path.join(
    WORKSPACE_DIR, 'ndr_global_run_database.sqlite3')

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(pathname)s.%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
logging.getLogger('taskgraph').setLevel(logging.INFO)


if __name__ == '__main__':
    for dir_path in [WORKSPACE_DIR]:
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

    task_graph.join()
    task_graph.close()
