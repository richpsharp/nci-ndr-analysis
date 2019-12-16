import argparse
import re
import sqlite3

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='reset watershed')
    parser.add_argument(
        'db_path', type=str, default=None, help='path to database')
    parser.add_argument(
        'watershed_id_pairs', type=str, nargs='+',
        help='list of [watershed_id]_[fid] to reset')
    args = parser.parse_args()

    watershed_fid_list = []
    for watershed_id_pair in args.watershed_id_pairs:
        watershed_basename, fid = (
            re.match('(.*)_(\d+)', watershed_id_pair).groups())
        watershed_fid_list.append((watershed_basename, int(fid)))
    print(watershed_fid_list)

    connection = sqlite3.connect(args.db_path)
    cursor = connection.cursor()
    cursor.executemany(
        'UPDATE job_status '
        'SET workspace_url='', job_status=\'PRESCHEDULED\' '
        'WHERE watershed_basename=? AND fid=?',
        watershed_fid_list)
    connection.commit()
    connection.close()
