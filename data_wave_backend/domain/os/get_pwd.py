import os
import subprocess

from domain.elt_map.create_job import get_sql_alchemy_conn


def get_airflow_home_and_backend_url():
    try:
        airflow_home = get_airflow_home_dir()
        backend_url = get_sql_alchemy_conn.get_sql_alchemy_conn(airflow_home)
    except:
        _ = backend_url
    return airflow_home, backend_url


def get_home_dir():
    return subprocess.check_output("echo $HOME", shell=True, ).decode('utf-8').replace("\n", '')


def get_airflow_home_dir():
    home = get_home_dir()
    airflow_home = home + '/airflow'
    return airflow_home


def check_csv_dir(airflow_home):
    csv_files_dir = airflow_home + '/csv_files'
    if not dir_exists(csv_files_dir):
        make_dir(csv_files_dir)
    return csv_files_dir


def def_csv_dir(airflow_home):
    csv_files_dir = check_csv_dir(airflow_home)
    return os.path.abspath(csv_files_dir)


def dir_exists(dir):
    return os.path.exists(dir)


def make_dir(dir):
    os.makedirs(dir)
