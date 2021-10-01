import pandas as pd
import sqlalchemy as sql

from app.domain.os.get_pwd import *
from get_information_from_user.const_value import *
from get_information_from_user.structs import dag_info
from app.exception.already_exists_dag_id_exception import AlreadyExistsDagIdException
from app.dto.dag.dag_info_dto import DagInfoDto


def get_dag_info(request):
    dag = DagInfoDto(request)
    dag.airflow_home, dag.backend_url = get_airflow_home_and_backend_url()
    engine = sql.create_engine(dag.backend_url)
    validate_dag_id(engine,dag.dag_id)
    # make datetime of yesterday's date
    # dag.start_date = '({year},{month},{day})'.format(year=dag.yesterday.year, month=dag.yesterday.month,
    #                                                  day=dag.yesterday.day)
    # dag.csv_files_directory = def_csv_dir(dag.airflow_home)
    # dag.schedule_interval = get_schedule_interval()
    return dag


def validate_dag_id(engine, dag_id):
    if is_job_exists(dag_id, engine):
        raise AlreadyExistsDagIdException()
    return dag_id, True


def get_dag_id_for_python(engine):
    while True:
        dag_id, useable = validate_dag_id(engine)
        if useable:
            break
        print(error_message.unuseable_dag_id)
    return dag_id


def is_job_exists(job_name, engine):
    """
    check if job exists

    :return: 1 when exists and programm will make user to write his job name again & 0 when not exsits
    """
    try:
        _ = pd.read_sql_query(
            "select job_name from info where job_name='{job_name}' and info_type='load' limit 1".format(
                job_name=job_name), engine
        )['dag_id'][0]
    except:  # job is not exist
        _ = None
        """ job must not exsist so this direction is correct direction. And so, when in this case, code does nothing."""
        return False
    else:  # job is exist
        error_dag_id_exists(job_name)
        return True


def get_schedule_interval():
    print(about_querypie_elt.about_schedule_interval)
    schedule_interval = input('schedule_interval : ')
    return schedule_interval
