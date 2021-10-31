from app.domain.dag import dag_service as service
from app.domain.dag.dag_info import DagInfo
from app.domain.os.get_pwd import *
from app.domain.utils.json_util import json
from app.dto import dag_info_dto
from app.exception.already_exists_dag_id_exception import AlreadyExistsDagIdException
from get_information_from_user.const_value import *


def save(request):
    dag = dag_info(request)
    service.save(json(dag))


def update(request: dag_info_dto.DagInfoDto):
    dag = service.find(request.uuid)
    dag = update(dag, request)

    save(dag)
    return dag


def update(dag: dag_info_dto.DagInfoDto, request: dag_info_dto.DagInfoDto):
    dag.dag_id = optional_or_else(request.dag_id)
    dag.schedule_interval = optional_or_else(request.schedule_interval)
    dag.catchup = optional_or_else(request.catchup)
    dag.owner = optional_or_else(request.owner)
    return dag


def dag_info(request):
    dag = DagInfo(request)
    dag.airflow_home, dag.backend_url = get_airflow_home_and_backend_url()
    # TODO for db -> engine = sql.create_engine(dag.backend_url)
    validate_dag_id(dag.dag_id)
    dag.csv_files_directory = def_csv_dir(dag.airflow_home)
    return dag


def validate_dag_id(dag_id):
    if service.is_exist(dag_id):
        raise AlreadyExistsDagIdException()


def get_dag_id_for_python(engine):
    while True:
        dag_id, useable = validate_dag_id(engine)
        if useable:
            break
        print(error_message.unuseable_dag_id)
    return dag_id


def delete(uuids):
    for uuid in  uuids:
        service.delete(uuid)


def optional_or_else(data: str):
    if data.__eq__(None) | (not data):
        return ''
    return data
