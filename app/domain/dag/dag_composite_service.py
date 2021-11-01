from sqlalchemy.orm import Session

from app.domain.dag import dag_service as service
from app.domain.dag.dag_info import DagInfo
from app.domain.os.get_pwd import *
from app.dto import dag_info_dto
from app.exception.already_exists_dag_id_exception import AlreadyExistsDagIdException
from get_information_from_user.const_value import *


def save(request: dag_info_dto.DagInfoDto, session: Session) -> DagInfo:
    dag = dag_info(request, session)
    return service.create(dag, session)


def update(request: dag_info_dto.DagInfoDto) -> DagInfo:
    dag = service.find(request.uuid)
    dag = update(dag, request)

    return save(dag)


def update(dag: dag_info_dto.DagInfoDto, request: dag_info_dto.DagInfoDto):
    dag.dag_id = optional_or_else(request.dag_id)
    dag.schedule_interval = optional_or_else(request.schedule_interval)
    dag.catchup = optional_or_else(request.catchup)
    dag.owner = optional_or_else(request.owner)
    return dag


def dag_info(request: dag_info_dto.DagInfoDto, session: Session):
    dag = DagInfo()
    dag.dag_id = request.dag_id
    dag.owner = dag.owner if not request.owner else request.owner
    dag.catchup = request.catchup
    dag.schedule_interval = request.schedule_interval
    dag.start_date = request.start_date
    dag.yesterday = request.yesterday

    dag.airflow_home, dag.backend_url = get_airflow_home_and_backend_url()
    validate_dag_id(dag.dag_id, session)
    dag.csv_files_directory = def_csv_dir(dag.airflow_home)
    return dag


def validate_dag_id(dag_id: str, session: Session):
    if service.is_exist(dag_id, session):
        raise AlreadyExistsDagIdException()


def get_dag_id_for_python(engine):
    while True:
        dag_id, useable = validate_dag_id(engine)
        if useable:
            break
        print(error_message.unuseable_dag_id)
    return dag_id


def delete(uuids):
    for uuid in uuids:
        service.delete(uuid)


def optional_or_else(data: str):
    if data.__eq__(None) | (not data):
        return ''
    return data
