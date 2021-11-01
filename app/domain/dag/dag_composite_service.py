from datetime import datetime
from typing import List

from sqlalchemy.orm import Session

from app.domain.dag import dag_service as service
from app.domain.dag.dag_info import DagInfo
from app.domain.os.get_pwd import *
from app.dto import dag_info_dto
from app.exception.already_exists_dag_id_exception import AlreadyExistsDagIdException
from app.exception.dag_not_found_exception import DagNotFoundException
from get_information_from_user.const_value import *


def save(request: dag_info_dto.DagCreateDto, session: Session):
    dag = dag_info(request, session)
    service.save(dag, session)


def update(request: dag_info_dto.DagUpdateDto, session: Session):
    validate_dag_id(request.dag_id, session)

    dag = service.find(request.uuid, session)
    dag.dag_id = request.dag_id
    dag.owner = dag.owner if not request.owner else request.owner
    dag.start_date = dag.start_date if not request.start_date else request.start_date
    dag.catchup = dag.catchup if not request.catchup else request.catchup
    dag.schedule_interval = dag.schedule_interval if not request.schedule_interval else request.schedule_interval
    dag.csv_files_directory = dag.csv_files_directory if not request.csv_files_directory else request.csv_files_directory
    dag.updated_at = datetime.now()

    service.save(dag, session)


def find(uuid: str, session: Session) -> dag_info_dto.DagInfoDto:
    dag = service.find(uuid, session)
    validate(dag)
    return dag_info_dto.of(dag)


def delete(uuids: List[str], session: Session):
    for uuid in uuids:
        service.delete(uuid, session)


def dag_info(request: dag_info_dto.DagCreateDto, session: Session):
    validate_dag_id(request.dag_id, session)

    dag = DagInfo()
    dag.dag_id = request.dag_id
    dag.owner = dag.owner if not request.owner else request.owner
    dag.catchup = request.catchup
    dag.schedule_interval = request.schedule_interval
    dag.start_date = request.start_date
    dag.yesterday = request.yesterday

    dag.airflow_home, dag.backend_url = get_airflow_home_and_backend_url()
    dag.csv_files_directory = def_csv_dir(dag.airflow_home)
    return dag


def validate_dag_id(dag_id: str, session: Session):
    if service.is_exist_dag_id(dag_id, session):
        raise AlreadyExistsDagIdException()


def get_dag_id_for_python(engine):
    while True:
        dag_id, useable = validate_dag_id(engine)
        if useable:
            break
        print(error_message.unuseable_dag_id)
    return dag_id


def optional_or_else(data: str):
    if data.__eq__(None) | (not data):
        return ''
    return data


def validate(dag: DagInfo):
    if dag is None:
        yield DagNotFoundException()
