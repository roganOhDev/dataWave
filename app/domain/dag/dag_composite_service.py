from datetime import datetime
from typing import List

from sqlalchemy.orm import Session

from app.domain.dag import dag_service as service
from app.domain.dag.dag_infoes import DagInfo
from app.domain.os.get_pwd import *
from app.dto import dag_info_dto
from app.dto.elt_map_dto import EltMapSaveDto
from app.exception.already_exists_dag_id_exception import AlreadyExistsDagIdException


def save(request: dag_info_dto.DagCreateDto, session: Session):
    dag = dag_info(request, session)
    service.save(dag, session)


def update(uuid: str, request: dag_info_dto.DagUpdateDto, session: Session):
    dag = service.find(uuid, session, True)
    validate_dag_id(request.dag_id, dag.id, False, session)

    dag.dag_id = dag.dag_id if not request.dag_id else request.dag_id
    dag.owner = dag.owner if not request.owner else request.owner
    dag.start_date = dag.start_date if not request.start_date else request.start_date
    dag.catchup = dag.catchup if not request.catchup else request.catchup
    dag.schedule_interval = dag.schedule_interval if not request.schedule_interval else request.schedule_interval
    dag.csv_files_directory = dag.csv_files_directory if not request.csv_files_directory else request.csv_files_directory
    dag.updated_at = datetime.now()

    service.save(dag, session)


def find(uuid: str, session: Session) -> dag_info_dto.DagInfoDto:
    dag = service.find(uuid, session, True)
    return dag_info_dto.of(dag)


def delete(uuids: List[str], session: Session):
    for uuid in uuids:
        dag = service.find(uuid, session, True)
        service.delete(dag, session)


def dag_info(request: dag_info_dto.DagCreateDto, session: Session):
    validate_dag_id(request.dag_id, 0, True, session)

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


def validate_dag_id(dag_id: str, id: int, new: bool, session: Session):
    if dag_id:
        dags = service.find_all(dag_id, session)
        if new:
            if dags:
                raise AlreadyExistsDagIdException()
        elif len(dags) > 1 | (False if not dags is None else dags[0].id != id):
            raise AlreadyExistsDagIdException()


def update_dag_usage(elt_map: EltMapSaveDto, session):
    dag = service.find(elt_map.dag_uuid, session, True)
    dag.using = not dag.using
    dag.updated_at = datetime.now()
    service.save(dag, session)
