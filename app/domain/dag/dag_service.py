from typing import List

from sqlalchemy.orm import Session

from app.domain.dag import dag_repository as repository
from app.domain.dag.dag_infoes import DagInfo
from app.exception.dag_not_found_exception import DagNotFoundException


def find_by_dag_id(dag_id: str, session: Session) -> DagInfo:
    return repository.find(dag_id, session)


def find(uuid: str, session: Session, validate: bool) -> DagInfo:
    dag = repository.find(uuid, session, validate)
    if validate & (not dag):
        raise DagNotFoundException
    return dag


def find_all(dag_id: str, session: Session) -> List[DagInfo]:
    return repository.find_all(dag_id, session)


def delete(uuid: str, session: Session):
    dag = find(uuid, session)
    if not dag:
        raise DagNotFoundException
    repository.delete(dag, session)


def save(dag: DagInfo, session: Session):
    repository.save(dag, session)
