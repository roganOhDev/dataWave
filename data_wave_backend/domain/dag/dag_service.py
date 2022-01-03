from typing import List

from sqlalchemy.orm import Session

from domain.dag import dag_repository as repository
from domain.dag.dag_infoes import DagInfo
from exception.dag_not_found_exception import DagNotFoundException
from exception.using_dag_exception import UsingDagException


def find_by_dag_id(dag_id: str, session: Session) -> DagInfo:
    return repository.find(dag_id, session)


def find(uuid: str, session: Session, validate: bool) -> DagInfo:
    dag = repository.find(uuid, session, validate)
    if validate & (not dag):
        raise DagNotFoundException
    return dag


def find_all(dag_id: str, session: Session) -> List[DagInfo]:
    return repository.find_all(dag_id, session)


def delete(dag: DagInfo, session: Session):
    if dag.using:
        raise UsingDagException()
    repository.delete(dag, session)


def save(dag: DagInfo, session: Session):
    repository.save(dag, session)
