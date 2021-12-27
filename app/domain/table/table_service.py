from typing import List

from sqlalchemy.orm import Session

from app.domain.connection.connection import Connection
from app.domain.dag.dag_infoes import DagInfo
from app.domain.table.table_list import Table_List
from app.exception.dag_not_found_exception import DagNotFoundException
from app.domain.table import table_repository as repository

def find_by_dag_id(dag_id: str, session: Session) -> DagInfo:
    return session.query(DagInfo).filter(DagInfo.dag_id == dag_id).first()


def find(uuid: str, session: Session, validate: bool) -> Connection:
    dag = repository.find(uuid, session)
    if validate & (not dag):
        raise Connection
    return dag

def find_all(dag_id: str, session: Session) -> List[DagInfo]:
    dag = session.query(DagInfo).filter(DagInfo.dag_id == dag_id).all()
    return dag

def delete(uuid: str, session: Session):
    dag = session.query(DagInfo).filter(DagInfo.uuid == uuid).first()
    if not dag:
        raise DagNotFoundException
    session.delete(dag)
    session.commit()


def save(table_list: Table_List, session: Session):
    repository.save(table_list, session)


