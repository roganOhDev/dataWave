from typing import List

from sqlalchemy.orm import Session

from app.domain.dag.dag_infoes import DagInfo
from app.exception.dag_not_found_exception import DagNotFoundException


def find_by_dag_id(dag_id: str, session: Session) -> DagInfo:
    return session.query(DagInfo).filter(DagInfo.dag_id == dag_id).first()


def find(uuid: str, session: Session, validate: bool) -> DagInfo:
    dag = session.query(DagInfo).filter(DagInfo.uuid == uuid).first()
    if validate & (not dag):
        raise DagNotFoundException
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


def save(dag: DagInfo, session: Session):
    session.add(dag)
    session.commit()
    session.refresh(dag)
