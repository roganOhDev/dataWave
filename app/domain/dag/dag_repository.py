from typing import List

from multipledispatch import dispatch
from sqlalchemy.orm import Session

from app.domain.dag.dag_infoes import DagInfo


@dispatch(str, Session)
def find(dag_id, session) -> DagInfo:
    return session.query(DagInfo).filter(DagInfo.dag_id == dag_id).first()


@dispatch(str, Session, bool)
def find(uuid, session, validate) -> DagInfo:
    return session.query(DagInfo).filter(DagInfo.uuid == uuid).first()


def find_all(dag_id: str, session: Session) -> List[DagInfo]:
    return session.query(DagInfo).filter(DagInfo.dag_id == dag_id).all()


def delete(dag: DagInfo, session: Session):
    session.delete(dag)
    session.commit()


def save(dag: DagInfo, session: Session):
    session.add(dag)
    session.commit()
    session.refresh(dag)
