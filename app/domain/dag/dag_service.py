from sqlalchemy.orm import Session

from app.domain.dag.dag_info import DagInfo
from app.exception.dag_not_found_exception import DagNotFoundException


def is_exist_dag_id(dag_id: str, session: Session) -> bool:
    return True if find_by_dag_id(dag_id, session) is not None else False


def find_by_dag_id(dag_id: str, session: Session) -> DagInfo:
    return session.query(DagInfo).filter(DagInfo.dag_id == dag_id).first()


def find(uuid: str, session: Session, validate: bool) -> DagInfo:
    dag = session.query(DagInfo).filter(DagInfo.uuid == uuid).first()
    if validate & (not dag):
        raise DagNotFoundException
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
