from sqlalchemy.orm import Session

from app.domain.dag.dag_info import DagInfo


def is_exist(dag_id: str, session: Session) -> bool:
    return True if find(dag_id, session) is not None else False


def find(dag_id: str, session: Session) -> DagInfo:
    return session.query(DagInfo).filter(DagInfo.dag_id == dag_id).first()


def create(dag: DagInfo, session: Session) -> DagInfo:
    session.add(dag)
    session.commit()
    session.refresh(dag)
    return dag
