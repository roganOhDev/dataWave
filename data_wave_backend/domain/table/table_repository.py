from sqlalchemy.orm import Session

from data_wave_backend.domain.dag.dag_infoes import DagInfo


def find(uuid: str, session: Session, validate: bool) -> DagInfo:
    return session.query(DagInfo).filter(DagInfo.uuid == uuid).first()
