from typing import List

from multipledispatch import dispatch
from sqlalchemy.orm import Session

from domain.job.job_infoes import JobInfo


@dispatch(str, Session)
def find(job_id, session) -> JobInfo:
    return session.query(JobInfo).filter(JobInfo.job_id == job_id).first()


@dispatch(str, Session, bool)
def find(uuid, session, validate) -> JobInfo:
    return session.query(JobInfo).filter(JobInfo.uuid == uuid).first()


def find_all(job_id: str, session: Session) -> List[JobInfo]:
    return session.query(JobInfo).filter(JobInfo.job_id == job_id).all()


def delete(job: JobInfo, session: Session):
    session.delete(job)
    session.commit()


def save(job: JobInfo, session: Session):
    session.add(job)
    session.commit()
    session.refresh(job)
