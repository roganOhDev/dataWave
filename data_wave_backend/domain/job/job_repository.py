from typing import List

from sqlalchemy.orm import Session

from domain.job.job_infoes import JobInfo


def find_by_job_id(job_id, session) -> JobInfo:
    return session.query(JobInfo).filter(JobInfo.job_id == job_id).first()


def find_by_uuid(uuid, session, validate) -> JobInfo:
    return session.query(JobInfo).filter(JobInfo.uuid == uuid).first()

def find_all_by_using(session: Session) -> List[JobInfo]:
    return session.query(JobInfo).filter(JobInfo.using).all()

def find_all_by_job_id(job_id: str, session: Session) -> List[JobInfo]:
    return session.query(JobInfo).filter(JobInfo.job_id == job_id).all()





def delete(job: JobInfo, session: Session):
    session.delete(job)
    session.commit()


def save(job: JobInfo, session: Session):
    session.add(job)
    session.commit()
    session.refresh(job)
