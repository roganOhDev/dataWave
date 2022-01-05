from sqlalchemy.orm import Session

from domain.job.job_infoes import JobInfo


def find(uuid: str, session: Session, validate: bool) -> JobInfo:
    return session.query(JobInfo).filter(JobInfo.uuid == uuid).first()
