from typing import List

from sqlalchemy.orm import Session

from domain.connection.connection import Connection
from domain.job.job_infoes import JobInfo
from domain.table.table_list import Table_List
from exception.job_not_found_exception import JobNotFoundException
from domain.table import table_repository as repository

def find_by_job_id(job_id: str, session: Session) -> JobInfo:
    return session.query(JobInfo).filter(JobInfo.job_id == job_id).first()


def find(uuid: str, session: Session, validate: bool) -> Connection:
    job = repository.find(uuid, session)
    if validate & (not job):
        raise Connection
    return job

def find_all(job_id: str, session: Session) -> List[JobInfo]:
    job = session.query(JobInfo).filter(JobInfo.job_id == job_id).all()
    return job

def delete(uuid: str, session: Session):
    job = session.query(JobInfo).filter(JobInfo.uuid == uuid).first()
    if not job:
        raise JobNotFoundException
    session.delete(job)
    session.commit()


def save(table_list: Table_List, session: Session):
    repository.save(table_list, session)


