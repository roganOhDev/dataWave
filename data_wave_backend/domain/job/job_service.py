from typing import List

from sqlalchemy.orm import Session

from domain.job import job_repository as repository
from domain.job.job_infoes import JobInfo
from exception.job_not_found_exception import JobNotFoundException
from exception.using_job_exception import UsingJobException


def find_by_job_id(job_id: str, session: Session) -> JobInfo:
    return repository.find(job_id, session)


def find(uuid: str, session: Session, validate: bool) -> JobInfo:
    dag = repository.find(uuid, session, validate)
    if validate & (not dag):
        raise JobNotFoundException()
    return dag


def find_all(job: str, session: Session) -> List[JobInfo]:
    return repository.find_all(job, session)


def delete(job: JobInfo, session: Session):
    if job.using:
        raise UsingJobException()
    repository.delete(job, session)


def save(job: JobInfo, session: Session):
    repository.save(job, session)
