from typing import List

from sqlalchemy.orm import Session

from domain.job import job_repository as repository
from domain.job.job_infoes import JobInfo
from exception.job_not_found_exception import JobNotFoundException
from exception.using_job_exception import UsingJobException


def find_by_job_id(job_id: str, session: Session) -> JobInfo:
    return repository.find_by_job_id(job_id, session)


def find_by_uuid(uuid: str, session: Session, validate: bool) -> JobInfo:
    dag = repository.find_by_uuid(uuid, session, validate)
    if validate & (not dag):
        raise JobNotFoundException()
    return dag


def find_all_by_job_id(job_id: str, session: Session) -> List[JobInfo]:
    return repository.find_all_by_job_id(job_id, session)


def find_all_by_using(session: Session) -> List[JobInfo]:
    return repository.find_all_by_using(session)


def delete(job: JobInfo, session: Session):
    if job.using:
        raise UsingJobException()
    repository.delete(job, session)


def save(job: JobInfo, session: Session):
    repository.save(job, session)
