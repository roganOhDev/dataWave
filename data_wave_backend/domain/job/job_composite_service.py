from datetime import datetime
from typing import List

from sqlalchemy.orm import Session

from domain.job import job_service as service
from domain.job.job_infoes import JobInfo
from dto import job_info_dto
from dto.elt_map_dto import EltMapSaveDto
from exception.already_exists_job_id_exception import AlreadyExistsJobIdException


def save(request: job_info_dto.JobCreateDto, session: Session):
    job = job_info(request, session)
    service.save(job, session)


def update(uuid: str, request: job_info_dto.JobUpdateDto, session: Session):
    job = service.find(uuid, session, True)
    validate_job_id(request.job_id, job.id, False, session)

    job.job_id = job.job_id if not request.job_id else request.job_id
    job.owner = job.owner if not request.owner else request.owner
    job.start_date = job.start_date if not request.start_date else request.start_date
    job.catchup = job.catchup if not request.catchup else request.catchup
    job.schedule_interval = job.schedule_interval if not request.schedule_interval else request.schedule_interval
    job.csv_files_directory = job.csv_files_directory if not request.csv_files_directory else request.csv_files_directory
    job.updated_at = datetime.now()

    service.save(job, session)


def find(uuid: str, session: Session) -> job_info_dto.JobInfoDto:
    dag = service.find(uuid, session, True)
    return job_info_dto.of(dag)


def delete(uuids: List[str], session: Session):
    for uuid in uuids:
        dag = service.find(uuid, session, True)
        service.delete(dag, session)


def job_info(request: job_info_dto.JobCreateDto, session: Session):
    validate_job_id(request.job_id, 0, True, session)

    job = JobInfo()
    job.job_id = request.job_id
    job.schedule_interval = request.schedule_interval
    job.start_date = request.start_date

    job.csv_files_directory = request.csv_files_directory
    return job


def validate_job_id(job_id: str, id: int, new: bool, session: Session):
    if job_id:
        jobs = service.find_all(job_id, session)
        if new:
            if jobs:
                raise AlreadyExistsJobIdException()
        elif len(jobs) > 1 | (False if not jobs is None else jobs[0].id != id):
            raise AlreadyExistsJobIdException()


def update_job_usage(elt_map: EltMapSaveDto, session):
    if elt_map.job_uuid:
        job = service.find(elt_map.job_uuid, session, True)
        job.using = not job.using
        job.updated_at = datetime.now()
        service.save(job, session)
