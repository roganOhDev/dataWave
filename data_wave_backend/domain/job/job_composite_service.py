from datetime import datetime
from typing import List

from croniter import croniter
from sqlalchemy.orm import Session

from domain.job import job_service as service
from domain.job.job_infoes import JobInfo
from dto import job_info_dto
from dto.elt_map_dto import EltMapSaveDto
from exception.already_exists_job_id_exception import AlreadyExistsJobIdException
from exception.not_valid_cron_expression import NotValidCronExpression


def save(request: job_info_dto.JobCreateDto, session: Session) -> job_info_dto.JobInfoDto:
    job = job_info(request, session)
    service.save(job, session)
    return job_info_dto.of(job)


def update(uuid: str, request: job_info_dto.JobUpdateDto, session: Session) -> job_info_dto.JobInfoDto:
    job = service.find_by_uuid(uuid, session, True)
    validate_job(job.schedule_interval, request.job_id, job.id, False, session)

    job.job_id = job.job_id if not request.job_id else request.job_id
    job.owner = job.owner if not request.owner else request.owner
    job.start_date = job.start_date if not request.start_date else request.start_date
    job.schedule_interval = job.schedule_interval if not request.schedule_interval else request.schedule_interval
    job.csv_files_directory = job.csv_files_directory if not request.csv_files_directory else request.csv_files_directory
    job.updated_at = datetime.now()

    service.save(job, session)
    return job_info_dto.of(job)


def find_by_uuid(uuid: str, session: Session) -> job_info_dto.JobInfoDto:
    job = service.find_by_uuid(uuid, session, True)
    return job_info_dto.of(job)


def find_all_by_using(session: Session) -> List[str]:
    jobs = service.find_all_by_using(session)
    return list(map(lambda job: job.job_id, jobs))


def delete(uuids: List[str], session: Session):
    for uuid in uuids:
        dag = service.find_by_uuid(uuid, session, True)
        service.delete(dag, session)


def job_info(request: job_info_dto.JobCreateDto, session: Session):
    validate_job(request.schedule_interval, request.job_id, 0, True, session)

    job = JobInfo()
    job.job_id = request.job_id
    job.schedule_interval = request.schedule_interval
    job.start_date = request.start_date

    job.csv_files_directory = request.csv_files_directory
    return job


def validate_job(cron_expression: str, job_id: str, id: int, new: bool, session: Session):
    validate_job_id(job_id, id, new, session)
    validate_cron(cron_expression)


def validate_job_id(job_id: str, id, new: bool, session: Session):
    if job_id:
        jobs = find_all_by_job_id(job_id, session)
        if new:
            if jobs:
                raise AlreadyExistsJobIdException()
        elif len(jobs) > 1 | (False if not jobs is None else jobs[0].id != id):
            raise AlreadyExistsJobIdException()


def validate_cron(cron_expression: str):
    if not croniter.is_valid(cron_expression):
        raise NotValidCronExpression()


def find_all_by_job_id(job_id: str, session: Session) -> List[JobInfo]:
    return service.find_all_by_job_id(job_id, session)


def update_job_usage(elt_map: EltMapSaveDto, session):
    if elt_map.job_uuid:
        job = service.find_by_uuid(elt_map.job_uuid, session, True)
        job.using = not job.using
        job.updated_at = datetime.now()
        service.save(job, session)
