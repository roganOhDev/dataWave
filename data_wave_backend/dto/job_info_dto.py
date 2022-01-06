import datetime as date
from datetime import datetime

from pydantic import BaseModel

from domain.job.job_infoes import JobInfo


class JobCreateDto(BaseModel):
    yesterday: datetime = datetime.today() - date.timedelta(1)
    job_id: str
    owner: str = ''
    start_date: str = '({year},{month},{day})'.format(year=yesterday.year, month=yesterday.month,
                                                      day=yesterday.day)
    schedule_interval: str = "@daily"
    csv_files_directory: str


class JobUpdateDto(BaseModel):
    job_id: str = ''
    owner: str = ''
    start_date: str = ''
    schedule_interval: str = ""
    csv_files_directory: str = ""


class JobInfoDto(BaseModel):
    id: int = None
    uuid: str = None
    job_id: str = None
    owner: str = None
    schedule_interval: str = None
    csv_files_directory: str = None
    start_date: str = None
    using: bool = None
    created_at: datetime = None
    updated_at: datetime = None


def of(job_info: JobInfo) -> JobInfoDto:
    job_info_dto = JobInfoDto()
    job_info_dto.id = job_info.id
    job_info_dto.uuid = job_info.uuid
    job_info_dto.job_id = job_info.job_id
    job_info_dto.owner = job_info.owner
    job_info_dto.schedule_interval = job_info.schedule_interval
    job_info_dto.csv_files_directory = job_info.csv_files_directory
    job_info_dto.start_date = job_info.start_date
    job_info_dto.using = job_info.using
    job_info_dto.created_at = job_info.created_at
    job_info_dto.updated_at = job_info.updated_at
    return job_info_dto
