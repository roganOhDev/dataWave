from typing import List

from common.utils import request_util
from client import Client
from domain.dto.job_info_dto import JobInfoDto


def get_activate_job_uuids() -> List[str]:
    response = request_util.get(Client.Job.job_using)
    return response.json()


def get_job_by_uuid(job_id: str) -> JobInfoDto:
    response = request_util.get(url=Client.Job.job + "/" + job_id)
    return JobInfoDto(**response.json()[0])
