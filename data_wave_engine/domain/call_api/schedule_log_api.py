from common.utils import request_util
from client import Client
from domain.dto.schedule_result_dto import of
from domain.enums.schedule_result import ScheduleResult
from exception.api_exception import ApiException
from datetime import datetime


def create(job_id: str, result: ScheduleResult, exec_time: datetime, message: str = ""):
    schedule_result = of(job_id, result, exec_time, message)
    response = request_util.put(url=Client.ScheduleLog.schedule_log, body=dict(schedule_result))
    if response.status_code != 200:
        raise ApiException(response.json()['detail'])
