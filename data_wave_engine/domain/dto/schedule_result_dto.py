from datetime import datetime

from pydantic import BaseModel

from domain.enums.schedule_result import ScheduleResult


class ScheduleResultDto(BaseModel):
    job_id: str = None
    result: int = None
    exec_time: datetime = None
    message: str = None


def of(job_id: str, result: ScheduleResult, exec_time: datetime, message: str) -> ScheduleResultDto:
    response = ScheduleResultDto()

    response.job_id = job_id
    response.result = result.value
    exec_time = exec_time
    response.message = message

    return response
