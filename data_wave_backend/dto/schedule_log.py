from datetime import datetime
from typing import List

from pydantic import BaseModel

from domain.schedule_log.schedule_log import ScheduleLog
from domain.schedule_log.schedule_result import ScheduleResult

class ScheduleLogDto(BaseModel):
    id: int = None
    uuid: str = None
    job_uuid: str = None
    result: int = None
    error_message: str = None
    created_at: datetime = None

class ScheduleLogSaveDto(BaseModel):
    job_uuid: str = None
    error_message: str = None
    result: int = None

def of(schedule_log: ScheduleLog) -> ScheduleLogDto:
    response = ScheduleLogDto()
    response.id = schedule_log.id
    response.uuid = schedule_log.uuid
    response.job_uuid = schedule_log.job_uuid
    response.result = ScheduleResult[schedule_log.result].value
    response.error_message = schedule_log.error_message
    response.created_at = schedule_log.created_at

    return response
