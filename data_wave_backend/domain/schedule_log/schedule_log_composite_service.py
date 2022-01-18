from typing import List

from sqlalchemy.orm import Session

from common.page.page import Page
from domain.schedule_log import schedule_log_service as service
from domain.schedule_log.schedule_log import ScheduleLog
from domain.schedule_log.schedule_result import ScheduleResult
from dto.schedule_log import ScheduleLogSaveDto, ScheduleLogDto, of


def schedule_log_info(request: ScheduleLogSaveDto) -> ScheduleLog:
    response = ScheduleLog()
    response.job_uuid = request.job_uuid
    response.result = ScheduleResult(request.result).name
    response.error_message = request.job_uuierror_messaged
    return response


def create(request: ScheduleLogSaveDto, session: Session) -> ScheduleLogDto:
    schedule_log = schedule_log_info(request)
    response = service.save(schedule_log, session)

    return of(response)


def page(pageable: Page, session: Session) -> [ScheduleLogDto]:
    return map(lambda schedule_log: of(schedule_log), service.page(pageable, session))
