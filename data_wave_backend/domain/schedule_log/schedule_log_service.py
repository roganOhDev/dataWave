from typing import List

from sqlalchemy.orm import Session

from common.page.page import Page
from domain.schedule_log import schedule_log_repository as repository
from domain.schedule_log.schedule_log import ScheduleLog


def save(request: ScheduleLog, session: Session) -> ScheduleLog:
    return repository.save(request, session)


def page(pageable: Page, session: Session) -> List[ScheduleLog]:
    return repository.page(pageable, session)
