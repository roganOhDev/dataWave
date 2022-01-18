from typing import List

from sqlalchemy.orm import Session

from common.page.page import Page
from domain.schedule_log.schedule_log import ScheduleLog


def save(schedule_log: ScheduleLog, session: Session) -> ScheduleLog:
    session.add(schedule_log)
    session.commit()
    session.refresh(schedule_log)

    return schedule_log


def page(page_able: Page, session: Session) -> List[ScheduleLog]:
    start_id, end_id = page_able.get_pageable
    return session.query(ScheduleLog).filter(ScheduleLog.id >= start_id & ScheduleLog.id <= end_id).all()
