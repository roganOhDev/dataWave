from domain.schedule_log.schedule_log import ScheduleLog
from sqlalchemy.orm import Session


def save(schedule_log: ScheduleLog, session: Session) -> ScheduleLog:
    session.add(schedule_log)
    session.commit()
    session.refresh(schedule_log)

    return schedule_log
