from typing import List

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from common.database import db
from domain.schedule_log import schedule_log_composite_service as composite_service
from dto.schedule_log import ScheduleLogSaveDto, ScheduleLogDto

router = APIRouter()


@router.put("")
def create(request: ScheduleLogSaveDto, session: Session = Depends(db.session)) -> ScheduleLogDto:
    return composite_service.create(request, session)
