from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from common.database import db
from common.page.page import Page
from domain.schedule_log import schedule_log_composite_service as composite_service
from dto.schedule_log import ScheduleLogSaveDto, ScheduleLogDto

router = APIRouter()


@router.put("")
def create(request: ScheduleLogSaveDto, session: Session = Depends(db.session)) -> ScheduleLogDto:
    return composite_service.create(request, session)


@router.get("/list")
def page(pageable: Page, session: Session = Depends(db.session)) -> [ScheduleLogDto]:
    return composite_service.page(pageable, session)
