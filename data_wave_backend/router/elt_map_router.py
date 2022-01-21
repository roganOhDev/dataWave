from typing import List

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from common.database import db
from domain.elt_map import elt_map_composite_service as composite_service
from dto.elt_map_dto import EltMapDto, EltMapSaveDto, EltMapUpdateStatus

router = APIRouter()


@router.get("")
def find(uuid: str, session: Session = Depends(db.session)) -> EltMapDto:
    return composite_service.find(uuid, session)


@router.get("/activated")
def get_activated(session: Session = Depends(db.session)) -> List[str]:
    return composite_service.find_activated_job_ids(session)


@router.put("")
def create(request: EltMapSaveDto, session: Session = Depends(db.session)) -> EltMapDto:
    return composite_service.create(request, session)


@router.put("/update_activate")
def update_is_activate(uuid: str, session: Session = Depends(db.session)) -> EltMapDto:
    return composite_service.update_is_activate(uuid, session)


@router.put("/update_status")
def update_is_activate(uuid: str, request: EltMapUpdateStatus, session: Session = Depends(db.session)) -> EltMapDto:
    return composite_service.update_is_activate(uuid, session)


@router.put("/{uuid}")
def update(uuid: str, request: EltMapSaveDto, session: Session = Depends(db.session)) -> EltMapDto:
    return composite_service.update(uuid, request, session)


@router.delete("")
def delete(uuids: List[str] = Query(None), session: Session = Depends(db.session)):
    composite_service.delete(uuids, session)
