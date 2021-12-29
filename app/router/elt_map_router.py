from typing import List

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.common.database import db
from app.domain.elt_map import elt_map_composite_service as composite_service
from app.dto.elt_map_dto import EltMapDto

router = APIRouter()


@router.get("")
def find(uuid: str, session: Session = Depends(db.session)) -> EltMapDto:
    return composite_service.find(uuid, session)


@router.put("")
def create(request: EltMapDto, session: Session = Depends(db.session)):
    composite_service.create(request, session)

@router.put("/{uuid}")
def update(uuid: str, request: EltMapDto, session: Session = Depends(db.session)):
    composite_service.update(uuid, request, session)

@router.delete("")
def delete(uuids: List[str] = Query(None), session: Session = Depends(db.session)):
    composite_service.delete(uuids, session)
