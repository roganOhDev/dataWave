from typing import List

from fastapi import APIRouter, Depends, Query, Response
from sqlalchemy.orm import Session

from app.common.database import db
from app.domain.connection import connection_composite_service as composite_service
from app.dto.connection_dto import ConnectionDto, BaseConnectionDto

router = APIRouter()


@router.put("", response_model=ConnectionDto)
def create(request: BaseConnectionDto, session: Session = Depends(db.session)):
    composite_service.save(request, session)


@router.put("/{uuid}", response_model=ConnectionDto)
def update(uuid: str, request: BaseConnectionDto, session: Session = Depends(db.session)):
    return Response(content=composite_service.update(uuid, request, session), media_type="application/json")


@router.get("", response_model=ConnectionDto)
def find(uuid: str, session: Session = Depends(db.session)):
    return composite_service.find(uuid, session)


@router.delete("")
def find(uuids: List[str] = Query(None), session: Session = Depends(db.session)):
    composite_service.delete(uuids, session)
