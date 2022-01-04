from typing import List

from fastapi import APIRouter, Depends, Query, Response, status
from sqlalchemy.orm import Session

from common.database import db
from domain.dag import dag_composite_service as composite_service
from dto.dag_info_dto import *

router = APIRouter()


@router.put("")
def create(request: DagCreateDto, session: Session = Depends(db.session)):
    return Response(content=composite_service.save(request, session), media_type="application/json")


@router.put("/{uuid}")
def update(uuid: str, request: DagUpdateDto, session: Session = Depends(db.session)):
    return Response(content=composite_service.update(uuid, request, session), media_type="application/json")


@router.get("")
def find(uuid: str, session: Session = Depends(db.session)):
    return composite_service.find(uuid, session)


@router.delete("")
def delete(uuids: List[str] = Query(None), session: Session = Depends(db.session)):
    composite_service.delete(uuids, session)
