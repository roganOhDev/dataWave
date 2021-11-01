from typing import List

from fastapi import APIRouter, Depends, Query
from fastapi import Response
from sqlalchemy.orm import Session

from app.common.database import db
from app.domain.dag import dag_composite_service as composite_service
from app.dto import dag_info_dto

router = APIRouter()


@router.put("/dag/save")
def create(request: dag_info_dto.DagCreateDto, session: Session = Depends(db.session)):
    return Response(content=composite_service.save(request, session), media_type="application/json")


@router.put("/dag/update", response_model=dag_info_dto.DagUpdateDto)
def update(request: dag_info_dto.DagUpdateDto, session: Session = Depends(db.session)):
    return Response(content=composite_service.update(request, session), media_type="application/json")


@router.get("/dag", response_model=dag_info_dto.DagInfoDto)
def find(uuid: str, session: Session = Depends(db.session)):
    return composite_service.find(uuid, session)


@router.delete("/dag")
def find(uuids: List[str] = Query(None), session: Session = Depends(db.session)):
    composite_service.delete(uuids, session)
