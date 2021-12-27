from typing import List

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from starlette.responses import Response

from app.common.database import db
from app.domain.table import table_composite_service as composite_service
from app.dto.table_list_dto import Table_List_Dto

router = APIRouter()


@router.get("")
def find_all_tables(connection_uuid: str, session: Session = Depends(db.session)):
    return composite_service.find_tables(connection_uuid, session)

@router.put("")
def create(request: Table_List_Dto, session: Session = Depends(db.session)):
    composite_service.create(request, session)

@router.put("/{uuid}")
def update(uuid: str, request: Table_List_Dto, session: Session = Depends(db.session)):
    composite_service.update(request, session)

@router.delete("")
def delete(uuids: List[str] = Query(None), session: Session = Depends(db.session)):
    composite_service.delete(uuids, session)
# @router.put("")
# def create(request: DagCreateDto, session: Session = Depends(db.session)):
#     return Response(content=composite_service.save(request, session), media_type="application/json")
#
#
# @router.put("/{uuid}", response_model=DagUpdateDto)
# def update(uuid: str, request: DagUpdateDto, session: Session = Depends(db.session)):
#     return Response(content=composite_service.update(uuid, request, session), media_type="application/json")
#
#
# @router.get("", response_model=DagInfoDto)
# def find(uuid: str, session: Session = Depends(db.session)):
#     return composite_service.find(uuid, session)
#
#
# @router.delete("")
# def delete(uuids: List[str] = Query(None), session: Session = Depends(db.session)):
#     composite_service.delete(uuids, session)
