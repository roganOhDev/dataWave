from typing import List

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from data_wave_backend.common.database import db
from data_wave_backend.domain.table import table_composite_service as composite_service
from data_wave_backend.dto.table_list_dto import Table_List_Create_Dto,Table_List_Update_Dto, Table_List_Dto

router = APIRouter()


# TODO: save 하는 것들 validate 필요 목표  : ui 만들어 보고 필요하면
# TODO: mysql 만 만들어 둠 / snowflake, aws 연결 만들어야 함 : ui 만든 이후
@router.get("/tables")
def find_all_tables(connection_uuid: str, session: Session = Depends(db.session)):
    return composite_service.find_tables(connection_uuid, session)


@router.get("", response_model=Table_List_Dto)
def find(uuid: str, session: Session = Depends(db.session)):
    return composite_service.find(uuid, session)


@router.put("")
def create(request: Table_List_Create_Dto, session: Session = Depends(db.session)):
    composite_service.create(request, session)

@router.put("/{uuid}/reset_pk")
def update(uuid: str, request: Table_List_Update_Dto, session: Session = Depends(db.session)):
    composite_service.update(uuid, request, session)

@router.put("/{uuid}")
def update(uuid: str, request: Table_List_Update_Dto, session: Session = Depends(db.session)):
    composite_service.update(uuid, request, session)


@router.delete("")
def delete(uuids: List[str] = Query(None), session: Session = Depends(db.session)):
    composite_service.delete(uuids, session)
