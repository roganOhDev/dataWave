from typing import List

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.common.database import db
from app.domain.table import table_composite_service as composite_service
from app.dto.table_list_dto import Table_List_Dto

router = APIRouter()


# TODO: save 하는 것들 validate 필요 목표 : 목
# TODO: mysql 만 만들어 둠 / snowflake, aws 연결 만들어야 함
@router.get("")
def find_all_tables(connection_uuid: str, session: Session = Depends(db.session)):
    return composite_service.find_tables(connection_uuid, session)


@router.put("")
def create(request: Table_List_Dto, session: Session = Depends(db.session)):
    composite_service.create(request, session)


@router.put("/{uuid}")
def update(uuid: str, request: Table_List_Dto, session: Session = Depends(db.session)):
    composite_service.update(uuid, request, session)


@router.delete("")
def delete(uuids: List[str] = Query(None), session: Session = Depends(db.session)):
    composite_service.delete(uuids, session)
