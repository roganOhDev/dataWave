from typing import List

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from common.database import db
from domain.elt_map import elt_map_composite_service as composite_service
from dto.elt_map_dto import EltMapDto, EltMapSaveDto

router = APIRouter()


@router.get("")
def find(uuid: str, session: Session = Depends(db.session)) -> EltMapDto:
    return composite_service.find(uuid, session)


@router.put("")
def create(request: EltMapSaveDto, session: Session = Depends(db.session)):
    composite_service.create(request, session)


@router.put("/activate")
def activate(uuid: str, session: Session = Depends(db.session)):
    composite_service.activate(uuid, session)


@router.put("/deactivate")
def activate(uuid: str, session: Session = Depends(db.session)):
    composite_service.deactivate(uuid, session)


@router.put("/{uuid}")
def update(uuid: str, request: EltMapSaveDto, session: Session = Depends(db.session)):
    composite_service.update(uuid, request, session)


@router.delete("")
def delete(uuids: List[str] = Query(None), session: Session = Depends(db.session)):
    composite_service.delete(uuids, session)

# TODO : active 할땐 dag 파일 만들고, deactive 할 땐 dag 파일 지우기 (파일 생성 코드 이후)
