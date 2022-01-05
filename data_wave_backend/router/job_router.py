from typing import List

from fastapi import APIRouter, Depends, Query, Response
from sqlalchemy.orm import Session

from common.database import db
from domain.job import job_composite_service as composite_service
from dto.job_info_dto import *

router = APIRouter()


@router.put("")
def create(request: JobCreateDto, session: Session = Depends(db.session)):
    return Response(content=composite_service.save(request, session), media_type="application/json")


@router.put("/{uuid}")
def update(uuid: str, request: JobUpdateDto, session: Session = Depends(db.session)):
    return Response(content=composite_service.update(uuid, request, session), media_type="application/json")


@router.get("")
def find(uuid: str, session: Session = Depends(db.session)):
    return composite_service.find_by_uuid(uuid, session)


@router.get("/using")
def find_using(session: Session = Depends(db.session)):
    return composite_service.find_all_by_using(session)


@router.get("/{job_id}")
def find_by_job_id(job_id: str, session: Session = Depends(db.session)):
    return composite_service.find_all_by_job_id(job_id, session)


@router.delete("")
def delete(uuids: List[str] = Query(None), session: Session = Depends(db.session)):
    composite_service.delete(uuids, session)
