from fastapi import APIRouter, Depends
from fastapi import Response
from sqlalchemy.orm import Session

from app.common.database import db
from app.domain.dag import dag_composite_service as composite_service
from app.domain.dag.dag_info import DagInfo
from app.dto import dag_info_dto

router = APIRouter()


@router.put("/dag/save", response_model=dag_info_dto.DagInfoDto)
def init_dag(request: dag_info_dto.DagInfoDto, session: Session = Depends(db.session)) -> DagInfo:
    return composite_service.save(request, session)


@router.put("/dag/update", response_model=dag_info_dto.DagInfoDto)
def init_dag(request: dag_info_dto.DagInfoDto):
    return Response(content=composite_service.update(request), media_type="application/json")
