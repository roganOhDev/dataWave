from fastapi import APIRouter,Depends
from sqlalchemy.orm import Session

from app.common.database import db
from app.domain.dag.dag_info import DagInfo
from app.domain.dag import dag_composite_service as composite_service
from app.dto import dag_info_dto
from fastapi import Response

router = APIRouter()


@router.put("/dag/save", response_model=dag_info_dto.DagInfoDto)
def init_dag(request: dag_info_dto.DagInfoDto):
    composite_service.save(request)

@router.put("/dag/update", response_model=dag_info_dto.DagInfoDto)
def init_dag(request: dag_info_dto.DagInfoDto):
    return Response(content=composite_service.update(request), media_type="application/json")

@router.put("/dag")
def aa(session: Session = Depends(db.session)):
    return session.query(DagInfo).all()
