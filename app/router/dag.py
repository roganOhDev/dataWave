from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder

from app.domain.dag import dag_composite_service as composite_service
from app.dto.dag import dag_info_dto

router = APIRouter()


@router.put("/dag/init", response_model=dag_info_dto.DagInfoDto)
def init_dag(request: dag_info_dto.DagInfoDto):
    item = jsonable_encoder(request)
    composite_service.dag_info(item)
