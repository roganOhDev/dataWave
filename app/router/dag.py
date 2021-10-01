from fastapi import APIRouter, HTTPException
from fastapi.encoders import jsonable_encoder

from app.domain.dag import dag_composite_service as composite_service
from app.dto.dag import daginfo

router = APIRouter()


@router.put("/dag/init", response_model=daginfo.DagInfo)
def init_dag(request: daginfo.DagInfo):
    item = jsonable_encoder(request)
    composite_service.get_dag_info(item)
