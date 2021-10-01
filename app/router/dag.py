from fastapi import APIRouter

from app.dto.dag import dag_info_dto
from app.domain.dag import dag_composite_service as composite_service

router = APIRouter()


# 새로운건 post default put update patch

@router.put("/dag/init", response_model=dag_info_dto.dag_info_dto)
def init_dag():
    composite_service.get_dag_info()

