from fastapi import APIRouter

from app.dto.connection_dto import ConnectionDto
from app.domain.connection import connection_composite_service as composite_service

router = APIRouter()


@router.put("/connection/create", response_model=ConnectionDto)
def init_dag(request: ConnectionDto):
    composite_service.save(request)
