from fastapi import APIRouter
from domain.service import job_service as service
router = APIRouter()

@router.put("")
def add(job_id:str):
    service.add(job_id)