from fastapi import APIRouter, Query

from domain.service.job import job_service as service
from domain.service.job.job import JobActivateRequest

router = APIRouter()


@router.put("")
def add_scheduler(request: JobActivateRequest):
    service.add_schedule(request)


@router.delete("/{job_id}")
def delete_scheduler(job_id: str = Query(None)):
    service.delete_schedule(job_id)
