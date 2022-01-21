from datetime import datetime
from typing import List

from pydantic import BaseModel


class EltMapSaveDto(BaseModel):
    job_uuid: str = None
    destination_connection_uuid: str = None
    table_list_uuids: List[str] = None
    status: int = 0


class EltMapDto(BaseModel):
    id: int = None
    uuid: str = None
    job_uuid: str = None
    integrate_connection_uuid: str = None
    destination_connection_uuid: str = None
    table_list_uuids: str = None
    active: bool = None
    status: int = 0
    created_at: datetime = None
    updated_at: datetime = None


class EltMapUpdateStatus(BaseModel):
    job_id: str = None
    status: int = 0
