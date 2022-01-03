from datetime import datetime
from typing import List

from pydantic import BaseModel

from data_wave_backend.domain.elt_map.elt_map import EltMap


class EltMapSaveDto(BaseModel):
    dag_uuid: str = None
    destination_connection_uuid: str = None
    table_list_uuids: List[str] = None


class EltMapDto(BaseModel):
    id: int = None
    uuid: str = None
    dag_uuid: str = None
    integrate_connection_uuid: str = None
    destination_connection_uuid: str = None
    table_list_uuids: str = None
    active: bool = None
    created_at: datetime = None
    updated_at: datetime = None


def of(elt_map: EltMap) -> EltMapDto:
    response = EltMapDto()
    response.id = elt_map.id
    response.uuid = elt_map.uuid
    response.dag_uuid = elt_map.dag_uuid
    response.integrate_connection_uuid = elt_map.integrate_connection_uuid
    response.destination_connection_uuid = elt_map.destination_connection_uuid
    response.table_list_uuids = elt_map.table_list_uuids
    response.active = elt_map.active
    response.created_at = elt_map.created_at
    response.created_at = elt_map.updated_at

    return response