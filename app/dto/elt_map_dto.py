from datetime import datetime

from pydantic import BaseModel

from app.domain.elt_map.elt_map import EltMap


class EltMapSaveDto(BaseModel):
    destination_connection_uuid: str = None
    table_list_uuid: str = None


class EltMapDto(BaseModel):
    id: int = None
    uuid: str = None
    integrate_connection_uuid: str = None
    destination_connection_uuid: str = None
    table_list_uuid: str = None
    created_at: datetime = None
    updated_at: datetime = None


def of(elt_map: EltMap) -> EltMapDto:
    response = EltMapDto()
    response.id = elt_map.id
    response.uuid = elt_map.uuid
    response.integrate_connection_uuid = elt_map.integrate_connection_uuid
    response.destination_connection_uuid = elt_map.destination_connection_uuid
    response.table_list_uuid = elt_map.table_list_uuid
    response.created_at = elt_map.created_at
    response.created_at = elt_map.updated_at

    return response
