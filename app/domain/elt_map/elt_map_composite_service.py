from typing import List

from sqlalchemy.orm import Session

from app.domain.elt_map import elt_map_service as service
from app.domain.elt_map.elt_map import EltMap
from app.dto.elt_map_dto import EltMapDto, of


def elt_map_info(elt_map_info_dto: EltMapDto, elt_map: EltMap = EltMap()) -> EltMap:
    elt_map.id = elt_map_info_dto.id
    elt_map.uuid = elt_map_info_dto.uuid
    elt_map.integrate_connection_uuid = elt_map_info_dto.integrate_connection_uuid
    elt_map.destination_connection_uuid = elt_map_info_dto.destination_connection_uuid
    elt_map.table_list_uuid = elt_map_info_dto.table_list_uuid
    elt_map.created_at = elt_map_info_dto.created_at

    return elt_map


def find(uuid: str, session: Session) -> EltMapDto:
    elt_map = service.find(uuid, session, True)
    return of(elt_map)


def create(request: EltMapDto, session: Session):
    elt_map = elt_map_info(request)
    service.save(elt_map, session)


def update(uuid: str, request: EltMapDto, session: Session):
    elt_map = service.find(uuid, session, True)
    elt_map = elt_map_info(request, elt_map)
    service.save(elt_map, session)


def delete(uuids: List[str], session: Session):
    service.delete(uuids, session)
