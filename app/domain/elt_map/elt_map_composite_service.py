from datetime import datetime
from typing import List

from sqlalchemy.orm import Session

from app.domain.elt_map import elt_map_service as service
from app.domain.table import table_composite_service
from app.domain.elt_map.elt_map import EltMap
from app.dto.elt_map_dto import EltMapDto, of, EltMapSaveDto


def elt_map_info(elt_map_info_dto: EltMapSaveDto, session: Session, elt_map: EltMap) -> EltMap:
    table_list = table_composite_service.find(elt_map_info_dto.table_list_uuid, session)

    elt_map.integrate_connection_uuid = table_list.connection_uuid
    elt_map.destination_connection_uuid = elt_map_info_dto.destination_connection_uuid
    elt_map.table_list_uuid = elt_map_info_dto.table_list_uuid
    elt_map.updated_at = datetime.now()

    return elt_map


def find(uuid: str, session: Session) -> EltMapDto:
    elt_map = service.find(uuid, session, True)
    return of(elt_map)


def create(request: EltMapSaveDto, session: Session):
    elt_map = elt_map_info(request, session, EltMap())
    service.save(elt_map, session)


def update(uuid: str, request: EltMapSaveDto, session: Session):
    elt_map = service.find(uuid, session, True)
    elt_map = elt_map_info(request, elt_map)
    service.save(elt_map, session)


def delete(uuids: List[str], session: Session):
    service.delete(uuids, session)
