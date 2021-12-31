from datetime import datetime
from typing import List

from sqlalchemy.orm import Session

from app.domain.dag import dag_composite_service
from app.domain.elt_map import elt_map_service as service
from app.domain.elt_map.elt_map import EltMap
from app.domain.table import table_composite_service
from app.dto.elt_map_dto import EltMapDto, of, EltMapSaveDto
from app.exception.caanot_use_this_dag_exception import CannotUseThisDagException


def elt_map_info(elt_map_info_dto: EltMapSaveDto, session: Session, elt_map: EltMap) -> EltMap:
    table_list = table_composite_service.find(elt_map_info_dto.table_list_uuid, session)

    elt_map.dag_uuid = elt_map_info_dto.dag_uuid
    elt_map.integrate_connection_uuid = table_list.connection_uuid
    elt_map.destination_connection_uuid = elt_map_info_dto.destination_connection_uuid
    elt_map.table_list_uuid = elt_map_info_dto.table_list_uuid
    elt_map.active = elt_map_info_dto.active
    elt_map.updated_at = datetime.now()

    return elt_map


def find(uuid: str, session: Session) -> EltMapDto:
    elt_map = service.find(uuid, session, True)
    return of(elt_map)


def create(request: EltMapSaveDto, session: Session):
    validate(request, session)
    elt_map = elt_map_info(request, session, EltMap())
    if request.dag_uuid:
        dag_composite_service.update_dag_usage(request, session)
    else:
        elt_map.active = False
    service.save(elt_map, session)


def update(uuid: str, request: EltMapSaveDto, session: Session):
    elt_map = service.find(uuid, session, True)
    if elt_map.dag_uuid != request.dag_uuid:
        validate(request, session)
        dag_composite_service.update_dag_usage(elt_map, session)
        dag_composite_service.update_dag_usage(request, session)
    elt_map = elt_map_info(request, session, elt_map)

    if not request.dag_uuid:
        elt_map.active = False
    service.save(elt_map, session)


def delete(uuids: List[str], session: Session):
    for uuid in uuids:
        elt_map = service.find(uuid, session, True)
        service.delete(elt_map, session)

def validate(request: EltMapSaveDto, session: Session):
    if request.dag_uuid:
        dag = dag_composite_service.find(request.dag_uuid, session)
        if dag.using:
            raise CannotUseThisDagException()
