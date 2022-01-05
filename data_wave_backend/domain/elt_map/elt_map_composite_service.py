import os
from datetime import datetime

from common.utils.list_converter import *
from common.utils.logger import logger
from domain.connection import connection_composite_service
from domain.connection.db_type import Db_Type
from domain.elt_map import elt_map_service as service
from domain.elt_map.create_job.db_vendor_raw_code import *
from domain.elt_map.create_job.job_format import job_format
from domain.elt_map.elt_map import EltMap
from domain.job import job_composite_service
from domain.table import table_composite_service
from dto.elt_map_dto import EltMapDto, of, EltMapSaveDto
from exception.caanot_use_this_job_exception import CannotUseThisJobException
from exception.connections_are_not_equal import ConnectionsAreNotEqual


def elt_map_info(elt_map_info_dto: EltMapSaveDto, session: Session, elt_map: EltMap) -> EltMap:
    table_list = table_composite_service.find(elt_map_info_dto.table_list_uuids[0], session)

    elt_map.job_uuid = elt_map_info_dto.job_uuid
    elt_map.integrate_connection_uuid = table_list.connection_uuid
    elt_map.destination_connection_uuid = elt_map_info_dto.destination_connection_uuid
    elt_map.table_list_uuids = convert_str_list_to_string(elt_map_info_dto.table_list_uuids)
    elt_map.updated_at = datetime.now()

    return elt_map


def find(uuid: str, session: Session) -> EltMapDto:
    elt_map = service.find(uuid, session, True)
    return of(elt_map)


def create(request: EltMapSaveDto, session: Session):
    validate(request, session)
    elt_map = elt_map_info(request, session, EltMap())
    if request.job_uuid:
        job_composite_service.update_job_usage(request, session)
    service.save(elt_map, session)


def update(uuid: str, request: EltMapSaveDto, session: Session):
    elt_map = service.find(uuid, session, True)
    if elt_map.job_uuid != request.job_uuid:
        validate(request, session)
        job_composite_service.update_job_usage(elt_map, session)
        job_composite_service.update_job_usage(request, session)
    elt_map = elt_map_info(request, session, elt_map)

    service.save(elt_map, session)


def delete(uuids: List[str], session: Session):
    for uuid in uuids:
        elt_map = service.find(uuid, session, True)
        service.delete(elt_map, session)


def activate(uuid: str, session: Session):
    elt_map = service.find(uuid, session, True)
    elt_map.active = True
    create_file(elt_map, session)
    service.save(elt_map, session)


def deactivate(uuid: str, session: Session):
    elt_map = service.find(uuid, session, True)
    elt_map.active = False
    delete_file(elt_map, session)
    service.save(elt_map, session)


def validate(request: EltMapSaveDto, session: Session):
    if request.job_uuid:
        job = job_composite_service.find(request.job_uuid, session)
        if job.using:
            raise CannotUseThisJobException()

    connection_uuids = []
    for table_list_uuid in request.table_list_uuids:
        connection_uuids.append(table_composite_service.find(table_list_uuid, session).connection_uuid)
    if all(connection_uuid == connection_uuids[0] for connection_uuid in connection_uuids):
        pass
    else:
        raise ConnectionsAreNotEqual()


def create_file(elt_map: EltMap, session: Session):
    job = job_composite_service.find(elt_map.job_uuid, session)
    extract_connection = connection_composite_service.find(elt_map.integrate_connection_uuid, session)
    load_connection = connection_composite_service.find(elt_map.destination_connection_uuid, session)
    extract_args = make_data_wave_raw_code(extract_connection, elt_map, session)
    load_args = make_data_wave_raw_code(load_connection, elt_map, session)

    job_code = job_format.format(
        extract_db_type=extract_connection.db_type.lower(),
        load_db_type=load_connection.db_type.lower(),
        extract_args=extract_args,
        load_args=load_args,
    )
    with open("data_wave_engine/elt_jobs/" + job.job_id + ".py", 'w', encoding="utf-8") as file:
        file.write('{}'.format(job_code))


def delete_file(elt_map: EltMap, session: Session):
    job = job_composite_service.find(elt_map.job_uuid, session)
    file = "data_wave_engine/elt_jobs/" + job.job_id + ".py"
    if os.path.isfile(file):
        os.remove(file)
    else:
        logger.warning(msg="File Doesn't Exist")


def make_data_wave_raw_code(connection: ConnectionDto, elt_map: EltMap, session: Session) -> str:
    job = job_composite_service.find(elt_map.job_uuid, session)
    table_lists = []
    table_list_uuids = elt_map.table_list_uuids.split(', ')

    for table_list_uuid in table_list_uuids:
        table_list = table_composite_service.find(table_list_uuid, session)
        table_lists.append(table_list)

    get_data = ""
    if connection.db_type == Db_Type.SNOWFLAKE.name:
        get_data = make_snowflake_raw_code(connection, job, table_lists, session)

    elif connection.db_type == Db_Type.MYSQL.name:
        get_data = make_mysql_raw_code(connection, job, table_lists, session)

    elif connection.db_type in (Db_Type.REDSHIFT.name, Db_Type.POSTGRESQL.name):
        get_data = make_amazon_raw_code(connection, job, table_lists, session)

    return get_data
