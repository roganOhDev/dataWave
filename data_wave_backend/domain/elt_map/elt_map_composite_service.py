import os
from datetime import datetime

from data_wave_backend.domain.connection import connection_composite_service
from data_wave_backend.domain.connection.db_type import Db_Type
from data_wave_backend.domain.dag import dag_composite_service
from data_wave_backend.domain.elt_map import elt_map_service as service
from data_wave_backend.domain.elt_map.create_dag.dag_format import dag_format
from data_wave_backend.domain.elt_map.create_dag.db_vendor_raw_code import *
from data_wave_backend.domain.elt_map.create_dag.get_sql_alchemy_conn import get_sql_alchemy_conn
from data_wave_backend.domain.elt_map.elt_map import EltMap
from data_wave_backend.domain.table import table_composite_service
from data_wave_backend.domain.utils.list_converter_util import *
from data_wave_backend.domain.utils.logger import logger
from data_wave_backend.dto.elt_map_dto import EltMapDto, of, EltMapSaveDto
from data_wave_backend.exception.caanot_use_this_dag_exception import CannotUseThisDagException
from data_wave_backend.exception.connections_are_not_equal import ConnectionsAreNotEqual


def elt_map_info(elt_map_info_dto: EltMapSaveDto, session: Session, elt_map: EltMap) -> EltMap:
    table_list = table_composite_service.find(elt_map_info_dto.table_list_uuids[0], session)

    elt_map.dag_uuid = elt_map_info_dto.dag_uuid
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
    if request.dag_uuid:
        dag_composite_service.update_dag_usage(request, session)
    service.save(elt_map, session)


def update(uuid: str, request: EltMapSaveDto, session: Session):
    elt_map = service.find(uuid, session, True)
    if elt_map.dag_uuid != request.dag_uuid:
        validate(request, session)
        dag_composite_service.update_dag_usage(elt_map, session)
        dag_composite_service.update_dag_usage(request, session)
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
    if request.dag_uuid:
        dag = dag_composite_service.find(request.dag_uuid, session)
        if dag.using:
            raise CannotUseThisDagException()

    connection_uuids = []
    for table_list_uuid in request.table_list_uuids:
        connection_uuids.append(table_composite_service.find(table_list_uuid, session).connection_uuid)
    if all(connection_uuid == connection_uuids[0] for connection_uuid in connection_uuids):
        pass
    else:
        raise ConnectionsAreNotEqual()


def create_file(elt_map: EltMap, session: Session):
    dag = dag_composite_service.find(elt_map.dag_uuid, session)

    extract_data_raw_code = get_extract_data_wave_raw_code(elt_map, session)
    load_data_raw_code = get_load_data_wave_raw_code(elt_map, session)

    dag_code = dag_format.format(dag_id=dag.dag_id,
                                 extract_task=extract_data_raw_code,
                                 load_task=load_data_raw_code,
                                 catchup=dag.catchup,
                                 schedule_interval=dag.schedule_interval,
                                 owner=dag.owner,
                                 start_date=dag.start_date,
                                 )

    with open(dag.airflow_home + "/dags/" + dag.dag_id + ".py", 'w', encoding="utf-8") as file:
        file.write('{}'.format(dag_code))


def delete_file(elt_map: EltMap, session: Session):
    dag = dag_composite_service.find(elt_map.dag_uuid, session)
    file = dag.airflow_home + "/dags/" + dag.dag_id + ".py"
    if os.path.isfile(file):
        os.remove(file)
    else:
        logger.warning(msg="File Doesn't Exist")


def get_extract_data_wave_raw_code(elt_map: EltMap, session: Session) -> str:
    return make_data_wave_raw_code(elt_map, 'extract', session)


def get_load_data_wave_raw_code(elt_map: EltMap, session: Session) -> str:
    return make_data_wave_raw_code(elt_map, 'load', session)


def make_data_wave_raw_code(elt_map: EltMap, connection_type, session: Session) -> str:
    connection = \
        connection_composite_service.find(elt_map.integrate_connection_uuid, session) if connection_type == 'extract' \
            else connection_composite_service.find(elt_map.destination_connection_uuid, session)
    dag = dag_composite_service.find(elt_map.dag_uuid, session)
    table_lists = []
    table_list_uuids = elt_map.table_list_uuids.split(', ')

    for table_list_uuid in table_list_uuids:
        table_list = table_composite_service.find(table_list_uuid, session)
        table_lists.append(table_list)

    get_data = ""
    if connection.db_type == Db_Type.SNOWFLAKE.name:
        get_data = make_snowflake_raw_code(connection, dag, table_lists, connection_type, session)

    elif connection.db_type == Db_Type.MYSQL.name:
        get_data = make_mysql_raw_code(connection, dag, table_lists, connection_type, session)

    elif connection.db_type in (Db_Type.REDSHIFT.name, Db_Type.POSTGRESQL.name):
        get_data = make_amazon_raw_code(connection, dag, table_lists, connection_type, session)

    return get_data
