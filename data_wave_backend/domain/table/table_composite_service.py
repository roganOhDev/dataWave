from datetime import datetime
from typing import List

import mysql.connector
from sqlalchemy.orm import Session

from domain.connection import connection_composite_service
from domain.connection.db_type import Db_Type
from domain.table import table_list_service
from domain.table.table_list import Table_List
from common.utils.json_util import json
from common.utils.query import *
from dto.table_list_dto import Table_List_Create_Dto, of, Table_List_Dto, Table_List_Update_Dto
from exception.cannot_show_table import CannotShowTable
from exception.columns_not_include_pk_exception import ColumnsNotIncludePk


def create(request: Table_List_Create_Dto, session: Session) -> List[str]:
    return_uuid = []
    validate(request)

    for column_info in request.columns_info:
        list = Table_List()

        list.connection_uuid = request.connection_uuid
        list.column_info = json(column_info)
        list.updated_at = datetime.now()

        return_uuid.append(list.uuid)
        table_list_service.save(list, session)

    return return_uuid


def update(uuid: str, request: Table_List_Update_Dto, session: Session) -> Table_List_Dto:
    table_list = table_list_service.find(uuid, session, True)

    table_list.connection_uuid = request.connection_uuid
    table_list.column_info = request.column_info
    table_list.max_pk = request.max_pk
    table_list.updated_at = datetime.now()

    table_list_service.save(table_list, session)
    return of(table_list)

def update_pk_max(uuid: str, pk_max: int, session: Session) -> Table_List_Dto:
    table_list = table_list_service.find(uuid, session, True)

    table_list.max_pk = pk_max

    table_list_service.save(table_list, session)
    return of(table_list)

def delete(uuids: List[str], session: Session):
    for uuid in uuids:
        table_list = table_list_service.find(uuid, session, True)
        table_list_service.delete(table_list, session)


def find(uuid: str, session: Session) -> Table_List_Dto:
    table_list = table_list_service.find(uuid, session, True)
    return of(table_list)

def find_tables(connection_uuid: str, session: Session) -> [str]:
    connection = connection_composite_service.find(connection_uuid, session)
    tables = []

    if connection.db_type.__eq__(Db_Type.MYSQL.name):
        try:
            with mysql.connector.connect(host=connection.host, port=connection.port, database=connection.database,
                                         user=connection.user, password=connection.password) as conn:

                cursor = conn.cursor()
                sql = Mysql.show_tables
                cursor.execute(sql)
                results = cursor.fetchall()
        except mysql.connector.Error as e:
            raise CannotShowTable(e.msg)

    for result in results:
        tables.append(*result)

    return tables


def validate(request: Table_List_Create_Dto):
    for column_info in request.columns_info:
        if column_info.pk not in column_info.columns:
            raise ColumnsNotIncludePk()
