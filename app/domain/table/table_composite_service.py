from typing import List

import mysql.connector
from sqlalchemy.orm import Session

from app.domain.connection import connection_composite_service
from app.domain.connection.db_type import Db_Type
from app.domain.table import table_list_service
from app.domain.table.table_list import Table_List
from app.domain.utils.query import *
from app.dto.table_list_dto import Table_List_Dto
from app.exception.cannot_show_table import CannotShowTable


def table_list(request: Table_List_Dto) -> Table_List:
    table_list = Table_List()
    table_list.connection_uuid = request.connection_uuid
    table_list.table_list = request.table_list
    table_list.rule_set = request.rule_set
    table_list.pk = request.pk

    return table_list


def create(request: Table_List_Dto, session: Session):
    list = table_list(request)
    table_list_service.save(list, session)


def delete(uuids: List[str], session: Session):
    for uuid in uuids:
        table_list = table_list_service.find(uuid, session)
        if not table_list:
            raise ConnectionNotFoundException()
        table_list_service.delete(table_list, session)


def find(uuid: str, session: Session) -> Table_List:
    table_list_service.find(uuid, session, True)


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
