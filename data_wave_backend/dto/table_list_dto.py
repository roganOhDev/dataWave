from datetime import datetime
from typing import List

from pydantic import BaseModel

from domain.table.table_list import Table_List


class Column_Info(BaseModel):
    table_name: str
    rule_set: int
    columns: List[str]
    pk: str
    update_column_name: str


class Base_Table_List_Dto(BaseModel):
    id: int = None
    uuid: str = None
    connection_uuid: str
    max_pk: int = 0
    created_at: datetime = None
    updated_at: datetime = None


class Table_List_Create_Dto(Base_Table_List_Dto):
    columns_info: List[Column_Info]

class Table_List_Update_Dto(Base_Table_List_Dto):
    column_info: Column_Info


class Table_List_Dto(BaseModel):
    id: int = None
    uuid: str = None
    connection_uuid: str = None
    columns_info: str = None
    max_pk: int = None
    created_at: datetime = None
    updated_at: datetime = None


def of(table_list: Table_List) -> Table_List_Dto:
    response = Table_List_Dto()
    response.id = table_list.id
    response.uuid = table_list.uuid
    response.connection_uuid = table_list.connection_uuid
    response.columns_info = table_list.column_info
    response.max_pk = table_list.max_pk
    response.created_at = table_list.created_at
    response.updated_at = table_list.updated_at

    return response
