from datetime import datetime
from typing import List

from pydantic import BaseModel
from app.domain.table.table_list import Table_List
from app.domain.utils import list_converter_util


class Base_Table_List_Dto(BaseModel):
    connection_uuid: str
    table_list: List[str]
    rule_set: List[int]
    pk: List[str]


class Table_List_Save_Dto(Base_Table_List_Dto):
    id: int = None
    uuid: str = None
    created_at: datetime = None
    updated_at: datetime = None

class Table_List_Dto(BaseModel):
    id: int = None
    uuid: str = None
    connection_uuid: str = None
    table_list: List[str] = None
    rule_set: List[int] = None
    pk: List[str] = None
    created_at: datetime = None
    updated_at: datetime = None

def of(table_list: Table_List) -> Table_List_Dto:
    response = Table_List_Dto()
    response.id = table_list.id
    response.uuid = table_list.uuid
    response.connection_uuid = table_list.connection_uuid
    response.table_list = list_converter_util.convert_string_to_str_list(table_list.table_list)
    response.rule_set = list_converter_util.convert_string_to_int_list(table_list.rule_set)
    response.pk = list_converter_util.convert_string_to_str_list(table_list.pk)
    response.created_at = table_list.created_at
    response.updated_at = table_list.updated_at

    return response
