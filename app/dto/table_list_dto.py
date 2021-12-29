from datetime import datetime
from typing import List

from pydantic import BaseModel
from app.domain.table.table_list import Table_List


class Base_Table_List_Dto(BaseModel):
    connection_uuid: str
    table_list: List[str]
    rule_set: List[int]
    pk: List[str]


class Table_List_Dto(Base_Table_List_Dto):
    id: int = None
    uuid: str = None
    created_at: datetime = None
    updated_at: datetime = None

def of(table_list: Table_List) -> Table_List_Dto:
    response = Table_List_Dto()
    response.id = table_list.id
    response.uuid = table_list.uuid
    response.connection_uuid = table_list.connection_uuid
    response.table_list = table_list.table_list
    response.rule_set = table_list.rule_set
    response.pk = table_list.pk
    response.created_at = table_list.created_at
    response.updated_at = table_list.updated_at

    return response
