from datetime import datetime
from typing import List

from pydantic import BaseModel


class Column_Info(BaseModel):
    table_name: str
    rule_set: int
    columns: List[str]
    pk: str


class Base_Table_List_Dto(BaseModel):
    id: int = None
    uuid: str = None
    connection_uuid: str
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
