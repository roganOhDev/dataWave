from datetime import datetime

from pydantic import BaseModel


class Base_Table_List_Dto(BaseModel):
    connection_uuid: str
    table_list: [str]
    rule_set: [int]
    pk: [str]


class Table_List_Dto(Base_Table_List_Dto):
    id: int = None
    uuid: str = None
    created_at: datetime = None
    updated_at: datetime = None
