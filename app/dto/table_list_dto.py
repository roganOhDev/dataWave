from datetime import datetime
from typing import List

from pydantic import BaseModel


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
