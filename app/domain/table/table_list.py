from datetime import datetime

from sqlalchemy import Column, Integer, String, DateTime
from pyxtension.streams import stream

from app.common.database import Base
from app.domain.utils import uuid_util
from app.dto.table_list_dto import Table_List_Dto


class Table_List(Base):
    __tablename__ = "table_list"

    id = Column(Integer, autoincrement=True, primary_key=True, index=True)
    uuid = Column(String, unique=True)
    connection_uuid = Column(String, nullable=False)
    table_list = Column(String, nullable=False)
    rule_set = Column(String, nullable=False)
    pk = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.now(), nullable=False)
    updated_at = Column(DateTime, default=datetime.now(), nullable=False)

    def __init__(self):
        self.uuid = uuid_util.uuid()


def of(table_list: Table_List, request: Table_List_Dto) -> Table_List:
    table_list.connection_uuid = request.connection_uuid
    table_list.table_list = convert_str_list_to_string(request.table_list)
    table_list.rule_set = convert_int_list_to_string(request.rule_set)
    table_list.pk = convert_str_list_to_string(request.pk)
    table_list.updated_at = datetime.now()

    return table_list


def convert_str_list_to_string(str_list) -> str:
    return ','.join(str_list)


def convert_int_list_to_string(int_list) -> str:
    return ','.join(stream(int_list).map(lambda x: str(x)))
