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
