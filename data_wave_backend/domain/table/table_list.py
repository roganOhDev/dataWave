from datetime import datetime

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey

from common.database import Base
from common.utils import uuid_util


class Table_List(Base):
    __tablename__ = "table_list"

    id = Column(Integer, autoincrement=True, primary_key=True, index=True)
    uuid = Column(String, unique=True)
    connection_uuid = Column(String, ForeignKey('connections.uuid'), nullable=False)
    column_info = Column(String, nullable=False)
    max_pk = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.now(), nullable=False)
    updated_at = Column(DateTime, default=datetime.now(), nullable=False)

    def __init__(self):
        self.uuid = uuid_util.uuid()
