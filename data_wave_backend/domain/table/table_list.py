from datetime import datetime

from sqlalchemy import Column, Integer, String, DateTime

from data_wave_backend.common.database import Base
from data_wave_backend.domain.utils import uuid_util


class Table_List(Base):
    __tablename__ = "table_list"

    id = Column(Integer, autoincrement=True, primary_key=True, index=True)
    uuid = Column(String, unique=True)
    connection_uuid = Column(String, nullable=False)
    column_info = Column(String, nullable=False)
    max_pk = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.now(), nullable=False)
    updated_at = Column(DateTime, default=datetime.now(), nullable=False)

    def __init__(self):
        self.uuid = uuid_util.uuid()
