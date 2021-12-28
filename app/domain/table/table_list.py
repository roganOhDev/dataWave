from datetime import datetime

from sqlalchemy import Column, Integer, String, DateTime, ARRAY

from app.common.database import Base
from app.domain.utils import uuid_util


class Table_List(Base):
    __tablename__ = "table_list"

    id = Column(Integer, autoincrement=True, primary_key=True, index=True)
    uuid = Column(String, unique=True)
    connection_uuid = Column(String, nullable=False)
    table_list = Column(ARRAY(String), nullable=False)
    rule_set = Column(ARRAY(int), nullable=False)
    pk = Column(ARRAY(String), nullable=False)
    created_at = Column(DateTime, default=datetime.now(), nullable=False)
    updated_at = Column(DateTime, default=datetime.now(), nullable=False)

    def __init__(self):
        self.uuid = uuid_util.uuid()
