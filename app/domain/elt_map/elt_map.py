from datetime import datetime

from sqlalchemy import Column, Integer, String, DateTime, Boolean, Date

from app.common.database import Base
from app.domain.utils import uuid_util


class EltMap(Base):
    __tablename__ = "elt_maps"

    id = Column(Integer, autoincrement=True, primary_key=True, index=True)
    uuid = Column(String, unique=True)
    integrate_connection_uuid = Column(String, nullable=False)
    destination_connection_uuid = Column(String, nullable=False)
    table_list_uuid = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.now(), nullable=False)
    updated_at = Column(DateTime, default=datetime.now(), nullable=False)

    def __init__(self):
        self.uuid = uuid_util.uuid()
