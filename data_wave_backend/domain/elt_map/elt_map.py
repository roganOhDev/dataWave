from datetime import datetime

from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey

from common.database import Base
from common.utils import uuid_util


class EltMap(Base):
    __tablename__ = "elt_maps"

    id = Column(Integer, autoincrement=True, primary_key=True, index=True)
    uuid = Column(String, unique=True)
    job_uuid = Column(String, ForeignKey('job_infoes.uuid'))
    integrate_connection_uuid = Column(String, ForeignKey('connections.uuid'), nullable=False)
    destination_connection_uuid = Column(String, ForeignKey('connections.uuid'), nullable=False)
    table_list_uuids = Column(String, ForeignKey('table_list.uuid'), nullable=False)
    active = Column(Boolean, default=False, nullable=False)
    status = Column(Integer, default=0, nullable=False)
    created_at = Column(DateTime, default=datetime.now(), nullable=False)
    updated_at = Column(DateTime, default=datetime.now(), nullable=False)

    def __init__(self):
        self.uuid = uuid_util.uuid()
