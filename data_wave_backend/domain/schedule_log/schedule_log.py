from datetime import datetime

from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey

from common.database import Base
from common.utils import uuid_util


class ScheduleLog(Base):
    __tablename__ = "schedule_logs"

    id = Column(Integer, autoincrement=True, primary_key=True, index=True)
    uuid = Column(String, unique=True)
    job_uuid = Column(String, ForeignKey('job_infoes.uuid'))
    result = Column(String, nullable=False)
    error_message = Column(String)
    created_at = Column(DateTime, default=datetime.now(), nullable=False)

    def __init__(self):
        self.uuid = uuid_util.uuid()
