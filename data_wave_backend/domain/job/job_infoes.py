from datetime import datetime

from sqlalchemy import Column, Integer, String, DateTime, Boolean, Date

from common.database import Base
from common.utils import uuid_util


class JobInfo(Base):
    __tablename__ = "job_infoes"

    id = Column(Integer, autoincrement=True, primary_key=True, index=True)
    uuid = Column(String, unique=True)
    job_id = Column(String, nullable=False)
    owner = Column(String, default="Rogan")
    schedule_interval = Column(String, default="@once", nullable=False)
    csv_files_directory = Column(String, default="@once", nullable=False)
    start_date = Column(String, nullable=False)
    using = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, default=datetime.now(), nullable=False)
    updated_at = Column(DateTime, default=datetime.now(), nullable=False)

    def __init__(self):
        self.uuid = uuid_util.uuid()
