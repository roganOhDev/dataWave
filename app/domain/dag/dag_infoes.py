from datetime import datetime

from sqlalchemy import Column, Integer, String, DateTime, Boolean, Date

from app.common.database import Base
from app.domain.utils import uuid_util


class DagInfo(Base):
    __tablename__ = "dag_infoes"

    id = Column(Integer, autoincrement=True, primary_key=True, index=True)
    uuid = Column(String, unique=True)
    dag_id = Column(String, nullable=False)
    airflow_home = Column(String, nullable=False)
    owner = Column(String, default="Rogan")
    catchup = Column(Boolean, default=False)
    schedule_interval = Column(String, default="@once", nullable=False)
    csv_files_directory = Column(String, default="@once", nullable=False)
    yesterday = Column(Date, nullable=False)
    start_date = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.now(), nullable=False)
    updated_at = Column(DateTime, default=datetime.now(), nullable=False)

    def __init__(self):
        self.uuid = uuid_util.uuid()
