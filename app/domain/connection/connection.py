from datetime import datetime

from sqlalchemy import Column, Integer, String, DateTime

from app.common.database import Base
from app.domain.utils import uuid_util


class Connection(Base):
    __tablename__ = "connections"

    id = Column(Integer, primary_key=True, index=True)
    uuid = Column(String, unique=True)
    name = Column(String, nullable=False)
    db_type = Column(String, nullable=False)
    host = Column(String, nullable=False)
    port = Column(String, nullable=False)
    account = Column(String)
    login_id = Column(String, nullable=False)
    password = Column(String, nullable=False)
    warehouse = Column(String)
    option = Column(String, default="?charset=utf8")
    role = Column(String)
    created_at = Column(DateTime, default=datetime.now(), nullable=False)
    updated_at = Column(DateTime, default=datetime.now(), nullable=False)

    def __init__(self):
        self.uuid = uuid_util.uuid()
