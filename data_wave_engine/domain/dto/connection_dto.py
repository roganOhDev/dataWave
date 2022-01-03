from datetime import datetime

from pydantic import BaseModel

class BaseConnectionSaveDto(BaseModel):
    name: str
    db_type: str
    host: str
    port: str
    account: str = ''
    user: str
    password: str
    database: str
    warehouse: str = ''
    option: str = ''
    role: str = ''


class ConnectionSaveDto(BaseConnectionSaveDto):
    id: int = None
    uuid: str = None
    created_at: datetime = None
    updated_at: datetime = None

class ConnectionDto(BaseModel):
    id: int = None
    uuid: str = None
    name: str = None
    db_type: str = None
    host: str = None
    port: str = None
    account: str = None
    user: str = None
    password: str = None
    database: str = None
    warehouse: str = None
    option: str = None
    role: str = None
    created_at: datetime = None
    updated_at: datetime = None
