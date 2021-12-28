from datetime import datetime

from pydantic import BaseModel

from app.domain.connection.connection import Connection


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

def of(connection: Connection) -> ConnectionDto:
    connection_dto = ConnectionDto()
    connection_dto.id = connection.id
    connection_dto.uuid = connection.uuid
    connection_dto.name = connection.name
    connection_dto.db_type = connection.db_type
    connection_dto.host = connection.host
    connection_dto.port = connection.port
    connection_dto.account = connection.account
    connection_dto.user = connection.user
    connection_dto.password = connection.password
    connection_dto.database = connection.database
    connection_dto.warehouse = connection.warehouse
    connection_dto.option = connection.option
    connection_dto.role = connection.role
    connection_dto.created_at = connection.created_at
    connection_dto.updated_at = connection.updated_at
    return connection_dto

