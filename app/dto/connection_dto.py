from datetime import datetime

from pydantic import BaseModel

from app.domain.connection.connection import Connection


class BaseConnectionDto(BaseModel):
    name: str
    db_type: str
    host: str
    port: str
    account: str = ''
    login_id: str
    password: str
    warehouse: str = ''
    option: str = ''
    role: str = ''


class ConnectionDto(BaseConnectionDto):
    id: int = None
    uuid: str = None
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
    connection_dto.login_id = connection.login_id
    connection_dto.password = connection.password
    connection_dto.warehouse = connection.warehouse
    connection_dto.option = connection.option
    connection_dto.role = connection.role
    return connection
