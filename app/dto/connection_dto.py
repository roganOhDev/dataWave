from pydantic import BaseModel

from app.domain.connection.db_type import DbType


class ConnectionDto(BaseModel):
    name: str
    db_type: DbType
    host: str = ''
    port: str = ''
    account: str = ''
    id: str
    pwd: str
    warehouse: str = ''
    option: str = ''
    role: str = ''
