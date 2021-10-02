from datetime import datetime

from app.domain.connection.db_type import DbType
from app.domain.utils.uuid_util import uuid
from app.dto.connection_dto import ConnectionDto


class Connection:
    def __init__(self, request: ConnectionDto):
        self.name: str = ''
        self.db_type: str
        self.host: str = ''
        self.port: str = ''
        self.account: str = ''
        self.id: str
        self.pwd: str
        self.warehouse: str = ''
        self.option: str = '?charset=utf8'
        self.role: str = ''
        self.uuid: str = uuid()
        self.created_at: datetime = datetime.now()
        self.updated_at: datetime = datetime.now()
        if request is not None:
            self.name = request.name
            self.db_type = DbType(request.db_type).name
            self.host = request.host
            self.port = request.port
            self.id = request.id
            self.pwd = request.pwd
            self.warehouse = request.warehouse
            self.option = request.option
            self.role = request.role
