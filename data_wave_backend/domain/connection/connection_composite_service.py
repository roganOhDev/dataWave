from datetime import datetime
from typing import List

from sqlalchemy.orm import Session

from data_wave_backend.domain.connection import connection_service as service
from data_wave_backend.domain.connection import db_type
from data_wave_backend.domain.connection.connection import Connection
from data_wave_backend.dto import connection_dto
from data_wave_backend.exception.connection_not_found_exception import ConnectionNotFoundException
from data_wave_backend.exception.empty_value_exception import EmptyValueException
from data_wave_backend.exception.not_supported_db_type_exception import NotSupportedDbTypeException


def save(request: connection_dto.BaseConnectionSaveDto, session: Session):
    connection = connection_info(request)
    validate(connection)
    service.save(connection, session)


def update(uuid: str, request: connection_dto.BaseConnectionSaveDto, session: Session):
    connection = service.find(uuid, session, True)

    connection.name = connection.name if not request.name else request.name
    connection.db_type = connection.db_type if not request.db_type else request.db_type
    connection.host = connection.host if not request.host else request.host
    connection.port = connection.port if not request.port else request.port
    connection.account = connection.account if not request.account else request.account
    connection.user = connection.user if not request.user else request.user
    connection.password = connection.password if not request.password else request.password
    connection.warehouse = connection.warehouse if not request.warehouse else request.warehouse
    connection.option = connection.option if not request.option else request.option
    connection.role = connection.role if not request.role else request.role
    connection.updated_at = datetime.now()

    service.save(connection, session)


def find(uuid: str, session: Session) -> connection_dto.ConnectionDto:
    connection = service.find(uuid, session, True)
    return connection_dto.of(connection)


def delete(uuids: List[str], session: Session):
    for uuid in uuids:
        connection = service.find(uuid, session, True)
        service.delete(connection, session)


def connection_info(request: connection_dto.BaseConnectionSaveDto) -> Connection:
    connection = Connection()
    connection.name = request.name
    connection.db_type = request.db_type
    connection.host = "" if not request.host else request.host
    connection.port = "" if not request.port else request.port
    connection.account = "" if not request.account else request.account
    connection.user = request.user
    connection.password = request.password
    connection.database = "" if not request.database else request.database
    connection.warehouse = "" if not request.warehouse else request.warehouse
    connection.option = connection.option if not request.option else request.option
    connection.role = "" if not request.role else request.role
    return connection


def validate(connection: Connection):
    if connection is None:
        raise ConnectionNotFoundException()
    elif connection.db_type not in (
            db_type.Db_Type.MYSQL.name, db_type.Db_Type.REDSHIFT.name, db_type.Db_Type.POSTGRESQL.name):
        raise NotSupportedDbTypeException()
    elif connection.db_type is db_type.Db_Type.SNOWFLAKE.name:
        if not connection.account:
            raise EmptyValueException()
