from datetime import datetime
from typing import List

from sqlalchemy.orm import Session

from app.domain.connection import connection_service as service
from app.domain.connection import db_type
from app.domain.connection.connection import Connection
from app.dto import connection_dto
from app.exception.already_exists_connection_name import AlreadyExitsConnectionName
from app.exception.connection_not_found_exception import ConnectionNotFoundException
from app.exception.empty_value_exception import EmptyValueException
from app.exception.not_supported_db_type_exception import NotSupportedDbTypeException


def save(request):
    connection = Connection(request)
    validate(connection)


def save(request: connection_dto.BaseConnectionDto, session: Session):
    connection = connection_info(request, session)
    service.save(connection, session)


def update(uuid: str, request: connection_dto.BaseConnectionDto, session: Session):
    connection = service.find(uuid, session, True)
    validate_connection_name(request.name, session)

    connection.name = connection.name if not request.name else request.name
    connection.db_type = connection.db_type if not request.db_type else request.db_type
    connection.host = connection.host if not request.host else request.host
    connection.port = connection.port if not request.port else request.port
    connection.account = connection.account if not request.account else request.account
    connection.login_id = connection.login_id if not request.login_id else request.login_id
    connection.password = connection.password if not request.password else request.password
    connection.warehouse = connection.warehouse if not request.warehouse else request.warehouse
    connection.option = connection.option if not request.option else request.option
    connection.role = connection.role if not request.role else request.role
    connection.updated_at = datetime.now()

    service.save(connection, session)


def find(uuid: str, session: Session) -> connection_dto.ConnectionDto:
    dag = service.find(uuid, session, True)
    return connection_dto.of(dag)


def delete(uuids: List[str], session: Session):
    for uuid in uuids:
        service.delete(uuid, session)


def connection_info(request: connection_dto.BaseConnectionDto, session: Session) -> Connection:
    validate_connection_name(request.name, session)

    connection = Connection()
    connection.name = request.name
    connection.db_type = request.db_type
    connection.host = "" if not request.host else request.host
    connection.port = "" if not request.port else request.port
    connection.account = "" if not request.account else request.account
    connection.login_id = request.login_id
    connection.password = request.password
    connection.warehouse = "" if not request.warehouse else request.warehouse
    connection.option = connection.option if not request.option else request.option
    connection.role = "" if not request.role else request.role
    return connection


def validate_connection_name(connection_name: str, session: Session):
    if (connection_name is not "") & service.is_exist_connection_name(connection_name, session):
        raise AlreadyExitsConnectionName()


def validate(connection: Connection):
    if connection is None:
        raise ConnectionNotFoundException()
    elif connection.db_type not in (
            db_type.DbType.MYSQL.name, db_type.DbType.REDSHIFT.name, db_type.DbType.POSTGRESQL.name):
        raise NotSupportedDbTypeException()
    elif connection.db_type is db_type.DbType.SNOWFLAKE.name:
        if not connection.account:
            raise EmptyValueException()
