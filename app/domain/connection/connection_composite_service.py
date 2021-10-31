from app.domain.connection import connection_service as service
from app.domain.connection import db_type
from app.domain.connection.connection import Connection
from app.domain.utils.json_util import json
from app.exception.already_exists_connection_name import AlreadyExitsConnectionName
from app.exception.empty_value_exception import EmptyValueException


def save(request):
    connection = Connection(request)
    validate(connection)
    service.save(json(connection))


def validate(request: Connection):
    if service.is_exist(request.name):
        raise AlreadyExitsConnectionName()
    elif request.db_type in (db_type.DbType.MYSQL.name, db_type.DbType.REDSHIFT.name, db_type.DbType.POSTGRESQL.name):
        if (not request.host) | (not request.port):
            raise EmptyValueException()
    elif request.db_type is db_type.DbType.SNOWFLAKE.name:
        if not request.account:
            raise EmptyValueException()
