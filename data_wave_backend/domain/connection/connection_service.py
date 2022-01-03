from sqlalchemy.orm import Session

from domain.connection import connection_repository as repository
from domain.connection.connection import Connection
from exception.connection_not_found_exception import ConnectionNotFoundException


def find(uuid: str, session: Session, validate: bool) -> Connection:
    connection = repository.find(uuid, session)
    if validate & (not connection):
        raise ConnectionNotFoundException()
    return connection


def delete(connection: Connection, session: Session):
    repository.delete(connection, session)


def save(connection: Connection, session: Session):
    repository.save(connection, session)
