from sqlalchemy.orm import Session

from app.domain.connection import connection_repository as repository
from app.domain.connection.connection import Connection
from app.exception.connection_not_found_exception import ConnectionNotFoundException


def find(uuid: str, session: Session, validate: bool) -> Connection:
    connection = repository.find(uuid, session)
    if validate & (not connection):
        raise ConnectionNotFoundException()
    return connection


def delete(uuid: str, session: Session):
    connection = repository.find(uuid, session)
    if not connection:
        raise ConnectionNotFoundException()
    repository.delete(connection, session)


def save(connection: Connection, session: Session):
    repository.save(connection, session)
