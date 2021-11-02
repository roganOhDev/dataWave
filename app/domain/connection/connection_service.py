from sqlalchemy.orm import Session

from app.domain.connection.connection import Connection
from app.exception.connection_not_found_exception import ConnectionNotFoundException


def is_exist_connection_name(connection_name: str, session: Session) -> bool:
    return True if find_by_dag_id(connection_name, session) is not None else False


def find_by_dag_id(connection_name: str, session: Session) -> Connection:
    return session.query(Connection).filter(Connection.name == connection_name).first()


def find(uuid: str, session: Session, validate: bool) -> Connection:
    connection = session.query(Connection).filter(Connection.uuid == uuid).first()
    if validate & (not connection):
        raise ConnectionNotFoundException()
    return connection


def delete(uuid: str, session: Session):
    connection = session.query(Connection).filter(Connection.uuid == uuid).first()
    if not connection:
        raise ConnectionNotFoundException()
    session.delete(connection)
    session.commit()


def save(connection: Connection, session: Session):
    session.add(connection)
    session.commit()
    session.refresh(connection)