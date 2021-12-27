from sqlalchemy.orm import Session

from app.domain.connection.connection import Connection


def find(uuid: str, session: Session) -> Connection:
    return session.query(Connection).filter(Connection.uuid == uuid).first()


def delete(connection: Connection, session: Session):
    session.delete(connection)
    session.commit()


def save(connection: Connection, session: Session):
    session.add(connection)
    session.commit()
    session.refresh(connection)
