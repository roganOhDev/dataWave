from sqlalchemy.orm import Session

from app.domain.table import table_list_repository as repository
from app.domain.table.table_list import Table_List


def save(table_list: Table_List, session: Session):
    repository.save(table_list, session)


def delete(table_list: Table_List, session: Session):
    repository.delete(table_list, session)


def find(uuid: str, session: Session, validate: bool) -> Table_List:
    connection = repository.find(uuid, session)
    if validate & (not connection):
        raise ConnectionNotFoundException()
    return connection
