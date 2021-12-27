from sqlalchemy.orm import Session

from app.domain.table.table_list import Table_List


def save(table_list: Table_List, session: Session):
    session.add(table_list)
    session.commit()
    session.refresh(table_list)


def delete(table_list: Table_List, session: Session):
    session.delete(table_list)
    session.commit()


def find(uuid: str, session: Session) -> Table_List:
    return session.query(Table_List).filter(Table_List.uuid == uuid).first()
