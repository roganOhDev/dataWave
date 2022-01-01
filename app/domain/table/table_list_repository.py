from sqlalchemy.orm import Session

from app.domain.table.table_list import Table_List
from app.domain.utils.logger import logger
from app.exception.api_exception import ApiException


def save(table_list: Table_List, session: Session):
    try:
        session.add(table_list)
        session.commit()
        session.refresh(table_list)
    except Exception as e:
        logger.error(e)
        raise ApiException(str(e))
    finally:
        session.close()


def delete(table_list: Table_List, session: Session):
    try:
        session.delete(table_list)
        session.commit()
    except Exception as e:
        logger.error(e)
        raise ApiException(str(e))
    finally:
        session.close()


def find(uuid: str, session: Session) -> Table_List:
    try:
        return session.query(Table_List).filter(Table_List.uuid == uuid).first()
    except Exception as e:
        logger.error(e)
        raise ApiException(str(e))
    finally:
        session.close()
