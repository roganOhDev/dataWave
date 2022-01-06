from typing import List

from sqlalchemy.orm import Session

from domain.elt_map.elt_map import EltMap


def find(uuid: str, session: Session) -> EltMap:
    return session.query(EltMap).filter(EltMap.uuid == uuid).first()

def find_all_by_using(session: Session) -> List[EltMap]:
    return session.query(EltMap).filter(EltMap.active).all()

def save(elt_map: EltMap, session: Session):
    session.add(elt_map)
    session.commit()
    session.refresh(elt_map)


def delete(elt_map: EltMap, session: Session):
    session.delete(elt_map)
    session.commit()
