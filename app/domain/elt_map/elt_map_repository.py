from sqlalchemy.orm import Session

from app.domain.elt_map.elt_map import EltMap


def find(uuid: str, session: Session, validate: bool) -> EltMap:
    return session.query(EltMap).filter(EltMap.uuid == uuid).first()


def save(elt_map: EltMap, session: Session):
    session.add(elt_map)
    session.commit()
    session.refresh(elt_map)


def delete(elt_map: EltMap, session: Session):
    session.delete(elt_map)
    session.commit()
