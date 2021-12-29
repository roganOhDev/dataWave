from typing import List

from sqlalchemy.orm import Session

from app.domain.elt_map import elt_map_repository as repository
from app.domain.elt_map.elt_map import EltMap


def find(uuid: str, session: Session, validate: bool) -> EltMap:
    elt_map = repository.find(uuid, session)
    if validate & (not elt_map):
        raise
    return elt_map


def save(request: EltMap, session: Session):
    repository.save(request, session)


def delete(uuids: List[str], session):
    for uuid in uuids:
        elt_map = find(uuid, session, True)
        repository.delete(elt_map, session)
