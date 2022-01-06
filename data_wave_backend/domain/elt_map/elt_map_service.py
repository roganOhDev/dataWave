from typing import List

from sqlalchemy.orm import Session

from domain.elt_map import elt_map_repository as repository
from domain.elt_map.elt_map import EltMap
from exception.elt_map_not_found_exception import EltMapNotFoundException


def find(uuid: str, session: Session, validate: bool) -> EltMap:
    elt_map = repository.find(uuid, session)
    if validate & (not elt_map):
        raise EltMapNotFoundException()
    return elt_map


def find_all_by_using(session: Session) -> List[EltMap]:
    return repository.find_all_by_using(session)


def save(request: EltMap, session: Session):
    repository.save(request, session)


def delete(elt_map: EltMap, session):
    repository.delete(elt_map, session)
