from typing import List

from sqlalchemy.orm import Session

from data_wave_backend.domain.elt_map import elt_map_repository as repository
from data_wave_backend.domain.elt_map.elt_map import EltMap
from data_wave_backend.exception.elt_map_not_found_exception import EltMapNotFoundException


def find(uuid: str, session: Session, validate: bool) -> EltMap:
    elt_map = repository.find(uuid, session)
    if validate & (not elt_map):
        raise EltMapNotFoundException()
    return elt_map


def save(request: EltMap, session: Session):
    repository.save(request, session)


def delete(elt_map: EltMap, session):
    repository.delete(elt_map, session)
