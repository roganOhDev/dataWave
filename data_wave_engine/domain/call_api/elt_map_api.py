from typing import List

from client import Client
from common.utils import request_util
from domain.dto.elt_map_dto import EltMapSaveDto
from domain.dto.emt_map_status import EltMapStatus
from exception.api_exception import ApiException


def get_activate_job_uuids() -> List[str]:
    response = request_util.get(Client.EltMap.elt_map_using)
    if response.status_code != 200:
        raise ApiException(response.json()['detail'])

    return response.json()


def update_status(job_id: str, status: EltMapStatus):
    elt_map = EltMapSaveDto()
    elt_map.job_id = job_id
    elt_map.status = status.value

    response = request_util.put(url=Client.EltMap.update, body=dict(elt_map))
    if response.status_code != 200:
        raise ApiException(response.json()['detail'])
