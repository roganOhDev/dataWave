from typing import List

from client import Client
from common.utils import request_util


def get_activate_job_uuids() -> List[str]:
    response = request_util.get(Client.Elt_Map.elt_map_using)
    return response.json()
