from typing import List

import requests
import client
from common.utils import request_util
from domain.dto.table_list_dto import Table_List_Dto


def find(uuid: str)-> Table_List_Dto:
    response = requests.get(url=client.Client.api_url + client.Client.Elt_Map + "?uuid=e9303c4d-8122-4f21-8eea-1dfbbf7dc882")

def get_activate_elt_map_uuids() -> List[str]:
    response = request_util.get(client.Client.Elt_Map.elt_map_using)
    return response.json()
#
