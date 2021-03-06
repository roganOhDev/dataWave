from typing import List

import requests

import client
from common.utils import request_util
from domain.dto.table_list_dto import Table_List_Dto, Table_List_Update_Dto
from exception.api_exception import ApiException


def find(uuid: str) -> Table_List_Dto:
    response = requests.get(url=client.Client.api_url + client.Client.Table.table_list + "?uuid=" + uuid)
    if response.status_code != 200:
        raise ApiException(response.json()['detail'])
    return Table_List_Dto(**response.json())


def get_activate_elt_map_uuids() -> List[str]:
    response = request_util.get(client.Client.EltMap.elt_map_using)
    return response.json()


def update_pk_max(pk_max: int, uuid: str):
    response = request_util.patch(url=client.Client.Table.update_pk_max.format(uuid, pk_max), body={})
    if response.status_code != 200:
        raise ApiException(response.json()['detail'])
