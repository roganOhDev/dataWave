import requests
import client
from domain.dto.table_list_dto import Table_List_Dto


def find(uuid: str)-> Table_List_Dto:
    response = requests.get(url=client.api_url+"elt_map?uuid=e9303c4d-8122-4f21-8eea-1dfbbf7dc882")
print(response)
print(response.json())