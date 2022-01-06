import requests
from domain.call_api import client
from domain.dto.table_list_dto import Table_List_Dto


def find(uuid: str)-> Table_List_Dto:
    response = requests.get(url=client.api_url + client.elt_map + "?uuid=e9303c4d-8122-4f21-8eea-1dfbbf7dc882")
def a():
    try:
        # response = requests.get(url = client.api_url+"dag?uuid=6bf70256-3bf2-11ec-a910-8c85907aa0b6")
        response = requests.get(url =client.api_url + "job?uuid=6bf70256-3bf2-11ec-a91-8c85907aa0b6")
        if response.status_code >=400:
            print(response.content.decode('UTF-8'))
        else:
            print(response.json())
    finally:
        print("fin")
#
