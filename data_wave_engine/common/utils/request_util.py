import requests

from domain.call_api.client import Client


def get(url: str):
    return requests.get(url=Client.api_url+url)

def put(url: str, body: dict):
    return requests.get(url=Client.api_url+url,data=body)

def delete(url: str):
    return requests.get(url=Client.api_url+url)
