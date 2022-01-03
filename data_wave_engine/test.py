import requests
import client

response = requests.get(url=client.api_url+"elt_map?uuid=e9303c4d-8122-4f21-8eea-1dfbbf7dc882")
print(response.json())