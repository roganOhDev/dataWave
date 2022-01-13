import socket


def get_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    return s.getsockname()[0]


class Client:
    # api_url = get_ip()
    api_url = "http://192.168.35.9:8000/"

    class Elt_Map:
        elt_map = "elt_map"
        elt_map_using = "elt_map/activated"

    class Job:
        job = "job"
        job_using = "job/using"

    class Table:
        table_list = "table_list"
        update_pk_max = table_list+"/{uuid}?pk_max={pk_max}"
