from pyxtension.streams import stream


def convert_str_list_to_string(str_list) -> str:
    return ','.join(str_list)


def convert_int_list_to_string(int_list) -> str:
    return ','.join(stream(int_list).map(lambda x: str(x)))
