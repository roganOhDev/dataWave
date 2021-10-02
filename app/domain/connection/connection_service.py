import pandas as pd

from app.domain.utils import json_util
from app.domain.utils import os_util


def pwd():
    return os_util.before_abs_pwd_with_join(__file__, "entity/connections.txt", 4)


def save(data):
    with open(pwd(), 'a', encoding="utf-8") as f:
        f.write(data + '\n')
        f.close()


def is_exist(name: str):
    with open(pwd(), 'r', encoding="utf-8") as f:
        lines = f.readlines()
        for line in lines:
            if json_util.loads(line)['name'] == name:
                return True
        f.close()
    return False
