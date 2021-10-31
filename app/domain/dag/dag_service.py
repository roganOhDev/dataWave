import pandas as pd

from app.domain.utils import json_util
from app.domain.utils import os_util
from app.exception.dag_not_found_exception import DagNotFoundException


def pwd():
    return os_util.before_abs_pwd_with_join(__file__, "entity/dags.txt", 4)


def save(data):
    with open(pwd(), 'a', encoding="utf-8") as f:
        f.write(data + '\n')
        f.close()

def find(uuid: str):
    with open(pwd(), 'r', encoding="utf-8") as f:
        lines = f.readlines()
        for line in lines:
            if json_util.loads(line)['uuid'] == uuid:
                return line
            raise DagNotFoundException
        f.close()

def delete(uuid: str):
    with open(pwd(), 'w', encoding="utf-8") as f:
        f.close()


def is_exist(dag_id: str):
    with open(pwd(), 'r', encoding="utf-8") as f:
        lines = f.readlines()
        for line in lines:
            if json_util.loads(line)['dag_id'] == dag_id:
                return True
        f.close()
    return False


def is_exists(job_name, engine):
    try:
        _ = pd.read_sql_query(
            "select job_name from info where job_name='{job_name}' and info_type='load' limit 1".format(
                job_name=job_name), engine
        )['dag_id'][0]
    except:  # job is not exist
        _ = None
        """ job must not exsist so this direction is correct direction. And so, when in this case, code does nothing."""
        return False
    else:  # job is exist
        return True
