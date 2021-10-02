from app.domain.utils import os_util


def save(data):
    file = os_util.before_abs_pwd_with_join(__file__, "entity/dags.txt",4)
    with open(file, 'a', encoding="utf-8") as f:
        f.write(data)
        f.close()
