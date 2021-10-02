import datetime
import json as base_json
from app.exception.type_exception import TypeException


def json(data):
    return base_json.dumps(data.__dict__, default=json_default)


def json_default(value):
    if isinstance(value, datetime.datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    raise TypeException(str(type(value)) + ' : not JSON serializable')

def loads(value):
    return base_json.loads(value)
