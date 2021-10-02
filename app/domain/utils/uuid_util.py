import uuid as built_in_uuid


def uuid():
    return str(built_in_uuid.uuid1())
