import json

from exception.api_exception import ApiException
from exception.exception_code import ExceptionCode


class ConnectionsAreNotEqual(ApiException):
    def __init__(self):
        self.status_code: int = 500
        self.code: str = ExceptionCode.Elt_Map.CONNECTIONS_ARE_NOT_EQUAL
        self.message: str = "Connections Are Not Equal"
        self.detail: str = json.dumps({"ApiException": {"code": self.code, "detail": self.message}})
        super().log()

    def __repr__(self) -> str:
        return "ApiException{" + \
               "code='" + self.code + '\'' + \
               ", detail='" + self.message + '\'' + \
               '}'
