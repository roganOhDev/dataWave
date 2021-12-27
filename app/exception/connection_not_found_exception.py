import json

from app.exception.api_exception import ApiException
from app.exception.exception_code import ExceptionCode


class ConnectionNotFoundException(ApiException):
    def __init__(self):
        self.status_code: int = 400
        self.code: str = ExceptionCode.connection.CONNECTION_NOT_FOUND
        self.message: str = "connection not found"
        self.detail: str = json.dumps({"ApiException": {"code": self.code, "detail": self.message}})
        super().log()

    def __repr__(self) -> str:
        return "ApiException{" + \
               "code='" + self.code + '\'' + \
               ", detail='" + self.message + '\'' + \
               '}'
