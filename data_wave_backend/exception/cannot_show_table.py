import json

from exception.api_exception import ApiException
from exception.exception_code import ExceptionCode


class CannotShowTable(ApiException):
    def __init__(self, message: str):
        self.status_code: int = 500
        self.code: str = ExceptionCode.Hook.CANNOT_SHOW_TABLE
        self.message: str = message
        self.detail: str = json.dumps({"ApiException": {"code": self.code, "detail": self.message}})
        super().log()

    def __repr__(self) -> str:
        return "ApiException{" + \
               "code='" + self.code + '\'' + \
               ", detail='" + self.message + '\'' + \
               '}'
