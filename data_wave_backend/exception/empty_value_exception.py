import json

from exception.api_exception import ApiException
from exception.exception_code import ExceptionCode


class EmptyValueException(ApiException):
    def __init__(self):
        self.status_code: int = 500
        self.code: str = ExceptionCode.EMPTY_VALUE_EXCEPTION
        self.message: str = "empty value exception"
        self.detail: str = json.dumps({"code": self.code, "detail": self.message})
        super().log()

    def __repr__(self) -> str:
        return "ApiException{" + \
               "code='" + self.code + '\'' + \
               ", detail='" + self.message + '\'' + \
               '}'
