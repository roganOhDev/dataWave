import json

from app.exception.api_exception import ApiException
from app.exception.exception_code import ExceptionCode


class TypeException(ApiException):

    def __init__(self, type_name: str):
        self.status_code: int = 400
        self.code: str = ExceptionCode.TYPE_EXCEPTION
        self.message: str = type_name + ' : not JSON serializable'
        self.detail: str = json.dumps({"ApiException": {"code": self.code, "detail": self.message}})
        super().log()

    def __repr__(self) -> str:
        return "ApiException{" + \
               "code='" + self.code + '\'' + \
               ", detail='" + self.message + '\'' + \
               '}'
