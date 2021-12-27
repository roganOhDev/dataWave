import json

from app.exception.api_exception import ApiException
from app.exception.exception_code import ExceptionCode


class NotSupportedDbTypeException(ApiException):
    def __init__(self):
        self.status_code: int = 400
        self.code: str = ExceptionCode.connection.NOT_SUPPORTED_DB_TYPE
        self.message: str = "not supported db type"
        self.detail: str = json.dumps({"ApiException": {"code": self.code, "detail": self.message}})
        super().log()

    def __repr__(self) -> str:
        return "ApiException{" + \
               "code='" + self.code + '\'' + \
               ", detail='" + self.message + '\'' + \
               '}'
