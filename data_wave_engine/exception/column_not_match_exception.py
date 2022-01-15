import json

from exception.engine_exception import EngineException
from exception.exception_code import ExceptionCode


class ColumnNotMatchException(EngineException):
    def __init__(self):
        self.status_code: int = 500
        self.code: str = ExceptionCode.El.COLUMN_NOT_MATCH
        self.message: str = "Column Not Match"
        self.detail: str = json.dumps({"code": self.code, "detail": self.message})
        super().log()

    def __repr__(self) -> str:
        return "ApiException{" + \
               "code='" + self.code + '\'' + \
               ", detail='" + self.message + '\'' + \
               '}'
