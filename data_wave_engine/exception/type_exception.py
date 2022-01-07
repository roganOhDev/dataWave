import json

from exception.engine_exception import EngineException
from exception.exception_code import ExceptionCode


class TypeException(EngineException):

    def __init__(self, type_name: str):
        self.status_code: int = 500
        self.code: str = ExceptionCode.TYPE_EXCEPTION
        self.message: str = type_name + ' : not JSON serializable'
        self.detail: str = json.dumps({"code": self.code, "detail": self.message})
        super().log()

    def __repr__(self) -> str:
        return "ApiException{" + \
               "code='" + self.code + '\'' + \
               ", detail='" + self.message + '\'' + \
               '}'
