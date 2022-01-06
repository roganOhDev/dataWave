import json

from exception.api_exception import ApiException
from exception.exception_code import ExceptionCode


class EngineException(ApiException):
    def __init__(self, message: str = None):
        self.status_code: int = 500
        self.code: str = ExceptionCode.Engine.ENGINE_EXCEPTION
        self.message: str = message if message else "Engine Raise Exception"
        self.detail: str = json.dumps({"code": self.code, "detail": self.message})
        super().log()

    def __repr__(self) -> str:
        return "ApiException{" + \
               "code='" + self.code + '\'' + \
               ", detail='" + self.message + '\'' + \
               '}'
