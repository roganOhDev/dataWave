import json

from data_wave_backend.exception.api_exception import ApiException
from data_wave_backend.exception.exception_code import ExceptionCode
from data_wave_backend.domain.utils.logger import logger


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
