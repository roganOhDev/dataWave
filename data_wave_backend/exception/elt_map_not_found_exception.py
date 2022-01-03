import json

from data_wave_backend.exception.api_exception import ApiException
from data_wave_backend.exception.exception_code import ExceptionCode


class EltMapNotFoundException(ApiException):
    def __init__(self):
        self.status_code: int = 500
        self.code: str = ExceptionCode.Elt_Map.ELT_MAP_NOT_FOUND
        self.message: str = "Elt Map Not Found"
        self.detail: str = json.dumps({"ApiException": {"code": self.code, "detail": self.message}})
        super().log()

    def __repr__(self) -> str:
        return "ApiException{" + \
               "code='" + self.code + '\'' + \
               ", detail='" + self.message + '\'' + \
               '}'
