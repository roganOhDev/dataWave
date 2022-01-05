import json

from exception.api_exception import ApiException
from exception.exception_code import ExceptionCode


class UsingJobException(ApiException):
    def __init__(self):
        self.status_code: int = 500
        self.code: str = ExceptionCode.Job.USING_JOB
        self.message: str = "Using Job. Please Delete Jab Usage In elt_map"
        self.detail: str = json.dumps({"code": self.code, "detail": self.message})
        super().log()

    def __repr__(self) -> str:
        return "ApiException{" + \
               "code='" + self.code + '\'' + \
               ", detail='" + self.message + '\'' + \
               '}'
