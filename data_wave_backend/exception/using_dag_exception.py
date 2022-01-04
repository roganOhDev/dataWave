import json

from exception.api_exception import ApiException
from exception.exception_code import ExceptionCode


class UsingDagException(ApiException):
    def __init__(self):
        self.status_code: int = 500
        self.code: str = ExceptionCode.Dag.USING_DAG
        self.message: str = "Using Dag. Please Delete Dag Usage In elt_map"
        self.detail: str = json.dumps({"code": self.code, "detail": self.message})
        super().log()

    def __repr__(self) -> str:
        return "ApiException{" + \
               "code='" + self.code + '\'' + \
               ", detail='" + self.message + '\'' + \
               '}'
