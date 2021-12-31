import json

from app.exception.api_exception import ApiException
from app.exception.exception_code import ExceptionCode

#TODO : classify status code
class CannotUseThisDagException(ApiException):

    def __init__(self):
        self.status_code: int = 500
        self.code: str = ExceptionCode.Elt_Map.USING_DAG
        self.message: str = "Using Dag Please Choose other dag"
        self.detail: str = json.dumps({"ApiException": {"code": self.code, "detail": self.message}})
        super().log()

    def __repr__(self) -> str:
        return "ApiException{" + \
               "code='" + self.code + '\'' + \
               ", detail='" + self.message + '\'' + \
               '}'
