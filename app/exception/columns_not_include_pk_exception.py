import json

from app.exception.api_exception import ApiException
from app.exception.exception_code import ExceptionCode


class ColumnsNotIncludePk(ApiException):
    def __init__(self):
        self.status_code: int = 500
        self.code: str = ExceptionCode.Table_List.COLUMNS_NOT_INCLUDE_PK
        self.message: str = "pk is not in columns"
        self.detail: str = json.dumps({"ApiException": {"code": self.code, "detail": self.message}})
        super().log()

    def __repr__(self) -> str:
        return "ApiException{" + \
               "code='" + self.code + '\'' + \
               ", detail='" + self.message + '\'' + \
               '}'
