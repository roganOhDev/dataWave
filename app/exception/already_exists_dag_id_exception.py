import json

from app.exception.api_exception import ApiException
from app.exception.exception_code import ExceptionCode
from app.domain.utils.logger import logger


class AlreadyExistsDagIdException(ApiException):
    def __init__(self):
        self.status_code: int = 500
        self.code: str = ExceptionCode.dag.ALREADY_EXITS_DAG_ID
        self.message: str = "Already Exist dag id"
        self.detail: str = json.dumps({"ApiException": {"code": self.code, "detail": self.message}})
        super().log()

    def __repr__(self) -> str:
        return "ApiException{" + \
               "code='" + self.code + '\'' + \
               ", detail='" + self.message + '\'' + \
               '}'
