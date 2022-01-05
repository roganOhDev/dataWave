import json

from exception.api_exception import ApiException
from exception.exception_code import ExceptionCode


class AlreadyExistsJobIdException(ApiException):
    def __init__(self):
        self.status_code: int = 500
        self.code: str = ExceptionCode.Job.ALREADY_EXITS_JOB_ID
        self.message: str = "Already Exist job id"
        self.detail: str = json.dumps( {"code": self.code, "detail": self.message})
        super().log()

    def __repr__(self) -> str:
        return "ApiException{" + \
               "code='" + self.code + '\'' + \
               ", detail='" + self.message + '\'' + \
               '}'
