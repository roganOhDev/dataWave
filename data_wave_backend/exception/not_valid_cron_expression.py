import json

from exception.api_exception import ApiException
from exception.exception_code import ExceptionCode

class NotValidCronExpression(ApiException):

    def __init__(self):
        self.status_code: int = 500
        self.code: str = ExceptionCode.Job.NOT_VALID_CRON_EXPRESSION
        self.message: str = "Cron Expression Doesn't Valid"
        self.detail: str = json.dumps({"code": self.code, "detail": self.message})
        super().log()

    def __repr__(self) -> str:
        return "ApiException{" + \
               "code='" + self.code + '\'' + \
               ", detail='" + self.message + '\'' + \
               '}'
