import json

from exception.engine_exception import EngineException
from exception.exception_code import ExceptionCode


class AlreadyExistSchedulerId(EngineException):
    def __init__(self):
        self.status_code: int = 500
        self.code: str = ExceptionCode.Scheduler.ALREADY_EXIST_SCHEDULER_ID
        self.message: str = "Already Exist Scheduler Id"
        self.detail: str = json.dumps({"code": self.code, "detail": self.message})
        super().log()

    def __repr__(self) -> str:
        return "ApiException{" + \
               "code='" + self.code + '\'' + \
               ", detail='" + self.message + '\'' + \
               '}'
