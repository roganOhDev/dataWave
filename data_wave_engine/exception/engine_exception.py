from fastapi import HTTPException


class EngineException(HTTPException):
    def __init__(self, message: str = None):
        self.status_code: int = 500 if message else ""
        self.code: str = ""
        self.message: str = message if message else ""
        self.detail: str = ""

    def __repr__(self) -> str:
        return ""

    # def log(self):
    #     logger.error(self.message)
