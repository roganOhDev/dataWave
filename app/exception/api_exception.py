from fastapi import HTTPException


class ApiException(HTTPException):
    def __init__(self):
        self.status_code: int = ""
        self.code: str = ""
        self.message: str = ""
        self.detail: str = ""

    def __repr__(self) -> str:
        return ""

