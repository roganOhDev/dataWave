import logging

from fastapi.logger import logger
from fastapi.requests import Request

logger.setLevel(logging.INFO)


async def api_logger(request: Request, response=None, error=None):
    # time_format = "%Y/%m/%d %H:%M:%S"
    # t = time()
    # status_code = error.status_code if error else response.status_code
    # error_log = None
    # user = request.state.user
    logger.info("hello world")
