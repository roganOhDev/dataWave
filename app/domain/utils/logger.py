import logging

# from fastapi.logger import logger
from fastapi.requests import Request


# logger.setLevel(logging.INFO)


def make_logger(name=None):
    logger = logging.getLogger(name)

    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "%(name)s:[%(asctime)s.%(msecs)03d] %(filename)s.%(funcName)s.%(lineno)d  %(levelname)s - %("
        "message)s", datefmt='%Y-%m-%d %H:%M:%S')

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)

    logger.propagate = False
    logger.addHandler(console)

    return logger


async def api_logger(request: Request, response=None, error=None):
    # time_format = "%Y/%m/%d %H:%M:%S"
    # t = time()
    # status_code = error.status_code if error else response.status_code
    # error_log = None
    # user = request.state.user
    logger = make_logger("rogan")
    logger.info("hello world")
    logger.debug("hello world")
    logger.error("hello world")
    logger.warning("hello world")
    logger.critical("hello world")
