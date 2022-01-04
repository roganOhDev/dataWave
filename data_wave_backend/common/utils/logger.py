import json
import logging
from time import time

from fastapi.requests import Request


def make_logger(name=None):
    logger = logging.getLogger(name)

    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "%(name)s:[%(asctime)s.%(msecs)03d]  %(levelname)s - %(message)s", datefmt='%Y-%m-%d %H:%M:%S')

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)

    logger.propagate = False
    logger.addHandler(console)

    return logger


logger = make_logger("rogan")


async def api_logger(request: Request, response=None, error=None):
    t = time() - request.state.start
    status_code = error.status_code if error else response.status_code
    error_log = None
    # body = await request.json()
    # body = body.decode('utf-8').replace("\n", "").strip()
    if error:
        if request.state.inspect:
            frame = request.state.inspect
            error_file = frame.f_code.co_filename
            error_func = frame.f_code.co_name
            error_line = frame.f_lineno
        else:
            error_func = error_file = error_line = "UNKNOWN"

    log_dict = dict(
        url=request.url.hostname + request.url.path,
        client=request.state.ip,
        method=str(request.method),
        # body=body if body else None,
        statusCode=status_code,
        processedTime=str(round(t * 1000, 5)) + "ms",
    )
    if error:
        logger.error(json.dumps(log_dict))
    else:
        logger.info(json.dumps(log_dict))
