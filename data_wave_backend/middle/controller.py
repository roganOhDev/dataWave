import time

from starlette.middleware.base import RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.responses import Response

from domain.utils.logger import api_logger
from exception.api_exception import ApiException


async def dispatch(request: Request, call_next: RequestResponseEndpoint) -> Response:
    try:
        request.state.start = time.time()
        request.state.inspect = None
        request.state.user = None
        request.state.ip = None
        ip = request.headers["x-forwarded-for"] if "x-forwarded-for" in request.headers.keys() else request.client.host
        request.state.ip = ip.split(",")[0] if "," in ip else ip
        headers = request.headers
        cookies = request.cookies
        url = request.url.path

        response = await call_next(request)
        await api_logger(request, response)
    except ApiException as e:
        error = e
        error_dict = dict(status=error.status_code, msg=error.message, detail=error.detail, code=error.code)
        response = JSONResponse(status_code=error.status_code, content=error_dict)
        await api_logger(request=request, error=error)
    return response
