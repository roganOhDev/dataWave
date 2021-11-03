from starlette.requests import Request
from starlette.middleware.base import RequestResponseEndpoint
from starlette.responses import Response
from app.domain.utils.logger import api_logger
import datetime


async def dispatch(request: Request, call_next: RequestResponseEndpoint) -> Response:
    request.state.start = datetime.datetime.now()
    request.state.user = "rogan"
    response = await call_next(request)
    await api_logger(request, response)
    return response
