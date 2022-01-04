import socket
import traceback

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from uvicorn.config import LOGGING_CONFIG

from common.database import db
from common.utils.logger import logger
from exception.api_exception import ApiException
from middle.controller import dispatch
from router import connection_router
from router import dag_router
from router import elt_map_router
from router import table_router


def create_app():
    app_birth = FastAPI()

    db.init_app(app_birth)
    add_router(app_birth)
    app_birth.add_middleware(middleware_class=BaseHTTPMiddleware, dispatch=dispatch)
    return app_birth


def add_router(app_birth):
    app_birth.include_router(connection_router.router, prefix="/connection")
    app_birth.include_router(dag_router.router, prefix="/dag")
    app_birth.include_router(table_router.router, prefix="/table_list")
    app_birth.include_router(elt_map_router.router, prefix="/elt_map")
    return app_birth


app = create_app()


async def catch_exceptions_middleware(request: Request, call_next):
    try:
        return await call_next(request)
    except Exception as e:
        logger.error(traceback.format_exc())
        error_dict = dict(error="Server Internal Error", detail=str(e))
        return JSONResponse(status_code=500, content=error_dict)


@app.exception_handler(ApiException)
async def api_exception_handler(request: Request, exc: ApiException):
    logger.error(traceback.format_exc())
    error_dict = dict(error="ApiException", detail=exc.detail)
    res = JSONResponse(status_code=exc.status_code, content=error_dict)
    return res


app.middleware('http')(catch_exceptions_middleware)


def run():
    LOGGING_CONFIG["formatters"]["default"]["fmt"] = "%(asctime)s [%(name)s] %(levelprefix)s %(message)s"
    uvicorn.run("api_application:app", host=get_ip(), port=8000, reload=True,
                reload_dirs=["./data_wave_backend"], use_colors=True)


def get_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    return s.getsockname()[0]


if __name__ == "__main__":
    run()
