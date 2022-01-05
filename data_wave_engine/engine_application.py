import os
import socket
import traceback

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from uvicorn.config import LOGGING_CONFIG

from common.utils.logger import logger
from domain.call_api import job_api
from exception.engine_exception import EngineException
from middle.controller import dispatch
from scheduler import sched

def create_app():
    app_birth = FastAPI()
    add_router(app_birth)
    app_birth.add_middleware(middleware_class=BaseHTTPMiddleware, dispatch=dispatch)
    return app_birth


def add_router(app_birth):
    return app_birth


app = create_app()


async def catch_exceptions_middleware(request: Request, call_next):
    try:
        return await call_next(request)
    except Exception as e:
        logger.error(traceback.format_exc())
        error_dict = dict(error="Server Internal Error(ENGINE)", detail=str(e))
        return JSONResponse(status_code=500, content=error_dict)


@app.exception_handler(EngineException)
async def api_exception_handler(request: Request, exc: EngineException):
    logger.error(traceback.format_exc())
    error_dict = dict(error="EngineException", detail=exc.detail)
    res = JSONResponse(status_code=exc.status_code, content=error_dict)
    return res


app.middleware('http')(catch_exceptions_middleware)


def run():
    LOGGING_CONFIG["formatters"]["default"]["fmt"] = "%(asctime)s [%(name)s] %(levelprefix)s %(message)s"
    uvicorn.run("engine_application:app", host=get_ip(), port=8800, reload=True,
                reload_dirs=["./data_wave_engine"], use_colors=True)


def get_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    return s.getsockname()[0]


@app.on_event("startup")
def startup():
    fill_scheduler()

def fill_scheduler():
    job_ids = job_api.get_activate_job_uuids()

    for job_id in job_ids:
        job = job_api.get_job_by_uuid(job_id)
        import_str = "from {0} import {1}".format("elt_jobs."+job_id, "add_job")
        exec(import_str,globals())
        add_job()

        # schedules = []
        # for job in sched.get_jobs():
        #     jobdict = {}
        #     for f in job.trigger.fields:
        #         curval = str(f)
        #         jobdict[f.name] = curval
        #
        #     schedules.append(jobdict)
        # print(schedules)


if __name__ == "__main__":
    run()
