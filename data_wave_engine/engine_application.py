import traceback

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from uvicorn.config import LOGGING_CONFIG

from common.utils.logger import logger
from domain.call_api import elt_map_api
from exception.engine_exception import EngineException
from middle.controller import dispatch
from router import job_router
import client


def create_app():
    app_birth = FastAPI()
    add_router(app_birth)
    app_birth.add_middleware(middleware_class=BaseHTTPMiddleware, dispatch=dispatch)
    return app_birth


def add_router(app_birth):
    app_birth.include_router(job_router.router, prefix="/job")
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
    uvicorn.run("engine_application:app", host=client.get_ip(), port=8800, reload=True,
                reload_dirs=["./data_wave_engine"],reload_excludes=["./data_wave_engine/elt_jobs"], use_colors=True)


@app.on_event("startup")
def startup():
    fill_scheduler()


def fill_scheduler():
    job_ids = elt_map_api.get_activate_job_uuids()

    for job_id in job_ids:
        try:
            import_str = "from {0} import {1}".format("elt_jobs." + job_id, "add_job")
            exec(import_str, globals())
            add_job()
            logger.log("{job_id} added in scheduler".format(job_id=job_id))
        except Exception as e:
            logger.error("{job_id} not addedd in scheduler by {error}".format(job_id=job_id, error=str(e)))

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
