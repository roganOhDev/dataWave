import uvicorn
from fastapi import FastAPI
from starlette.middleware.base import BaseHTTPMiddleware
from uvicorn.config import LOGGING_CONFIG

from app.common.database import db
from app.router import connection_router
from app.router import dag_router
from app.router import table_router
from app.router import elt_map_router
from middle.controller import dispatch


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


def run():
    LOGGING_CONFIG["formatters"]["default"]["fmt"] = "%(asctime)s [%(name)s] %(levelprefix)s %(message)s"
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, use_colors=True)


if __name__ == "__main__":
    run()
