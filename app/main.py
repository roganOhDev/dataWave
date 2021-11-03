import uvicorn
from fastapi import FastAPI
from starlette.middleware.base import BaseHTTPMiddleware
from uvicorn.config import LOGGING_CONFIG

from app.common.database import db
from app.router import connection_router
from app.router import dag_router
from get_information_from_user.const_value import about_querypie_elt
from middle.controller import dispatch


def create_app():
    app = FastAPI()

    db.init_app(app)
    add_router(app)
    app.add_middleware(middleware_class=BaseHTTPMiddleware, dispatch=dispatch)
    return app


def add_router(app):
    app.include_router(connection_router.router, prefix="/connection")
    app.include_router(dag_router.router, prefix="/dag")
    return app


print(about_querypie_elt.welcome)
app = create_app()


def run():
    LOGGING_CONFIG["formatters"]["default"]["fmt"] = "%(asctime)s [%(name)s] %(levelprefix)s %(message)s"
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, use_colors=True)


if __name__ == "__main__":
    run()
