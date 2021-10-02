import uvicorn
from fastapi import FastAPI

from app.router import test2
from app.router import dag
from get_information_from_user.const_value import about_querypie_elt


def create_app():
    app = FastAPI()

    add_router(app)
    return app


def add_router(app):
    app.include_router(test2.router)
    app.include_router(dag.router)
    return app


print(about_querypie_elt.welcome)
app = create_app()

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
