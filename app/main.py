import uvicorn
from fastapi import FastAPI

from app.router import test2


def create_app():
    app = FastAPI()

    add_router(app)
    return app


def add_router(app):
    app.include_router(test2.router)
    return app


app = create_app()

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
