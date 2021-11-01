import json
import logging
import os

from fastapi import FastAPI
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


class SQLAlchemy:
    def __init__(self, app: FastAPI = None):
        self._engine = None
        self._session = None
        if app is not None:
            self.init_app(app=app)

    def init_app(self, app: FastAPI):
        BASE_DIR = os.path.dirname(os.path.abspath(""))
        SECRET_FILE = os.path.join(BASE_DIR, 'config/db.json')
        ECHO = True
        POOL_RECYCLE = 900
        secrets = json.loads(open(SECRET_FILE).read())
        DB = secrets["DB"]
        db_url = f"mysql+pymysql://{DB['USERNAME']}:{DB['PASSWORD']}@{DB['DBHOST']}:{DB['DBPORT']}/{DB['DATABASENAME']}?charset=utf8"
        self._engine = create_engine(
            db_url,
            echo=ECHO,
            pool_recycle=POOL_RECYCLE,
            pool_pre_ping=True,
            encoding='utf-8'
        )
        self._session = sessionmaker(autocommit=False, autoflush=False, bind=self._engine)

        @app.on_event("startup")
        def startup():
            self._engine.connect()
            logging.info("DB connected")

        @app.on_event("shutdown")
        def shutdown():
            self._session.close_all()
            self._engine.dispose()
            logging.info("DB disconnected")

    def get_db(self):
        """
        요청마다 DB 세션 유지 함수
        :return:
        """
        if self._session is None:
            raise Exception("must be called 'init_app'")
        db_session = None
        try:
            db_session = self._session()
            yield db_session
        finally:
            db_session.close()

    @property
    def session(self):
        return self.get_db

    @property
    def engine(self):
        return self._engine


db = SQLAlchemy()
Base = declarative_base()