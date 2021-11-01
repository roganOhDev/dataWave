from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.common.database import db
from app.domain.connection import connection_composite_service as composite_service
from app.domain.connection.connection import Connection
from app.dto.connection_dto import ConnectionDto

router = APIRouter()


@router.put("/connection/create", response_model=ConnectionDto)
def init_dag(request: ConnectionDto):
    composite_service.save(request)


@router.put("/connections", response_model=list)
def init_dag(session: Session = Depends(db.session)):
    # a = session.query(Connection).all()
    # return a
    return a(session)

def a(session: Session):
    return session.query(Connection).all()