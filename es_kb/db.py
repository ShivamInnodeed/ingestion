from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from .config import AppSettings
from .db_models import Base


def create_engine_from_settings(settings: AppSettings) -> Engine:
    connect_args: dict[str, object] = {}
    if settings.db_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
    return create_engine(settings.db_url, future=True, connect_args=connect_args)


def init_db(engine: Engine) -> None:
    Base.metadata.create_all(engine)


def build_session_factory(settings: AppSettings) -> sessionmaker[Session]:
    engine = create_engine_from_settings(settings)
    init_db(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
