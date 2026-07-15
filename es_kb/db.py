from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL
from sqlalchemy.orm import Session, sessionmaker

from .config import AppSettings
from .db_models import Base

_MISSING_DB_URL_MSG = (
    "No database URL configured. Set ORACLE_HOST, ORACLE_PORT, ORACLE_SERVICE_NAME, "
    "ORACLE_USER, and ORACLE_PASSWORD; or set ES_DB_URL / --db-url / config db_url."
)


def build_oracle_db_url(
    *,
    user: str,
    password: str,
    host: str,
    port: int | str,
    service_name: str,
) -> str:
    """Build an oracle+oracledb SQLAlchemy URL (password special chars handled safely)."""
    url = URL.create(
        "oracle+oracledb",
        username=user,
        password=password,
        host=host,
        port=int(port),
        query={"service_name": service_name},
    )
    return url.render_as_string(hide_password=False)


def create_engine_from_settings(settings: AppSettings) -> Engine:
    db_url = (settings.db_url or "").strip()
    if not db_url:
        raise ValueError(_MISSING_DB_URL_MSG)

    connect_args: dict[str, object] = {}
    engine_kwargs: dict[str, object] = {"future": True, "connect_args": connect_args}

    if db_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
    elif db_url.startswith("oracle"):
        engine_kwargs["pool_pre_ping"] = True

    return create_engine(db_url, **engine_kwargs)


def init_db(engine: Engine) -> None:
    Base.metadata.create_all(engine)


def build_session_factory(settings: AppSettings) -> sessionmaker[Session]:
    engine = create_engine_from_settings(settings)
    init_db(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
