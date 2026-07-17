# db.py
"""
Database helper module for the project.

* Builds an Oracle URL from individual components.
* Creates a SQLAlchemy Engine with Oracle‑specific JSON support.
* Provides a session factory and a helper to initialise the schema.
"""

from __future__ import annotations

import json
from typing import Any, Dict

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine, URL
from sqlalchemy.orm import Session, sessionmaker

# Local imports – adjust the package name if this file lives elsewhere
from .config import AppSettings
from .db_models import Base
from .oracle_types import ORACLE_TS_TZ_FORMAT

# --------------------------------------------------------------------------- #
# Configuration helpers
# --------------------------------------------------------------------------- #

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
    """
    Construct a fully‑qualified Oracle URL for SQLAlchemy.

    Parameters
    ----------
    user, password, host, port, service_name : str
        Connection credentials.

    Returns
    -------
    str
        Rendered URL string (password is *not* hidden).
    """
    url = URL.create(
        "oracle+oracledb",
        username=user,
        password=password,
        host=host,
        port=int(port),
        query={"service_name": service_name},
    )
    return url.render_as_string(hide_password=False)


# --------------------------------------------------------------------------- #
# Oracle session configuration
# --------------------------------------------------------------------------- #

def _configure_oracle_session(dbapi_connection, connection_record=None) -> None:
    """
    Listener that runs when a new DBAPI connection is created.

    Sets the NLS_TIMESTAMP_TZ_FORMAT so that TIMESTAMP WITH TIME ZONE values
    are returned in a predictable format.
    """
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute(
            f"ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = '{ORACLE_TS_TZ_FORMAT}'"
        )
    finally:
        cursor.close()


# --------------------------------------------------------------------------- #
# JSON helpers for Oracle
# --------------------------------------------------------------------------- #

def _oracle_json_serializer(value: Any) -> str:
    """Serialize a Python object to a JSON string (UTF‑8)."""
    return json.dumps(value, ensure_ascii=False)


def _oracle_json_deserializer(value: Any) -> Any | None:
    """
    Deserialize a JSON string (or bytes) returned from Oracle.

    Oracle may return JSON as a CLOB or a BLOB; this helper normalises
    the input before calling `json.loads`.
    """
    if value is None:
        return None

    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")

    if isinstance(value, str):
        return json.loads(value)

    return value


# --------------------------------------------------------------------------- #
# Engine creation
# --------------------------------------------------------------------------- #

def create_engine_from_settings(settings: AppSettings) -> Engine:
    """
    Create a SQLAlchemy Engine based on the supplied AppSettings.

    The function accepts a full database URL or individual components
    (via `build_oracle_db_url`).  Oracle‑specific JSON helpers are attached
    to the dialect *after* the engine is created, which avoids the
    ``TypeError`` raised by passing them to ``create_engine``.
    """
    db_url = (settings.db_url or "").strip()
    if not db_url:
        raise ValueError(_MISSING_DB_URL_MSG)

    connect_args: Dict[str, Any] = {}
    engine_kwargs: Dict[str, Any] = {"future": True, "connect_args": connect_args}

    is_oracle = db_url.startswith("oracle")
    if db_url.startswith("sqlite"):
        # SQLite needs this flag when used from multiple threads.
        connect_args["check_same_thread"] = False
    elif is_oracle:
        engine_kwargs["pool_pre_ping"] = True
        # JSON helpers are *not* accepted by create_engine for Oracle.
        # They will be attached to the dialect after the engine is created.

    engine = create_engine(db_url, **engine_kwargs)

    if is_oracle:
        # Attach JSON helpers to the dialect – the Oracle dialect expects
        # the private attributes `_json_serializer` / `_json_deserializer`.
        engine.dialect._json_serializer = _oracle_json_serializer
        engine.dialect._json_deserializer = _oracle_json_deserializer

        # For completeness, also expose the public names (future‑proofing).
        engine.dialect.json_serializer = _oracle_json_serializer
        engine.dialect.json_deserializer = _oracle_json_deserializer

        # Ensure the session configuration listener is registered.
        event.listen(engine, "connect", _configure_oracle_session)

    return engine


# --------------------------------------------------------------------------- #
# Schema initialisation
# --------------------------------------------------------------------------- #

def init_db(engine: Engine) -> None:
    """
    Create all tables defined in the declarative Base.

    This is a convenience wrapper that can be called during application
    start‑up or in tests.
    """
    Base.metadata.create_all(engine)


# --------------------------------------------------------------------------- #
# Session factory
# --------------------------------------------------------------------------- #

def build_session_factory(settings: AppSettings) -> sessionmaker[Session]:
    """
    Build a SQLAlchemy sessionmaker bound to an Engine created from settings.

    The returned factory produces sessions that are:
    * autoflush=False
    * autocommit=False
    * expire_on_commit=False
    """
    engine = create_engine_from_settings(settings)
    init_db(engine)
    return sessionmaker(
        bind=engine,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
    )