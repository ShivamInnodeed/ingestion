from __future__ import annotations

"""Audit schema used by ingestion.

Tables:
- website_crawl_run: one row per run and aggregate counters.
- website_url_master: one row per normalized URL and latest state/hash.
- website_url_snapshot: one row per URL per run (versioned audit history).
"""

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import JSON, Boolean, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .oracle_types import TzDateTime


class Base(DeclarativeBase):
    pass


class CrawlRun(Base):
    __tablename__ = "website_crawl_run"

    run_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    crawl_source_name: Mapped[str] = mapped_column(String(128), default="sbicard")
    root_url: Mapped[str] = mapped_column(String(2048), default="")
    scheduled_for: Mapped[datetime | None] = mapped_column(TzDateTime(), nullable=True)
    started_at: Mapped[datetime] = mapped_column(TzDateTime(), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(TzDateTime(), nullable=True)
    run_status: Mapped[str] = mapped_column(String(32), default="RUNNING")

    total_urls: Mapped[int] = mapped_column(Integer, default=0)
    success_urls: Mapped[int] = mapped_column(Integer, default=0)
    failed_urls: Mapped[int] = mapped_column(Integer, default=0)
    new_urls: Mapped[int] = mapped_column(Integer, default=0)
    updated_urls: Mapped[int] = mapped_column(Integer, default=0)
    unchanged_urls: Mapped[int] = mapped_column(Integer, default=0)
    deleted_urls: Mapped[int] = mapped_column(Integer, default=0)

    run_metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    total_bytes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    failed_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(TzDateTime(), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        TzDateTime(),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        Index("ix_crawl_run_started_at", "started_at"),
        Index("ix_crawl_run_status_started_at", "run_status", "started_at"),
    )


class UrlMaster(Base):
    __tablename__ = "website_url_master"

    url_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    normalized_url: Mapped[str] = mapped_column(String(4000), nullable=False, unique=True)
    source_url: Mapped[str] = mapped_column(String(4000), default="")
    canonical_url: Mapped[str | None] = mapped_column(String(4000), nullable=True)
    url_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    domain_name: Mapped[str] = mapped_column(String(255), default="")
    first_seen_at: Mapped[datetime] = mapped_column(TzDateTime(), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(TzDateTime(), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    deleted_at: Mapped[datetime | None] = mapped_column(TzDateTime(), nullable=True)
    last_content_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_change_status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    index_status: Mapped[str] = mapped_column(String(32), default="PENDING")
    created_at: Mapped[datetime] = mapped_column(TzDateTime(), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        TzDateTime(),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        Index("ix_url_master_active_last_seen", "is_active", "last_seen_at"),
        Index("ix_url_master_domain_name", "domain_name"),
    )


class UrlSnapshot(Base):
    __tablename__ = "website_url_snapshot"

    snapshot_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(64), ForeignKey("website_crawl_run.run_id"), nullable=False)
    url_id: Mapped[str] = mapped_column(String(64), ForeignKey("website_url_master.url_id"), nullable=False)
    normalized_url: Mapped[str] = mapped_column(String(4000), nullable=False)
    source_url: Mapped[str] = mapped_column(String(4000), default="")
    parent_url: Mapped[str | None] = mapped_column(String(4000), nullable=True)
    discovered_from_url: Mapped[str | None] = mapped_column(String(4000), nullable=True)
    depth_level: Mapped[int | None] = mapped_column(Integer, nullable=True)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    meta_description: Mapped[str | None] = mapped_column(Text, nullable=True)
    canonical_url: Mapped[str | None] = mapped_column(String(4000), nullable=True)
    raw_content: Mapped[str | None] = mapped_column(Text, nullable=True)
    json_content: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    previous_content_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    version_no: Mapped[int] = mapped_column(Integer, nullable=False)
    change_status: Mapped[str] = mapped_column(String(32), nullable=False)
    changed_fields_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    http_status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    fetch_status: Mapped[str] = mapped_column(String(32), nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(TzDateTime(), default=lambda: datetime.now(timezone.utc))
    response_time_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    request_method: Mapped[str | None] = mapped_column(String(32), nullable=True)
    request_headers_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    response_headers_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    cookies_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    user_agent: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    index_status: Mapped[str] = mapped_column(String(32), default="PENDING")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    deleted_at: Mapped[datetime | None] = mapped_column(TzDateTime(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(TzDateTime(), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        TzDateTime(),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        UniqueConstraint("url_id", "version_no", name="uq_snapshot_url_version"),
        Index("ix_snapshot_run_id", "run_id"),
        Index("ix_snapshot_url_id_fetched_at", "url_id", "fetched_at"),
        Index("ix_snapshot_status_run_id", "change_status", "run_id"),
    )
