from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import Select, desc, select, update
from sqlalchemy.orm import Session

from .db_models import CrawlRun, UrlMaster, UrlSnapshot
from .normalize_hash import compute_sha256


def _new_id() -> str:
    return uuid4().hex


class CrawlRunRepository:
    def __init__(self, session: Session):
        self.session = session

    def create_run(
        self,
        *,
        crawl_source_name: str,
        root_url: str,
        scheduled_for: datetime | None = None,
        run_metadata: dict | None = None,
    ) -> CrawlRun:
        now = datetime.now(timezone.utc)
        run = CrawlRun(
            run_id=_new_id(),
            crawl_source_name=crawl_source_name,
            root_url=root_url,
            scheduled_for=scheduled_for,
            started_at=now,
            run_status="RUNNING",
            run_metadata_json=run_metadata or {},
        )
        self.session.add(run)
        self.session.flush()
        return run

    def finalize_run(
        self,
        run_id: str,
        *,
        run_status: str,
        total_urls: int,
        success_urls: int,
        failed_urls: int,
        new_urls: int,
        updated_urls: int,
        unchanged_urls: int,
        deleted_urls: int,
        total_bytes: int | None = None,
        duration_ms: int | None = None,
        failed_reason: str | None = None,
        outcome_metadata: dict[str, Any] | None = None,
    ) -> None:
        run = self.session.get(CrawlRun, run_id)
        if not run:
            return
        run.finished_at = datetime.now(timezone.utc)
        run.run_status = run_status
        run.total_urls = total_urls
        run.success_urls = success_urls
        run.failed_urls = failed_urls
        run.new_urls = new_urls
        run.updated_urls = updated_urls
        run.unchanged_urls = unchanged_urls
        run.deleted_urls = deleted_urls
        run.total_bytes = total_bytes
        run.duration_ms = duration_ms
        run.failed_reason = failed_reason
        if outcome_metadata:
            merged = dict(run.run_metadata_json or {})
            ingest = dict(merged.get("ingest_outcome") or {})
            ingest.update(outcome_metadata)
            merged["ingest_outcome"] = ingest
            run.run_metadata_json = merged

    def get_latest_completed_run(self, *, exclude_run_id: str | None = None) -> CrawlRun | None:
        query: Select[tuple[CrawlRun]] = select(CrawlRun).where(
            CrawlRun.run_status.in_(["COMPLETED", "PARTIAL"])
        )
        if exclude_run_id:
            query = query.where(CrawlRun.run_id != exclude_run_id)
        query = query.order_by(desc(CrawlRun.started_at)).limit(1)
        return self.session.scalar(query)


class UrlRepository:
    def __init__(self, session: Session):
        self.session = session

    def get_by_normalized_url(self, normalized_url: str) -> UrlMaster | None:
        return self.session.scalar(select(UrlMaster).where(UrlMaster.normalized_url == normalized_url))

    def upsert_url(
        self,
        *,
        normalized_url: str,
        source_url: str,
        domain_name: str,
        canonical_url: str | None,
        now: datetime,
    ) -> UrlMaster:
        existing = self.get_by_normalized_url(normalized_url)
        if existing:
            existing.source_url = source_url
            existing.domain_name = domain_name
            existing.canonical_url = canonical_url
            existing.last_seen_at = now
            existing.is_active = True
            existing.deleted_at = None
            return existing

        item = UrlMaster(
            url_id=_new_id(),
            normalized_url=normalized_url,
            source_url=source_url,
            canonical_url=canonical_url,
            url_hash=compute_sha256(normalized_url),
            domain_name=domain_name,
            first_seen_at=now,
            last_seen_at=now,
            is_active=True,
            deleted_at=None,
        )
        self.session.add(item)
        self.session.flush()
        return item

    def mark_deleted(self, normalized_url: str, when: datetime) -> UrlMaster | None:
        item = self.get_by_normalized_url(normalized_url)
        if not item:
            return None
        item.is_active = False
        item.deleted_at = when
        item.last_change_status = "DELETED"
        item.index_status = "PENDING"
        return item

    def mark_indexed_for_normalized_urls(self, normalized_urls: list[str]) -> None:
        """Set ``INDEXED`` on URL master rows that were ``PENDING`` after a successful ES index."""
        unique = list(dict.fromkeys(normalized_urls))
        if not unique:
            return
        self.session.execute(
            update(UrlMaster)
            .where(UrlMaster.normalized_url.in_(unique), UrlMaster.index_status == "PENDING")
            .values(index_status="INDEXED")
        )


class SnapshotRepository:
    def __init__(self, session: Session):
        self.session = session

    def get_latest_snapshot_for_url(self, url_id: str) -> UrlSnapshot | None:
        query = (
            select(UrlSnapshot)
            .where(UrlSnapshot.url_id == url_id)
            .order_by(desc(UrlSnapshot.version_no))
            .limit(1)
        )
        return self.session.scalar(query)

    def list_normalized_urls_by_run(self, run_id: str) -> set[str]:
        rows = self.session.scalars(
            select(UrlSnapshot.normalized_url).where(
                UrlSnapshot.run_id == run_id,
                UrlSnapshot.change_status != "DELETED",
            )
        )
        return {value for value in rows if value}

    def create_snapshot(self, **kwargs) -> UrlSnapshot:
        snapshot = UrlSnapshot(snapshot_id=_new_id(), **kwargs)
        self.session.add(snapshot)
        self.session.flush()
        return snapshot

    def mark_indexed_for_run(self, run_id: str, normalized_urls: list[str]) -> None:
        """Set ``INDEXED`` on snapshots for this run that were queued as ``PENDING``."""
        unique = list(dict.fromkeys(normalized_urls))
        if not unique:
            return
        self.session.execute(
            update(UrlSnapshot)
            .where(
                UrlSnapshot.run_id == run_id,
                UrlSnapshot.normalized_url.in_(unique),
                UrlSnapshot.index_status == "PENDING",
                UrlSnapshot.fetch_status == "SUCCESS",
            )
            .values(index_status="INDEXED")
        )
