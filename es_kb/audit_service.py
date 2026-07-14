from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit

from sqlalchemy.orm import Session

from .change_detection import STATUS_DELETED, STATUS_NEW, STATUS_UNCHANGED, STATUS_UPDATED, classify_change
from .normalize_hash import build_doc_id, compute_content_hash, normalize_url
from .repositories import CrawlRunRepository, SnapshotRepository, UrlRepository


@dataclass
class RunCounters:
    total_urls: int = 0
    success_urls: int = 0
    failed_urls: int = 0
    new_urls: int = 0
    updated_urls: int = 0
    unchanged_urls: int = 0
    deleted_urls: int = 0
    total_bytes: int = 0


@dataclass
class AuditResult:
    run_id: str
    counters: RunCounters
    payload_delta_records: list[dict[str, Any]] = field(default_factory=list)
    deleted_doc_ids: list[str] = field(default_factory=list)


@dataclass
class AuditRawResult:
    run_id: str
    counters: RunCounters
    changed_normalized_urls: set[str] = field(default_factory=set)
    deleted_doc_ids: list[str] = field(default_factory=list)


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _get_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _build_payload_lookup(payload_records: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    lookup: dict[str, list[dict[str, Any]]] = {}
    for payload in payload_records:
        metadata = _safe_dict(payload.get("metadata"))
        normalized = normalize_url(_get_text(metadata.get("source_url")))
        if normalized:
            lookup.setdefault(normalized, []).append(payload)
    return lookup


def process_run(
    session: Session,
    *,
    crawl_records: list[dict[str, Any]],
    payload_records: list[dict[str, Any]],
    crawl_source_name: str,
    root_url: str,
    run_metadata: dict[str, Any] | None = None,
) -> AuditResult:
    run_repo = CrawlRunRepository(session)
    url_repo = UrlRepository(session)
    snapshot_repo = SnapshotRepository(session)

    run = run_repo.create_run(
        crawl_source_name=crawl_source_name,
        root_url=root_url,
        run_metadata=run_metadata,
    )
    # Note: we intentionally capture a per-record timestamp inside the loop to avoid
    # many rows sharing the exact same second when viewed in SQLite GUIs.
    payload_lookup = _build_payload_lookup(payload_records)
    counters = RunCounters(total_urls=len(crawl_records))
    audit_result = AuditResult(run_id=run.run_id, counters=counters)
    current_urls: set[str] = set()

    for record in crawl_records:
        now = datetime.now(timezone.utc)
        source_url = _get_text(record.get("source_url"))
        normalized_url = normalize_url(source_url)
        if not normalized_url:
            continue
        current_urls.add(normalized_url)

        metadata = _safe_dict(record.get("metadata"))
        canonical_url = _get_text(metadata.get("canonical_url")) or None
        domain_name = urlsplit(normalized_url).netloc
        master = url_repo.upsert_url(
            normalized_url=normalized_url,
            source_url=source_url,
            domain_name=domain_name,
            canonical_url=canonical_url,
            now=now,
        )
        previous_snapshot = snapshot_repo.get_latest_snapshot_for_url(master.url_id)

        title = _get_text(record.get("title"))
        description = _get_text(record.get("description"))
        markdown = _get_text(record.get("markdown"))
        success = bool(record.get("success"))
        status_code = _safe_int(record.get("status_code"))

        if success:
            counters.success_urls += 1
            counters.total_bytes += len(markdown.encode("utf-8"))
        else:
            counters.failed_urls += 1

        current_hash = compute_content_hash(title, description, markdown)
        previous_hash = previous_snapshot.content_hash if previous_snapshot else None
        previous_content = _safe_dict(previous_snapshot.json_content) if previous_snapshot else {}
        previous_title = _get_text(previous_content.get("title"))
        previous_description = _get_text(previous_content.get("description"))
        previous_markdown = _get_text(previous_content.get("markdown"))

        if success:
            change = classify_change(
                title=title,
                description=description,
                markdown=markdown,
                previous_hash=previous_hash,
                previous_title=previous_title,
                previous_description=previous_description,
                previous_markdown=previous_markdown,
            )
            change_status = change.status
            changed_fields = change.changed_fields
            previous_content_hash = change.previous_hash
            content_hash = change.current_hash
        else:
            # Failed fetches are still audited, but they do not trigger ES content updates.
            change_status = STATUS_NEW if not previous_snapshot else STATUS_UNCHANGED
            changed_fields = []
            previous_content_hash = previous_hash
            content_hash = previous_hash or current_hash

        if change_status == STATUS_NEW:
            counters.new_urls += 1
        elif change_status == STATUS_UPDATED:
            counters.updated_urls += 1
        elif change_status == STATUS_UNCHANGED:
            counters.unchanged_urls += 1

        version_no = (previous_snapshot.version_no + 1) if previous_snapshot else 1
        fetch_status = "SUCCESS" if success else "FAILED"
        index_status = "PENDING" if success and change_status in {STATUS_NEW, STATUS_UPDATED} else "SKIPPED"
        doc_id = build_doc_id(normalized_url)

        snapshot_repo.create_snapshot(
            run_id=run.run_id,
            url_id=master.url_id,
            normalized_url=normalized_url,
            source_url=source_url,
            parent_url=_get_text(record.get("parent_url")) or None,
            discovered_from_url=_get_text(record.get("parent_url")) or None,
            depth_level=_safe_int(record.get("depth")),
            title=title or None,
            meta_description=description or None,
            canonical_url=canonical_url,
            raw_content=markdown or None,
            json_content={
                "title": title,
                "description": description,
                "markdown": markdown,
                "metadata": metadata,
            },
            content_hash=content_hash,
            previous_content_hash=previous_content_hash,
            source_hash=build_doc_id(normalized_url),
            version_no=version_no,
            change_status=change_status,
            changed_fields_json={"fields": changed_fields},
            http_status_code=status_code,
            fetch_status=fetch_status,
            response_time_ms=_safe_int(record.get("response_time_ms")),
            request_method=_get_text(record.get("request_method")) or "GET",
            request_headers_json=_safe_dict(record.get("request_headers_final"))
            or _safe_dict(record.get("request_headers_sent")),
            response_headers_json=_safe_dict(record.get("response_headers")),
            cookies_json=_safe_dict(record.get("request_cookies_final"))
            or _safe_dict(record.get("request_cookies_sent")),
            user_agent=_get_text(record.get("user_agent"))
            or _get_text(_safe_dict(record.get("request_headers_sent")).get("User-Agent")),
            error_message=_get_text(record.get("error")) or None,
            index_status=index_status,
            is_active=True,
            deleted_at=None,
        )

        master.last_content_hash = content_hash
        master.last_change_status = change_status
        master.index_status = index_status
        master.is_active = True
        master.deleted_at = None

        if success and change_status in {STATUS_NEW, STATUS_UPDATED}:
            payloads = payload_lookup.get(normalized_url) or []
            for payload in payloads:
                payload_metadata = dict(_safe_dict(payload.get("metadata")))
                payload_metadata["normalized_url"] = normalized_url
                # Note: chunked pipelines may already set a unique per-chunk doc_id.
                payload_metadata.setdefault("doc_id", doc_id)
                payload_metadata["content_hash"] = content_hash
                payload_metadata["change_status"] = change_status
                payload_metadata["is_active"] = True
                payload["metadata"] = payload_metadata
                audit_result.payload_delta_records.append(payload)

    previous_run = run_repo.get_latest_completed_run(exclude_run_id=run.run_id)
    if previous_run:
        previous_urls = snapshot_repo.list_normalized_urls_by_run(previous_run.run_id)
        for normalized_url in sorted(previous_urls - current_urls):
            master = url_repo.get_by_normalized_url(normalized_url)
            if not master:
                continue
            previous_snapshot = snapshot_repo.get_latest_snapshot_for_url(master.url_id)
            if not previous_snapshot:
                continue
            version_no = previous_snapshot.version_no + 1
            deleted_at = datetime.now(timezone.utc)
            doc_id = build_doc_id(normalized_url)
            snapshot_repo.create_snapshot(
                run_id=run.run_id,
                url_id=master.url_id,
                normalized_url=normalized_url,
                source_url=master.source_url,
                parent_url=None,
                discovered_from_url=None,
                depth_level=None,
                title=None,
                meta_description=None,
                canonical_url=master.canonical_url,
                raw_content=None,
                json_content={"deleted_url": normalized_url},
                content_hash=previous_snapshot.content_hash,
                previous_content_hash=previous_snapshot.content_hash,
                source_hash=build_doc_id(normalized_url),
                version_no=version_no,
                change_status=STATUS_DELETED,
                changed_fields_json={"fields": ["deleted"]},
                http_status_code=None,
                fetch_status="DELETED",
                response_time_ms=None,
                request_method=None,
                request_headers_json={},
                response_headers_json={},
                cookies_json={},
                user_agent=None,
                error_message=None,
                index_status="PENDING",
                is_active=False,
                deleted_at=deleted_at,
            )
            master.is_active = False
            master.deleted_at = deleted_at
            master.last_change_status = STATUS_DELETED
            master.index_status = "PENDING"
            counters.deleted_urls += 1
            audit_result.deleted_doc_ids.append(doc_id)

    return audit_result


def process_run_raw(
    session: Session,
    *,
    crawl_records: list[dict[str, Any]],
    crawl_source_name: str,
    root_url: str,
    run_metadata: dict[str, Any] | None = None,
) -> AuditRawResult:
    """
    Persist crawl snapshots + change detection in SQLite and return the URL delta set.

    This intentionally does NOT depend on chunk payloads. Chunk payloads are produced later
    and filtered separately so the pipeline creates exactly one SQLite run per crawl.
    """
    run_repo = CrawlRunRepository(session)
    url_repo = UrlRepository(session)
    snapshot_repo = SnapshotRepository(session)

    run = run_repo.create_run(
        crawl_source_name=crawl_source_name,
        root_url=root_url,
        run_metadata=run_metadata,
    )
    # Note: we intentionally capture a per-record timestamp inside the loop to avoid
    # many rows sharing the exact same second when viewed in SQLite GUIs.
    counters = RunCounters(total_urls=len(crawl_records))
    audit_result = AuditRawResult(run_id=run.run_id, counters=counters)
    current_urls: set[str] = set()

    for record in crawl_records:
        now = datetime.now(timezone.utc)
        source_url = _get_text(record.get("source_url"))
        normalized_url = normalize_url(source_url)
        if not normalized_url:
            continue
        current_urls.add(normalized_url)

        metadata = _safe_dict(record.get("metadata"))
        canonical_url = _get_text(metadata.get("canonical_url")) or None
        domain_name = urlsplit(normalized_url).netloc
        master = url_repo.upsert_url(
            normalized_url=normalized_url,
            source_url=source_url,
            domain_name=domain_name,
            canonical_url=canonical_url,
            now=now,
        )
        previous_snapshot = snapshot_repo.get_latest_snapshot_for_url(master.url_id)

        title = _get_text(record.get("title"))
        description = _get_text(record.get("description"))
        markdown = _get_text(record.get("markdown"))
        success = bool(record.get("success"))
        status_code = _safe_int(record.get("status_code"))

        if success:
            counters.success_urls += 1
            counters.total_bytes += len(markdown.encode("utf-8"))
        else:
            counters.failed_urls += 1

        current_hash = compute_content_hash(title, description, markdown)
        previous_hash = previous_snapshot.content_hash if previous_snapshot else None
        previous_content = _safe_dict(previous_snapshot.json_content) if previous_snapshot else {}
        previous_title = _get_text(previous_content.get("title"))
        previous_description = _get_text(previous_content.get("description"))
        previous_markdown = _get_text(previous_content.get("markdown"))

        if success:
            change = classify_change(
                title=title,
                description=description,
                markdown=markdown,
                previous_hash=previous_hash,
                previous_title=previous_title,
                previous_description=previous_description,
                previous_markdown=previous_markdown,
            )
            change_status = change.status
            changed_fields = change.changed_fields
            previous_content_hash = change.previous_hash
            content_hash = change.current_hash
        else:
            # Failed fetches are still audited, but they do not trigger ES content updates.
            change_status = STATUS_NEW if not previous_snapshot else STATUS_UNCHANGED
            changed_fields = []
            previous_content_hash = previous_hash
            content_hash = previous_hash or current_hash

        if change_status == STATUS_NEW:
            counters.new_urls += 1
        elif change_status == STATUS_UPDATED:
            counters.updated_urls += 1
        elif change_status == STATUS_UNCHANGED:
            counters.unchanged_urls += 1

        version_no = (previous_snapshot.version_no + 1) if previous_snapshot else 1
        fetch_status = "SUCCESS" if success else "FAILED"
        index_status = "PENDING" if success and change_status in {STATUS_NEW, STATUS_UPDATED} else "SKIPPED"

        snapshot_repo.create_snapshot(
            run_id=run.run_id,
            url_id=master.url_id,
            normalized_url=normalized_url,
            source_url=source_url,
            parent_url=_get_text(record.get("parent_url")) or None,
            discovered_from_url=_get_text(record.get("parent_url")) or None,
            depth_level=_safe_int(record.get("depth")),
            title=title or None,
            meta_description=description or None,
            canonical_url=canonical_url,
            raw_content=markdown or None,
            json_content={
                "title": title,
                "description": description,
                "markdown": markdown,
                "metadata": metadata,
            },
            content_hash=content_hash,
            previous_content_hash=previous_content_hash,
            source_hash=build_doc_id(normalized_url),
            version_no=version_no,
            change_status=change_status,
            changed_fields_json={"fields": changed_fields},
            http_status_code=status_code,
            fetch_status=fetch_status,
            response_time_ms=_safe_int(record.get("response_time_ms")),
            request_method=_get_text(record.get("request_method")) or "GET",
            request_headers_json=_safe_dict(record.get("request_headers_final"))
            or _safe_dict(record.get("request_headers_sent")),
            response_headers_json=_safe_dict(record.get("response_headers")),
            cookies_json=_safe_dict(record.get("request_cookies_final"))
            or _safe_dict(record.get("request_cookies_sent")),
            user_agent=_get_text(record.get("user_agent"))
            or _get_text(_safe_dict(record.get("request_headers_sent")).get("User-Agent")),
            error_message=_get_text(record.get("error")) or None,
            index_status=index_status,
            is_active=True,
            deleted_at=None,
        )

        master.last_content_hash = content_hash
        master.last_change_status = change_status
        master.index_status = index_status
        master.is_active = True
        master.deleted_at = None

        if success and change_status in {STATUS_NEW, STATUS_UPDATED}:
            audit_result.changed_normalized_urls.add(normalized_url)

    previous_run = run_repo.get_latest_completed_run(exclude_run_id=run.run_id)
    if previous_run:
        previous_urls = snapshot_repo.list_normalized_urls_by_run(previous_run.run_id)
        for normalized_url in sorted(previous_urls - current_urls):
            master = url_repo.get_by_normalized_url(normalized_url)
            if not master:
                continue
            previous_snapshot = snapshot_repo.get_latest_snapshot_for_url(master.url_id)
            if not previous_snapshot:
                continue
            version_no = previous_snapshot.version_no + 1
            deleted_at = datetime.now(timezone.utc)
            doc_id = build_doc_id(normalized_url)
            snapshot_repo.create_snapshot(
                run_id=run.run_id,
                url_id=master.url_id,
                normalized_url=normalized_url,
                source_url=master.source_url,
                parent_url=None,
                discovered_from_url=None,
                depth_level=None,
                title=None,
                meta_description=None,
                canonical_url=master.canonical_url,
                raw_content=None,
                json_content={"deleted_url": normalized_url},
                content_hash=previous_snapshot.content_hash,
                previous_content_hash=previous_snapshot.content_hash,
                source_hash=build_doc_id(normalized_url),
                version_no=version_no,
                change_status=STATUS_DELETED,
                changed_fields_json={"fields": ["deleted"]},
                http_status_code=None,
                fetch_status="DELETED",
                response_time_ms=None,
                request_method=None,
                request_headers_json={},
                response_headers_json={},
                cookies_json={},
                user_agent=None,
                error_message=None,
                index_status="PENDING",
                is_active=False,
                deleted_at=deleted_at,
            )
            master.is_active = False
            master.deleted_at = deleted_at
            master.last_change_status = STATUS_DELETED
            master.index_status = "PENDING"
            counters.deleted_urls += 1
            audit_result.deleted_doc_ids.append(doc_id)

    return audit_result
