from __future__ import annotations

import logging
from collections.abc import Iterable, Iterator
from datetime import datetime, timezone
from typing import Any

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from .config import AppSettings
from .ingest import _extract_bulk_error_reason
from .mapping import ensure_index
from .models import EmbeddingRecord, indexed_document_from_record
from .normalize_hash import build_doc_id, normalize_url

logger = logging.getLogger(__name__)


def _failed_index_doc_ids(errors: list[Any] | bool | None) -> set[str]:
    """Collect Elasticsearch `_id` values for failed index/create bulk operations."""
    if not errors or errors is False:
        return set()
    failed: set[str] = set()
    for item in errors:
        if not isinstance(item, dict):
            continue
        for op_name, detail in item.items():
            if op_name not in ("index", "create") or not isinstance(detail, dict):
                continue
            if not detail.get("error"):
                continue
            doc_id = detail.get("_id")
            if doc_id:
                failed.add(str(doc_id))
    return failed


def _record_doc_id(record: EmbeddingRecord) -> str:
    metadata = dict(record.metadata or {})
    explicit = str(metadata.get("doc_id") or "").strip()
    if explicit:
        return explicit
    normalized_url = normalize_url(str(metadata.get("normalized_url") or metadata.get("source_url") or ""))
    return build_doc_id(normalized_url)


def _iter_es_actions(
    settings: AppSettings,
    records: Iterable[EmbeddingRecord],
    deleted_doc_ids: list[str],
) -> Iterator[dict[str, Any]]:
    for record in records:
        try:
            doc = indexed_document_from_record(record, settings.embedding_dims)
        except Exception:
            logger.exception("Skipping invalid embedding record during ES sync")
            continue
        source = doc.to_source()
        source.setdefault("is_active", True)
        source["deleted_at"] = None
        source["change_status"] = str((record.metadata or {}).get("change_status") or "")
        yield {
            "_op_type": "index",
            "_index": settings.index_name,
            "_id": _record_doc_id(record),
            "_source": source,
        }

    if settings.es_delete_mode == "hard_delete":
        for doc_id in deleted_doc_ids:
            yield {
                "_op_type": "delete",
                "_index": settings.index_name,
                "_id": doc_id,
            }
    else:
        deleted_at = datetime.now(timezone.utc).isoformat()
        for doc_id in deleted_doc_ids:
            yield {
                "_op_type": "update",
                "_index": settings.index_name,
                "_id": doc_id,
                "doc": {
                    "is_active": False,
                    "deleted_at": deleted_at,
                    "change_status": "DELETED",
                },
                "doc_as_upsert": False,
            }


def sync_index_changes(
    es: Elasticsearch,
    settings: AppSettings,
    records: Iterable[EmbeddingRecord],
    deleted_doc_ids: list[str],
    *,
    recreate_index: bool = False,
) -> tuple[int, int, int, list[str]]:
    """Run bulk index/delete against ES.

    Returns ``(ok_ops, delete_count, error_count, indexed_normalized_urls)`` where
    ``indexed_normalized_urls`` is the list of crawl URLs whose **index** operation
    succeeded (for updating SQLite ``index_status`` to ``INDEXED``).
    """
    ensure_index(es, settings, recreate=recreate_index)

    records_list = list(records)
    index_pairs: list[tuple[str, str]] = []
    for record in records_list:
        try:
            indexed_document_from_record(record, settings.embedding_dims)
        except Exception:
            logger.exception("Skipping invalid embedding record during ES sync")
            continue
        doc_id = _record_doc_id(record)
        metadata = dict(record.metadata or {})
        normalized_url = normalize_url(str(metadata.get("normalized_url") or metadata.get("source_url") or ""))
        if normalized_url:
            index_pairs.append((doc_id, normalized_url))

    ok, errors = bulk(
        es,
        _iter_es_actions(settings, records_list, deleted_doc_ids),
        chunk_size=settings.bulk_chunk_size,
        raise_on_error=False,
        refresh=settings.bulk_refresh,
    )
    error_count = len(errors) if errors else 0
    for idx, error_item in enumerate((errors or [])[:3], start=1):
        logger.error("ES sync error #%s: %s", idx, _extract_bulk_error_reason(error_item))

    failed_ids = _failed_index_doc_ids(errors if errors else False)
    indexed_normalized_urls = [norm for did, norm in index_pairs if did not in failed_ids]

    return int(ok or 0), len(deleted_doc_ids), error_count, indexed_normalized_urls
