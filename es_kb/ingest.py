from __future__ import annotations

import csv
import json
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from .config import AppSettings
from .mapping import ensure_index
from .models import EmbeddingRecord, indexed_document_from_record
from .normalize_hash import build_doc_id, normalize_url

logger = logging.getLogger(__name__)


def _extract_bulk_error_reason(error_item: Any) -> str:
    if not isinstance(error_item, dict):
        return str(error_item)
    if len(error_item) == 1:
        op_data = next(iter(error_item.values()))
        if isinstance(op_data, dict):
            err = op_data.get("error")
            if isinstance(err, dict):
                reason = err.get("reason")
                error_type = err.get("type")
                if error_type and reason:
                    return f"{error_type}: {reason}"
                if reason:
                    return str(reason)
            if err:
                return str(err)
    return str(error_item)


def iter_input_files(input_path: Path) -> Iterator[Path]:
    if input_path.is_file():
        yield input_path
        return
    if not input_path.exists():
        raise FileNotFoundError(f"Input path does not exist: {input_path}")
    for pattern in ("*.jsonl", "*.json", "*.csv"):
        yield from sorted(input_path.glob(pattern))


def _iter_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        text = line.strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON on line {line_no}: {path}") from exc
        if isinstance(payload, dict):
            yield payload


def _iter_json(path: Path) -> Iterator[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                yield item
    elif isinstance(payload, dict):
        if isinstance(payload.get("records"), list):
            for item in payload["records"]:
                if isinstance(item, dict):
                    yield item
        else:
            yield payload


def _iter_csv(path: Path) -> Iterator[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            out: dict[str, Any] = {
                "embedding_text": row.get("embedding_text", ""),
                "embedding": json.loads(row.get("embedding", "[]") or "[]"),
            }
            if row.get("metadata"):
                out["metadata"] = json.loads(row["metadata"])
            else:
                out["metadata"] = {
                    key: value
                    for key, value in row.items()
                    if key not in {"embedding_text", "embedding", "metadata"} and value not in (None, "")
                }
            yield out


def iter_embedding_records(input_path: Path) -> Iterator[EmbeddingRecord]:
    for file_path in iter_input_files(input_path):
        logger.info("Reading %s", file_path)
        suffix = file_path.suffix.lower()
        if suffix == ".jsonl":
            iterator = _iter_jsonl(file_path)
        elif suffix == ".json":
            iterator = _iter_json(file_path)
        elif suffix == ".csv":
            iterator = _iter_csv(file_path)
        else:
            continue

        for raw in iterator:
            try:
                yield EmbeddingRecord.model_validate(raw)
            except Exception:
                logger.exception("Skipping invalid record from %s", file_path)


def bulk_index_documents(
    es: Elasticsearch,
    settings: AppSettings,
    records: Iterator[EmbeddingRecord],
    recreate_index: bool = False,
) -> tuple[int, int]:
    ensure_index(es, settings, recreate=recreate_index)

    def actions() -> Iterator[dict[str, Any]]:
        for record in records:
            try:
                doc = indexed_document_from_record(record, settings.embedding_dims)
            except Exception:
                logger.exception("Skipping record with invalid embedding/model shape")
                continue
            metadata = dict(record.metadata or {})
            source = doc.to_source()
            source.setdefault("is_active", True)
            source["deleted_at"] = None
            explicit_doc_id = str(metadata.get("doc_id") or "").strip()
            normalized_url = normalize_url(
                str(metadata.get("normalized_url") or metadata.get("source_url") or "")
            )
            doc_id = explicit_doc_id or (build_doc_id(normalized_url) if normalized_url else None)
            yield {
                "_op_type": "index",
                "_index": settings.index_name,
                "_source": source,
                **({"_id": doc_id} if doc_id else {}),
            }

    ok, errors = bulk(
        es,
        actions(),
        chunk_size=settings.bulk_chunk_size,
        raise_on_error=False,
        refresh=settings.bulk_refresh,
    )
    error_count = len(errors) if errors else 0
    logger.info("Bulk indexed=%s errors=%s", ok, error_count)
    if error_count:
        for idx, error_item in enumerate(errors[:3], start=1):
            logger.error("Bulk reject sample #%s: %s", idx, _extract_bulk_error_reason(error_item))
    return int(ok or 0), error_count
