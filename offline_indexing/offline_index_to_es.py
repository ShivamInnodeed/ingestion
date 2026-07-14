#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import logging
import ssl
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

logger = logging.getLogger("offline_es_indexer")


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split()).strip()


def normalize_keywords(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        parts = [p.strip().lower() for p in value.split(",")]
    elif isinstance(value, list):
        parts = [normalize_text(v).lower() for v in value]
    else:
        parts = [normalize_text(value).lower()]

    out: list[str] = []
    seen: set[str] = set()
    for part in parts:
        if not part or part in seen:
            continue
        seen.add(part)
        out.append(part)
    return out


def normalize_url(value: str | None) -> str:
    if not value:
        return ""
    parts = urlsplit(value.strip())
    scheme = (parts.scheme or "https").lower()
    netloc = (parts.netloc or "").lower()
    path = parts.path or "/"
    query_items = parse_qsl(parts.query, keep_blank_values=True)
    query_items.sort(key=lambda item: item[0])
    normalized_query = urlencode(query_items, doseq=True)
    return urlunsplit((scheme, netloc, path, normalized_query, ""))


def build_doc_id(normalized_url: str) -> str:
    return hashlib.sha256(normalized_url.encode("utf-8")).hexdigest()


def build_index_body(embedding_dims: int) -> dict[str, Any]:
    return {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "embedding_text": {"type": "text"},
                "embedding": {"type": "dense_vector", "dims": embedding_dims, "index": True, "similarity": "cosine"},
                "metadata": {"enabled": True},
                "title": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 512}}},
                "description": {"type": "text"},
                "keywords": {"type": "keyword"},
                "keywords_joined": {"type": "text"},
                "search_text": {"type": "text"},
                "title_suggest": {"type": "search_as_you_type"},
                "keywords_suggest": {"type": "search_as_you_type"},
                "source_url": {"type": "keyword", "ignore_above": 2048},
                "normalized_url": {"type": "keyword", "ignore_above": 4096},
                "path": {"type": "keyword", "ignore_above": 1024},
                "category": {"type": "keyword", "ignore_above": 256},
                "sub_category": {"type": "keyword", "ignore_above": 256},
                "parsed_url_path_text": {"type": "keyword", "ignore_above": 512},
                "content_hash": {"type": "keyword", "ignore_above": 128},
                "change_status": {"type": "keyword", "ignore_above": 64},
                "is_active": {"type": "boolean"},
                "deleted_at": {"type": "date"},
                "depth": {"type": "integer"},
                "status_code": {"type": "integer"},
                "success": {"type": "boolean"},
            }
        },
    }


class EsHttpClient:
    def __init__(
        self,
        *,
        base_url: str,
        username: str | None,
        password: str | None,
        verify_certs: bool,
        request_timeout: int,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.request_timeout = request_timeout
        self.auth_header: str | None = None
        if username and password:
            token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
            self.auth_header = f"Basic {token}"

        self.ssl_context: ssl.SSLContext | None = None
        if self.base_url.startswith("https://"):
            if verify_certs:
                self.ssl_context = ssl.create_default_context()
            else:
                self.ssl_context = ssl._create_unverified_context()  # noqa: SLF001

    def _request(
        self,
        method: str,
        path: str,
        *,
        body: bytes | None = None,
        content_type: str = "application/json",
    ) -> tuple[int, bytes]:
        url = f"{self.base_url}{path}"
        headers = {"Accept": "application/json"}
        if self.auth_header:
            headers["Authorization"] = self.auth_header
        if body is not None:
            headers["Content-Type"] = content_type

        request = urllib.request.Request(url, method=method, data=body, headers=headers)
        try:
            with urllib.request.urlopen(
                request,
                timeout=self.request_timeout,
                context=self.ssl_context,
            ) as response:
                return int(response.status), response.read()
        except urllib.error.HTTPError as exc:
            return int(exc.code), exc.read()

    def request_json(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        expected_statuses: tuple[int, ...] = (200,),
    ) -> dict[str, Any]:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8") if payload is not None else None
        status, raw = self._request(method, path, body=body, content_type="application/json")
        text = raw.decode("utf-8", errors="replace") if raw else ""
        if status not in expected_statuses:
            raise RuntimeError(f"HTTP {status} for {method} {path}: {text}")
        if not text.strip():
            return {}
        value = json.loads(text)
        if not isinstance(value, dict):
            raise ValueError(f"Expected JSON object from {method} {path}, got: {type(value).__name__}")
        return value

    def request_ndjson(
        self,
        path: str,
        *,
        ndjson_payload: str,
        expected_statuses: tuple[int, ...] = (200,),
    ) -> dict[str, Any]:
        status, raw = self._request(
            "POST",
            path,
            body=ndjson_payload.encode("utf-8"),
            content_type="application/x-ndjson",
        )
        text = raw.decode("utf-8", errors="replace") if raw else ""
        if status not in expected_statuses:
            raise RuntimeError(f"HTTP {status} for POST {path}: {text}")
        value = json.loads(text or "{}")
        if not isinstance(value, dict):
            raise ValueError(f"Expected JSON object from POST {path}, got: {type(value).__name__}")
        return value


def _exists_index(client: EsHttpClient, index_name: str) -> bool:
    status, _ = client._request("HEAD", f"/{index_name}")
    if status == 200:
        return True
    if status == 404:
        return False
    raise RuntimeError(f"Unexpected status while checking index existence: {status}")


def ensure_index(
    client: EsHttpClient,
    *,
    index_name: str,
    embedding_dims: int,
    recreate_index: bool,
) -> None:
    if recreate_index and _exists_index(client, index_name):
        client.request_json("DELETE", f"/{index_name}", expected_statuses=(200,))
    if _exists_index(client, index_name):
        return
    body = build_index_body(embedding_dims=embedding_dims)
    client.request_json("PUT", f"/{index_name}", payload=body, expected_statuses=(200,))


def _extract_bulk_error_reason(error_item: Any) -> str:
    if not isinstance(error_item, dict):
        return str(error_item)
    if len(error_item) == 1:
        op_data = next(iter(error_item.values()))
        if isinstance(op_data, dict):
            err = op_data.get("error")
            if isinstance(err, dict):
                err_type = err.get("type")
                reason = err.get("reason")
                if err_type and reason:
                    return f"{err_type}: {reason}"
                if reason:
                    return str(reason)
            if err:
                return str(err)
    return str(error_item)


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return payload


def _iter_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            value = json.loads(text)
            if not isinstance(value, dict):
                raise ValueError(f"Expected object at {path}:{line_no}")
            yield value


def _as_int(value: Any) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _build_source(record: dict[str, Any], embedding_dims: int) -> tuple[dict[str, Any], str]:
    embedding_text = normalize_text(record.get("embedding_text"))
    embedding_raw = record.get("embedding") or []
    if not isinstance(embedding_raw, list):
        raise ValueError("embedding must be list[float]")
    embedding = [float(value) for value in embedding_raw]
    if len(embedding) != embedding_dims:
        raise ValueError(f"embedding dims mismatch: {len(embedding)} != {embedding_dims}")

    metadata = dict(record.get("metadata") or {})
    title = normalize_text(metadata.get("title"))
    description = normalize_text(metadata.get("description"))
    keywords = normalize_keywords(metadata.get("keywords"))
    keywords_joined = " ".join(keywords)
    extras = [
        normalize_text(metadata.get("source_url")),
        normalize_text(metadata.get("path")),
        normalize_text(metadata.get("category")),
        normalize_text(metadata.get("sub_category")),
        normalize_text(metadata.get("parsed_url_path_text")),
    ]
    search_text = " ".join(part for part in [title, description, keywords_joined, *extras] if part)
    success = metadata.get("success")
    success_bool = success if isinstance(success, bool) else None

    source = {
        "embedding_text": embedding_text,
        "embedding": embedding,
        "metadata": metadata,
        "title": title,
        "description": description,
        "keywords": keywords,
        "keywords_joined": keywords_joined,
        "search_text": search_text,
        "title_suggest": title,
        "keywords_suggest": keywords_joined,
        "source_url": normalize_text(metadata.get("source_url")) or None,
        "normalized_url": normalize_text(metadata.get("normalized_url")) or None,
        "path": normalize_text(metadata.get("path")) or None,
        "category": normalize_text(metadata.get("category")) or None,
        "sub_category": normalize_text(metadata.get("sub_category")) or None,
        "parsed_url_path_text": normalize_text(metadata.get("parsed_url_path_text")) or None,
        "content_hash": normalize_text(metadata.get("content_hash")) or None,
        "change_status": normalize_text(metadata.get("change_status")) or None,
        "is_active": bool(metadata.get("is_active")) if metadata.get("is_active") is not None else True,
        "deleted_at": normalize_text(metadata.get("deleted_at")) or None,
        "depth": _as_int(metadata.get("depth")),
        "status_code": _as_int(metadata.get("status_code")),
        "success": success_bool,
    }
    source = {key: value for key, value in source.items() if value is not None}
    source.setdefault("is_active", True)
    source["deleted_at"] = None
    source["change_status"] = str(metadata.get("change_status") or "")

    explicit_doc_id = str(metadata.get("doc_id") or "").strip()
    if explicit_doc_id:
        doc_id = explicit_doc_id
    else:
        normalized = normalize_url(str(metadata.get("normalized_url") or metadata.get("source_url") or ""))
        doc_id = build_doc_id(normalized)
    return source, doc_id


def _deleted_doc_ids(path: Path) -> list[str]:
    if not path.is_file():
        return []
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _bulk_lines_from_actions(actions: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    for action in actions:
        op_type = str(action.get("_op_type") or "").strip()
        index_name = str(action.get("_index") or "").strip()
        doc_id = str(action.get("_id") or "").strip()
        if not op_type or not index_name or not doc_id:
            raise ValueError(f"Invalid action payload: {action}")
        meta = {op_type: {"_index": index_name, "_id": doc_id}}
        lines.append(json.dumps(meta, ensure_ascii=False))
        if op_type == "index":
            lines.append(json.dumps(action.get("_source") or {}, ensure_ascii=False))
        elif op_type == "update":
            lines.append(
                json.dumps(
                    {
                        "doc": action.get("doc") or {},
                        "doc_as_upsert": bool(action.get("doc_as_upsert", False)),
                    },
                    ensure_ascii=False,
                )
            )
    return "\n".join(lines) + "\n"


def _chunked(items: list[dict[str, Any]], size: int):
    if size <= 0:
        size = 200
    for i in range(0, len(items), size):
        yield items[i : i + size]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay offline embedding bundle into Elasticsearch.")
    parser.add_argument("--bundle-dir", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--es-url", default=None, help="Override Elasticsearch URL from bundle settings.")
    parser.add_argument("--es-username", default=None, help="Override Elasticsearch username.")
    parser.add_argument("--es-password", default=None, help="Override Elasticsearch password.")
    parser.add_argument("--recreate-index", action="store_true", help="Delete and recreate index before indexing.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s %(message)s")

    bundle_dir = args.bundle_dir.resolve()
    manifest_path = bundle_dir / "bundle_manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Missing bundle manifest: {manifest_path}")
    manifest = _read_json(manifest_path)

    files = dict(manifest.get("files") or {})
    settings_path = bundle_dir / str(files.get("settings") or "es_effective_config.json")
    embeddings_path = bundle_dir / str(files.get("embeddings") or "embedding_vectors.jsonl")
    deleted_doc_ids_path = bundle_dir / str(files.get("deleted_doc_ids") or "deleted_doc_ids.jsonl")

    settings = _read_json(settings_path)
    es_url = str(args.es_url or settings.get("elasticsearch_url") or "").strip()
    if not es_url:
        raise ValueError("Elasticsearch URL is required (bundle settings or --es-url).")

    es_username = args.es_username if args.es_username is not None else settings.get("elasticsearch_username")
    es_password = args.es_password if args.es_password is not None else settings.get("elasticsearch_password")
    verify_certs = bool(settings.get("verify_certs", True))
    request_timeout = int(settings.get("request_timeout", 120))
    index_name = str(settings.get("index_name") or "kb_documents")
    embedding_dims = int(settings.get("embedding_dims", 384))
    bulk_chunk_size = int(settings.get("bulk_chunk_size", 200))
    bulk_refresh = settings.get("bulk_refresh", False)
    es_delete_mode = str(settings.get("es_delete_mode") or "hard_delete")

    client = EsHttpClient(
        base_url=es_url,
        username=es_username,
        password=es_password,
        verify_certs=verify_certs,
        request_timeout=request_timeout,
    )
    ensure_index(
        client,
        index_name=index_name,
        embedding_dims=embedding_dims,
        recreate_index=bool(args.recreate_index),
    )

    indexed_records = 0
    deleted_doc_ids = _deleted_doc_ids(deleted_doc_ids_path)

    def actions():
        nonlocal indexed_records
        for raw in _iter_jsonl(embeddings_path):
            source, doc_id = _build_source(raw, embedding_dims=embedding_dims)
            indexed_records += 1
            yield {"_op_type": "index", "_index": index_name, "_id": doc_id, "_source": source}

        if es_delete_mode == "hard_delete":
            for doc_id in deleted_doc_ids:
                yield {"_op_type": "delete", "_index": index_name, "_id": doc_id}
        else:
            deleted_at = datetime.now(timezone.utc).isoformat()
            for doc_id in deleted_doc_ids:
                yield {
                    "_op_type": "update",
                    "_index": index_name,
                    "_id": doc_id,
                    "doc": {"is_active": False, "deleted_at": deleted_at, "change_status": "DELETED"},
                    "doc_as_upsert": False,
                }

    all_actions = list(actions())
    ok = 0
    errors: list[Any] = []
    refresh_query = ""
    if isinstance(bulk_refresh, bool):
        refresh_query = f"?refresh={'true' if bulk_refresh else 'false'}"
    elif str(bulk_refresh).strip():
        refresh_query = f"?refresh={str(bulk_refresh).strip()}"

    for chunk in _chunked(all_actions, bulk_chunk_size):
        payload = _bulk_lines_from_actions(chunk)
        response = client.request_ndjson(f"/_bulk{refresh_query}", ndjson_payload=payload, expected_statuses=(200,))
        items = response.get("items") if isinstance(response, dict) else None
        if not isinstance(items, list):
            raise RuntimeError(f"Unexpected bulk response shape: {response}")
        for item in items:
            if not isinstance(item, dict) or len(item) != 1:
                errors.append(item)
                continue
            op_data = next(iter(item.values()))
            if not isinstance(op_data, dict):
                errors.append(item)
                continue
            status = int(op_data.get("status") or 0)
            if 200 <= status < 300:
                ok += 1
            else:
                errors.append(item)

    error_count = len(errors)
    for idx, error_item in enumerate(errors[:3], start=1):
        logger.error("Bulk error #%s: %s", idx, _extract_bulk_error_reason(error_item))

    summary = {
        "bundle_dir": str(bundle_dir),
        "index_name": index_name,
        "indexed_records": indexed_records,
        "deleted_doc_ids": len(deleted_doc_ids),
        "ok_ops": int(ok),
        "error_count": error_count,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 1 if error_count > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
