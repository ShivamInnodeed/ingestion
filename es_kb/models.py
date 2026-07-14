from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, Field, field_validator


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
    for p in parts:
        if not p or p in seen:
            continue
        seen.add(p)
        out.append(p)
    return out


class EmbeddingRecord(BaseModel):
    embedding_text: str = ""
    embedding: list[float] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("embedding", mode="before")
    @classmethod
    def parse_embedding(cls, value: Any) -> list[float]:
        if value is None:
            return []
        if isinstance(value, str):
            return [float(x) for x in json.loads(value)]
        if isinstance(value, list):
            return [float(x) for x in value]
        raise ValueError("embedding must be list[float] or JSON string")


class IndexedDocument(BaseModel):
    embedding_text: str
    embedding: list[float]
    metadata: dict[str, Any]

    title: str = ""
    description: str = ""
    keywords: list[str] = Field(default_factory=list)
    keywords_joined: str = ""
    search_text: str = ""

    title_suggest: str = ""
    keywords_suggest: str = ""

    source_url: str | None = None
    normalized_url: str | None = None
    path: str | None = None
    category: str | None = None
    sub_category: str | None = None
    parsed_url_path_text: str | None = None
    content_hash: str | None = None
    change_status: str | None = None
    is_active: bool | None = None
    deleted_at: str | None = None
    depth: int | None = None
    status_code: int | None = None
    success: bool | None = None

    def to_source(self) -> dict[str, Any]:
        return self.model_dump(exclude_none=True)


def indexed_document_from_record(record: EmbeddingRecord, embedding_dims: int) -> IndexedDocument:
    if len(record.embedding) != embedding_dims:
        raise ValueError(f"embedding dims mismatch: {len(record.embedding)} != {embedding_dims}")

    metadata = dict(record.metadata or {})
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
    search_text = " ".join(p for p in [title, description, keywords_joined, *extras] if p)

    def as_int(value: Any) -> int | None:
        if value is None or str(value).strip() == "":
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    success = metadata.get("success")
    success_bool = success if isinstance(success, bool) else None

    return IndexedDocument(
        embedding_text=normalize_text(record.embedding_text),
        embedding=record.embedding,
        metadata=metadata,
        title=title,
        description=description,
        keywords=keywords,
        keywords_joined=keywords_joined,
        search_text=search_text,
        title_suggest=title,
        keywords_suggest=keywords_joined,
        source_url=normalize_text(metadata.get("source_url")) or None,
        normalized_url=normalize_text(metadata.get("normalized_url")) or None,
        path=normalize_text(metadata.get("path")) or None,
        category=normalize_text(metadata.get("category")) or None,
        sub_category=normalize_text(metadata.get("sub_category")) or None,
        parsed_url_path_text=normalize_text(metadata.get("parsed_url_path_text")) or None,
        content_hash=normalize_text(metadata.get("content_hash")) or None,
        change_status=normalize_text(metadata.get("change_status")) or None,
        is_active=bool(metadata.get("is_active")) if metadata.get("is_active") is not None else True,
        deleted_at=normalize_text(metadata.get("deleted_at")) or None,
        depth=as_int(metadata.get("depth")),
        status_code=as_int(metadata.get("status_code")),
        success=success_bool,
    )
