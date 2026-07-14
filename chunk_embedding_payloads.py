from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Iterable

from build_embedding_payloads import (
    build_metadata,
    clean_keywords,
    normalize_text,
    parse_url_path_text,
)
from es_kb.normalize_hash import compute_sha256, normalize_markdown_for_hash, normalize_url

DEFAULT_CHUNK_SIZE = 1200
DEFAULT_CHUNK_OVERLAP = 200


def load_pages(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and isinstance(payload.get("pages"), list):
        return [item for item in payload["pages"] if isinstance(item, dict)]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    raise ValueError(f"Unsupported crawl payload shape in {path}")


def is_successful_page(record: dict[str, Any]) -> bool:
    return record.get("success") is True


def get_page_body(record: dict[str, Any]) -> str:
    for key in ("content", "markdown", "body"):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def normalize_body_text(text: str) -> str:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    normalized = re.sub(r"[ \t]+\n", "\n", normalized)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized.strip()


def split_markdown_blocks(text: str) -> list[str]:
    return [block.strip() for block in re.split(r"\n\s*\n+", text) if block.strip()]


def _split_block_to_fit(block: str, *, chunk_size: int) -> list[str]:
    if len(block) <= chunk_size:
        return [block]
    pieces: list[str] = []
    remaining = block
    while remaining:
        if len(remaining) <= chunk_size:
            pieces.append(remaining.strip())
            break
        cut = remaining.rfind(" ", 0, chunk_size + 1)
        if cut <= 0:
            cut = chunk_size
        piece = remaining[:cut].strip()
        if piece:
            pieces.append(piece)
        remaining = remaining[cut:].lstrip()
    return [piece for piece in pieces if piece]


def _chunk_with_overlap(parts: list[str], *, chunk_size: int, chunk_overlap: int) -> list[str]:
    if not parts:
        return []

    chunks: list[str] = []
    current_parts: list[str] = []
    current_len = 0

    for part in parts:
        joiner_len = 2 if current_parts else 0
        candidate_len = current_len + joiner_len + len(part)
        if candidate_len <= chunk_size:
            current_parts.append(part)
            current_len = candidate_len
            continue

        if current_parts:
            chunk = "\n\n".join(current_parts).strip()
            if chunk:
                chunks.append(chunk)

            if chunk_overlap > 0:
                overlap_parts: list[str] = []
                overlap_len = 0
                for existing in reversed(current_parts):
                    add_len = len(existing) + (2 if overlap_parts else 0)
                    if overlap_len + add_len > chunk_overlap:
                        break
                    overlap_parts.insert(0, existing)
                    overlap_len += add_len
                current_parts = overlap_parts
                current_len = len("\n\n".join(current_parts)) if current_parts else 0
            else:
                current_parts = []
                current_len = 0

        joiner_len = 2 if current_parts else 0
        candidate_len = current_len + joiner_len + len(part)
        if candidate_len <= chunk_size:
            current_parts.append(part)
            current_len = candidate_len
            continue

        if current_parts:
            chunk = "\n\n".join(current_parts).strip()
            if chunk and (not chunks or chunks[-1] != chunk):
                chunks.append(chunk)
            current_parts = []
            current_len = 0
        current_parts.append(part)
        current_len = len(part)

    if current_parts:
        chunk = "\n\n".join(current_parts).strip()
        if chunk:
            chunks.append(chunk)
    return chunks


def chunk_text(text: str, *, chunk_size: int, chunk_overlap: int) -> list[str]:
    normalized = normalize_body_text(text)
    if not normalized:
        return []
    blocks = split_markdown_blocks(normalized)
    if not blocks:
        return []
    normalized_parts: list[str] = []
    for block in blocks:
        normalized_parts.extend(_split_block_to_fit(block, chunk_size=chunk_size))
    return [
        chunk
        for chunk in _chunk_with_overlap(normalized_parts, chunk_size=chunk_size, chunk_overlap=chunk_overlap)
        if chunk.strip()
    ]


def build_parent_doc_id(source_url: str) -> str:
    normalized = normalize_url(source_url)
    return compute_sha256(normalized or source_url.strip().lower())


def build_chunk_doc_id(parent_doc_id: str, chunk_index: int, chunk_text_value: str) -> str:
    chunk_hash = compute_sha256(normalize_markdown_for_hash(chunk_text_value))
    return compute_sha256(f"{parent_doc_id}:{chunk_index}:{chunk_hash}")


def build_chunk_embedding_text(record: dict[str, Any], chunk_text_value: str) -> str:
    path_value = normalize_text(record.get("path") or record.get("source_url"))
    parts = [
        normalize_text(record.get("title")),
        normalize_text(record.get("description")),
        clean_keywords(record.get("keywords")),
        parse_url_path_text(path_value),
        normalize_text(chunk_text_value),
    ]
    return "\n".join(part for part in parts if part)


def build_chunk_metadata(
    record: dict[str, Any],
    *,
    parent_doc_id: str,
    chunk_index: int,
    chunk_count: int,
    chunk_text_value: str,
) -> dict[str, Any]:
    metadata = build_metadata(record)
    normalized_url = normalize_url(str(metadata.get("source_url") or record.get("source_url") or ""))
    chunk_hash = compute_sha256(normalize_markdown_for_hash(chunk_text_value))
    metadata.update(
        {
            "normalized_url": normalized_url,
            "parent_doc_id": parent_doc_id,
            "chunk_index": chunk_index,
            "chunk_count": chunk_count,
            "chunk_hash": chunk_hash,
            # Important: unique doc per chunk (used downstream in ES ingest)
            "doc_id": build_chunk_doc_id(parent_doc_id, chunk_index, chunk_text_value),
        }
    )
    return metadata


def build_chunk_payload_records(
    pages: Iterable[dict[str, Any]],
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    chunk_overlap: int = DEFAULT_CHUNK_OVERLAP,
) -> list[dict[str, Any]]:
    output_records: list[dict[str, Any]] = []

    for page in pages:
        if not is_successful_page(page):
            continue

        body_text = get_page_body(page)
        if not body_text:
            continue

        chunks = chunk_text(body_text, chunk_size=chunk_size, chunk_overlap=chunk_overlap)
        if not chunks:
            continue

        parent_doc_id = build_parent_doc_id(str(page.get("source_url") or ""))
        chunk_count = len(chunks)

        for chunk_index, chunk in enumerate(chunks):
            embedding_text = build_chunk_embedding_text(page, chunk)
            if not embedding_text:
                continue
            output_records.append(
                {
                    "embedding_text": embedding_text,
                    "metadata": build_chunk_metadata(
                        page,
                        parent_doc_id=parent_doc_id,
                        chunk_index=chunk_index,
                        chunk_count=chunk_count,
                        chunk_text_value=chunk,
                    ),
                }
            )

    return output_records


def write_jsonl(records: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(record, ensure_ascii=False) for record in records]
    path.write_text("\n".join(lines), encoding="utf-8")

