from __future__ import annotations

import hashlib
import re
import unicodedata
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

# Zero-width spaces and BOM — often differ between crawls without semantic change.
_ZW_RE = re.compile(r"[\u200b-\u200d\ufeff]")


def normalize_text_for_hash(value: str | None) -> str:
    """Normalize text used for content hashing."""
    if not value:
        return ""
    text = unicodedata.normalize("NFC", str(value))
    text = _ZW_RE.sub("", text)
    text = text.lower().strip()
    text = re.sub(r"\s+", " ", text)
    return text


def normalize_markdown_for_hash(value: str | None) -> str:
    """Normalize markdown before hashing to reduce run-to-run extraction noise."""
    if not value:
        return ""
    text = str(value).replace("\r\n", "\n").replace("\r", "\n")
    text = unicodedata.normalize("NFC", text)
    text = _ZW_RE.sub("", text)
    text = (
        text.replace("\u201c", '"')
        .replace("\u201d", '"')
        .replace("\u2018", "'")
        .replace("\u2019", "'")
        .replace("\u2013", "-")
        .replace("\u2014", "-")
    )
    text = re.sub(r"\n{4,}", "\n\n\n", text)
    text = text.lower().strip()
    text = re.sub(r"\s+", " ", text)
    return text


def normalize_url(value: str | None) -> str:
    """Build a stable URL key used across DB and Elasticsearch."""
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


def compute_sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def compute_content_hash(title: str | None, description: str | None, markdown: str | None) -> str:
    # CRITICAL: content hash must be stable across LLM enrichment.
    # We intentionally hash ONLY crawl-extracted markdown so LLM-generated title/description
    # variations do not create false UPDATED signals on subsequent runs.
    markdown_norm = normalize_markdown_for_hash(markdown)
    return compute_sha256(markdown_norm)


def build_doc_id(normalized_url: str) -> str:
    return compute_sha256(normalized_url)
