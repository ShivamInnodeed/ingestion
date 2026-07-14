from __future__ import annotations

import logging
from typing import Any

from elasticsearch import Elasticsearch

from .config import AppSettings

logger = logging.getLogger(__name__)


def _normalize_prefix(prefix: str) -> str:
    return " ".join(prefix.lower().split())


def _keyword_matches_prefix_tokens(keyword: str, prefix_tokens: list[str]) -> bool:
    """Match typeahead-style: each typed token must prefix-match successive keyword tokens.

    Uses a sliding window so "address change" matches "billing address change" but not "pin".
    """
    if not prefix_tokens:
        return True
    kt = " ".join(keyword.lower().split()).split()
    if len(kt) < len(prefix_tokens):
        return False
    m = len(prefix_tokens)
    for start in range(0, len(kt) - m + 1):
        if all(kt[start + i].startswith(prefix_tokens[i]) for i in range(m)):
            return True
    return False


def build_suggest_query(prefix: str, size: int) -> dict[str, Any]:
    return {
        "size": max(size, 1),
        "source": ["title", "keywords"],
        "query": {
            "bool": {
                "filter": [
                    {
                        "bool": {
                            "should": [
                                {"term": {"is_active": True}},
                                {"bool": {"must_not": {"exists": {"field": "is_active"}}}},
                            ],
                            "minimum_should_match": 1,
                        }
                    }
                ],
                "must": {
                    "multi_match": {
                        "query": prefix,
                        "type": "bool_prefix",
                        "fields": [
                            "title_suggest",
                            "title_suggest._2gram",
                            "title_suggest._3gram",
                            "keywords_suggest",
                            "keywords_suggest._2gram",
                            "keywords_suggest._3gram",
                        ],
                    }
                },
            }
        },
    }


def suggest_candidates(
    es: Elasticsearch,
    settings: AppSettings,
    prefix: str,
    size: int | None = None,
) -> list[dict[str, str]]:
    max_size = size or settings.suggest_size_default
    prefix_norm = _normalize_prefix(prefix)
    if not prefix_norm:
        return []
    prefix_tokens = prefix_norm.split()

    payload = build_suggest_query(prefix, max(max_size * 3, max_size + 8))
    response = es.search(index=settings.index_name, **payload)
    hits = response.get("hits", {}).get("hits", [])

    out: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def add(value: str, source_field: str) -> None:
        suggestion = " ".join(value.split())
        if not suggestion:
            return
        key = (suggestion.lower(), source_field)
        if key in seen:
            return
        seen.add(key)
        out.append({"suggestion": suggestion, "source_field": source_field})

    for hit in hits:
        source = hit.get("_source") or {}
        title = str(source.get("title") or "").strip()
        if title:
            add(title, "title")

        keywords = source.get("keywords") or []
        if isinstance(keywords, list):
            for keyword in keywords:
                if isinstance(keyword, str):
                    stripped = keyword.strip()
                    if stripped and _keyword_matches_prefix_tokens(stripped, prefix_tokens):
                        add(stripped, "keywords")

        if len(out) >= max_size:
            break

    logger.info("suggest_candidates prefix=%r suggestions=%s", prefix_norm, len(out))
    return out[:max_size]
