from __future__ import annotations

import logging
import re
import time
from functools import lru_cache
from typing import Any

from elasticsearch import Elasticsearch

from .config import AppSettings

logger = logging.getLogger(__name__)


@lru_cache(maxsize=4)
def _load_model(model_name: str):
    from sentence_transformers import SentenceTransformer

    logger.info("Loading SentenceTransformer model %s", model_name)
    return SentenceTransformer(model_name)


def _tokenize(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", text.lower())


def _extract_core_tokens(suggestion: str, settings: AppSettings) -> list[str]:
    if not settings.strict_query_filter_enabled:
        return []

    tokens = _tokenize(suggestion)
    if len(tokens) < max(1, settings.strict_query_min_tokens):
        # Keep current behavior for short/suggestion-like queries.
        return []

    stopwords = {token.strip().lower() for token in settings.strict_query_stopwords if token.strip()}
    core_tokens: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if len(token) <= 1:
            continue
        if token in stopwords:
            continue
        if token in seen:
            continue
        seen.add(token)
        core_tokens.append(token)
    return core_tokens


def encode_query_vector(settings: AppSettings, text: str) -> list[float]:
    model = _load_model(settings.model_name)
    vector = model.encode(
        text,
        normalize_embeddings=settings.normalize_embeddings,
        show_progress_bar=False,
    )
    return vector.tolist()


def metadata_filter_clause(suggestion: str, settings: AppSettings) -> dict[str, Any]:
    lowered = " ".join(suggestion.lower().split())
    tokens = [token for token in lowered.split() if token]
    should: list[dict[str, Any]] = [
        {"wildcard": {"keywords_joined": {"value": f"*{lowered}*", "case_insensitive": True}}},
        {"match": {"keywords_joined": {"query": suggestion, "operator": "or"}}},
    ]
    for token in tokens:
        should.append({"term": {"keywords": token}})

    if settings.metadata_filter_mode == "broad":
        should.extend(
            [
                {"match": {"title": {"query": suggestion, "operator": "or"}}},
                {"match": {"description": {"query": suggestion, "operator": "or"}}},
                {"match": {"search_text": {"query": suggestion, "operator": "or"}}},
                {"wildcard": {"path": {"value": f"*{lowered}*", "case_insensitive": True}}},
                {"wildcard": {"source_url": {"value": f"*{lowered}*", "case_insensitive": True}}},
            ]
        )

    active_filter: dict[str, Any] = {
        "bool": {
            "should": [
                {"term": {"is_active": True}},
                {"bool": {"must_not": {"exists": {"field": "is_active"}}}},
            ],
            "minimum_should_match": 1,
        }
    }
    base_filter: dict[str, Any] = {"bool": {"should": should, "minimum_should_match": 1}}

    core_tokens = _extract_core_tokens(suggestion, settings)
    if not core_tokens:
        return {"bool": {"must": [active_filter, base_filter]}}

    per_token_clauses: list[dict[str, Any]] = []
    for token in core_tokens:
        per_token_clauses.append(
            {
                "bool": {
                    "should": [
                        {"term": {"keywords": token}},
                        {"match": {"title": {"query": token, "operator": "or"}}},
                        {
                            "wildcard": {
                                "path": {
                                    "value": f"*{token}*",
                                    "case_insensitive": True,
                                }
                            }
                        },
                        {
                            "wildcard": {
                                "parsed_url_path_text": {
                                    "value": f"*{token}*",
                                    "case_insensitive": True,
                                }
                            }
                        },
                    ],
                    "minimum_should_match": 1,
                }
            }
        )

    min_core_hits = max(1, int(settings.strict_query_min_core_token_hits))
    min_core_hits = min(min_core_hits, len(per_token_clauses))
    strict_clause = {
        "bool": {
            "should": per_token_clauses,
            "minimum_should_match": min_core_hits,
        }
    }
    return {"bool": {"must": [active_filter, base_filter, strict_clause]}}


def build_lexical_query(suggestion: str, settings: AppSettings) -> dict[str, Any]:
    weights = settings.search_weights
    return {
        "bool": {
            "filter": [metadata_filter_clause(suggestion, settings)],
            "must": [
                {
                    "multi_match": {
                        "query": suggestion,
                        "type": "best_fields",
                        "fields": [
                            f"title^{weights.title_boost}",
                            f"description^{weights.description_boost}",
                            f"keywords_joined^{weights.keyword_boost}",
                        ],
                        "operator": "or",
                    }
                }
            ],
        }
    }


def build_knn_block(
    suggestion: str,
    query_vector: list[float],
    settings: AppSettings,
    size: int,
) -> dict[str, Any]:
    num_candidates = min(10_000, max(size * settings.knn_num_candidates_factor, size * 5))
    return {
        "field": "embedding",
        "query_vector": query_vector,
        "k": max(size, 10),
        "num_candidates": num_candidates,
        "filter": metadata_filter_clause(suggestion, settings),
    }


def _clone_with_boost(query: dict[str, Any], boost: float) -> dict[str, Any]:
    if "bool" in query and isinstance(query["bool"], dict):
        bool_query = dict(query["bool"])
        bool_query["boost"] = boost
        return {"bool": bool_query}
    out = dict(query)
    out["boost"] = boost
    return out


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def search_after_suggestion(
    es: Elasticsearch,
    settings: AppSettings,
    suggestion: str,
    *,
    query_vector: list[float] | None = None,
    size: int | None = None,
) -> list[dict[str, Any]]:
    """Elasticsearch-native hybrid search using combined query + knn scoring."""
    limit = size or settings.search_size_default
    suggestion_clean = " ".join(suggestion.split())
    if not suggestion_clean:
        return []

    t_encode = time.perf_counter()
    vector = query_vector or encode_query_vector(settings, suggestion_clean)
    encode_ms = (time.perf_counter() - t_encode) * 1000.0

    weights = settings.search_weights
    lexical_query = _clone_with_boost(
        build_lexical_query(suggestion_clean, settings),
        weights.lexical_boost,
    )
    knn_block = build_knn_block(suggestion_clean, vector, settings, limit)
    knn_block["boost"] = weights.vector_boost

    t_hybrid = time.perf_counter()
    hybrid_response = es.search(
        index=settings.index_name,
        size=limit,
        query=lexical_query,
        knn=knn_block,
    )
    hybrid_ms = (time.perf_counter() - t_hybrid) * 1000.0

    # Debug-only component scores from lightweight side queries.
    debug_size = max(limit * 3, limit + 10)
    t_lex = time.perf_counter()
    lexical_debug = es.search(
        index=settings.index_name,
        size=debug_size,
        query=build_lexical_query(suggestion_clean, settings),
    )
    lexical_ms = (time.perf_counter() - t_lex) * 1000.0

    t_knn = time.perf_counter()
    knn_debug = es.search(
        index=settings.index_name,
        size=debug_size,
        knn=build_knn_block(suggestion_clean, vector, settings, debug_size),
    )
    knn_ms = (time.perf_counter() - t_knn) * 1000.0

    lexical_scores: dict[str, float] = {}
    for hit in lexical_debug.get("hits", {}).get("hits", []):
        doc_id = hit["_id"]
        lexical_scores[doc_id] = float(hit.get("_score") or 0.0)

    vector_scores: dict[str, float] = {}
    for hit in knn_debug.get("hits", {}).get("hits", []):
        doc_id = hit["_id"]
        vector_scores[doc_id] = float(hit.get("_score") or 0.0)

    hybrid_hits = hybrid_response.get("hits", {}).get("hits", [])

    logger.info(
        "search_after_suggestion suggestion=%r filter_mode=%s encode_ms=%.1f hybrid_ms=%.1f lexical_debug_ms=%.1f "
        "knn_debug_ms=%.1f hybrid_hits=%s lex_debug_hits=%s knn_debug_hits=%s",
        suggestion_clean,
        settings.metadata_filter_mode,
        encode_ms,
        hybrid_ms,
        lexical_ms,
        knn_ms,
        len(hybrid_hits),
        len(lexical_scores),
        len(vector_scores),
    )

    lexical_max = 0.0
    for hit in hybrid_hits:
        doc_id = hit.get("_id")
        if not doc_id:
            continue
        lexical_max = max(lexical_max, lexical_scores.get(doc_id, 0.0))

    final_lexical_weight = max(0.0, float(weights.final_lexical_weight))
    final_vector_weight = max(0.0, float(weights.final_vector_weight))
    final_weight_sum = final_lexical_weight + final_vector_weight
    if final_weight_sum <= 0:
        final_lexical_weight = 0.5
        final_vector_weight = 0.5
        final_weight_sum = 1.0

    output: list[dict[str, Any]] = []
    for hit in hybrid_hits:
        doc_id = hit.get("_id")
        if not doc_id:
            continue
        source = hit.get("_source") or {}
        title_norm = str(source.get("title") or "").strip().lower()
        raw_score = float(hit.get("_score") or 0.0)
        lexical_score = lexical_scores.get(doc_id, 0.0)
        lexical_score_normalized = 0.0 if lexical_max <= 0 else (lexical_score / lexical_max)
        lexical_score_normalized = _clamp(lexical_score_normalized, 0.0, 1.0)

        vector_score = vector_scores.get(doc_id, 0.0)
        vector_score_cosine = (2.0 * vector_score) - 1.0
        vector_score_calibrated = _clamp(vector_score_cosine, 0.0, 1.0)

        score_out = (
            (final_lexical_weight * lexical_score_normalized)
            + (final_vector_weight * vector_score_calibrated)
        ) / final_weight_sum
        score_out *= 100.0
        suggestion_norm = suggestion_clean.lower()
        keywords = source.get("keywords") or []
        keywords_norm = {
            str(keyword).strip().lower()
            for keyword in keywords
            if str(keyword).strip()
        }
        phrase_bonus_applied = bool(
            (suggestion_norm and suggestion_norm in title_norm)
            or (suggestion_norm and suggestion_norm in keywords_norm)
        )

        output.append(
            {
                "id": doc_id,
                "score": round(score_out, 2),
                "raw_score": round(raw_score, 4),
                "lexical_score": round(lexical_score, 4),
                "vector_score": round(vector_score, 4),
                "lexical_score_normalized": round(lexical_score_normalized, 4),
                "vector_score_cosine": round(vector_score_cosine, 4),
                "vector_score_calibrated": round(vector_score_calibrated, 4),
                "exact_match_bonus_applied": bool(title_norm and title_norm == suggestion_clean.lower()),
                "phrase_bonus_applied": phrase_bonus_applied,
                "title": source.get("title"),
                "description": source.get("description"),
                "keywords": source.get("keywords"),
                "source_url": source.get("source_url"),
                "path": source.get("path"),
                "category": source.get("category"),
                "sub_category": source.get("sub_category"),
                "parsed_url_path_text": source.get("parsed_url_path_text"),
                "metadata": source.get("metadata") or {},
            }
        )
    output.sort(key=lambda item: (float(item.get("score") or 0.0), float(item.get("raw_score") or 0.0)), reverse=True)
    return output
