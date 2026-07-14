from __future__ import annotations

from typing import Any

from elasticsearch import Elasticsearch

from .config import AppSettings


def build_index_body(settings: AppSettings) -> dict[str, Any]:
    """Sample index mapping for metadata + vectors + typeahead."""
    return {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
        },
        "mappings": {
            "properties": {
                "embedding_text": {"type": "text"},
                "embedding": {
                    "type": "dense_vector",
                    "dims": settings.embedding_dims,
                    "index": True,
                    "similarity": "cosine",
                },
                "metadata": {"enabled": True},
                "title": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 512}},
                },
                "description": {"type": "text"},
                "keywords": {"type": "keyword"},
                "keywords_joined": {"type": "text"},
                "search_text": {"type": "text"},
                # search_as_you_type is a practical typeahead default with lower index bloat than full ngram.
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


def ensure_index(es: Elasticsearch, settings: AppSettings, recreate: bool = False) -> None:
    name = settings.index_name
    if recreate and es.indices.exists(index=name):
        es.indices.delete(index=name)
    if es.indices.exists(index=name):
        return
    body = build_index_body(settings)
    es.indices.create(index=name, settings=body["settings"], mappings=body["mappings"])
