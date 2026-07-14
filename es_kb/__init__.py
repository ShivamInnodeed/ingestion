"""Elasticsearch KB package for indexing and hybrid search."""

from .config import AppSettings, get_elasticsearch_client, load_settings
from .hybrid_search import search_after_suggestion
from .ingest import bulk_index_documents, iter_embedding_records
from .suggestions import suggest_candidates

__all__ = [
    "AppSettings",
    "load_settings",
    "get_elasticsearch_client",
    "iter_embedding_records",
    "bulk_index_documents",
    "suggest_candidates",
    "search_after_suggestion",
]
