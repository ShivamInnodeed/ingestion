from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Mapping

from elasticsearch import Elasticsearch
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from common.paths import CONFIG_DIR, ROOT_DIR

DEFAULT_CONFIG_PATH = CONFIG_DIR / "es_search_config.json"
LEGACY_CONFIG_PATH = ROOT_DIR / "es_search_config.json"


class SearchWeights(BaseModel):
    keyword_filter_boost: float = 1.0
    # query boost used in ES native hybrid scoring (query + knn)
    lexical_boost: float = 2.2
    # final score blend weight for normalized lexical score
    final_lexical_weight: float = 0.5
    title_boost: float = 3.0
    description_boost: float = 1.5
    keyword_boost: float = 2.0
    # knn boost used in ES native hybrid scoring (query + knn)
    vector_boost: float = 2.5
    # final score blend weight for calibrated vector score
    final_vector_weight: float = 0.5
    exact_match_boost: float = 5.0


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ES_", extra="ignore")

    elasticsearch_url: str = "http://localhost:9200"
    elasticsearch_username: str | None = None
    elasticsearch_password: str | None = None
    verify_certs: bool = True
    ssl_show_warn: bool = True
    request_timeout: int = 120

    index_name: str = "kb_documents"
    embedding_dims: int = 384
    bulk_chunk_size: int = 200
    bulk_refresh: bool | str = False
    db_url: str = "sqlite:///./genie.db"
    es_delete_mode: str = "hard_delete"  # hard_delete | soft_delete
    crawl_source_name: str = "sbicard"
    root_url: str = "https://www.sbicard.com"

    model_name: str = "all-MiniLM-L6-v2"
    normalize_embeddings: bool = True

    suggest_size_default: int = 15
    search_size_default: int = 25
    knn_num_candidates_factor: int = 5
    metadata_filter_mode: str = "keyword_strict"  # keyword_strict | broad
    strict_query_filter_enabled: bool = False
    strict_query_min_tokens: int = 4
    strict_query_stopwords: list[str] = Field(
        default_factory=lambda: [
            "i",
            "me",
            "my",
            "we",
            "our",
            "you",
            "your",
            "want",
            "need",
            "book",
            "please",
            "can",
            "could",
            "would",
            "how",
            "what",
            "is",
            "are",
            "to",
            "for",
            "a",
            "an",
            "the",
            "of",
            "on",
            "in",
            "with",
            "do",
        ]
    )
    strict_query_min_core_token_hits: int = 1

    search_weights: SearchWeights = Field(default_factory=SearchWeights)


def load_settings(
    config_path: Path | None = None,
    *,
    overrides: Mapping[str, Any] | None = None,
) -> AppSettings:
    path = config_path or DEFAULT_CONFIG_PATH
    if not path.is_file() and not config_path and LEGACY_CONFIG_PATH.is_file():
        path = LEGACY_CONFIG_PATH
    merged: dict = {}
    if path.is_file():
        merged = json.loads(path.read_text(encoding="utf-8"))

    # Keep secrets out of JSON when possible.
    env_to_key = {
        "ES_ELASTICSEARCH_URL": "elasticsearch_url",
        "ES_ELASTICSEARCH_USERNAME": "elasticsearch_username",
        "ES_ELASTICSEARCH_PASSWORD": "elasticsearch_password",
        "ES_DB_URL": "db_url",
        "ES_DELETE_MODE": "es_delete_mode",
    }
    for env_key, target_key in env_to_key.items():
        if os.environ.get(env_key):
            merged[target_key] = os.environ[env_key]

    # Prefer discrete ORACLE_* env vars over ES_DB_URL / JSON db_url when all are set.
    oracle_host = os.environ.get("ORACLE_HOST")
    oracle_port = os.environ.get("ORACLE_PORT")
    oracle_service = os.environ.get("ORACLE_SERVICE_NAME")
    oracle_user = os.environ.get("ORACLE_USER")
    oracle_password = os.environ.get("ORACLE_PASSWORD")
    if all([oracle_host, oracle_port, oracle_service, oracle_user, oracle_password]):
        # Deferred import avoids circular import with es_kb.db -> AppSettings.
        from .db import build_oracle_db_url

        merged["db_url"] = build_oracle_db_url(
            user=oracle_user,
            password=oracle_password,
            host=oracle_host,
            port=oracle_port,
            service_name=oracle_service,
        )

    # CLI/runtime overrides take highest precedence.
    if overrides:
        for key, value in overrides.items():
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            merged[key] = value

    return AppSettings.model_validate(merged) if merged else AppSettings()


def get_elasticsearch_client(settings: AppSettings) -> Elasticsearch:
    options: dict = {
        "request_timeout": settings.request_timeout,
        "verify_certs": settings.verify_certs,
        "ssl_show_warn": settings.ssl_show_warn,
    }
    if settings.elasticsearch_username and settings.elasticsearch_password:
        options["basic_auth"] = (settings.elasticsearch_username, settings.elasticsearch_password)
    return Elasticsearch(settings.elasticsearch_url, **options)
