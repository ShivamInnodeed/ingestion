#!/usr/bin/env python3
"""LangGraph ingestion flow: crawl -> payloads -> embeddings -> Elasticsearch index."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import subprocess
import sys
import time
from collections import deque
from pathlib import Path
from typing import Any, TypedDict
from urllib.parse import urlparse

from langgraph.graph import END, START, StateGraph
from sentence_transformers import SentenceTransformer

from common.paths import OUTPUT_DIR
from build_embedding_payloads import build_output_records, load_records, write_output
from build_sentence_transformer_embeddings import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_MODEL_NAME,
    build_output_record,
    generate_embeddings,
    get_embedding_text,
    is_valid_record,
    iter_batches,
    load_records as load_payload_records,
    write_output as write_embeddings_output,
)
from crawl_sbicard_markdown import crawl_site
from chunk_embedding_payloads import DEFAULT_CHUNK_OVERLAP, DEFAULT_CHUNK_SIZE, build_chunk_payload_records, write_jsonl
from es_kb.config import get_elasticsearch_client, load_settings
from es_kb.db import build_session_factory
from es_kb.es_sync import sync_index_changes
from es_kb.audit_service import process_run_raw
from es_kb.ingest import iter_embedding_records
from es_kb.repositories import SnapshotRepository, UrlRepository
from es_kb.normalize_hash import normalize_url
from offline_indexing.bundle import DEFAULT_BUNDLE_DIRNAME, export_offline_bundle


class IngestionState(TypedDict, total=False):
    crawl_output_dir: str
    crawl_output_path: str
    payload_output_path: str
    embeddings_output_path: str
    config_path: str | None
    model_name: str
    batch_size: int
    max_depth: int
    max_pages: int
    retry_failed_pages: bool
    max_retry_rounds: int
    max_retry_pages: int
    run_crawl: bool
    recreate_index: bool
    payload_delta_output_path: str
    run_started_epoch_ms: int
    run_id: str | None
    deleted_doc_ids: list[str]
    payload_count: int
    payload_delta_count: int
    embeddings_count: int
    indexed_count: int
    deleted_count: int
    indexing_errors: int
    total_urls: int
    success_urls: int
    failed_urls: int
    new_urls: int
    updated_urls: int
    unchanged_urls: int
    deleted_urls: int
    total_bytes: int
    timings: dict[str, float]
    error: str | None
    stage: str
    strict_partial_crawl: bool  # if True, treat crawl failures like index failures (PARTIAL)
    settings_overrides: dict[str, Any]
    generate_offline_bundle: bool
    offline_bundle_dir: str | None
    offline_bundle_manifest_path: str | None
    scrape_offers: bool
    offer_output_path: str | None
    offer_merge_count: int
    scrape_rewards: bool
    reward_output_path: str | None
    reward_merge_count: int
    scrape_about_us: bool
    about_us_output_path: str | None
    about_us_merge_count: int
    max_offers: int | None
    max_rewards: int | None
    max_llm_pages: int | None
    chunk_size: int
    chunk_overlap: int
    changed_normalized_urls: list[str]
    enable_llm_enrichment: bool
    llm_change_input_path: str | None
    llm_output_path: str | None


logger = logging.getLogger(__name__)

_REMOVE_URL_LIST_RAW: tuple[str, ...] = (
    "https://www.sbicard.com",
    "https://www.sbicard.com/hi/404-error.page",
    "https://www.sbicard.com/hi/home.page",
    "https://www.sbicard.com/hi/most-important-terms-and-conditions.page",
    "https://www.sbicard.com/hi/personal/benefits/encash.page",
    "https://www.sbicard.com/hi/help.page",
    "https://www.sbicard.com/hi/personal/pay.page",
    "https://www.sbicard.com/hi/personal/benefits/lower-interest-option/balance-transfer-on-emi.page",
    "https://www.sbicard.com/hi/personal/benefits/lower-interest-option/flexi-pay.page",
    "https://www.sbicard.com/hi/tnc.page",
    "https://www.sbicard.com/hi/personal/pay.page.page",
    "https://www.sbicard.com/hi/personal/benefits/lower-interest-option/balance-transfer.page",
    "https://www.sbicard.com/hi/personal/credit-cards/rewards/sbi-card-prime.page",
    "https://www.sbicard.com/hi/personal/credit-cards.page",
    "https://www.sbicard.com/sitesmailto:Corporate.Communications@sbicard.com",
    "https://www.sbicard.com/en/personal/credit-cards/www.rupay.co.in/rupay-offers",
    "https://www.sbicard.com/en/personal/credit-cards/www.visa.co.in/en_in/visa-offers-and-perks/",
    "https://www.sbicard.com/sites/en/corporate-most-important-terms-and-conditions.page",
    "https://www.sbicard.com/sites/en/most-important-terms-and-conditions.page",
    "https://www.sbicard.com/en/resolution-framework-2-0-policy.page",
    "https://www.sbicard.com/en/personal/credit-cards/www.air.irctc.co.in",
    "https://www.sbicard.com/en/personal/credit-cards/shopping/styleup-contactless-card.page",
    "https://www.sbicard.com/en/personal/credit-cards/shopping/nature-basket-sbi-card.page",
    "https://www.sbicard.com/en/personal/credit-cards/shopping/nature-basket-sbi-card-elite.page",
    "https://www.sbicard.com/en/personal/credit-cards/banking-partnership/bank-of-maharashtra-sbi-card.page",
    "https://www.sbicard.com/en/personal/benefits/money-simplified/easy-money.page",
    "https://www.sbicard.com/en/personal/credit-cards/banking-partnership/bank-of-maharashtra-sbi-platinum-card.page",
    "https://www.sbicard.com/en/home",
    "https://www.sbicard.com/en/faq/easy-money.page",
    "https://www.sbicard.com/en/faq/sbicard.com",
    "https://www.sbicard.com/en/corporate/credit-cards/compare-cards.page",
    "https://www.sbicard.com/en/faq/milestone-hub-faq.page",
    "https://www.sbicard.com/en/credit-cards/compare-cards.page",
    "https://www.sbicard.com/en/who-we-are/about-us.page",
)


def _sanitize_remove_url(raw: str) -> str:
    cleaned = str(raw or "").strip().rstrip(",").strip()
    while cleaned.endswith("%22"):
        cleaned = cleaned[:-3].rstrip().rstrip(",").strip()
    return cleaned


def _build_remove_url_matchers() -> tuple[frozenset[str], frozenset[str], frozenset[str]]:
    exact: set[str] = set()
    paths: set[str] = set()
    basenames: set[str] = set()
    for raw in _REMOVE_URL_LIST_RAW:
        cleaned = _sanitize_remove_url(raw)
        if not cleaned:
            continue
        norm = normalize_url(cleaned)
        if norm:
            exact.add(norm)
        parsed = urlparse(cleaned)
        path = (parsed.path or "").lower().rstrip("/")
        if path:
            paths.add(path)
            bn = path.rsplit("/", 1)[-1]
            if bn:
                basenames.add(bn)
    return frozenset(exact), frozenset(paths), frozenset(basenames)


_REMOVE_URLS_EXACT, _REMOVE_PATHS, _REMOVE_BASENAMES = _build_remove_url_matchers()


def _filter_crawl_json_inplace(crawl_json_path: Path) -> dict[str, int]:
    """
    Remove blocked pages + 404 pages from the crawl JSON before audit/LLM/indexing.

    This is intentionally post-crawl so it does not depend on Crawl4AI internals.
    """
    payload = json.loads(crawl_json_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("pages"), list):
        raise ValueError("Crawl JSON missing 'pages' list")

    pages_in: list[dict[str, Any]] = [p for p in payload["pages"] if isinstance(p, dict)]
    kept: list[dict[str, Any]] = []
    removed_404 = 0
    removed_blocklist = 0

    for p in pages_in:
        status_code = p.get("status_code")
        if isinstance(status_code, int) and status_code == 404:
            removed_404 += 1
            continue

        src = str(p.get("source_url") or "").strip()
        norm = normalize_url(src)
        if norm and norm in _REMOVE_URLS_EXACT:
            removed_blocklist += 1
            continue

        parsed = urlparse(src)
        path = (parsed.path or "").lower().rstrip("/")
        if path and path in _REMOVE_PATHS:
            removed_blocklist += 1
            continue

        basename = path.rsplit("/", 1)[-1] if path else ""
        if basename and basename in _REMOVE_BASENAMES:
            removed_blocklist += 1
            continue

        kept.append(p)

    payload["pages"] = kept
    summary = payload.get("summary")
    if isinstance(summary, dict):
        summary["removed_pages_404"] = removed_404
        summary["removed_pages_blocklist"] = removed_blocklist
        summary["saved_pages"] = len(kept)

    crawl_json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "removed_404": removed_404,
        "removed_blocklist": removed_blocklist,
        "kept_pages": len(kept),
        "input_pages": len(pages_in),
    }


def _resolve_local_model_name(model_name: str) -> str:
    """
    Offline-safe model resolution.
    If the Docker image has pre-downloaded models under /app/models/<model_name>,
    prefer that local path so SentenceTransformer doesn't need internet.
    """
    name = str(model_name or "").strip()
    if not name:
        return name
    try:
        local = Path("/app/models") / name
        if local.exists():
            return str(local)
    except Exception:
        return name
    return name


def _settings_overrides_from_state(state: IngestionState) -> dict[str, Any]:
    raw = state.get("settings_overrides")
    if isinstance(raw, dict):
        return raw
    return {}


def _resolve_offline_bundle_dir(state: IngestionState) -> Path:
    explicit = str(state.get("offline_bundle_dir") or "").strip()
    if explicit:
        return Path(explicit)
    return Path(state["crawl_output_dir"]) / DEFAULT_BUNDLE_DIRNAME


def _ingest_outcome_dict(state: IngestionState) -> dict[str, Any]:
    return {
        "pre_finalize_stage": str(state.get("stage") or ""),
        "indexed_count": int(state.get("indexed_count") or 0),
        "indexing_errors": int(state.get("indexing_errors") or 0),
        "deleted_count": int(state.get("deleted_count") or 0),
        "failed_urls": int(state.get("failed_urls") or 0),
        "success_urls": int(state.get("success_urls") or 0),
        "total_urls": int(state.get("total_urls") or 0),
        "new_urls": int(state.get("new_urls") or 0),
        "updated_urls": int(state.get("updated_urls") or 0),
        "unchanged_urls": int(state.get("unchanged_urls") or 0),
        "deleted_urls": int(state.get("deleted_urls") or 0),
        "strict_partial_crawl": bool(state.get("strict_partial_crawl")),
    }


def _compose_finalize_failed_reason(
    *,
    run_status: str,
    state: IngestionState,
    pipeline_error: str | None,
) -> str | None:
    o = _ingest_outcome_dict(state)
    tail = (
        f"pre_finalize_stage={o['pre_finalize_stage']}; "
        f"indexing_errors={o['indexing_errors']}; indexed_count={o['indexed_count']}; "
        f"deleted_count={o['deleted_count']}; crawl_failed_urls={o['failed_urls']}; "
        f"success_urls={o['success_urls']}; total_urls={o['total_urls']}"
    )
    if run_status == "FAILED" and pipeline_error:
        return f"{pipeline_error} | {tail}"
    if run_status == "PARTIAL":
        return (
            f"PARTIAL: elasticsearch_bulk_errors={o['indexing_errors']} indexed_count={o['indexed_count']} "
            f"deleted_count={o['deleted_count']} crawl_failed_urls={o['failed_urls']} | {tail}"
        )
    return None


def _persist_finalize_node_exception(
    run_id: str,
    *,
    config_path: Path | None,
    state: IngestionState,
    exc: Exception,
) -> None:
    """If finalize_run_node crashes after audit, mark the crawl run FAILED so it never stays RUNNING."""
    session: Any = None
    try:
        settings = load_settings(config_path, overrides=_settings_overrides_from_state(state))
        session_factory = build_session_factory(settings)
        session = session_factory()
        from es_kb.repositories import CrawlRunRepository

        elapsed_ms = int(time.time() * 1000) - int(state.get("run_started_epoch_ms") or int(time.time() * 1000))
        msg = f"{type(exc).__name__}: {exc}"
        outcome = _ingest_outcome_dict(state)
        outcome["finalize_exception"] = msg
        run_repo = CrawlRunRepository(session)
        tail = (
            f"pre_finalize_stage={outcome.get('pre_finalize_stage')}; "
            f"indexing_errors={outcome.get('indexing_errors')}; indexed_count={outcome.get('indexed_count')}"
        )
        run_repo.finalize_run(
            run_id=run_id,
            run_status="FAILED",
            total_urls=int(state.get("total_urls") or 0),
            success_urls=int(state.get("success_urls") or 0),
            failed_urls=int(state.get("failed_urls") or 0),
            new_urls=int(state.get("new_urls") or 0),
            updated_urls=int(state.get("updated_urls") or 0),
            unchanged_urls=int(state.get("unchanged_urls") or 0),
            deleted_urls=int(state.get("deleted_urls") or 0),
            total_bytes=int(state.get("total_bytes") or 0),
            duration_ms=elapsed_ms,
            failed_reason=f"finalize_run_node crashed: {msg} | {tail}",
            outcome_metadata=outcome,
        )
        session.commit()
    except Exception:
        logger.exception("Could not persist FAILED crawl run after finalize_run_node error (run_id=%s)", run_id)
    finally:
        if session is not None:
            session.close()


def _record_error(state: IngestionState, stage: str, exc: Exception) -> IngestionState:
    logger.exception("Stage '%s' failed", stage)
    return {
        **state,
        "stage": stage,
        "error": f"{type(exc).__name__}: {exc}",
    }


def _offer_output_path_for_repo(repo_root: Path) -> Path:
    # offer/sbi_offer_scraper.py writes to <repo_root>/offer/offers_json/sbi_offers_all.json
    return repo_root / "offer" / "offers_json" / "sbi_offers_all.json"


def _reward_output_path_for_repo(repo_root: Path) -> Path:
    # reward/sbi_reward_scraper.py writes to <repo_root>/reward/rewards_json/sbi_rewards_all.json
    return repo_root / "reward" / "rewards_json" / "sbi_rewards_all.json"


def _about_us_json_path_for_repo(repo_root: Path) -> Path:
    # about_us/sbi_about_us_scraper.py writes <repo_root>/about_us/sbi_about_us.json
    return repo_root / "about_us" / "sbi_about_us.json"


def _reward_to_crawl_page(reward: dict[str, Any]) -> dict[str, Any] | None:
    source_url = str(reward.get("source_url") or "").strip()
    if not source_url:
        return None

    parsed = urlparse(source_url)
    path = parsed.path or "/"

    product_name = str(reward.get("product_name") or "").strip()
    reward_text = str(reward.get("reward_text") or "").strip()
    product_description = str(reward.get("product_description") or "").strip()

    # Title/description improvements (no URLs inside title)
    title = product_name or "SBI Reward"
    if reward_text and reward_text != title and not reward_text.startswith(f"{title} |"):
        # Keep a compact enrichment without duplicating product_name.
        title = f"{title} | {reward_text}"

    # Prefer product_description; fall back to chunk_description (strip prefix).
    description = product_description
    if not description:
        chunk_desc = str(reward.get("chunk_description") or "").strip()
        description = re.sub(r"^\[description\]\s*", "", chunk_desc).strip()

    # Keep markdown rich for embeddings / RAG.
    chunk_terms = str(reward.get("chunk_terms") or "").strip()
    chunk_features = str(reward.get("chunk_features") or "").strip()

    points_only = reward.get("points_only")
    points_pay_points = reward.get("points_pay_points")
    points_pay_cash = reward.get("points_pay_cash")

    brand = str(reward.get("brand_name") or "").strip()
    category_raw = str(reward.get("category") or "").strip()
    redemption_type = str(reward.get("redemption_type") or "").strip()

    tags = reward.get("tags") if isinstance(reward.get("tags"), list) else []
    keywords = ",".join(str(t) for t in tags if str(t).strip())

    cities = reward.get("available_cities") if isinstance(reward.get("available_cities"), list) else []
    cities_count = int(reward.get("available_cities_count") or len(cities) or 0)
    cities_preview = [str(c) for c in cities[:20] if str(c).strip()]

    md_parts: list[str] = []
    if description:
        md_parts.append(description)
    md_parts.append("")
    md_parts.append("## Reward details")
    if points_only is not None:
        md_parts.append(f"- Points only: {points_only}")
    if points_pay_points is not None:
        md_parts.append(f"- Points + Pay: {points_pay_points} + ₹{points_pay_cash or 0}")
    if category_raw:
        md_parts.append(f"- Category: {category_raw}")
    if redemption_type:
        md_parts.append(f"- Redemption type: {redemption_type}")
    if brand:
        md_parts.append(f"- Brand: {brand}")
    if cities_count:
        md_parts.append(f"- City restriction: {cities_count} cities")
    if cities_preview:
        md_parts.append(f"- Cities (preview): {', '.join(cities_preview)}")
    if chunk_features:
        md_parts.append("")
        md_parts.append("## Features")
        md_parts.append(chunk_features)
    if chunk_terms:
        md_parts.append("")
        md_parts.append("## Terms & conditions")
        md_parts.append(chunk_terms)

    markdown = "\n".join(md_parts).strip()

    page: dict[str, Any] = {
        "title": title,
        "description": description,
        "keywords": keywords,
        "category": "Rewards",
        "sub_category": category_raw or None,
        "source_url": source_url,
        "path": path,
        "depth": 0,
        "success": True,
        "status_code": 200,
        "redirected_url": source_url,
        "redirected_status_code": 200,
        "error": "",
        "request_headers_sent": {},
        "request_cookies_sent": [],
        "request_headers_final": {},
        "request_cookies_final": {},
        "response_headers": {},
        "metadata": {
            "source_url": source_url,
            "domain": parsed.netloc,
            "path": path,
            "query": parsed.query or "",
            "title": title,
            "description": description,
            "keywords": keywords,
            "depth": "0",
            "reward_item_id": reward.get("item_id"),
            "reward_document_id": reward.get("document_id"),
            "reward_brand_name": brand,
            "reward_category": category_raw,
            "reward_redemption_type": redemption_type,
            "reward_points_only": points_only,
            "reward_points_pay_points": points_pay_points,
            "reward_points_pay_cash": points_pay_cash,
            "reward_item_code_points": reward.get("item_code_points"),
            "reward_item_code_pointspay": reward.get("item_code_pointspay"),
            "reward_product_image_url": reward.get("product_image_url"),
            "reward_thumbnail_url": reward.get("thumbnail_url"),
            "reward_available_cities_count": cities_count,
            "reward_available_cities_preview": cities_preview,
            "reward_tags": tags,
        },
        "markdown": markdown,
    }
    return page


def _merge_rewards_into_crawl_json(
    *,
    crawl_json_path: Path,
    rewards_json_path: Path,
) -> int:
    if not crawl_json_path.is_file():
        raise FileNotFoundError(f"Crawl JSON missing for rewards merge: {crawl_json_path}")
    if not rewards_json_path.is_file():
        raise FileNotFoundError(f"Rewards JSON missing for merge: {rewards_json_path}")

    crawl_payload = json.loads(crawl_json_path.read_text(encoding="utf-8"))
    if not isinstance(crawl_payload, dict):
        raise ValueError(f"Unexpected crawl payload root type: {type(crawl_payload).__name__}")
    pages = crawl_payload.get("pages")
    if not isinstance(pages, list):
        raise ValueError("Crawl JSON missing 'pages' list")

    rewards_payload = json.loads(rewards_json_path.read_text(encoding="utf-8"))
    if not isinstance(rewards_payload, list):
        raise ValueError(f"Unexpected rewards payload root type: {type(rewards_payload).__name__}")

    existing_by_url: set[str] = set()
    for p in pages:
        if isinstance(p, dict) and p.get("source_url"):
            existing_by_url.add(str(p.get("source_url")))

    added = 0
    for reward in rewards_payload:
        if not isinstance(reward, dict):
            continue
        page = _reward_to_crawl_page(reward)
        if not page:
            continue
        url = str(page.get("source_url") or "")
        if not url or url in existing_by_url:
            continue
        pages.append(page)
        existing_by_url.add(url)
        added += 1

    crawl_payload["pages"] = pages
    summary = crawl_payload.get("summary")
    if isinstance(summary, dict):
        summary["rewards_merged"] = added
        summary["saved_pages"] = len([p for p in pages if isinstance(p, dict)])

    crawl_json_path.write_text(json.dumps(crawl_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return added


def _merge_about_us_into_crawl_json(
    *,
    crawl_json_path: Path,
    about_json_path: Path,
) -> int:
    """Upsert About Us page(s) from about_us/sbi_about_us.json into sbicard_crawl.json."""
    if not crawl_json_path.is_file():
        raise FileNotFoundError(f"Crawl JSON missing for about-us merge: {crawl_json_path}")
    if not about_json_path.is_file():
        raise FileNotFoundError(f"About Us JSON missing for merge: {about_json_path}")

    crawl_payload = json.loads(crawl_json_path.read_text(encoding="utf-8"))
    if not isinstance(crawl_payload, dict):
        raise ValueError(f"Unexpected crawl payload root type: {type(crawl_payload).__name__}")
    pages = crawl_payload.get("pages")
    if not isinstance(pages, list):
        raise ValueError("Crawl JSON missing 'pages' list")

    about_payload = json.loads(about_json_path.read_text(encoding="utf-8"))
    if not isinstance(about_payload, dict):
        raise ValueError(f"Unexpected about-us payload root type: {type(about_payload).__name__}")
    about_pages = about_payload.get("pages")
    if not isinstance(about_pages, list):
        raise ValueError("About Us JSON missing 'pages' list")

    merged = 0
    for ap in about_pages:
        if not isinstance(ap, dict):
            continue
        src = str(ap.get("source_url") or "").strip()
        if not src:
            continue
        target_key = normalize_url(src)
        idx_replace: int | None = None
        for i, p in enumerate(pages):
            if not isinstance(p, dict):
                continue
            pu = str(p.get("source_url") or "").strip()
            if pu and normalize_url(pu) == target_key:
                idx_replace = i
                break
        if idx_replace is not None:
            pages[idx_replace] = ap
        else:
            pages.append(ap)
        merged += 1

    crawl_payload["pages"] = pages
    summary = crawl_payload.get("summary")
    if isinstance(summary, dict):
        summary["about_us_merged"] = merged
        summary["saved_pages"] = len([p for p in pages if isinstance(p, dict)])

    crawl_json_path.write_text(json.dumps(crawl_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return merged


def _format_offer_title(offer: dict[str, Any]) -> str:
    brand_name = str(offer.get("brand_name") or "").strip()
    summary_text = offer.get("summary_text") if isinstance(offer.get("summary_text"), list) else []
    summary_bits = [str(x).strip() for x in summary_text if isinstance(x, (str, int, float)) and str(x).strip()]
    # Keep the first few summary lines; downstream embedder uses title+description heavily.
    summary_compact = " | ".join(summary_bits[:6])
    parts = [p for p in (brand_name, summary_compact) if p]
    return " | ".join(parts) if parts else "SBI Offer"


def _format_offer_description(offer: dict[str, Any]) -> str:
    summary_text = offer.get("summary_text") if isinstance(offer.get("summary_text"), list) else []
    summary_bits = [str(x).strip() for x in summary_text if isinstance(x, (str, int, float)) and str(x).strip()]
    table = offer.get("summary_table")
    table_payload = table if isinstance(table, list) else []
    table_text = ""
    if table_payload:
        # Stable-ish compact representation.
        table_text = json.dumps(table_payload, ensure_ascii=False)
    parts = []
    if summary_bits:
        parts.append(" | ".join(summary_bits))
    if table_text:
        parts.append(table_text)
    return "\n".join(parts).strip()


def _offer_to_crawl_page(offer: dict[str, Any]) -> dict[str, Any] | None:
    source_url = str(offer.get("source_url") or "").strip()
    if not source_url:
        return None
    parsed = urlparse(source_url)
    path = parsed.path or "/"
    title = _format_offer_title(offer)
    description = _format_offer_description(offer)

    # Minimal "crawl page" record that matches sbicard_crawl.json page objects.
    tags = offer.get("tags") if isinstance(offer.get("tags"), list) else []
    keywords = ",".join(str(t) for t in tags if str(t).strip())
    page: dict[str, Any] = {
        "title": title,
        "description": description,
        "keywords": keywords,
        "category": "Offers",
        "sub_category": None,
        "source_url": source_url,
        "path": path,
        "depth": 0,
        "success": True,
        "status_code": 200,
        "redirected_url": source_url,
        "redirected_status_code": 200,
        "error": "",
        "request_headers_sent": {},
        "request_cookies_sent": [],
        "request_headers_final": {},
        "request_cookies_final": {},
        "response_headers": {},
        "metadata": {
            "source_url": source_url,
            "domain": parsed.netloc,
            "path": path,
            "query": parsed.query or "",
            "title": title,
            "description": description,
            "keywords": keywords,
            "depth": "0",
            "offer_brand_name": offer.get("brand_name"),
            "offer_id": offer.get("offer_id"),
            "offer_type": offer.get("offer_type"),
            "offer_tags": tags,
        },
        "markdown": description,
    }
    return page


def _merge_offers_into_crawl_json(
    *,
    crawl_json_path: Path,
    offers_json_path: Path,
) -> int:
    if not crawl_json_path.is_file():
        raise FileNotFoundError(f"Crawl JSON missing for offer merge: {crawl_json_path}")
    if not offers_json_path.is_file():
        raise FileNotFoundError(f"Offer JSON missing for merge: {offers_json_path}")

    crawl_payload = json.loads(crawl_json_path.read_text(encoding="utf-8"))
    if not isinstance(crawl_payload, dict):
        raise ValueError(f"Unexpected crawl payload root type: {type(crawl_payload).__name__}")
    pages = crawl_payload.get("pages")
    if not isinstance(pages, list):
        raise ValueError("Crawl JSON missing 'pages' list")

    offers_payload = json.loads(offers_json_path.read_text(encoding="utf-8"))
    if not isinstance(offers_payload, list):
        raise ValueError(f"Unexpected offers payload root type: {type(offers_payload).__name__}")

    existing_by_url: set[str] = set()
    for p in pages:
        if isinstance(p, dict) and p.get("source_url"):
            existing_by_url.add(str(p.get("source_url")))

    added = 0
    for offer in offers_payload:
        if not isinstance(offer, dict):
            continue
        page = _offer_to_crawl_page(offer)
        if not page:
            continue
        url = str(page.get("source_url") or "")
        if not url or url in existing_by_url:
            continue
        pages.append(page)
        existing_by_url.add(url)
        added += 1

    crawl_payload["pages"] = pages
    summary = crawl_payload.get("summary")
    if isinstance(summary, dict):
        summary["offers_merged"] = added
        # Best-effort update; keep existing telemetry fields.
        summary["saved_pages"] = len([p for p in pages if isinstance(p, dict)])

    crawl_json_path.write_text(json.dumps(crawl_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return added


def crawl_node(state: IngestionState) -> IngestionState:
    if state.get("error"):
        return state

    stage = "crawl"
    try:
        output_dir = Path(state["crawl_output_dir"])
        output_path = output_dir / "sbicard_crawl.json"
        t0 = time.perf_counter()

        if state.get("run_crawl", True):
            logger.info("Starting crawl into %s", output_dir)
            asyncio.run(
                crawl_site(
                    output_dir=output_dir,
                    max_depth=state["max_depth"],
                    max_pages=state["max_pages"],
                    retry_failed_pages=bool(state.get("retry_failed_pages", True)),
                    max_retry_rounds=int(state.get("max_retry_rounds", 2)),
                    max_retry_pages=int(state.get("max_retry_pages", 40)),
                )
            )
        else:
            logger.info("Skipping crawl step (--skip-crawl)")
            if not output_path.exists():
                raise FileNotFoundError(f"Crawl output does not exist: {output_path}")

        offer_merge_count = 0
        reward_merge_count = 0
        about_us_merge_count = 0
        repo_root = Path(__file__).resolve().parent
        offers_out = _offer_output_path_for_repo(repo_root)
        if bool(state.get("scrape_offers")):
            logger.info("scrape_offers enabled: running offer scraper")
            cmd = [sys.executable, str(repo_root / "offer" / "sbi_offer_scraper.py")]
            mo = state.get("max_offers")
            if mo is not None and int(mo) > 0:
                cmd.extend(["--max-offers", str(int(mo))])
                logger.info("Offer scrape cap: max_offers=%s", int(mo))
            tail_lines: deque[str] = deque(maxlen=250)
            proc = subprocess.Popen(  # noqa: S603
                cmd,
                cwd=str(repo_root),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            assert proc.stdout is not None  # for type-checkers
            for line in proc.stdout:
                line = line.rstrip("\n")
                if line:
                    tail_lines.append(line)
                    logger.info("[offer] %s", line)
            returncode = proc.wait()
            if returncode != 0:
                tail = "\n".join(tail_lines)
                raise RuntimeError(f"Offer scraper failed (exit={returncode}). Tail:\n{tail}")
            try:
                offer_merge_count = _merge_offers_into_crawl_json(
                    crawl_json_path=output_path,
                    offers_json_path=offers_out,
                )
                logger.info("Merged %s offer page(s) into %s", offer_merge_count, output_path)
            except Exception:
                logger.exception("Offer merge failed")
                raise

        rewards_out = _reward_output_path_for_repo(repo_root)
        if bool(state.get("scrape_rewards")):
            logger.info("scrape_rewards enabled: running rewards scraper")
            cmd = [sys.executable, str(repo_root / "reward" / "sbi_reward_scraper.py")]
            mr = state.get("max_rewards")
            if mr is not None and int(mr) > 0:
                cmd.extend(["--max-items", str(int(mr))])
                logger.info("Rewards scrape cap: max_rewards=%s", int(mr))
            tail_lines: deque[str] = deque(maxlen=250)
            proc = subprocess.Popen(  # noqa: S603
                cmd,
                cwd=str(repo_root),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            assert proc.stdout is not None
            for line in proc.stdout:
                line = line.rstrip("\n")
                if line:
                    tail_lines.append(line)
                    logger.info("[reward] %s", line)
            returncode = proc.wait()
            if returncode != 0:
                tail = "\n".join(tail_lines)
                raise RuntimeError(f"Rewards scraper failed (exit={returncode}). Tail:\n{tail}")
            try:
                reward_merge_count = _merge_rewards_into_crawl_json(
                    crawl_json_path=output_path,
                    rewards_json_path=rewards_out,
                )
                logger.info("Merged %s reward page(s) into %s", reward_merge_count, output_path)
            except Exception:
                logger.exception("Rewards merge failed")
                raise

        about_us_out = _about_us_json_path_for_repo(repo_root)
        if bool(state.get("scrape_about_us")):
            logger.info("scrape_about_us enabled: running About Us scraper")
            cmd = [sys.executable, str(repo_root / "about_us" / "sbi_about_us_scraper.py")]
            tail_lines = deque(maxlen=250)
            proc = subprocess.Popen(  # noqa: S603
                cmd,
                cwd=str(repo_root),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            assert proc.stdout is not None
            for line in proc.stdout:
                line = line.rstrip("\n")
                if line:
                    tail_lines.append(line)
                    logger.info("[about_us] %s", line)
            returncode = proc.wait()
            if returncode != 0:
                tail = "\n".join(tail_lines)
                raise RuntimeError(f"About Us scraper failed (exit={returncode}). Tail:\n{tail}")
            try:
                about_us_merge_count = _merge_about_us_into_crawl_json(
                    crawl_json_path=output_path,
                    about_json_path=about_us_out,
                )
                logger.info("Merged About Us (%s page(s)) into %s", about_us_merge_count, output_path)
            except Exception:
                logger.exception("About Us merge failed")
                raise

        # Final post-crawl filtering: remove known-bad URLs and HTTP 404 pages so they
        # never reach SQLite audit, LLM enrichment, embeddings, or ES indexing.
        try:
            stats = _filter_crawl_json_inplace(output_path)
            logger.info(
                "Post-crawl filter applied: kept=%s removed_404=%s removed_blocklist=%s (input=%s)",
                stats["kept_pages"],
                stats["removed_404"],
                stats["removed_blocklist"],
                stats["input_pages"],
            )
        except Exception:
            logger.exception("Post-crawl filter failed (continuing with unfiltered crawl JSON)")

        elapsed = time.perf_counter() - t0
        timings = dict(state.get("timings") or {})
        timings["crawl_seconds"] = round(elapsed, 3)

        logger.info("Crawl stage complete in %.2fs", elapsed)
        return {
            **state,
            "stage": stage,
            "crawl_output_path": str(output_path),
            "offer_output_path": str(offers_out) if offers_out else None,
            "offer_merge_count": int(offer_merge_count or 0),
            "reward_output_path": str(rewards_out) if rewards_out else None,
            "reward_merge_count": int(reward_merge_count or 0),
            "about_us_output_path": str(about_us_out) if about_us_out else None,
            "about_us_merge_count": int(about_us_merge_count or 0),
            "timings": timings,
        }
    except Exception as exc:
        return _record_error(state, stage, exc)


def payload_node(state: IngestionState) -> IngestionState:
    if state.get("error"):
        return state

    stage = "build_payloads"
    try:
        crawl_output_path = Path(state["crawl_output_path"])
        payload_output_path = Path(state["payload_output_path"])
        t0 = time.perf_counter()

        records = load_records(crawl_output_path)
        payload_records = build_chunk_payload_records(
            records,
            chunk_size=int(state.get("chunk_size") or DEFAULT_CHUNK_SIZE),
            chunk_overlap=int(state.get("chunk_overlap") or DEFAULT_CHUNK_OVERLAP),
        )
        write_jsonl(payload_records, payload_output_path)

        elapsed = time.perf_counter() - t0
        timings = dict(state.get("timings") or {})
        timings["payload_seconds"] = round(elapsed, 3)

        logger.info(
            "Payload stage complete in %.2fs (%s records)",
            elapsed,
            len(payload_records),
        )
        return {
            **state,
            "stage": stage,
            "payload_count": len(payload_records),
            "timings": timings,
        }
    except Exception as exc:
        return _record_error(state, stage, exc)


def audit_raw_node(state: IngestionState) -> IngestionState:
    if state.get("error"):
        return state

    stage = "audit_raw"
    session: Any = None
    try:
        crawl_output_path = Path(state["crawl_output_path"])
        config_path = Path(state["config_path"]) if state.get("config_path") else None
        t0 = time.perf_counter()

        crawl_records = load_records(crawl_output_path)
        settings = load_settings(config_path, overrides=_settings_overrides_from_state(state))
        session_factory = build_session_factory(settings)
        session = session_factory()

        result = process_run_raw(
            session,
            crawl_records=crawl_records,
            crawl_source_name=settings.crawl_source_name,
            root_url=settings.root_url,
            run_metadata={
                "max_depth": state.get("max_depth"),
                "max_pages": state.get("max_pages"),
                "retry_failed_pages": state.get("retry_failed_pages"),
            },
        )
        session.commit()

        elapsed = time.perf_counter() - t0
        timings = dict(state.get("timings") or {})
        timings["audit_seconds"] = round(elapsed, 3)

        logger.info(
            "Audit(raw) stage complete in %.2fs (run_id=%s new=%s updated=%s unchanged=%s deleted=%s)",
            elapsed,
            result.run_id,
            result.counters.new_urls,
            result.counters.updated_urls,
            result.counters.unchanged_urls,
            result.counters.deleted_urls,
        )
        return {
            **state,
            "stage": stage,
            "run_id": result.run_id,
            "deleted_doc_ids": result.deleted_doc_ids,
            "changed_normalized_urls": sorted(result.changed_normalized_urls),
            "total_urls": result.counters.total_urls,
            "success_urls": result.counters.success_urls,
            "failed_urls": result.counters.failed_urls,
            "new_urls": result.counters.new_urls,
            "updated_urls": result.counters.updated_urls,
            "unchanged_urls": result.counters.unchanged_urls,
            "deleted_urls": result.counters.deleted_urls,
            "total_bytes": result.counters.total_bytes,
            "timings": timings,
        }
    except Exception as exc:
        if session is not None:
            session.rollback()
        return _record_error(state, stage, exc)
    finally:
        if session is not None:
            session.close()


def delta_select_node(state: IngestionState) -> IngestionState:
    if state.get("error"):
        return state

    stage = "delta_select"
    try:
        payload_output_path = Path(state["payload_output_path"])
        payload_delta_output_path = Path(state["payload_delta_output_path"])
        t0 = time.perf_counter()

        changed = set(state.get("changed_normalized_urls") or [])
        payload_records = load_payload_records(payload_output_path)
        if not changed:
            write_output([], payload_delta_output_path)
            elapsed = time.perf_counter() - t0
            timings = dict(state.get("timings") or {})
            timings["delta_select_seconds"] = round(elapsed, 3)
            logger.info("Delta select complete in %.2fs (0 changed URLs)", elapsed)
            return {**state, "stage": stage, "payload_delta_count": 0, "timings": timings}

        delta_records: list[dict[str, Any]] = []
        for record in payload_records:
            metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
            src = str(metadata.get("source_url") or "")
            if not src:
                continue
            if normalize_url(src) in changed:
                delta_records.append(record)

        write_output(delta_records, payload_delta_output_path)
        elapsed = time.perf_counter() - t0
        timings = dict(state.get("timings") or {})
        timings["delta_select_seconds"] = round(elapsed, 3)
        logger.info(
            "Delta select complete in %.2fs (%s / %s payload records)",
            elapsed,
            len(delta_records),
            len(payload_records),
        )
        return {**state, "stage": stage, "payload_delta_count": len(delta_records), "timings": timings}
    except Exception as exc:
        return _record_error(state, stage, exc)


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def llm_enrich_node(state: IngestionState) -> IngestionState:
    if state.get("error"):
        return state

    stage = "llm_enrich"
    try:
        if not bool(state.get("enable_llm_enrichment", False)):
            return {**state, "stage": stage, "llm_change_input_path": None, "llm_output_path": None}

        crawl_output_path = Path(state["crawl_output_path"])
        repo_root = Path(__file__).resolve().parent
        out_dir = Path(state.get("crawl_output_dir") or crawl_output_path.parent)
        change_input_path = out_dir / "llm_change_input_crawl.json"
        llm_output_path = out_dir / "llm_enrich.json"

        changed = set(state.get("changed_normalized_urls") or [])
        if not changed:
            _write_json(change_input_path, [])
            _write_json(llm_output_path, [])
            logger.info("LLM enrichment skipped (0 changed URLs)")
            return {
                **state,
                "stage": stage,
                "llm_change_input_path": str(change_input_path),
                "llm_output_path": str(llm_output_path),
            }

        crawl_payload = _read_json(crawl_output_path)
        pages: list[dict[str, Any]]
        if isinstance(crawl_payload, dict) and isinstance(crawl_payload.get("pages"), list):
            pages = [p for p in crawl_payload["pages"] if isinstance(p, dict)]
        elif isinstance(crawl_payload, list):
            pages = [p for p in crawl_payload if isinstance(p, dict)]
        else:
            raise ValueError("crawl_output_path must be a JSON list or object with 'pages' list")

        delta_pages: list[dict[str, Any]] = []
        for page in pages:
            src = str(page.get("source_url") or "")
            if src and normalize_url(src) in changed:
                delta_pages.append(page)

        _write_json(change_input_path, delta_pages)
        logger.info("LLM input prepared: %s page(s)", len(delta_pages))

        # Local/dev fallback: allow reusing a pre-generated file (no live LLM).
        sample_path = os.getenv("LLM_SAMPLE_PATH")
        if sample_path:
            sample = Path(sample_path)
            if sample.exists():
                _write_json(llm_output_path, _read_json(sample))
                logger.info("LLM output populated from sample: %s", sample)
                return {
                    **state,
                    "stage": stage,
                    "llm_change_input_path": str(change_input_path),
                    "llm_output_path": str(llm_output_path),
                }

        cmd = [
            sys.executable,
            str(repo_root / "llm_page_enricher.py"),
            "--input",
            str(change_input_path),
            "--output",
            str(llm_output_path),
        ]
        mlp = state.get("max_llm_pages")
        if mlp is not None and int(mlp) > 0:
            cmd.extend(["--max-pages", str(int(mlp))])
            logger.info("LLM enrich page cap: max_llm_pages=%s", int(mlp))
        logger.info("Calling LLM enricher (%s pages)", len(delta_pages))
        proc = subprocess.Popen(  # noqa: S603
            cmd,
            cwd=str(repo_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            line = line.rstrip("\n")
            if line:
                logger.info("[llm] %s", line)
        returncode = proc.wait()
        if returncode != 0:
            raise RuntimeError(f"llm_page_enricher failed with exit code {returncode}")

        return {
            **state,
            "stage": stage,
            "llm_change_input_path": str(change_input_path),
            "llm_output_path": str(llm_output_path),
        }
    except Exception as exc:
        return _record_error(state, stage, exc)


def apply_llm_node(state: IngestionState) -> IngestionState:
    if state.get("error"):
        return state

    stage = "apply_llm"
    try:
        if not bool(state.get("enable_llm_enrichment", False)):
            return {**state, "stage": stage}

        crawl_output_path = Path(state["crawl_output_path"])
        llm_output_path = Path(state.get("llm_output_path") or (Path(state["crawl_output_dir"]) / "llm_enrich.json"))

        if not llm_output_path.exists():
            raise FileNotFoundError(f"LLM output not found: {llm_output_path}")

        llm_records = _read_json(llm_output_path)
        if not isinstance(llm_records, list):
            raise ValueError("LLM output must be a JSON list of enrichment records")

        lookup: dict[str, dict[str, Any]] = {}
        for rec in llm_records:
            if not isinstance(rec, dict):
                continue
            src = str(rec.get("source_url") or "").strip()
            if src:
                lookup[normalize_url(src)] = rec

        crawl_payload = _read_json(crawl_output_path)
        if isinstance(crawl_payload, dict) and isinstance(crawl_payload.get("pages"), list):
            pages = crawl_payload["pages"]
        elif isinstance(crawl_payload, list):
            pages = crawl_payload
        else:
            raise ValueError("crawl_output_path must be a JSON list or object with 'pages' list")

        applied = 0
        for page in pages:
            if not isinstance(page, dict):
                continue
            src = str(page.get("source_url") or "")
            if not src:
                continue
            rec = lookup.get(normalize_url(src))
            if not rec:
                continue

            # Overwrite core fields (but keep crawl markdown unchanged).
            if rec.get("ai_title"):
                page["title"] = rec.get("ai_title")
            if rec.get("ai_summary"):
                page["description"] = rec.get("ai_summary")
            if isinstance(rec.get("keywords"), list):
                page["keywords"] = rec.get("keywords")

            for k in ("page_purpose", "customer_blurb", "facet_tags", "key_facts", "page_type", "card_name"):
                if k in rec:
                    page[k] = rec.get(k)

            meta = page.get("metadata")
            if isinstance(meta, dict):
                if "title" in page:
                    meta["title"] = page.get("title")
                if "description" in page:
                    meta["description"] = page.get("description")
                if "keywords" in page:
                    meta["keywords"] = page.get("keywords")
                page["metadata"] = meta

            # Enriched body used for chunking/embedding (hash still uses markdown only).
            key_facts = rec.get("key_facts") if isinstance(rec.get("key_facts"), list) else []
            facts_block = "\n".join([f"- {str(x).strip()}" for x in key_facts if str(x).strip()]) if key_facts else ""
            content_bits = [
                str(page.get("title") or "").strip(),
                str(page.get("page_purpose") or "").strip(),
                str(page.get("description") or "").strip(),
                facts_block,
            ]
            page["content"] = "\n\n".join([b for b in content_bits if b])
            applied += 1

        _write_json(crawl_output_path, crawl_payload)
        logger.info("Applied LLM enrichment to %s page(s)", applied)
        return {**state, "stage": stage, "llm_applied_count": applied}
    except Exception as exc:
        return _record_error(state, stage, exc)


def embedding_node(state: IngestionState) -> IngestionState:
    if state.get("error"):
        return state

    stage = "build_embeddings"
    try:
        payload_output_path = Path(state.get("payload_delta_output_path") or state["payload_output_path"])
        embeddings_output_path = Path(state["embeddings_output_path"])
        batch_size = int(state["batch_size"])
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than 0")

        t0 = time.perf_counter()
        payload_records = load_payload_records(payload_output_path)
        if not payload_records:
            # Avoid model downloads / initialization when there is no delta payload
            # to encode for this run (common when all pages are unchanged or failed).
            write_embeddings_output([], embeddings_output_path)
            elapsed = time.perf_counter() - t0
            timings = dict(state.get("timings") or {})
            timings["embedding_seconds"] = round(elapsed, 3)
            logger.info("Embedding stage skipped in %.2fs (0 records)", elapsed)
            return {
                **state,
                "stage": stage,
                "embeddings_count": 0,
                "timings": timings,
            }

        embedding_records: list[dict[str, Any]] | None = None

        # Prefer the internal embedding_service when available (offline-friendly on servers
        # that cannot reach HuggingFace). Falls back to SentenceTransformer if the wheel
        # is not installed or is explicitly disabled.
        use_embedding_service = str(os.getenv("USE_EMBEDDING_SERVICE", "1")).strip() not in ("0", "false", "False")
        if use_embedding_service:
            try:
                from embedding_service import create_embedding_service  # type: ignore

                service = create_embedding_service(state["model_name"])
                try:
                    valid_records = [record for record in payload_records if is_valid_record(record)]
                    skipped_empty = len(payload_records) - len(valid_records)
                    logger.info("Encoding %s valid records", len(valid_records))
                    if skipped_empty:
                        logger.info("Skipped %s records with empty embedding_text", skipped_empty)

                    output_records: list[dict[str, Any]] = []
                    for batch_number, batch in enumerate(iter_batches(valid_records, batch_size), start=1):
                        texts = [get_embedding_text(record) for record in batch]
                        logger.info("Encoding batch %s with %s records", batch_number, len(batch))
                        embeddings = service.get_embeddings(texts)
                        for record, embedding in zip(batch, embeddings):
                            output_records.append(build_output_record(record, embedding))
                    embedding_records = output_records
                finally:
                    try:
                        service.close()
                    except Exception:
                        logger.debug("embedding_service.close() failed", exc_info=True)
            except Exception:
                logger.warning(
                    "embedding_service unavailable or failed; falling back to SentenceTransformer.",
                    exc_info=True,
                )

        if embedding_records is None:
            model = SentenceTransformer(_resolve_local_model_name(state["model_name"]))
            embedding_records = generate_embeddings(payload_records, model, batch_size)
        write_embeddings_output(embedding_records, embeddings_output_path)

        elapsed = time.perf_counter() - t0
        timings = dict(state.get("timings") or {})
        timings["embedding_seconds"] = round(elapsed, 3)

        logger.info(
            "Embedding stage complete in %.2fs (%s records)",
            elapsed,
            len(embedding_records),
        )
        return {
            **state,
            "stage": stage,
            "embeddings_count": len(embedding_records),
            "timings": timings,
        }
    except Exception as exc:
        return _record_error(state, stage, exc)


def index_node(state: IngestionState) -> IngestionState:
    if state.get("error"):
        return state

    stage = "sync_elasticsearch"
    try:
        config_path = Path(state["config_path"]) if state.get("config_path") else None
        embeddings_output_path = Path(state["embeddings_output_path"])
        t0 = time.perf_counter()

        settings_overrides = _settings_overrides_from_state(state)
        settings = load_settings(config_path, overrides=settings_overrides)
        offline_bundle_manifest_path: str | None = None
        if bool(state.get("generate_offline_bundle", True)):
            bundle_dir = _resolve_offline_bundle_dir(state)
            manifest_path = export_offline_bundle(
                bundle_dir=bundle_dir,
                embeddings_output_path=embeddings_output_path,
                deleted_doc_ids=list(state.get("deleted_doc_ids") or []),
                settings=settings,
                settings_overrides=settings_overrides,
                recreate_index_requested=bool(state.get("recreate_index", False)),
                source_payload_path=Path(state["payload_output_path"]),
                source_payload_delta_path=Path(state["payload_delta_output_path"]),
            )
            offline_bundle_manifest_path = str(manifest_path)
            logger.info("Offline bundle exported at %s", bundle_dir)

        es = get_elasticsearch_client(settings)
        indexed, deleted, errors, indexed_norm_urls = sync_index_changes(
            es,
            settings,
            iter_embedding_records(embeddings_output_path),
            deleted_doc_ids=list(state.get("deleted_doc_ids") or []),
            recreate_index=bool(state.get("recreate_index", False)),
        )

        run_id = state.get("run_id")
        if run_id and indexed_norm_urls:
            db_session = build_session_factory(settings)()
            try:
                SnapshotRepository(db_session).mark_indexed_for_run(run_id, indexed_norm_urls)
                UrlRepository(db_session).mark_indexed_for_normalized_urls(indexed_norm_urls)
                db_session.commit()
                logger.info(
                    "Marked %s URL(s) as INDEXED in SQLite for run_id=%s",
                    len(indexed_norm_urls),
                    run_id,
                )
            except Exception:
                db_session.rollback()
                logger.exception(
                    "Failed to persist index_status=INDEXED after ES sync (run_id=%s)",
                    run_id,
                )
            finally:
                db_session.close()

        elapsed = time.perf_counter() - t0
        timings = dict(state.get("timings") or {})
        timings["index_seconds"] = round(elapsed, 3)

        logger.info(
            "ES sync stage complete in %.2fs (indexed=%s deleted=%s errors=%s)",
            elapsed,
            indexed,
            deleted,
            errors,
        )
        return {
            **state,
            "stage": stage,
            "indexed_count": indexed,
            "deleted_count": deleted,
            "indexing_errors": errors,
            "timings": timings,
            "offline_bundle_manifest_path": offline_bundle_manifest_path,
        }
    except Exception as exc:
        return _record_error(state, stage, exc)


def finalize_run_node(state: IngestionState) -> IngestionState:
    run_id = state.get("run_id")
    if not run_id:
        logger.warning(
            "finalize_run_node skipped: no run_id in state (audit stage likely did not create a crawl run)."
        )
        return {**state, "stage": "finalize_run"}

    stage = "finalize_run"
    session: Any = None
    cfg_path = Path(state["config_path"]) if state.get("config_path") else None
    try:
        settings = load_settings(cfg_path, overrides=_settings_overrides_from_state(state))
        session_factory = build_session_factory(settings)
        session = session_factory()

        elapsed_ms = int(time.time() * 1000) - int(state.get("run_started_epoch_ms") or int(time.time() * 1000))
        pipeline_error = state.get("error")
        pipeline_error_str = pipeline_error if isinstance(pipeline_error, str) and pipeline_error.strip() else None

        strict = bool(state.get("strict_partial_crawl"))
        idx_err = int(state.get("indexing_errors") or 0)
        fu = int(state.get("failed_urls") or 0)

        if pipeline_error_str:
            run_status = "FAILED"
        elif idx_err > 0 or (strict and fu > 0):
            run_status = "PARTIAL"
        else:
            run_status = "COMPLETED"

        outcome = _ingest_outcome_dict(state)
        outcome["run_status_final"] = run_status
        if run_status == "COMPLETED" and fu > 0:
            outcome["crawl_failures_note"] = (
                f"{fu} crawl URL(s) failed; run marked COMPLETED because Elasticsearch reported no bulk item errors."
            )

        failed_reason_db = _compose_finalize_failed_reason(
            run_status=run_status,
            state=state,
            pipeline_error=pipeline_error_str,
        )

        from es_kb.repositories import CrawlRunRepository

        run_repo = CrawlRunRepository(session)
        run_repo.finalize_run(
            run_id=run_id,
            run_status=run_status,
            total_urls=int(state.get("total_urls") or 0),
            success_urls=int(state.get("success_urls") or 0),
            failed_urls=int(state.get("failed_urls") or 0),
            new_urls=int(state.get("new_urls") or 0),
            updated_urls=int(state.get("updated_urls") or 0),
            unchanged_urls=int(state.get("unchanged_urls") or 0),
            deleted_urls=int(state.get("deleted_urls") or 0),
            total_bytes=int(state.get("total_bytes") or 0),
            duration_ms=elapsed_ms,
            failed_reason=failed_reason_db,
            outcome_metadata=outcome,
        )
        session.commit()
        return {**state, "stage": stage}
    except Exception as exc:
        if session is not None:
            session.rollback()
        _persist_finalize_node_exception(run_id, config_path=cfg_path, state=state, exc=exc)
        return _record_error(state, stage, exc)
    finally:
        if session is not None:
            session.close()


def build_graph():
    graph = StateGraph(IngestionState)
    graph.add_node("crawl", crawl_node)
    graph.add_node("audit_raw", audit_raw_node)
    graph.add_node("llm_enrich", llm_enrich_node)
    graph.add_node("apply_llm", apply_llm_node)
    graph.add_node("build_payloads", payload_node)
    graph.add_node("delta_select", delta_select_node)
    graph.add_node("build_embeddings", embedding_node)
    graph.add_node("sync_elasticsearch", index_node)
    graph.add_node("finalize_run", finalize_run_node)

    graph.add_edge(START, "crawl")
    graph.add_edge("crawl", "audit_raw")
    graph.add_edge("audit_raw", "llm_enrich")
    graph.add_edge("llm_enrich", "apply_llm")
    graph.add_edge("apply_llm", "build_payloads")
    graph.add_edge("build_payloads", "delta_select")
    graph.add_edge("delta_select", "build_embeddings")
    graph.add_edge("build_embeddings", "sync_elasticsearch")
    graph.add_edge("sync_elasticsearch", "finalize_run")
    graph.add_edge("finalize_run", END)
    return graph.compile()


def run_ingestion_flow(
    *,
    crawl_output_dir: Path,
    payload_output_path: Path,
    embeddings_output_path: Path,
    config_path: Path | None,
    model_name: str,
    batch_size: int,
    max_depth: int,
    max_pages: int,
    retry_failed_pages: bool,
    max_retry_rounds: int,
    max_retry_pages: int,
    run_crawl: bool,
    recreate_index: bool,
    strict_partial_crawl: bool = False,
    scrape_offers: bool = False,
    scrape_rewards: bool = False,
    scrape_about_us: bool = False,
    max_offers: int | None = None,
    max_rewards: int | None = None,
    max_llm_pages: int | None = None,
    es_url: str | None = None,
    es_username: str | None = None,
    es_password: str | None = None,
    db_url: str | None = None,
    generate_offline_bundle: bool = True,
    offline_bundle_dir: Path | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    chunk_overlap: int = DEFAULT_CHUNK_OVERLAP,
    enable_llm_enrichment: bool = False,
) -> IngestionState:
    app = build_graph()
    settings_overrides: dict[str, Any] = {
        "elasticsearch_url": es_url,
        "elasticsearch_username": es_username,
        "elasticsearch_password": es_password,
        "db_url": db_url,
    }
    initial: IngestionState = {
        "crawl_output_dir": str(crawl_output_dir),
        "crawl_output_path": str(crawl_output_dir / "sbicard_crawl.json"),
        "payload_output_path": str(payload_output_path),
        "payload_delta_output_path": str(payload_output_path.with_name("embedding_payloads_delta.jsonl")),
        "embeddings_output_path": str(embeddings_output_path),
        "config_path": str(config_path) if config_path else None,
        "model_name": model_name,
        "batch_size": batch_size,
        "max_depth": max_depth,
        "max_pages": max_pages,
        "retry_failed_pages": retry_failed_pages,
        "max_retry_rounds": max_retry_rounds,
        "max_retry_pages": max_retry_pages,
        "run_crawl": run_crawl,
        "recreate_index": recreate_index,
        "strict_partial_crawl": strict_partial_crawl,
        "scrape_offers": bool(scrape_offers),
        "scrape_rewards": bool(scrape_rewards),
        "scrape_about_us": bool(scrape_about_us),
        "max_offers": int(max_offers) if max_offers is not None and int(max_offers) > 0 else None,
        "max_rewards": int(max_rewards) if max_rewards is not None and int(max_rewards) > 0 else None,
        "max_llm_pages": int(max_llm_pages) if max_llm_pages is not None and int(max_llm_pages) > 0 else None,
        "settings_overrides": settings_overrides,
        "generate_offline_bundle": bool(generate_offline_bundle),
        "offline_bundle_dir": str(offline_bundle_dir) if offline_bundle_dir else None,
        "offline_bundle_manifest_path": None,
        "offer_output_path": None,
        "offer_merge_count": 0,
        "reward_output_path": None,
        "reward_merge_count": 0,
        "about_us_output_path": None,
        "about_us_merge_count": 0,
        "chunk_size": int(chunk_size),
        "chunk_overlap": int(chunk_overlap),
        "enable_llm_enrichment": bool(enable_llm_enrichment),
        "run_started_epoch_ms": int(time.time() * 1000),
        "deleted_doc_ids": [],
        "timings": {},
        "error": None,
        "stage": "init",
    }
    return app.invoke(initial)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--crawl-output-dir", type=Path, default=OUTPUT_DIR)
    parser.add_argument(
        "--payload-output",
        type=Path,
        default=OUTPUT_DIR / "embedding_payloads.jsonl",
    )
    parser.add_argument(
        "--embeddings-output",
        type=Path,
        default=OUTPUT_DIR / "embedding_vectors.jsonl",
    )
    parser.add_argument("--config", type=Path, default=None, help="Path to ES config JSON.")
    parser.add_argument("--es-url", default=None, help="Override Elasticsearch URL.")
    parser.add_argument("--es-username", default=None, help="Override Elasticsearch username.")
    parser.add_argument("--es-password", default=None, help="Override Elasticsearch password.")
    parser.add_argument("--db-url", default=None, help="Override SQLAlchemy DB URL.")
    parser.add_argument(
        "--offline-bundle-dir",
        type=Path,
        default=None,
        help=f"Directory to write offline replay bundle. Defaults to <crawl-output-dir>/{DEFAULT_BUNDLE_DIRNAME}.",
    )
    parser.add_argument(
        "--no-generate-offline-bundle",
        action="store_true",
        help="Disable offline replay bundle export.",
    )
    parser.add_argument("--model-name", default=DEFAULT_MODEL_NAME)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--max-depth", type=int, default=2)
    parser.add_argument("--max-pages", type=int, default=150)
    parser.add_argument("--retry-failed-pages", action="store_true", default=True)
    parser.add_argument(
        "--no-retry-failed-pages",
        action="store_true",
        help="Disable retry recursion for failed crawl pages.",
    )
    parser.add_argument("--max-retry-rounds", type=int, default=2)
    parser.add_argument("--max-retry-pages", type=int, default=40)
    parser.add_argument("--skip-crawl", action="store_true")
    parser.add_argument("--recreate-index", action="store_true")
    parser.add_argument(
        "--scrape-offers",
        action="store_true",
        help="Also run offer scraper and merge into sbicard_crawl.json before building payloads.",
    )
    parser.add_argument(
        "--scrape-rewards",
        action="store_true",
        help="Also run rewards scraper and merge into sbicard_crawl.json before building payloads.",
    )
    parser.add_argument(
        "--scrape-about-us",
        action="store_true",
        help="Run About Us scraper and merge its page into sbicard_crawl.json.",
    )
    parser.add_argument(
        "--max-offers",
        type=int,
        default=None,
        help="With --scrape-offers: cap number of offer detail pages (omit for full scrape).",
    )
    parser.add_argument(
        "--max-rewards",
        type=int,
        default=None,
        help="With --scrape-rewards: cap number of reward items (omit for full scrape).",
    )
    parser.add_argument(
        "--strict-partial-crawl",
        action="store_true",
        help="Mark run as PARTIAL when any crawl URL failed (legacy behavior). Default: only ES bulk errors set PARTIAL.",
    )
    parser.add_argument("--output", type=Path, default=None, help="Optional output state JSON.")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE, help="Approx max chars per chunk.")
    parser.add_argument("--chunk-overlap", type=int, default=DEFAULT_CHUNK_OVERLAP, help="Char overlap between chunks.")
    parser.add_argument(
        "--enable-llm-enrichment",
        action="store_true",
        help="Call LLM for NEW/UPDATED pages and apply enrichment before chunking/embedding.",
    )
    parser.add_argument(
        "--max-llm-pages",
        type=int,
        default=None,
        help="With --enable-llm-enrichment: cap pages sent to the LLM (omit for no cap).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s %(message)s")

    try:
        final_state = run_ingestion_flow(
            crawl_output_dir=args.crawl_output_dir,
            payload_output_path=args.payload_output,
            embeddings_output_path=args.embeddings_output,
            config_path=args.config,
            model_name=args.model_name,
            batch_size=args.batch_size,
            max_depth=args.max_depth,
            max_pages=args.max_pages,
            retry_failed_pages=(False if args.no_retry_failed_pages else args.retry_failed_pages),
            max_retry_rounds=args.max_retry_rounds,
            max_retry_pages=args.max_retry_pages,
            run_crawl=not args.skip_crawl,
            recreate_index=args.recreate_index,
            strict_partial_crawl=bool(args.strict_partial_crawl),
            scrape_offers=bool(args.scrape_offers),
            scrape_rewards=bool(args.scrape_rewards),
            scrape_about_us=bool(args.scrape_about_us),
            max_offers=args.max_offers,
            max_rewards=args.max_rewards,
            max_llm_pages=args.max_llm_pages,
            es_url=args.es_url,
            es_username=args.es_username,
            es_password=args.es_password,
            db_url=args.db_url,
            generate_offline_bundle=not args.no_generate_offline_bundle,
            offline_bundle_dir=args.offline_bundle_dir,
            chunk_size=int(args.chunk_size),
            chunk_overlap=int(args.chunk_overlap),
            enable_llm_enrichment=bool(args.enable_llm_enrichment),
        )
    except Exception:
        logger.exception("Ingestion flow failed unexpectedly")
        return 1

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(final_state, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("Wrote flow state to %s", args.output)

    print(json.dumps(final_state, ensure_ascii=False, indent=2))
    if final_state.get("error"):
        return 2
    if int(final_state.get("indexing_errors") or 0) > 0:
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
