import argparse
import asyncio
import hashlib
import html
import inspect
import json
import re
import sys
import os
from http.cookies import SimpleCookie
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai.content_filter_strategy import PruningContentFilter
from crawl4ai.deep_crawling import BFSDeepCrawlStrategy
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai import ProxyConfig  # or from crawl4ai.async_configs

proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")

START_URL = "https://www.sbicard.com"
DEFAULT_OUTPUT_DIR = "output_markdown"
RETRYABLE_ERROR_MARKERS = (
    "timeout",
    "timed out",
    "navigation",
    "failed on navigating",
    "connection",
    "net::",
    "err_empty_response",
    "proxy direct failed",
    "acs-goto",
    "page.goto: timeout",
    "blocked by anti-bot",
    "anti-bot protection",
    "429",
    "503",
    "temporarily unavailable",
)

ALLOWED_CRAWL_HOST = (urlparse(START_URL).netloc or "").lower()

_SKIP_PATH_SUBSTRINGS = (
    "/sbi-card-en/assets/",
    "/static-resources/",
)
_SKIP_EXTENSIONS = (
    ".pdf",
    ".png",
    ".jpg",
    ".jpeg",
    ".webp",
    ".gif",
    ".svg",
    ".zip",
    ".mp4",
    ".mov",
    ".avi",
    ".mp3",
    ".wav",
)
# Substrings matched anywhere in the URL path (or raw URL for malformed links).
_BLOCKED_PATH_SUBSTRINGS = (
    "/hi/",
    "/sites",
    "mailto:",
    "www.rupay",
    "www.visa",
    "www.air.irctc",
    "/en/corporate/",
    "resolution-framework",
    "compare-cards.page",
    "/en/faq/sbicard.com",
    "banking-partnership/bank-of-maharashtra",
    "/en/credit-cards/compare-cards.page",
)
_BLOCKLIST_URLS_RAW: tuple[str, ...] = (
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


def _sanitize_blocklist_url(raw: str) -> str:
    cleaned = str(raw or "").strip().rstrip(",").strip()
    while cleaned.endswith("%22"):
        cleaned = cleaned[:-3].rstrip().rstrip(",").strip()
    return cleaned


def _build_url_blocklist() -> tuple[frozenset[str], frozenset[str], frozenset[str]]:
    from es_kb.normalize_hash import normalize_url

    exact: set[str] = set()
    paths: set[str] = set()
    basenames: set[str] = set()
    for raw in _BLOCKLIST_URLS_RAW:
        cleaned = _sanitize_blocklist_url(raw)
        if not cleaned:
            continue
        norm = normalize_url(cleaned)
        if norm:
            exact.add(norm)
        path = (urlparse(cleaned).path or "").lower().rstrip("/")
        if path:
            paths.add(path)
            basename = path.rsplit("/", 1)[-1]
            if basename:
                basenames.add(basename)
    return frozenset(exact), frozenset(paths), frozenset(basenames)


_BLOCKED_URLS_EXACT, _BLOCKED_URL_PATHS, _BLOCKED_URL_BASENAMES = _build_url_blocklist()


def is_allowed_target_url(url: str, status_code: int | None = None) -> tuple[bool, str]:
    """Return (allowed, reason). Reason is used for telemetry."""
    if status_code == 404:
        return False, "http_404"

    raw = str(url or "").strip()
    if not raw:
        return False, "empty_url"
    if "[" in raw or "]" in raw:
        return False, "bracket_url"

    parsed = urlparse(raw)
    host = (parsed.netloc or "").lower()
    if host != ALLOWED_CRAWL_HOST:
        return False, "non_www_domain"

    raw_lower = raw.lower()
    path_lower = (parsed.path or "").lower()
    path_key = path_lower.rstrip("/")

    from es_kb.normalize_hash import normalize_url

    normalized = normalize_url(raw)
    if normalized in _BLOCKED_URLS_EXACT:
        return False, "blocklisted_url"
    if path_key and path_key in _BLOCKED_URL_PATHS:
        return False, "blocklisted_path"
    path_basename = path_lower.rsplit("/", 1)[-1]
    if path_basename and path_basename in _BLOCKED_URL_BASENAMES:
        return False, "blocklisted_basename"

    if "/en/en/en/" in path_lower:
        return False, "repeated_lang_segments"
    if any(part in path_lower for part in _SKIP_PATH_SUBSTRINGS):
        return False, "asset_path"
    if len(path_lower) > 350:
        return False, "path_too_long"
    if "%22" in raw_lower:
        return False, "malformed_url"
    if any(part in path_lower or part in raw_lower for part in _BLOCKED_PATH_SUBSTRINGS):
        return False, "blocked_path_substring"

    for ext in _SKIP_EXTENSIONS:
        if path_lower.endswith(ext):
            return False, f"ext_{ext.lstrip('.')}"

    return True, "ok"


def make_safe_filename(url: str, used_names: set[str]) -> str:
    parsed = urlparse(url)
    path_part = parsed.path.strip("/").replace("/", "_") or "home"
    query_part = ""
    if parsed.query:
        query_hash = hashlib.sha1(parsed.query.encode("utf-8")).hexdigest()[:8]
        query_part = f"_{query_hash}"

    raw_name = f"{parsed.netloc}_{path_part}{query_part}"
    safe_name = re.sub(r"[^a-zA-Z0-9._-]+", "-", raw_name).strip("-_.")
    safe_name = (safe_name or "page")[:140]

    candidate = safe_name
    suffix = 2
    while candidate in used_names:
        candidate = f"{safe_name}-{suffix}"
        suffix += 1

    used_names.add(candidate)
    return f"{candidate}.md"


def pick_markdown(result) -> str:
    markdown_obj = getattr(result, "markdown", None)
    if not markdown_obj:
        return ""
    fit_markdown = (getattr(markdown_obj, "fit_markdown", "") or "").strip()
    if fit_markdown:
        return fit_markdown
    return (getattr(markdown_obj, "raw_markdown", "") or "").strip()


def sanitize_markdown(markdown_text: str) -> str:
    lines = [line.rstrip() for line in markdown_text.splitlines()]

    deduped_lines = []
    prev_line = None
    for line in lines:
        if line == prev_line and line.strip():
            continue
        deduped_lines.append(line)
        prev_line = line

    cta_starters = (
        "apply now",
        "know more",
        "learn more",
        "click here",
        "read more",
        "check now",
        "download app",
    )
    line_counts: dict[str, int] = {}
    for line in deduped_lines:
        normalized = line.strip().lower()
        if normalized:
            line_counts[normalized] = line_counts.get(normalized, 0) + 1

    cleaned_lines = []
    for line in deduped_lines:
        stripped = line.strip()
        normalized = stripped.lower()
        if (
            stripped
            and len(stripped) <= 80
            and line_counts.get(normalized, 0) >= 3
            and normalized.startswith(cta_starters)
        ):
            continue
        cleaned_lines.append(line)

    text = "\n".join(cleaned_lines)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def is_login_redirect_url(url: str | None) -> bool:
    if not url:
        return False
    parsed = urlparse(str(url))
    return "login" in (parsed.path or "").lower()


def extract_last_path_param(path_value: str) -> str:
    if not path_value:
        return ""
    parsed = urlparse(path_value)
    path_only = parsed.path if parsed.path else path_value
    segments = [segment for segment in path_only.split("/") if segment]
    if not segments:
        return ""
    last_segment = segments[-1].split("?", 1)[0].split("#", 1)[0]
    last_segment = last_segment.rsplit(".", 1)[0]
    cleaned = re.sub(r"\s+", " ", last_segment.replace("-", " ").replace("_", " ")).strip()
    return cleaned


def prefix_text_with_path_param(text: str, path_param: str) -> str:
    normalized_path_param = path_param.strip()
    normalized_text = (text or "").strip()
    if not normalized_path_param:
        return normalized_text
    if not normalized_text:
        return normalized_path_param
    if normalized_text.lower() == normalized_path_param.lower():
        return normalized_text
    if normalized_text.lower().startswith(f"{normalized_path_param.lower()} | "):
        return normalized_text
    return f"{normalized_path_param} | {normalized_text}"


def prefix_keywords_with_path_param(keywords_text: str, path_param: str) -> str:
    normalized_path_param = path_param.strip()
    normalized_keywords = (keywords_text or "").strip()
    if not normalized_path_param:
        return normalized_keywords
    if not normalized_keywords:
        return normalized_path_param

    parts = [part.strip() for part in normalized_keywords.split(",") if part.strip()]
    if parts and parts[0].lower() == normalized_path_param.lower():
        return ", ".join(parts)
    return ", ".join([normalized_path_param, *parts])


def extract_head_metadata(html_text: str) -> dict[str, str]:
    if not html_text:
        return {}

    patterns = {
        "title": r"<title[^>]*>(.*?)</title>",
        "description": r'<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']',
        "keywords": r'<meta[^>]+name=["\']keywords["\'][^>]+content=["\'](.*?)["\']',
        "canonical_url": r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\'](.*?)["\']',
        "robots": r'<meta[^>]+name=["\']robots["\'][^>]+content=["\'](.*?)["\']',
        "language": r'<html[^>]+lang=["\'](.*?)["\']',
        "og:title": r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\'](.*?)["\']',
        "og:description": r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\'](.*?)["\']',
        "og:url": r'<meta[^>]+property=["\']og:url["\'][^>]+content=["\'](.*?)["\']',
        "og:type": r'<meta[^>]+property=["\']og:type["\'][^>]+content=["\'](.*?)["\']',
        "twitter:title": r'<meta[^>]+name=["\']twitter:title["\'][^>]+content=["\'](.*?)["\']',
        "twitter:description": r'<meta[^>]+name=["\']twitter:description["\'][^>]+content=["\'](.*?)["\']',
    }

    extracted: dict[str, str] = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, html_text, re.IGNORECASE | re.DOTALL)
        if match:
            value = html.unescape(match.group(1)).strip()
            if value:
                extracted[key] = re.sub(r"\s+", " ", value)

    return extracted


def flatten_metadata(value, prefix: str = "") -> dict[str, str]:
    flattened: dict[str, str] = {}

    if isinstance(value, dict):
        for key, nested_value in value.items():
            next_prefix = f"{prefix}.{key}" if prefix else str(key)
            flattened.update(flatten_metadata(nested_value, next_prefix))
        return flattened

    if isinstance(value, (list, tuple, set)):
        items = []
        for item in value:
            if isinstance(item, (dict, list, tuple, set)):
                items.append(json.dumps(item, ensure_ascii=False, sort_keys=True))
            else:
                item_str = str(item).strip()
                if item_str:
                    items.append(item_str)
        if items:
            flattened[prefix] = ", ".join(items)
        return flattened

    if value is None:
        return flattened

    value_str = str(value).strip()
    if value_str:
        flattened[prefix] = value_str
    return flattened


def parse_cookie_header(cookie_header: str) -> dict[str, str]:
    if not cookie_header:
        return {}

    cookie = SimpleCookie()
    try:
        cookie.load(cookie_header)
    except Exception:
        return {}

    return {key: morsel.value for key, morsel in cookie.items()}


def simplify_sent_cookies(cookies: list[dict]) -> list[dict[str, str]]:
    simplified = []
    for cookie in cookies:
        if not isinstance(cookie, dict):
            continue
        simplified.append(
            {
                "name": str(cookie.get("name", "")),
                "value": str(cookie.get("value", "")),
                "domain": str(cookie.get("domain", "")),
                "path": str(cookie.get("path", "")),
            }
        )
    return simplified


def get_sent_request_headers(browser_config: BrowserConfig) -> dict[str, str]:
    headers = dict(browser_config.headers or {})
    if browser_config.user_agent and "User-Agent" not in headers and "user-agent" not in headers:
        headers["User-Agent"] = browser_config.user_agent
    return headers


def get_final_request_details(result) -> tuple[dict[str, str], dict[str, str]]:
    network_requests = getattr(result, "network_requests", None) or []
    navigation_requests = [
        entry
        for entry in network_requests
        if entry.get("event_type") == "request" and entry.get("is_navigation_request")
    ]
    if not navigation_requests:
        return {}, {}

    target_urls = {
        getattr(result, "url", "") or "",
        getattr(result, "redirected_url", "") or "",
    }
    final_request = None
    for entry in reversed(navigation_requests):
        if entry.get("url", "") in target_urls:
            final_request = entry
            break
    if final_request is None:
        final_request = navigation_requests[-1]

    final_headers = dict(final_request.get("headers") or {})
    cookie_header = final_headers.get("cookie") or final_headers.get("Cookie") or ""
    return final_headers, parse_cookie_header(cookie_header)


def build_metadata_block(result, parsed_url) -> tuple[str, dict[str, str]]:
    raw_metadata = getattr(result, "metadata", {}) or {}
    page_html = getattr(result, "html", "") or ""

    flattened = flatten_metadata(raw_metadata)
    head_metadata = extract_head_metadata(page_html)

    merged_metadata: dict[str, str] = {}
    merged_metadata["source_url"] = getattr(result, "url", "") or parsed_url.geturl()
    merged_metadata["domain"] = parsed_url.netloc
    merged_metadata["path"] = parsed_url.path or "/"
    merged_metadata["query"] = parsed_url.query

    for key, value in flattened.items():
        if value:
            merged_metadata[key.replace(".", ":")] = value

    for key, value in head_metadata.items():
        if value and key not in merged_metadata:
            merged_metadata[key] = value

    if "depth" not in merged_metadata:
        merged_metadata["depth"] = "0"

    title = (
        merged_metadata.get("title")
        or merged_metadata.get("og:title")
        or merged_metadata.get("twitter:title")
        or parsed_url.path
        or parsed_url.netloc
    )
    return title.strip(), merged_metadata


def classify_menu_taxonomy(parsed_url, page_title: str, markdown_text: str) -> tuple[str, str]:
    path = parsed_url.path or "/"
    path_lower = path.lower()
    title_lower = page_title.lower()
    body_lower = markdown_text.lower()

    if "404 page not found" in title_lower or "page requested by you does not exist" in body_lower:
        return "Personal", "help"

    if "/corporate/" in path_lower or title_lower.startswith("corporate"):
        if "offers" in path_lower or "offer" in title_lower:
            return "Corporate", "offers"
        if "customized-solutions" in path_lower or "customized solution" in title_lower:
            return "Corporate", "customized-solutions"
        return "Corporate", "credit-cards"

    if path_lower.startswith("/en/tnc.page") or "most-important-terms-and-conditions" in path_lower:
        return "Personal", "terms-condition"

    if path_lower.startswith("/en/help.page") or path_lower.startswith("/en/faq/") or path_lower == "/en/faq.page":
        return "Personal", "help"

    if path_lower.startswith("/en/personal/offers.page") or "/offers/" in path_lower:
        return "Personal", "offers"

    if path_lower.startswith("/en/personal/benefits/lower-interest-option/"):
        return "Personal", "pay"

    if path_lower.startswith("/en/personal/benefits/"):
        return "Personal", "benefits"

    if path_lower.startswith("/en/eapply/track-credit-card-application.page"):
        return "Personal", "help"

    if path_lower.startswith("/en/eapply/"):
        return "Personal", "credit-cards"

    if path_lower.startswith("/creditcards/app/"):
        return "Personal", "help"

    if path_lower.startswith("/sprint/"):
        return "Personal", "credit-cards"

    if path_lower.startswith("/en/personal/rewards") or "/reward" in path_lower:
        if path_lower.startswith("/en/personal/credit-cards/"):
            return "Personal", "credit-cards"
        return "Personal", "rewards"

    if path_lower.startswith("/en/personal/credit-cards") or "/personal/credit-cards/" in path_lower:
        return "Personal", "credit-cards"

    if path_lower.startswith("/en/personal/"):
        return "Personal", "help"

    return "Personal", "help"


def is_internal_crawl_url(url: str) -> bool:
    parsed = urlparse(url)
    return (parsed.netloc or "").lower() == ALLOWED_CRAWL_HOST


def is_retryable_page_record(page_record: dict) -> bool:
    if page_record.get("success") is True:
        return False
    url = str(page_record.get("source_url") or "")
    if not is_internal_crawl_url(url):
        return False
    status_code = page_record.get("status_code")
    status_code_int = int(status_code) if isinstance(status_code, int) else None
    allowed, _ = is_allowed_target_url(url, status_code=status_code_int)
    if not allowed:
        return False
    error_text = str(page_record.get("error") or "").lower()
    return any(marker in error_text for marker in RETRYABLE_ERROR_MARKERS)


def _crawl_page_rank(page: dict) -> tuple[int, int]:
    """Prefer successful captures, then longer markdown (usually more complete)."""
    ok = 1 if page.get("success") else 0
    return (ok, len(page.get("markdown") or ""))


def dedupe_crawl_pages_by_url(pages: list[dict]) -> tuple[list[dict], int]:
    """Keep one row per normalized URL (same key as audit / ES). Merges duplicate BFS visits."""
    from es_kb.normalize_hash import normalize_url

    no_key: list[dict] = []
    order: list[str] = []
    best: dict[str, dict] = {}
    for page in pages:
        raw = str(page.get("source_url") or "")
        key = normalize_url(raw)
        if not key:
            key = raw.strip().lower()
        if not key:
            no_key.append(page)
            continue
        if key not in best:
            order.append(key)
            best[key] = page
        elif _crawl_page_rank(page) > _crawl_page_rank(best[key]):
            best[key] = page
    deduped = [best[k] for k in order] + no_key
    return deduped, len(pages) - len(deduped)


def build_page_record(result, browser_config: BrowserConfig) -> tuple[dict, bool]:
    url = getattr(result, "url", "")
    parsed = urlparse(url)
    metadata = getattr(result, "metadata", {}) or {}
    depth = int(metadata.get("depth", 0))

    status_code = getattr(result, "status_code", None)
    error_message = getattr(result, "error_message", None)
    success = bool(getattr(result, "success", False))
    has_error_status = isinstance(status_code, int) and status_code >= 400

    page_title, metadata_block = build_metadata_block(result, parsed)
    page_description = metadata_block.get("description", "")
    page_keywords = metadata_block.get("keywords", "")
    redirected_url = getattr(result, "redirected_url", None)

    # Some source URLs resolve to login pages; preserve source intent by prefixing
    # the last source path param to user-facing fields.
    if is_login_redirect_url(redirected_url):
        path_value = parsed.path or "/"
        path_param = extract_last_path_param(path_value)
        if path_param:
            page_title = prefix_text_with_path_param(page_title, path_param)
            page_description = prefix_text_with_path_param(page_description, path_param)
            page_keywords = prefix_keywords_with_path_param(page_keywords, path_param)
            metadata_block["path"] = path_value
            metadata_block["title"] = page_title
            metadata_block["description"] = page_description
            metadata_block["keywords"] = page_keywords

    if not success:
        return (
            {
                "title": page_title,
                "description": page_description,
                "keywords": page_keywords,
                "category": None,
                "sub_category": None,
                "source_url": url,
                "path": parsed.path or "/",
                "depth": depth,
                "success": False,
                "status_code": status_code,
                "redirected_url": redirected_url,
                "redirected_status_code": getattr(result, "redirected_status_code", None),
                "error": error_message or "Fetch failed",
                "request_headers_sent": get_sent_request_headers(browser_config),
                "request_cookies_sent": simplify_sent_cookies(browser_config.cookies),
                "request_headers_final": {},
                "request_cookies_final": {},
                "response_headers": getattr(result, "response_headers", None) or {},
                "metadata": metadata_block,
                "markdown": "",
            },
            depth,
        )

    markdown_text = pick_markdown(result)
    if markdown_text:
        markdown_text = sanitize_markdown(markdown_text)
    category, sub_category = classify_menu_taxonomy(parsed, page_title, markdown_text)
    final_request_headers, final_request_cookies = get_final_request_details(result)

    page_record = {
        "title": page_title,
        "description": page_description,
        "keywords": page_keywords,
        "category": category,
        "sub_category": sub_category,
        "source_url": url,
        "path": parsed.path or "/",
        "depth": depth,
        "success": success and not has_error_status,
        "status_code": status_code,
        "redirected_url": redirected_url,
        "redirected_status_code": getattr(result, "redirected_status_code", None),
        "error": error_message or (f"HTTP {status_code}" if has_error_status else ""),
        "request_headers_sent": get_sent_request_headers(browser_config),
        "request_cookies_sent": simplify_sent_cookies(browser_config.cookies),
        "request_headers_final": final_request_headers,
        "request_cookies_final": final_request_cookies,
        "response_headers": getattr(result, "response_headers", None) or {},
        "metadata": metadata_block,
        "markdown": markdown_text or "",
    }
    return page_record, depth


def _adaptive_crawler_kwargs(max_depth: int, max_pages: int) -> dict[str, Any]:
    """Tune Crawl4AI runtime knobs for very large crawls, if available."""
    # Treat moderate crawls as "large" for stability on sbicard.com, which tends to
    # slow/timeout under load and triggers Crawl4AI memory guards when concurrency is high.
    is_large = max_depth >= 3 or max_pages >= 150
    if not is_large:
        return {}

    kwargs: dict[str, Any] = {}
    try:
        params = inspect.signature(CrawlerRunConfig.__init__).parameters
    except Exception:
        params = {}

    def add_if_supported(name: str, value: Any) -> None:
        if name in params:
            kwargs[name] = value

    # Reduce parallel pressure; this is the biggest lever for memory guard trips.
    add_if_supported("semaphore_count", 2)
    # Add slight jittered pacing where supported.
    add_if_supported("mean_delay", 0.2)
    add_if_supported("max_range", 0.3)
    # Increase page timeout to reduce false failures on slower pages.
    add_if_supported("page_timeout", 60000)
    return kwargs


async def crawl_site(
    output_dir: Path,
    max_depth: int,
    max_pages: int,
    retry_failed_pages: bool = True,
    max_retry_rounds: int = 2,
    max_retry_pages: int = 40,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    browser_config =  BrowserConfig(
    headless=True,
    proxy_config={"server": proxy} if proxy else None,
)
    # Crawl4AI network capture is useful for request/cookie metadata on smaller runs,
    # but it is memory-heavy and can trigger internal capture bugs on large crawls.
    capture_network_requests = not (max_depth >= 3 or max_pages >= 150)
    adaptive_kwargs = _adaptive_crawler_kwargs(max_depth=max_depth, max_pages=max_pages)
    if not capture_network_requests:
        print(
            "Large crawl detected: disabling network request capture for stability "
            f"(max_depth={max_depth}, max_pages={max_pages})."
        )
    if adaptive_kwargs:
        print(f"Large crawl detected: applying adaptive crawl settings: {adaptive_kwargs}")

    pruning_filter = PruningContentFilter(
        threshold=0.5,
        threshold_type="dynamic",
        min_word_threshold=10,
    )
    markdown_generator = DefaultMarkdownGenerator(
        content_filter=pruning_filter,
        options={
            "ignore_links": True,
            "ignore_images": True,
            "skip_internal_links": True,
            "body_width": 0,
        },
    )

    config = CrawlerRunConfig(
        deep_crawl_strategy=BFSDeepCrawlStrategy(
            max_depth=max_depth,
            include_external=False,
            max_pages=max_pages,
        ),
        markdown_generator=markdown_generator,
        excluded_tags=[
            "footer",
            "script",
            "style",
            "noscript",
            "svg",
            "canvas",
            "iframe",
            "form",
            "aside",
        ],
        excluded_selector=".ads, .advert, .cookie, .consent, .newsletter, .popup, .modal, .overlay, .chat-widget, [role='dialog']",
        remove_overlay_elements=True,
        remove_consent_popups=True,
        remove_forms=True,
        exclude_all_images=True,
        word_count_threshold=15,
        capture_network_requests=capture_network_requests,
        stream=False,
        verbose=True,
        **adaptive_kwargs,
    )
    retry_config = CrawlerRunConfig(
        markdown_generator=markdown_generator,
        excluded_tags=config.excluded_tags,
        excluded_selector=config.excluded_selector,
        remove_overlay_elements=True,
        remove_consent_popups=True,
        remove_forms=True,
        exclude_all_images=True,
        word_count_threshold=15,
        capture_network_requests=capture_network_requests,
        stream=False,
        verbose=False,
        **adaptive_kwargs,
    )

    async with AsyncWebCrawler(config=browser_config) as crawler:
        results = await crawler.arun(START_URL, config=config)

    if not isinstance(results, list):
        results = [results]

    skipped_targets: dict[str, int] = {}
    saved_pages = []
    failure_count = 0
    max_seen_depth = 0
    non_internal_urls = []

    for result in results:
        url = getattr(result, "url", "")
        parsed = urlparse(url)

        page_record, depth = build_page_record(result, browser_config)
        max_seen_depth = max(max_seen_depth, depth)

        source_url = str(page_record.get("source_url") or url)
        status_code = page_record.get("status_code")
        status_code_int = int(status_code) if isinstance(status_code, int) else None
        allowed, reason = is_allowed_target_url(source_url, status_code=status_code_int)
        if not allowed:
            skipped_targets[reason] = skipped_targets.get(reason, 0) + 1
            continue
        if (parsed.netloc or "").lower() != ALLOWED_CRAWL_HOST:
            non_internal_urls.append(url)

        if not page_record.get("success"):
            failure_count += 1
        saved_pages.append(page_record)

    retry_candidates_initial = [
        page for page in saved_pages if is_retryable_page_record(page)
    ]
    retry_attempted = 0
    retry_recovered = 0
    recovered_urls: set[str] = set()

    if retry_failed_pages and retry_candidates_initial:
        url_to_index = {
            str(page.get("source_url") or ""): idx
            for idx, page in enumerate(saved_pages)
            if page.get("source_url")
        }
        async with AsyncWebCrawler(config=browser_config) as retry_crawler:
            for round_no in range(1, max_retry_rounds + 1):
                pending_urls = [
                    str(page.get("source_url") or "")
                    for page in saved_pages
                    if is_retryable_page_record(page)
                    and str(page.get("source_url") or "") not in recovered_urls
                ]
                pending_urls = [url for url in pending_urls if url][:max_retry_pages]
                if not pending_urls:
                    break
                print(f"Retry round {round_no}: attempting {len(pending_urls)} failed pages...")
                for url in pending_urls:
                    retry_attempted += 1
                    try:
                        retry_result = await retry_crawler.arun(url, config=retry_config)
                        if isinstance(retry_result, list):
                            retry_result = retry_result[0] if retry_result else None
                        if retry_result is None:
                            continue
                        retry_record, retry_depth = build_page_record(retry_result, browser_config)
                        max_seen_depth = max(max_seen_depth, retry_depth)
                        retry_status = retry_record.get("status_code")
                        retry_status_int = int(retry_status) if isinstance(retry_status, int) else None
                        retry_allowed, _ = is_allowed_target_url(
                            str(retry_record.get("source_url") or url),
                            status_code=retry_status_int,
                        )
                        if not retry_allowed:
                            continue
                        if retry_record.get("success") is True and url in url_to_index:
                            saved_pages[url_to_index[url]] = retry_record
                            recovered_urls.add(url)
                            retry_recovered += 1
                    except Exception:
                        # Keep original failed record if retry also fails.
                        continue

    saved_pages, duplicates_removed = dedupe_crawl_pages_by_url(saved_pages)
    failure_count = sum(1 for page in saved_pages if not page.get("success"))
    retry_failed = retry_attempted - retry_recovered

    # Telemetry (log-only): help tune crawl settings without changing downstream logic.
    failure_reasons: dict[str, int] = {}
    for page in saved_pages:
        if page.get("success") is True:
            continue
        err = str(page.get("error") or "").lower()
        bucket = "other"
        if "anti-bot" in err or "antibot" in err:
            bucket = "antibot"
        elif "timeout" in err or "timed out" in err:
            bucket = "timeout"
        elif "err_empty_response" in err or "empty response" in err:
            bucket = "empty_response"
        elif "failed on navigating" in err or "acs-goto" in err:
            bucket = "navigation"
        failure_reasons[bucket] = failure_reasons.get(bucket, 0) + 1

    output_file = output_dir / "sbicard_crawl.json"
    payload = {
        "start_url": START_URL,
        "max_depth": max_depth,
        "max_pages": max_pages,
        "pages": saved_pages,
        "summary": {
            "visited_results": len(results),
            "saved_pages": len(saved_pages),
            "duplicates_removed": duplicates_removed,
            "failed_pages": failure_count,
            "max_observed_depth": max_seen_depth,
            "all_results_internal": not non_internal_urls,
            "non_internal_urls": non_internal_urls,
            "retry_candidates": len(retry_candidates_initial),
            "retry_attempted": retry_attempted,
            "retry_recovered": retry_recovered,
            "retry_failed": retry_failed,
        },
    }
    output_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Crawl complete. Visited results: {len(results)}")
    if duplicates_removed:
        print(f"Removed {duplicates_removed} duplicate URL row(s) after merge by normalized URL.")
    print(f"Saved JSON pages: {len(saved_pages)}")
    print(f"Failed pages: {failure_count}")
    print(f"Max observed depth: {max_seen_depth}")
    print(f"Retry candidates: {len(retry_candidates_initial)}")
    print(f"Retry attempted: {retry_attempted}")
    print(f"Retry recovered: {retry_recovered}")
    print(f"Retry failed: {retry_failed}")
    if non_internal_urls:
        print(
            "Warning: non-internal URLs found in results:",
            ", ".join(non_internal_urls[:10]),
        )
    else:
        print("All crawled URLs are internal to sbicard.com.")
    print(f"Output file: {output_file.resolve()}")
    if skipped_targets:
        top_skips = sorted(skipped_targets.items(), key=lambda kv: kv[1], reverse=True)[:10]
        print("Skipped target breakdown (top 10): " + ", ".join(f"{k}={v}" for k, v in top_skips))
    if failure_reasons:
        top_fail = sorted(failure_reasons.items(), key=lambda kv: kv[1], reverse=True)
        print("Failure reason breakdown: " + ", ".join(f"{k}={v}" for k, v in top_fail))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deep crawl sbicard.com and save clean markdown."
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for markdown output files.",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=2,
        help="Maximum crawl depth (default: 2).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=150,
        help="Safety cap for total pages crawled (default: 150).",
    )
    parser.add_argument(
        "--retry-failed-pages",
        action="store_true",
        default=False,
        help="Retry failed internal URLs after initial crawl.",
    )
    parser.add_argument(
        "--max-retry-rounds",
        type=int,
        default=2,
        help="Maximum retry rounds for failed pages.",
    )
    parser.add_argument(
        "--max-retry-pages",
        type=int,
        default=40,
        help="Maximum failed pages retried per round.",
    )
    return parser.parse_args()


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    args = parse_args()
    asyncio.run(
        crawl_site(
            output_dir=Path(args.output_dir),
            max_depth=args.max_depth,
            max_pages=args.max_pages,
            retry_failed_pages=args.retry_failed_pages,
            max_retry_rounds=args.max_retry_rounds,
            max_retry_pages=args.max_retry_pages,
        )
    )


if __name__ == "__main__":
    main()
