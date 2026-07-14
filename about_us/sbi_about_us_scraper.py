"""
SBI Card — About Us page scraper  v4.0.0
=========================================
URL: https://www.sbicard.com/en/who-we-are/about-us.page

Output (JSON only, crawl-compatible)
--------------------------------------
  about_us/sbi_about_us.json

Root shape matches main crawl export:
  { "pages": [ <single page object> ], "summary": { ... } }

Structured scrape (history, board, management, differentiators) is embedded in
``markdown`` as a single fenced ``json`` block for ingestion / tooling. ``metadata``
holds only standard crawl-style string fields (no duplicate nested sections).

Dependencies
------------
  pip install crawl4ai beautifulsoup4 lxml
  playwright install chromium
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai.async_configs import CacheMode
from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sbi_about")

BASE_URL = "https://www.sbicard.com"
ABOUT_URL = "https://www.sbicard.com/en/who-we-are/about-us.page"
SCRAPER_VERSION = "4.0.0"

SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = SCRIPT_DIR
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _abs(url: str) -> str:
    if not url:
        return ""
    url = url.strip()
    return url if url.startswith("http") else urljoin(BASE_URL, url)


def _clean(text: object) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def extract_modal_bios(soup: BeautifulSoup) -> dict[str, dict]:
    bio_map: dict[str, dict] = {}
    modals = soup.select("div.modal.teamAbout")
    log.info("Found %d teamAbout modals", len(modals))

    for modal in modals:
        h4 = modal.select_one("h4.modal-title")
        if not h4:
            continue

        span = h4.find("span")
        modal_title = _clean(span.get_text()) if span else ""
        if span:
            span.extract()

        name = _clean(h4.get_text())
        if not name:
            continue

        body = modal.select_one("div.modal-body")
        if not body:
            continue

        raw_body = body.get_text(separator="\n")
        paras = [
            _clean(line)
            for line in raw_body.splitlines()
            if _clean(line) and len(_clean(line)) > 10
        ]
        bio_text = " ".join(paras)

        bio_map[name] = {
            "modal_title": modal_title,
            "bio": bio_text,
        }
        log.info(
            "  Modal %-40s | title=%-25s | bio_len=%d",
            name,
            modal_title,
            len(bio_text),
        )

    log.info("extract_modal_bios → %d entries", len(bio_map))
    return bio_map


def _find_name_and_title_after_img(img_tag) -> tuple[str, str]:
    texts: list[str] = []
    for sib in img_tag.next_siblings:
        if hasattr(sib, "name"):
            if sib.name == "img":
                break
            if sib.name == "a":
                continue
            t = _clean(sib.get_text(" "))
        else:
            t = _clean(str(sib))

        if t and t.lower() != "read more":
            texts.append(t)

        if len(texts) >= 2:
            break

    name = texts[0] if len(texts) > 0 else ""
    title = texts[1] if len(texts) > 1 else ""
    return name, title


def _parse_card_grid(
    soup: BeautifulSoup,
    section_h2_re: str,
    bio_map: dict[str, dict],
    include_bio: bool,
) -> list[dict]:
    people: list[dict] = []

    h2 = soup.find("h2", string=re.compile(section_h2_re, re.I))
    if not h2:
        log.warning("Section '%s' not found", section_h2_re)
        return people

    section = h2.find_parent(["section", "div"]) or soup

    NAME_PREFIX = re.compile(r"^(Mr\.|Ms\.|Mrs\.|Smt\.|CA\s|Dr\.)", re.I)
    SKIP_SRC = ("placeholder", "app-", "solutely", "appstore", "playstore")

    seen: set[str] = set()

    for img in section.find_all("img"):
        alt = _clean(img.get("alt") or img.get("title") or "")
        src = _abs(img.get("src") or "")

        if not NAME_PREFIX.match(alt):
            continue
        if any(kw in src.lower() for kw in SKIP_SRC):
            continue

        name_from_alt = re.split(r"\s*[-–—]\s*", alt)[0].strip()

        display_name, title = _find_name_and_title_after_img(img)
        if not display_name:
            display_name = name_from_alt

        display_name = _clean(display_name)
        if not display_name or display_name in seen:
            continue
        seen.add(display_name)

        entry: dict = {
            "name": display_name,
            "title": _clean(title),
            "photo_url": src,
        }

        if include_bio:
            bio_data = bio_map.get(display_name) or bio_map.get(name_from_alt)
            if not bio_data:
                last = display_name.split()[-1].lower()
                for k, v in bio_map.items():
                    if k.split()[-1].lower() == last:
                        bio_data = v
                        break

            entry["modal_title"] = bio_data["modal_title"] if bio_data else ""
            entry["bio"] = bio_data["bio"] if bio_data else ""

        people.append(entry)

    return people


def parse_board_of_directors(soup: BeautifulSoup, bio_map: dict) -> list[dict]:
    result = _parse_card_grid(soup, r"Board of Directors", bio_map, include_bio=True)
    with_bio = sum(1 for d in result if d.get("bio"))
    log.info("Board of Directors: %d members, %d with bio", len(result), with_bio)
    return result


def parse_management_team(soup: BeautifulSoup, bio_map: dict) -> list[dict]:
    result = _parse_card_grid(soup, r"Our Management", bio_map, include_bio=False)
    log.info("Management team: %d members", len(result))
    return result


def parse_history(soup: BeautifulSoup) -> dict:
    result: dict = {"paragraphs": [], "milestones": []}
    h2 = soup.find("h2", string=re.compile(r"Our History", re.I))
    if not h2:
        log.warning("'Our History' not found")
        return result

    container = h2.find_parent(["section", "div", "li"]) or h2

    for p in container.find_all("p"):
        t = _clean(p.get_text(" "))
        if t and len(t) > 10:
            result["paragraphs"].append(t)

    ul = container.find("ul")
    if ul:
        for li in ul.find_all("li"):
            t = _clean(li.get_text(" "))
            if t and len(t) > 5:
                result["milestones"].append(t)

    log.info(
        "History: %d paragraph(s), %d milestone(s)",
        len(result["paragraphs"]),
        len(result["milestones"]),
    )
    return result


def parse_differentiators(soup: BeautifulSoup) -> list[dict]:
    diffs: list[dict] = []
    h2 = soup.find("h2", string=re.compile(r"How We are Different", re.I))
    if not h2:
        log.warning("'How We are Different' not found")
        return diffs

    section = h2.find_parent(["section", "div"]) or soup
    SKIP_PREFIX = re.compile(r"^(Mr\.|Ms\.|Mrs\.|Smt\.|CA\s|Dr\.)", re.I)
    seen: set[str] = set()

    for h4 in section.find_all("h4"):
        title = _clean(h4.get_text(" "))
        if not title or SKIP_PREFIX.match(title) or title in seen:
            continue
        seen.add(title)

        short = ""
        for sib in h4.next_siblings:
            if hasattr(sib, "name") and sib.name == "p":
                short = _clean(sib.get_text(" "))
                break

        long_paras: list[str] = []
        for sib in h4.next_siblings:
            if hasattr(sib, "name"):
                if sib.name == "h4":
                    break
                if sib.name == "p":
                    t = _clean(sib.get_text(" "))
                    if t:
                        long_paras.append(t)

        diffs.append(
            {
                "title": title,
                "short_description": short,
                "long_description": " ".join(long_paras) if len(long_paras) > 1 else "",
            }
        )

    log.info("Differentiators: %d items", len(diffs))
    return diffs


async def fetch_page() -> str | None:
    browser_cfg = BrowserConfig(headless=True, verbose=False)
    run_cfg = CrawlerRunConfig(
        wait_for=None,
        delay_before_return_html=4.0,
        scan_full_page=True,
        scroll_delay=0.5,
        wait_for_images=False,
        cache_mode=CacheMode.BYPASS,
        scraping_strategy=LXMLWebScrapingStrategy(),
        page_timeout=60000,
        verbose=True,
    )
    log.info("Crawling: %s", ABOUT_URL)
    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        result = await crawler.arun(url=ABOUT_URL, config=run_cfg)
    if not result.success:
        log.error("Crawl failed: %s", result.error_message)
        return None
    log.info("HTML received: %d bytes", len(result.html))
    modal_count = result.html.count("teamAbout")
    log.info("teamAbout modal count in HTML: %d", modal_count)
    return result.html


def parse_page(html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")

    for tag in soup(["script", "style", "noscript", "nav", "footer", "header", "iframe", "svg"]):
        tag.decompose()

    bio_map = extract_modal_bios(soup)

    return {
        "history": parse_history(soup),
        "board_of_directors": parse_board_of_directors(soup, bio_map),
        "management_team": parse_management_team(soup, bio_map),
        "differentiators": parse_differentiators(soup),
    }


def build_markdown_with_embedded_payload(
    *,
    scraped_at: str,
    page_data: dict,
    success: bool,
    error: str,
) -> str:
    """Put canonical About Us structure inside markdown as a fenced JSON block."""
    if not success:
        err_blob = json.dumps(
            {"error": error or "unknown", "source_url": ABOUT_URL, "page": "about-us"},
            ensure_ascii=False,
            indent=2,
        )
        return f"# About Us | SBI Card\n\nCrawl failed. Payload:\n\n```json\n{err_blob}\n```\n"

    payload = {
        "source_url": ABOUT_URL,
        "page": "about-us",
        "scraped_at": scraped_at,
        "scraper_version": SCRAPER_VERSION,
        "history": page_data.get("history") or {"paragraphs": [], "milestones": []},
        "board_of_directors": page_data.get("board_of_directors") or [],
        "management_team": page_data.get("management_team") or [],
        "differentiators": page_data.get("differentiators") or [],
    }
    body = json.dumps(payload, ensure_ascii=False, indent=2)
    return (
        "# About Us | SBI Card\n\n"
        "Structured About Us payload (machine-readable JSON):\n\n"
        f"```json\n{body}\n```\n"
    )


def build_crawl_page(
    *,
    scraped_at: str,
    page_data: dict,
    success: bool,
    status_code: int | None,
    error: str,
) -> dict:
    parsed = urlparse(ABOUT_URL)
    path = parsed.path or "/"
    title = "About Us | SBI Card"
    history = page_data.get("history") or {}
    paras = history.get("paragraphs") or []
    description = paras[0] if paras else "SBI Card — About Us (who we are)."
    keywords = "about us, sbi card, board of directors, management, history"

    metadata: dict[str, str] = {
        "source_url": ABOUT_URL,
        "domain": parsed.netloc or "www.sbicard.com",
        "path": path,
        "query": parsed.query or "",
        "title": title,
        "description": description,
        "keywords": keywords,
        "depth": "0",
        "scraper": "sbi_about_us_scraper",
        "scraper_version": SCRAPER_VERSION,
        "scraped_at": scraped_at,
    }

    markdown = build_markdown_with_embedded_payload(
        scraped_at=scraped_at,
        page_data=page_data,
        success=success,
        error=error,
    )

    return {
        "title": title,
        "description": description,
        "keywords": keywords,
        "category": "Personal",
        "sub_category": "who-we-are",
        "source_url": ABOUT_URL,
        "path": path,
        "depth": 0,
        "success": success,
        "status_code": status_code,
        "redirected_url": None,
        "redirected_status_code": None,
        "error": error,
        "request_headers_sent": {},
        "request_cookies_sent": {},
        "request_headers_final": {},
        "request_cookies_final": {},
        "response_headers": {},
        "metadata": metadata,
        "markdown": markdown,
    }


async def run() -> int:
    scraped_at = datetime.now(timezone.utc).isoformat()

    html = await fetch_page()
    if html is None:
        out_path = OUTPUT_DIR / "sbi_about_us.json"
        page = build_crawl_page(
            scraped_at=scraped_at,
            page_data={},
            success=False,
            status_code=None,
            error="Crawl failed",
        )
        doc = {
            "pages": [page],
            "summary": {
                "source": "sbicard.com",
                "page": "about-us",
                "scraped_at": scraped_at,
                "scraper_version": SCRAPER_VERSION,
                "success": False,
            },
        }
        out_path.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
        log.info("Saved (failed crawl) → %s", out_path)
        return 1

    log.info("Parsing HTML sections …")
    page_data = parse_page(html)

    page = build_crawl_page(
        scraped_at=scraped_at,
        page_data=page_data,
        success=True,
        status_code=200,
        error="",
    )

    doc = {
        "pages": [page],
        "summary": {
            "source": "sbicard.com",
            "page": "about-us",
            "scraped_at": scraped_at,
            "scraper_version": SCRAPER_VERSION,
            "success": True,
            "saved_pages": 1,
        },
    }

    out = OUTPUT_DIR / "sbi_about_us.json"
    out.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("Saved → %s", out)

    directors_with_bio = sum(1 for d in page_data["board_of_directors"] if d.get("bio"))
    log.info(
        "Done — history=%d paras | directors=%d (%d with bio) | managers=%d | differentiators=%d",
        len(page_data["history"]["paragraphs"]),
        len(page_data["board_of_directors"]),
        directors_with_bio,
        len(page_data["management_team"]),
        len(page_data["differentiators"]),
    )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(run()))
