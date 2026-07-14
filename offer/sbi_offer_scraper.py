import argparse
import asyncio
import hashlib
import json
import logging
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin
from urllib.parse import urlparse
from typing import Any

from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai.async_configs import CacheMode
from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sbi_scraper")

BASE_URL = "https://www.sbicard.com"
LISTING_URL = "https://www.sbicard.com/en/personal/offers.page"
SCRAPER_VERSION = "4.0.0"

SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_JSON_DIR = SCRIPT_DIR / "offers_json"
OUTPUT_JSON_DIR.mkdir(parents=True, exist_ok=True)

JS_LOAD_ALL_OFFERS = r"""
(async () => {
    const sleep = ms => new Promise(r => setTimeout(r, ms));
    let clicks = 0;
    const maxClicks = 50;
    const deadline  = Date.now() + 120000;

    while (Date.now() < deadline && clicks < maxClicks) {
        const btn = Array.from(
            document.querySelectorAll('button, a, div[role="button"], span')
        ).find(el =>
            /load\s+more/i.test((el.innerText || el.textContent || '').trim())
        );
        if (!btn) break;
        if (btn.disabled || getComputedStyle(btn).display === 'none') break;
        btn.scrollIntoView({ behavior: 'smooth', block: 'center' });
        await sleep(1000);
        btn.click();
        clicks++;
        await sleep(3500);
    }
    return { clicks };
})();
"""


def normalize_offer_url(raw: str) -> str:
    if not raw:
        return raw

    url = raw.strip()
    if url.startswith("/"):
        url = BASE_URL + url

    if "offer-detail.page" in url and "offer-id=" in url:
        offer_id = url.split("offer-id=")[-1].split("&")[0].strip()
        offer_id = re.sub(r"(\.page)+$", "", offer_id)
        return f"{BASE_URL}/en/personal/offer/{offer_id}.page"

    url = re.sub(r"(\.page){2,}$", ".page", url)
    return url


def _abs(url: str) -> str:
    if not url:
        return ""
    if url.startswith("http"):
        return url
    return urljoin(BASE_URL, url)


def make_offer_id(url: str) -> str:
    last = url.rstrip("/").split("/")[-1]
    return re.sub(r"(\.page)+$", "", last) or hashlib.md5(url.encode()).hexdigest()[:12]


def make_document_id(offer_id: str, scraped_at: str) -> str:
    return hashlib.sha256(f"{offer_id}::{scraped_at}".encode()).hexdigest()[:24]


def make_asset_id(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:16]


MONTH_MAP = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}

_P_FULL = re.compile(
    r"(\d{1,2})(?:st|nd|rd|th)?\s+"
    r"(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
    r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
    r"\s+(\d{4})",
    re.IGNORECASE,
)
_P_COMPACT = re.compile(
    r"(\d{1,2})\s*[-/']?\s*"
    r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
    r"['\-\s]?(\d{2,4})",
    re.IGNORECASE,
)
_SEP_RE = re.compile(r"\s*(?:–|—|-|to)\s*", re.IGNORECASE)


def _parse_one_date(text: str) -> datetime | None:
    for pat in (_P_FULL, _P_COMPACT):
        m = pat.search(text)
        if m:
            day, mon_str, year = m.group(1), m.group(2), m.group(3)
            mon = MONTH_MAP.get(mon_str.lower()[:3])
            if mon:
                yr = int(year)
                if yr < 100:
                    yr += 2000
                try:
                    return datetime(yr, mon, int(day))
                except ValueError:
                    pass
    return None


def parse_date_range(text: str) -> tuple[Any, Any, Any]:
    if not text:
        return None, None, None
    text = text.strip()
    parts = _SEP_RE.split(text, maxsplit=1)
    if len(parts) == 2:
        s = _parse_one_date(parts[0])
        e = _parse_one_date(parts[1])
        if s and e:
            return text, s.strftime("%Y-%m-%d"), e.strftime("%Y-%m-%d")
    return None, None, None


def parse_listing_page(html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    cards: list[dict[str, Any]] = []
    seen: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        if "/offer/" not in href and "offer-id=" not in href:
            continue
        full_url = normalize_offer_url(urljoin(BASE_URL, href))
        if re.search(r"/offers\.page$", full_url):
            continue
        if full_url in seen:
            continue
        seen.add(full_url)
        cards.append(_card_from_anchor(anchor, full_url))

    for el in soup.find_all(attrs={"data-url": True}):
        href = el["data-url"]
        if "/offer/" not in href:
            continue
        full_url = normalize_offer_url(urljoin(BASE_URL, href))
        if full_url in seen:
            continue
        seen.add(full_url)
        cards.append(_card_from_anchor(el, full_url))

    log.info("Listing parser: %d unique offer URLs found", len(cards))
    return cards


def _card_from_anchor(el, full_url: str) -> dict[str, Any]:
    container = el
    for _ in range(6):
        p = container.parent
        if p and p.name in ("div", "li", "article", "section"):
            container = p
        else:
            break

    brand_name = _sel_text(container, [".brand-name", ".brand", "[data-brand]", "figcaption"])
    if not brand_name:
        brand_name = _img_alt(container)
    hover_text = (container.get("title") or container.get("data-title") or "").strip()
    category = (container.get("data-category") or "").strip()
    sub_cat = (container.get("data-sub-category") or container.get("data-subcategory") or "").strip()
    discount_txt = _sel_text(container, [".discount", ".offer-tag", ".badge", ".tag"])
    img_src = _sel_attr(container, "img", "src")

    if not brand_name:
        slug = make_offer_id(full_url)
        brand_name = slug.split("-")[0].title() if slug else ""

    return {
        "offer_url": full_url,
        "brand_name": brand_name,
        "brand_logo_url": _abs(img_src),
        "card_image_url": _abs(img_src),
        "hover_text": hover_text or None,
        "primary_category": category,
        "sub_category": sub_cat,
        "discount_text": discount_txt or None,
    }


def _sel_text(el, selectors: list[str]) -> str:
    for sel in selectors:
        tag = el.select_one(sel)
        if tag:
            t = tag.get_text(" ", strip=True)
            if t:
                return t
    return ""


def _sel_attr(el, sel: str, attr: str) -> str:
    tag = el.select_one(sel)
    return (tag.get(attr) or "") if tag else ""


def _img_alt(el) -> str:
    img = el.find("img", alt=True)
    return (img["alt"] or "").strip() if img else ""


_NOISE_EXACT = {
    "login",
    "apply now",
    "contact us",
    "faq",
    "personal",
    "corporate",
    "about sbi card",
    "credit cards",
    "card payment",
    "redeem rewards",
    "benefits",
    "deals & offers",
    "help me find a card",
    "compare cards",
    "track my application",
    "secure your card",
    "tokenisation",
    "link pan with aadhaar",
    "link pan with adhaar",
    "terms & conditions",
    "order of payment settlement",
    "customer notices",
    "posh policy",
    "forms central",
    "offers terms & conditions",
    "cardholder agreement",
    "customer grievance redressal policy",
    "fair practice code",
    "procurement news",
    "odr portal link & circular for shareholders",
    "credit card inssuarance & conduct policy",
    "balance transfer",
    "flexipay",
    "spend analyzer",
    "statement",
    "bill pay & recharge",
    "check reward points",
    "earn reward points",
    "redeem reward points",
    "view catalogue",
    "address change kyc",
    "download forms",
    "raise a dispute",
    "request for card closure",
    "customer care",
    "report lost card",
    "digital platform updates",
    "digital membership kits",
    "go digital",
    "card security tips",
    "paynet - pay online",
    "book flexipay",
    "top & recharge",
    "paynet- online payment",
    "standing instruction",
    "electronic bill payment",
    "mastercard moneysend",
    "mobile app",
    "upi & qr codes",
    "via yono",
    "debit card",
    "visa credit card pay",
    "over the counter",
    "cheque - manual drop box",
    "sbi atm",
    "view all payment modes",
    "all offers",
    "offers this week",
    "convert to emi",
    "visa offers on sbi card",
    "mobile icon",
    "back to offers list",
    "offer details",
    "terms and conditions",
    "avail now",
    "download mobile app",
    "all",
    "dept stores & grocery",
    "dining",
    "education",
    "electronics & mobiles",
    "entertainment",
    "fashion & lifestyle",
    "health & fitness",
    "hill station offers",
    "jewellery",
    "mall offers",
    "my city offers",
    "network offers",
    "online marketplace",
    "travel & lodging",
    "utilities and services",
    "hello!",
    "interactive assistant",
    "confirm exit",
    "chat history",
    "app store",
    "play store",
    "get it on google play",
    "download on the app store",
    "send link to your phone",
    "know more",
    "explore",
    "services",
    "important links",
    "home",
    "offers",
    "offer-detail",
}

_NOISE_SUBSTR = [
    "© 20",
    "sbi-card-en/resources",
    "sbi card en/resources",
    "click here for merchant emi",
    "app solutely simple",
    "appstore",
    "playstore",
    "google play",
    "send link to your phone",
    "download mobile app",
    "all dept stores",
    "dept stores & grocery dining",
]


def _is_noise(text: str) -> bool:
    if not text:
        return True
    t = text.strip()
    if len(t) < 3:
        return True
    tl = t.lower()
    if tl in _NOISE_EXACT:
        return True
    return any(n in tl for n in _NOISE_SUBSTR)


_RE_VALIDITY = re.compile(r"offer\s*validity", re.I)
_RE_ELIGIBLE = re.compile(r"eligible\s*card", re.I)
_RE_SUMMARY = re.compile(r"^summary$", re.I)
_RE_STEPS = re.compile(r"steps?\s+to\s+avail", re.I)
_RE_TERMS = re.compile(r"terms?\s*(and|&)\s*condition", re.I)
_RE_CATEGORY_NAV = re.compile(
    r"all\s+dept\s+stores|dining\s+education\s+electronics|"
    r"entertainment\s+fashion|health\s+(and|&)\s+fitness",
    re.I,
)


def parse_offer_detail(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript", "iframe", "svg", "nav", "footer", "header"]):
        tag.decompose()

    offer_title = _extract_title(soup)
    offer_description = _extract_meta_desc(soup)

    elements = soup.find_all(
        ["h1", "h2", "h3", "h4", "strong", "b", "p", "li", "td", "th", "span", "div"],
        limit=2000,
    )

    section: str | None = None
    validity_raw = None
    start_iso = None
    end_iso = None
    eligible: list[str] = []
    summary_text: list[str] = []
    summary_table: list[dict[str, Any]] = []
    steps: list[str] = []
    terms: list[str] = []
    promo_code = None
    discount_text = None

    for tag in elements:
        if tag.name == "div" and len(list(tag.children)) > 3:
            continue

        text = tag.get_text(" ", strip=True)
        if not text or len(text) < 2:
            continue

        lower = text.lower()

        if _RE_VALIDITY.search(lower):
            section = "validity"
            inline = _RE_VALIDITY.sub("", text, count=1).strip(" :-")
            if inline:
                raw, s, e = parse_date_range(inline)
                if raw and not validity_raw:
                    validity_raw, start_iso, end_iso = raw, s, e
            continue

        if _RE_ELIGIBLE.search(lower):
            section = "eligible"
            continue

        if _RE_SUMMARY.match(lower):
            section = "summary"
            tbl = tag.find_next("table")
            if tbl:
                summary_table = _parse_table(tbl)
            continue

        if _RE_STEPS.search(lower):
            section = "steps"
            continue

        if _RE_TERMS.search(lower):
            section = "terms"
            continue

        if _is_noise(text):
            continue

        if _RE_CATEGORY_NAV.search(text):
            continue

        if not promo_code:
            m = re.search(r"(?:promo|coupon|use)\s*code[:\s]+([A-Z0-9_\-]{4,20})", text, re.I)
            if m:
                promo_code = m.group(1).strip()

        if section == "validity" and not validity_raw:
            raw, s, e = parse_date_range(text)
            if raw:
                validity_raw, start_iso, end_iso = raw, s, e
        elif section == "eligible":
            if len(text) > 4 and text not in eligible:
                eligible.append(text)
        elif section == "summary":
            if len(text) > 4 and text not in summary_text:
                summary_text.append(text)
        elif section == "steps":
            if len(text) > 4 and text not in steps:
                steps.append(text)
        elif section == "terms":
            if len(text) > 4 and text not in terms:
                terms.append(text)

        if not discount_text and section is None:
            if re.search(
                r"\b(off|cashback|discount|free|reward|bonus|upto|up\s+to|emi|avail|convert)\b",
                lower,
            ) and 5 < len(text) < 150:
                discount_text = text

    if not summary_table:
        for tbl in soup.find_all("table"):
            rows = _parse_table(tbl)
            if rows:
                summary_table = rows
                break

    brand_logo = None
    for sel in ["[class*='brand'] img", "[class*='partner'] img", "img[alt*='logo']", "img[class*='brand']"]:
        img = soup.select_one(sel)
        if img and img.get("src"):
            brand_logo = _abs(img["src"])
            break

    return {
        "offer_title": offer_title,
        "offer_description": offer_description,
        "offer_validity_raw": validity_raw,
        "offer_start_date": start_iso,
        "offer_end_date": end_iso,
        "eligible_cards": eligible,
        "summary_text": summary_text,
        "summary_table": summary_table,
        "steps_to_avail": steps,
        "terms_and_conditions": terms,
        "promo_code": promo_code,
        "discount_text": discount_text,
        "brand_logo_url": brand_logo,
        "offer_type": _infer_type(summary_text, summary_table, steps),
    }


def _extract_title(soup: BeautifulSoup) -> str:
    for h1 in soup.find_all("h1"):
        t = h1.get_text(" ", strip=True)
        if t and "sbi credit card offers" not in t.lower() and len(t) > 3:
            return t
    for h2 in soup.find_all("h2"):
        t = h2.get_text(" ", strip=True)
        if t and "sbi credit card offers" not in t.lower() and len(t) > 5:
            if _RE_ELIGIBLE.search(t) or _RE_SUMMARY.match(t) or _RE_STEPS.search(t):
                continue
            return t
    if soup.title:
        raw = soup.title.get_text(strip=True)
        for seg in raw.split("|"):
            seg = seg.strip()
            if "sbi credit card offers" not in seg.lower() and len(seg) > 5:
                return seg
        return raw
    return "SBI Credit Card Offer"


def _extract_meta_desc(soup: BeautifulSoup) -> str | None:
    meta = soup.find("meta", {"name": "description"})
    if not meta:
        return None
    raw = (meta.get("content") or "").strip()
    if not raw:
        return None
    half = raw[: len(raw) // 2].rstrip(". ")
    if raw.lower().startswith(half.lower()):
        return half
    return raw


def _parse_table(tbl) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    header: list[str] = []
    for tr in tbl.find_all("tr"):
        cells = [td.get_text(" ", strip=True) for td in tr.find_all(["th", "td"])]
        if not cells:
            continue
        if not header:
            header = cells
        else:
            if len(cells) == len(header):
                rows.append(dict(zip(header, cells)))
            elif cells:
                rows.append({f"col_{i}": v for i, v in enumerate(cells)})
    return rows


def _infer_type(summary: list[str], table: list[dict[str, Any]], steps: list[str]) -> str:
    sig = " ".join(summary + [str(r) for r in table] + steps).lower()
    if "no cost emi" in sig:
        return "no_cost_emi"
    if "emi" in sig:
        return "emi"
    if "cashback" in sig:
        return "cashback"
    if "discount" in sig or " off" in sig:
        return "discount"
    if "reward" in sig:
        return "reward_points"
    if "voucher" in sig or "gift card" in sig:
        return "voucher"
    if "free" in sig:
        return "free_benefit"
    if "travel" in sig or "airport" in sig:
        return "travel"
    if "dining" in sig or "restaurant" in sig:
        return "dining"
    return "general"


def build_es_document(card_meta: dict[str, Any], detail: dict[str, Any], scraped_at: str) -> dict[str, Any]:
    url = card_meta["offer_url"]
    offer_id = make_offer_id(url)
    doc_id = make_document_id(offer_id, scraped_at)
    asset_id = make_asset_id(url)

    brand = card_meta.get("brand_name") or ""

    offer_text = " | ".join(
        filter(
            None,
            [
                detail.get("offer_title", ""),
                detail.get("offer_description", "") or "",
                " ".join(detail.get("eligible_cards", [])),
                " ".join(detail.get("summary_text", [])),
                " ".join(str(r) for r in detail.get("summary_table", [])),
                " ".join(detail.get("steps_to_avail", [])),
                detail.get("discount_text", "") or "",
                card_meta.get("hover_text", "") or "",
            ],
        )
    )

    return {
        "_id": doc_id,
        "offer_id": offer_id,
        "document_id": doc_id,
        "asset_id": asset_id,
        "source": "sbicard.com",
        "source_url": url,
        "scraped_at": scraped_at,
        "scraper_version": SCRAPER_VERSION,
        "index_name": "sbi_offers",
        "offer_title": detail.get("offer_title") or card_meta.get("hover_text") or "",
        "offer_text": offer_text,
        "offer_description": detail.get("offer_description"),
        "discount_text": detail.get("discount_text") or card_meta.get("discount_text") or None,
        "hover_text": card_meta.get("hover_text") or None,
        "promo_code": detail.get("promo_code"),
        "offer_type": detail.get("offer_type", "general"),
        "offer_validity_raw": detail.get("offer_validity_raw"),
        "offer_start_date": detail.get("offer_start_date"),
        "offer_end_date": detail.get("offer_end_date"),
        "eligible_cards": detail.get("eligible_cards", []),
        "summary_text": detail.get("summary_text", []),
        "summary_table": detail.get("summary_table", []),
        "steps_to_avail": detail.get("steps_to_avail", []),
        "terms_and_conditions": detail.get("terms_and_conditions", []),
        "brand_name": brand,
        "brand_logo_url": detail.get("brand_logo_url") or card_meta.get("brand_logo_url") or None,
        "card_image_url": card_meta.get("card_image_url") or None,
        "primary_category": card_meta.get("primary_category") or None,
        "sub_category": card_meta.get("sub_category") or None,
        "tags": _build_tags(card_meta, detail),
        "chunk_eligible_cards": _chunk("eligible_cards", detail.get("eligible_cards", [])),
        "chunk_summary": _chunk("summary", detail.get("summary_text", [])),
        "chunk_steps": _chunk("steps_to_avail", detail.get("steps_to_avail", [])),
        "chunk_terms": _chunk("terms", detail.get("terms_and_conditions", [])),
    }


def _build_tags(card_meta: dict[str, Any], detail: dict[str, Any]) -> list[str]:
    tags = set()
    if card_meta.get("primary_category"):
        tags.add(str(card_meta["primary_category"]))
    if card_meta.get("sub_category"):
        tags.add(str(card_meta["sub_category"]))
    if detail.get("offer_type"):
        tags.add(str(detail["offer_type"]))
    if detail.get("promo_code"):
        tags.add("has_promo_code")
    if detail.get("summary_table"):
        tags.add("has_table")
    if detail.get("offer_start_date"):
        tags.add("has_validity")
    if detail.get("eligible_cards"):
        tags.add("has_eligible_cards")
    return sorted(t for t in tags if t)


def _chunk(section: str, items: list[Any]) -> str:
    if not items:
        return ""
    return f"[{section}] " + " | ".join(str(i) for i in items)


async def crawl_listing(crawler: AsyncWebCrawler, max_offers: int | None, use_js: bool = True) -> list[dict[str, Any]]:
    log.info("Stage 1: Listing page (js=%s) → %s", use_js, LISTING_URL)
    config = CrawlerRunConfig(
        js_code=JS_LOAD_ALL_OFFERS if use_js else None,
        wait_for=None,
        scan_full_page=True,
        scroll_delay=0.6,
        wait_for_images=False,
        cache_mode=CacheMode.BYPASS,
        scraping_strategy=LXMLWebScrapingStrategy(),
        page_timeout=130000,
        verbose=True,
    )
    result = await crawler.arun(url=LISTING_URL, config=config)
    if not result.success:
        log.error("Listing crawl failed: %s", result.error_message)
        return []
    cards = parse_listing_page(result.html)
    if max_offers:
        cards = cards[:max_offers]
        log.info("Capped to %d offers (--max-offers)", max_offers)
    return cards


async def crawl_detail(
    crawler: AsyncWebCrawler,
    card_meta: dict[str, Any],
    semaphore: asyncio.Semaphore,
    scraped_at: str,
) -> dict[str, Any] | None:
    url = card_meta["offer_url"]
    async with semaphore:
        config = CrawlerRunConfig(
            session_id=f"offer_{uuid.uuid4().hex[:8]}",
            wait_for=None,
            delay_before_return_html=3.0,
            scan_full_page=False,
            cache_mode=CacheMode.BYPASS,
            scraping_strategy=LXMLWebScrapingStrategy(),
            page_timeout=35000,
            verbose=False,
        )
        try:
            result = await crawler.arun(url=url, config=config)
        except Exception as exc:
            log.warning("Exception [%s]: %s", url, exc)
            return None

        if not result.success:
            log.warning("Failed [%s]: %s", url, result.error_message)
            return None

        html = result.html

    detail = parse_offer_detail(html)
    offer_id = make_offer_id(url)
    doc = build_es_document(card_meta, detail, scraped_at)

    validity_str = (
        f"{detail.get('offer_start_date')} → {detail.get('offer_end_date')}"
        if detail.get("offer_start_date")
        else "no validity"
    )
    log.info(
        "OK [%-40s] eligible=%d summary=%d steps=%d validity=%s",
        offer_id,
        len(detail.get("eligible_cards", [])),
        len(detail.get("summary_text", [])) + len(detail.get("summary_table", [])),
        len(detail.get("steps_to_avail", [])),
        validity_str,
    )
    return doc


async def run(args: argparse.Namespace) -> int:
    scraped_at = datetime.now(timezone.utc).isoformat()
    browser_cfg = BrowserConfig(headless=True, verbose=False)

    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        cards = await crawl_listing(crawler, args.max_offers, use_js=True)
        if not cards:
            log.warning("JS listing returned 0 cards — retrying without JS")
            cards = await crawl_listing(crawler, args.max_offers, use_js=False)
        if not cards:
            log.error("No offer URLs found. Exiting.")
            return 1

        log.info("Stage 1 complete → %d offers queued", len(cards))
        log.info("Stage 2: Detail pages [concurrency=%d, delay=3s/page]", args.concurrency)

        sem = asyncio.Semaphore(args.concurrency)
        tasks = [crawl_detail(crawler, c, sem, scraped_at) for c in cards]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_docs = [r for r in results if isinstance(r, dict)]

    master = OUTPUT_JSON_DIR / "sbi_offers_all.json"
    master.write_text(json.dumps(all_docs, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("Done. Master JSON: %s (docs=%d)", master, len(all_docs))
    return 0


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="SBI Card offer scraper v4.0 — master JSON only",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--max-offers", type=int, default=None, help="Cap total offers (smoke test)")
    ap.add_argument("--concurrency", type=int, default=3, help="Parallel detail crawls (keep ≤5)")
    return ap.parse_args()


if __name__ == "__main__":
    sys.exit(asyncio.run(run(parse_args())))

