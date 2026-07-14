import argparse
import hashlib
import json
import logging
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sbi_rewards")

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
_SCRIPT_DIR = Path(__file__).parent
BASE_URL = "https://www.sbicard.com"
LISTING_URL = "https://www.sbicard.com/en/personal/rewards.page"
SITEMAP_URL = "https://www.sbicard.com/sitemap.xml"
SCRAPER_VERSION = "2.0.0"

OUTPUT_JSON_DIR = _SCRIPT_DIR / "rewards_json"
OUTPUT_JSON_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-IN,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": LISTING_URL,
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)
SESSION.max_redirects = 500

_PRODUCT_URL_RE = re.compile(r"https://www\.sbicard\.com/en/personal/rewards/[^/?#\s]+")


def _get(url: str, retries: int = 3, backoff: float = 2.0) -> str:
    """GET with retry + exponential back-off. Returns HTML string or ''."""
    for attempt in range(1, retries + 1):
        try:
            resp = SESSION.get(url, timeout=25)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as exc:
            log.warning("attempt %d/%d failed [%s]: %s", attempt, retries, url, exc)
            if attempt < retries:
                time.sleep(backoff * attempt + random.uniform(0, 1))
    log.error("giving up on: %s", url)
    return ""


def abs_url(path: str) -> str:
    if not path:
        return ""
    if path.startswith("http"):
        return path
    return urljoin(BASE_URL, path)


def make_item_id(url: str) -> str:
    slug = url.rstrip("/").split("/")[-1]
    slug = re.sub(r"\.page$", "", slug)
    return slug or hashlib.md5(url.encode()).hexdigest()[:12]


def make_document_id(item_id: str, scraped_at: str) -> str:
    return hashlib.sha256(f"{item_id}::{scraped_at}".encode()).hexdigest()[:24]


def parse_listing_from_embedded_json(html: str) -> list[dict]:
    """
    The rewards listing page embeds a JSON blob:
      window.rewards = {"reward":[ ... ]}
    This contains the full catalogue (often hundreds of items).
    """
    m = re.search(r"window\.rewards\s*=\s*(\{.*?\});\s*", html, flags=re.S)
    if not m:
        return []

    try:
        data = json.loads(m.group(1))
    except Exception:
        return []

    rewards = data.get("reward")
    if not isinstance(rewards, list) or not rewards:
        return []

    items: list[dict] = []
    seen: set[str] = set()

    for r in rewards:
        if not isinstance(r, dict):
            continue

        reward_id = (r.get("rewardId") or "").strip()
        if not reward_id:
            continue

        slug = reward_id.strip().strip("/").lower()
        det_url = f"{BASE_URL}/en/personal/rewards/{slug}"
        if det_url in seen:
            continue
        seen.add(det_url)

        points_only = None
        item_list = r.get("item")
        if isinstance(item_list, list) and item_list:
            first = item_list[0] if isinstance(item_list[0], dict) else {}
            pt = first.get("point")
            if isinstance(pt, str):
                mt = re.search(r"\d+", pt.replace(",", ""))
                if mt:
                    points_only = int(mt.group())
            elif isinstance(pt, (int, float)):
                points_only = int(pt)

        thumb = abs_url((r.get("thumbImage") or "").strip())

        items.append(
            {
                "item_id": make_item_id(det_url),
                "detail_url": det_url,
                "product_name": (r.get("itemName") or "").strip() or reward_id,
                "points_only": points_only,
                "category": (r.get("category") or "").strip(),
                "brand_name": (r.get("brand") or "").strip(),
                "card_types": [],
                "thumbnail_url": thumb,
            }
        )

    return items


def _parse_sitemap_xml_for_locs(xml_text: str) -> tuple[list[str], list[str]]:
    locs = re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", xml_text, flags=re.I)
    child_sitemaps = [u for u in locs if u.lower().endswith(".xml")]
    return locs, child_sitemaps


def fetch_listing_via_sitemap(max_sitemaps: int = 50) -> list[dict]:
    to_visit = [SITEMAP_URL]
    visited: set[str] = set()
    reward_urls: set[str] = set()

    while to_visit and len(visited) < max_sitemaps:
        sm = to_visit.pop(0)
        if sm in visited:
            continue
        visited.add(sm)

        xml_text = _get(sm, retries=2, backoff=1.5)
        if not xml_text:
            continue

        locs, child_sitemaps = _parse_sitemap_xml_for_locs(xml_text)

        for child in child_sitemaps:
            if child.startswith("http") and child not in visited:
                to_visit.append(child)

        for u in locs:
            if "/en/personal/rewards/" in u and not u.endswith("/en/personal/rewards.page"):
                if _PRODUCT_URL_RE.match(u):
                    reward_urls.add(u)

    if not reward_urls:
        log.info("Sitemap listing: no reward URLs found (visited %d sitemaps)", len(visited))
        return []

    items: list[dict] = []
    for u in sorted(reward_urls):
        items.append(
            {
                "item_id": make_item_id(u),
                "detail_url": u,
                "product_name": make_item_id(u).replace("-", " ").title(),
                "points_only": None,
                "category": "",
                "brand_name": "",
                "card_types": [],
                "thumbnail_url": "",
            }
        )

    log.info("Sitemap listing complete: %d unique reward items found", len(items))
    return items


def get_listing_items() -> tuple[list[dict], dict]:
    html = _get(LISTING_URL)
    if html:
        products = parse_listing_from_embedded_json(html)
        if products:
            log.info("Embedded JSON catalogue: %d items found", len(products))
            return products, {"source": "embedded_json", "listing_url": LISTING_URL}

    products = fetch_listing_via_sitemap()
    if products:
        return products, {"source": "sitemap", "sitemap_url": SITEMAP_URL}

    return [], {}


def _first_word_brand(name: str) -> str:
    words = name.split()
    if not words:
        return ""
    if words[0].lower() in {"a", "the", "my", "new", "sbi", "buy", "get"}:
        return " ".join(words[:2]) if len(words) >= 2 else words[0]
    return words[0]


def parse_detail_page(html: str, url: str) -> dict:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript", "nav", "footer", "header"]):
        tag.decompose()

    product_name = ""
    h1 = soup.find("h1", class_="product-title")
    if h1:
        product_name = h1.get_text(" ", strip=True)
    if not product_name and soup.title:
        segs = [s.strip() for s in soup.title.get_text().split("|")]
        product_name = segs[0] if segs else ""

    points_only = None
    price_h2 = soup.find("h2", class_="new-price")
    if price_h2:
        m = re.search(r"(\d[\d,]+)", price_h2.get_text())
        if m:
            points_only = int(m.group(1).replace(",", ""))

    points_pay_points = None
    points_pay_cash = None
    ppay = soup.find("div", id=re.compile(r"^pointsnpay"))
    if ppay:
        txt = ppay.get_text(" ", strip=True)
        m = re.search(r"Pay[:\s]+([\d,]+)\s*\+\s*(?:Rs\.?|₹)\s*([\d,]+)", txt, re.I)
        if m:
            points_pay_points = int(m.group(1).replace(",", ""))
            points_pay_cash = int(m.group(2).replace(",", ""))

    item_code_points = None
    item_code_pointspay = None
    for inp in soup.find_all("input", {"data-itemcode": True}):
        code = inp.get("data-itemcode", "").strip()
        paytype = inp.get("data-paytype", "").lower().replace(" ", "")
        if "points+pay" in paytype:
            item_code_pointspay = code
        elif code:
            item_code_points = code

    if not item_code_points:
        ic = soup.find("span", id="item-code")
        if ic:
            item_code_points = ic.get_text(strip=True)

    features: list[str] = []
    feat_div = soup.find("div", class_="brief-features")
    if feat_div:
        for li in feat_div.find_all("li"):
            t = li.get_text(" ", strip=True)
            if t:
                features.append(t)

    delivery_days = None
    del_h3 = soup.find("h3", id="estimated-delivery")
    if del_h3:
        m = re.search(r"\d+", del_h3.get_text())
        if m:
            delivery_days = int(m.group())

    available_cities: list[str] = []
    tooltip = soup.find("span", class_="rwd-tooltip")
    if tooltip:
        raw = re.sub(
            r"^only\s+available\s+in\s*",
            "",
            tooltip.get_text(" ", strip=True),
            flags=re.I,
        )
        available_cities = [c.strip() for c in raw.split(",") if c.strip()]

    description = ""
    desc_div = soup.find("div", class_="product-description")
    if desc_div:
        text_div = desc_div.find("div", class_="text")
        description = (text_div or desc_div).get_text(" ", strip=True)

    terms: list[str] = []
    tc_div = soup.find("div", class_="product-terms-and-conditions")
    if tc_div:
        for li in tc_div.find_all("li"):
            t = li.get_text(" ", strip=True)
            if t:
                terms.append(t)

    product_image_url = ""
    main_img = soup.find("div", class_="main-image")
    if main_img:
        img = main_img.find("img")
        if img:
            product_image_url = abs_url(img.get("src") or "")

    if not product_image_url:
        carousel = soup.find("div", class_="rwd-product-detail-images-carousel")
        if carousel:
            img = carousel.find("img")
            if img:
                product_image_url = abs_url(img.get("src") or "")

    return {
        "product_name": product_name,
        "points_only": points_only,
        "points_pay_points": points_pay_points,
        "points_pay_cash": points_pay_cash,
        "item_code_points": item_code_points,
        "item_code_pointspay": item_code_pointspay,
        "features": features,
        "estimated_delivery_days": delivery_days,
        "available_cities": available_cities,
        "available_cities_count": len(available_cities),
        "product_description": description,
        "terms_and_conditions": terms,
        "product_image_url": product_image_url,
    }


def _infer_category(detail: dict) -> str:
    sig = (detail.get("product_description", "") + " " + " ".join(detail.get("features", []))).lower()
    mapping = [
        ("Electronics", ["headphone", "speaker", "earphone", "camera", "laptop", "phone", "mobile", "tv", "tablet"]),
        ("Travel & Holiday", ["hotel", "flight", "holiday", "travel", "airport", "cleartrip", "makemytrip", "yatra"]),
        ("e-Voucher", ["voucher", "e-voucher", "gift card", "egift", "digital code"]),
        ("Health & Fitness", ["health", "fitness", "gym", "pharma", "wellness", "spa", "yoga", "protein"]),
        ("Entertainment & Dining", ["movie", "dining", "restaurant", "ott", "streaming", "pvr", "bookmyshow", "zomato", "swiggy"]),
        ("Lifestyle", ["lifestyle", "watch", "sunglasses", "perfume", "grooming", "cologne"]),
        ("Apparel & Superstore", ["apparel", "clothing", "shoes", "fashion", "sneaker", "denim", "shirt", "saree"]),
        ("Accessories", ["bag", "luggage", "wallet", "handbag", "backpack", "suitcase", "belt"]),
        ("Homeware", ["home", "kitchen", "cookware", "appliance", "furniture", "bedding", "mattress"]),
        ("Kids Zone", ["kids", "toy", "children", "baby", "game", "puzzle"]),
        ("Luxury", ["luxury", "premium", "designer", "gold", "diamond", "platinum"]),
        ("Memberships", ["membership", "subscription", "club", "prime", "pass", "annual"]),
        ("Books & Periodicals", ["book", "novel", "magazine", "periodical", "kindle", "ebook"]),
    ]
    for cat, kws in mapping:
        if any(k in sig for k in kws):
            return cat
    return "General"


def _infer_redemption_type(name: str, category: str, desc: str) -> str:
    sig = (name + " " + category + " " + desc).lower()
    if any(k in sig for k in ["voucher", "e-voucher", "gift card", "egift", "digital code"]):
        return "e_voucher"
    if any(k in sig for k in ["membership", "subscription", "club", "prime"]):
        return "membership"
    if any(k in sig for k in ["airmile", "air mile", "air india", "indigo", "vistara"]):
        return "airmile"
    if any(k in sig for k in ["hotel", "holiday", "flight", "travel"]):
        return "travel"
    return "physical_product"


def _build_tags(listing: dict, detail: dict, redemption: str) -> list[str]:
    tags = set()
    if listing.get("category"):
        tags.add(listing["category"].lower().replace(" ", "_").replace("&", "and").replace("/", "_"))
    if redemption:
        tags.add(redemption)
    if detail.get("points_pay_points"):
        tags.add("has_points_pay_option")
    if detail.get("features"):
        tags.add("has_features")
    if detail.get("product_description"):
        tags.add("has_description")
    if detail.get("available_cities_count", 0) > 0:
        tags.add("city_restricted")
    else:
        tags.add("pan_india")
    if listing.get("card_types"):
        tags.add("card_restricted")
    return sorted(t for t in tags if t)


def _chunk(section: str, items: list) -> str:
    items = [str(i) for i in items if i]
    return (f"[{section}] " + " | ".join(items)) if items else ""


def build_es_document(listing: dict, detail: dict, scraped_at: str) -> dict:
    item_id = listing["item_id"]
    doc_id = make_document_id(item_id, scraped_at)
    url = listing["detail_url"]
    product_name = detail.get("product_name") or listing.get("product_name") or ""
    points_only = detail.get("points_only") or listing.get("points_only")
    category = listing.get("category") or _infer_category(detail)
    brand = listing.get("brand_name") or _first_word_brand(product_name)
    product_img = (detail.get("product_image_url") or listing.get("thumbnail_url") or "")
    redemption = _infer_redemption_type(product_name, category, detail.get("product_description", ""))

    reward_text = " | ".join(
        filter(
            None,
            [
                product_name,
                detail.get("product_description", ""),
                " ".join(detail.get("features", [])),
                category,
                brand,
            ],
        )
    )

    return {
        "_id": doc_id,
        "item_id": item_id,
        "document_id": doc_id,
        "source": "sbicard.com",
        "source_url": url,
        "scraped_at": scraped_at,
        "scraper_version": SCRAPER_VERSION,
        "index_name": "sbi_rewards",
        "product_name": product_name,
        "reward_text": reward_text,
        "brand_name": brand,
        "category": category,
        "redemption_type": redemption,
        "points_only": points_only,
        "points_pay_points": detail.get("points_pay_points"),
        "points_pay_cash": detail.get("points_pay_cash"),
        "has_points_pay": detail.get("points_pay_points") is not None,
        "item_code_points": detail.get("item_code_points"),
        "item_code_pointspay": detail.get("item_code_pointspay"),
        "product_description": detail.get("product_description") or None,
        "features": detail.get("features", []),
        "features_count": len(detail.get("features", [])),
        "estimated_delivery_days": detail.get("estimated_delivery_days"),
        "available_cities": detail.get("available_cities", []),
        "available_cities_count": detail.get("available_cities_count", 0),
        "is_pan_india": detail.get("available_cities_count", 0) == 0,
        "eligible_card_types": listing.get("card_types", []),
        "eligible_card_types_text": " ".join(listing.get("card_types", [])),
        "product_image_url": product_img,
        "thumbnail_url": listing.get("thumbnail_url") or "",
        "terms_and_conditions": detail.get("terms_and_conditions", []),
        "terms_count": len(detail.get("terms_and_conditions", [])),
        "tags": _build_tags(listing, detail, redemption),
        "chunk_description": _chunk("description", [detail.get("product_description", "")]),
        "chunk_features": _chunk("features", detail.get("features", [])),
        "chunk_terms": _chunk("terms", detail.get("terms_and_conditions", [])),
    }


def _process_detail(listing_meta: dict, scraped_at: str) -> dict | None:
    url = listing_meta["detail_url"]
    html = _get(url)
    if not html:
        return None
    detail = parse_detail_page(html, url)
    es_doc = build_es_document(listing_meta, detail, scraped_at)
    log.info(
        "✔ [%-48s] pts=%-7s feat=%d cities=%d",
        listing_meta["item_id"][:48],
        f"{es_doc['points_only']:,}" if es_doc.get("points_only") else "?",
        len(detail.get("features", [])),
        detail.get("available_cities_count", 0),
    )
    return es_doc


def main(args: argparse.Namespace) -> int:
    scraped_at = datetime.now(timezone.utc).isoformat()

    log.info("Stage 1: Building catalogue")
    products, catalogue_meta = get_listing_items()
    if not products:
        log.error("No reward items found. Exiting.")
        return 1

    if args.max_items:
        products = products[: args.max_items]
        log.info("Capped to %d items (--max-items)", args.max_items)

    log.info("Stage 1 complete → %d products queued", len(products))

    all_docs: list[dict] = []

    if args.no_detail:
        log.info("--no-detail: building docs from listing data only")
        for p in products:
            all_docs.append(build_es_document(p, {}, scraped_at))
    else:
        log.info("Stage 2: Detail pages [threads=%d]", args.concurrency)
        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            futures = {pool.submit(_process_detail, p, scraped_at): p for p in products}
            for fut in as_completed(futures):
                try:
                    result = fut.result()
                    if result:
                        all_docs.append(result)
                except Exception as exc:
                    log.warning("Worker exception: %s", exc)

    all_docs.sort(key=lambda d: d.get("points_only") or 0)
    master = OUTPUT_JSON_DIR / "sbi_rewards_all.json"
    master.write_text(json.dumps(all_docs, ensure_ascii=False, indent=2), encoding="utf-8")

    log.info("✅ Done. Master JSON: %s (items=%d)", master, len(all_docs))
    log.info("Catalogue source meta: %s", json.dumps(catalogue_meta, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="SBI Card rewards scraper v2.0 — pure requests (no browser).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--max-items", type=int, default=None, help="Cap total items for smoke test")
    ap.add_argument("--max-item", dest="max_items", type=int, default=None, help=argparse.SUPPRESS)
    ap.add_argument("--concurrency", type=int, default=6, help="Parallel HTTP threads for detail pages")
    ap.add_argument("--no-detail", action="store_true", help="Skip detail pages (fastest)")
    sys.exit(main(ap.parse_args()))

