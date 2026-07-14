import json
import sys
from pathlib import Path


# ── CONFIG ────────────────────────────────────────────────────────────────────
JSON1_FILE = "ai_metadata.json"        # list of AI-enriched page objects
JSON2_FILE = "sbicard_crawl.json"      # crawl result (your base structure)
OUTPUT_FILE = "updated.json"
# ─────────────────────────────────────────────────────────────────────────────

# Keys to COPY directly from JSON1 → JSON2 page (added if missing, overwritten if present)
KEYS_TO_ADD = [
    "card_name",
    "page_type",
    "page_purpose",
    "customer_blurb",
    "facet_tags",
    "key_facts",
    "page_index",
]


def load_json(filepath: str):
    path = Path(filepath)
    if not path.exists():
        print(f"[ERROR] File not found: {filepath}")
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_lookup(ai_list: list) -> dict:
    """Build a source_url → ai_page dict from JSON1 list."""
    lookup = {}
    for item in ai_list:
        url = item.get("source_url")
        if url:
            lookup[url] = item
    print(f"  AI metadata entries loaded : {len(lookup)}")
    return lookup


def enrich_pages(pages: list, lookup: dict) -> tuple[list, int, int]:
    enriched_count = 0
    skipped_count = 0
    result = []

    for page in pages:
        url = page.get("source_url")
        ai = lookup.get(url)

        if ai:
            page = dict(page)  # shallow copy — don't mutate original

            # ── 3 key overwrites ──────────────────────────────────────────
            if ai.get("ai_title"):
                page["title"] = str(ai["ai_title"]).lower()

            if ai.get("ai_summary"):
                page["description"] = str(ai["ai_summary"]).lower()

            if ai.get("keywords") is not None:
                # JSON1 keywords is a list → join to comma-separated string
                kw = ai["keywords"]
                page["keywords"] = ", ".join(kw) if isinstance(kw, list) else kw

            # ── extra keys from JSON1 → JSON2 ─────────────────────────────
            for key in KEYS_TO_ADD:
                if key in ai:
                    page[key] = ai[key]

            enriched_count += 1
        else:
            skipped_count += 1

        # Always normalize casing for title/description, even without AI match.
        # This enforces a consistent downstream embedding/indexing corpus.
        if isinstance(page.get("title"), str):
            page["title"] = page["title"].lower()
        if isinstance(page.get("description"), str):
            page["description"] = page["description"].lower()
        if isinstance(page.get("keywords"), str):
            page["keywords"] = page["keywords"].lower()
        elif isinstance(page.get("keywords"), list):
            page["keywords"] = [str(k).lower() for k in page["keywords"] if str(k).strip()]

        # Keep metadata.* in sync when present (crawl JSON usually stores these too).
        meta = page.get("metadata")
        if isinstance(meta, dict):
            if isinstance(page.get("title"), str):
                meta["title"] = page["title"]
            if isinstance(page.get("description"), str):
                meta["description"] = page["description"]
            if "keywords" in page:
                meta["keywords"] = page["keywords"]
            page["metadata"] = meta

        result.append(page)

    return result, enriched_count, skipped_count


def main():
    print(f"Loading JSON1 (AI metadata) : {JSON1_FILE}")
    ai_list = load_json(JSON1_FILE)
    if not isinstance(ai_list, list):
        print("[ERROR] JSON1 must be a JSON array (list) at the top level.")
        sys.exit(1)

    print(f"Loading JSON2 (crawl data)  : {JSON2_FILE}")
    crawl_data = load_json(JSON2_FILE)
    if not isinstance(crawl_data, dict) or "pages" not in crawl_data:
        print("[ERROR] JSON2 must be a JSON object with a 'pages' key.")
        sys.exit(1)

    pages = crawl_data["pages"]
    print(f"  Crawl pages loaded         : {len(pages)}")

    lookup = build_lookup(ai_list)

    print("\nEnriching pages ...")
    enriched_pages, enriched, skipped = enrich_pages(pages, lookup)

    # Build output — exact same structure as JSON2, pages replaced
    output = dict(crawl_data)
    output["pages"] = enriched_pages

    out_path = Path(OUTPUT_FILE)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Done!")
    print(f"   Pages enriched             : {enriched}")
    print(f"   Pages with no AI match     : {skipped}")
    print(f"   Total pages in output      : {len(enriched_pages)}")
    print(f"   Output written to          : {out_path.resolve()}")


if __name__ == "__main__":
    main()