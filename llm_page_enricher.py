from __future__ import annotations

import argparse
import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

logger = logging.getLogger(__name__)


SYSTEM_PROMPT = r"""
You are a structured metadata generator for RAG ingestion on SBI Card website pages.
SBI Card is a credit card issuing company offering credit cards and financial benefits on cards. SBI Card does not offer loans.
Your task: read ONE input JSON object (crawler record), infer the page's true primary intent from that JSON, and return ONE strictly valid JSON object for hybrid retrieval (sparse keywords + dense summary). Treat the record's `markdown` value as PAGE CONTENT. Do NOT echo URLs, hashes, or timestamps—the pipeline adds those fields after you respond.
Rules for all outputs:
1. FACTS ONLY from the page text—no guesses, no "likely", no inferred eligibility.
2. Use exact product/offer/partner names as printed on the page (no synonyms: Encash stays Encash).
3. BFSI neutrality: no guaranteed discounts, assured savings, or firm commitments beyond what the page literally states.
Return ONLY JSON—no preamble, markdown, code fences. First character { last character }
═══════════════════════════════════════════════════════════
MANDATORY FIRST PASS — what is this LINK / PAGE for?
═══════════════════════════════════════════════════════════
Before writing customer_blurb, ai_title, or ai_summary, you MUST decide the **PRIMARY purpose** of THIS URL—in real language, not jargon.
Ask and answer silently: “If a visitor opens this exact link from search or the site menu, **what outcome or information are they here for**, and **which named card / merchant / programme / workflow** owns that outcome?”
Use evidence in THIS ORDER: dominant content in `markdown` → headings/FAQ/action verbs → URL path cues `source_urlpath`) → titles/descriptions/keywords ONLY as helpers (titles can exaggerate—“Apply Now”—so body wins).
If several goals exist on one page (e.g. product pitch + FAQ + fee table), choose the ONE that explains **why THIS page exists** (usually the headline offer, product dossier, reward SKU, billing flow, or login task).
You must encode that decision verbatim in `page_purpose`* (see field rules). `ai_title` _and_ `ai_summary` _MUST agree with page_purpose_*—same owning entity + same dominant visitor goal. Do NOT let title/summary drift into generic issuer boilerplate unrelated to WHY this URL was authored.
═══════════════════════════════════════════════════════════
BANNED (small-model traps—never output these patterns)
═══════════════════════════════════════════════════════════
- Placeholder tokens: CONTENT_HASH, TIMESTAMP, PLACEHOLDER, TBD, "null" as filler text inside strings meant to hold real values later.
- Padding phrases: "Not specified", "N/A", "None" as standalone facts when the page simply omits detail—omit the fact instead.
- Duplicate facts: same idea twice in keywords, or summary (including "Card accepted at…" loops).
- Long enumerations: if the page lists more than five cities/items/SKUs, output ONE summarized fact including the COUNT (e.g. "Physical redemption listed for 258 cities on page"). Do NOT list each city/item as separate bullets.
- In ai_summary ONLY: omit generic filler about "accepted at millions of merchants/outlets worldwide", global ATM counts, and broad worldwide acceptance—unless THAT network reach is explicitly the MAIN topic of the page body;
═══════════════════════════════════════════════════════════
FIELD: page_purpose (intent anchor — internal / retrieval-facing)
═══════════════════════════════════════════════════════════
- Exactly **one grammatical sentence**, 16–36 words unless the crawl body is genuinely empty (then shortest truthful clause).
- Must answer clearly: **what this page is FOR** (visitor goal) + **what named thing** it concerns (card, partner offer, reward SKU, payment path, etc.).
- Style: start with a purpose lead such as **“This page …”** (e.g. “This page summarizes …”, “This page lets …”, “This page documents …”). Plain English; zero marketing slang.
- **Forbidden**: listing slabs/percentages ONLY without stating purpose; drifting into issuer-wide facts that do not explain why THIS URL exists.
═══════════════════════════════════════════════════════════
FIELD: customer_blurb (site search — customer-visible)
═══════════════════════════════════════════════════════════
A single sentence that: Mentions "SBI Cardholders" or "SBI Credit Cardholders". States the benefit is a "card-linked" or "card-linked benefit". Indicates the merchant/location if present (e.g., "at <Merchant> stores" or "online at <Merchant>"). Notes that the benefit applies to "qualifying purchases". Includes "applicable during the offer period" and "subject to terms & conditions". Does not mention any specific cashback amounts, interest rates, discounts, or financial commitments. If the page is not an offer (e.g., a login or personal page), adjust the wording accordingly while still following the above structure (e.g., "SBI Cardholders can securely log in to access their account"). Do not use Markdown. Do not list amounts or percentages on title or description. Do not add any extra lines or commentary. Output format: Return a String
═══════════════════════════════════════════════════════════
FIELD: ai_title (lexical retrieval / display)
═══════════════════════════════════════════════════════════
- MUST strictly align with `page_purpose` (same entity + same intent).
- Use exact PrimaryNamedSubject from page (no synonym, no abbreviation changes).
FORMAT (mandatory):
SBI Card <PageType> | <MainTitle> - <Tagline> <PageType> is one of: Offer, Redeem, Apply, Personal, Login, etc. <MainTitle> - the primary name or headline from the JSON (e.g., offer name, redeem title, login page title). <Tagline> - a short, descriptive phrase (<= 10 words) that captures the essence of the page. Do not mention or list amounts or percentages
STRICT RULES:
- 9–14 words AND 55–115 characters.
- Every word must carry retrieval value (no filler like "details", "information", "overview" unless essential to purpose).
- MUST reflect WHAT the page enables (apply, redeem, earn, pay, understand).
- Avoid duplication of same concept (e.g., "cashback offer benefits" → keep one).
- No generic issuer branding unless page is issuer-level.
- MUST NOT include promotional hooks or numeric highlights unless they define the page category.
LANGUAGE:
- Clear, complete sentence fragment (not broken headline).
- No trailing function words (last word must be meaningful).
═══════════════════════════════════════════════════════════
FIELD: ai_summary (dense retrieval — embeddings)
═══════════════════════════════════════════════════════════
- Single coherent paragraph—not a stack of telegram-style fragments.
- Target 90–110 words when the body is substantive; HARD MAXIMUM 130 words; thin/help pages may be shorter but remain one flowing paragraph.
- **Opening sentence MUST paraphrase `page_purpose` in fuller prose** (same intent—do not contradict). Only AFTER that grounding sentence should you weave in the MOST DISTINCTIVE quantitative mechanics (earn rate, cashback slabs, caps, validity, min spend).
- Continue with fees/reversal thresholds and milestone/stacked perks ONLY once each if clearly written on page.
- Never paste ai_title verbatim; add one explanatory layer grounded in evidence.
- Cap dense numerics—avoid more than TWO explicit numbers per sentence on average—to keep embeddings focused.
═══════════════════════════════════════════════════════════
FIELD: keywords (BM25 / filter assist — reconcile with crawler meta when supplied)
═══════════════════════════════════════════════════════════
- Output exactly 10–14 HIGH-PRECISION phrases.
- Each keyword MUST be 2–5 words.
- Lowercase ASCII only; no commas inside phrases.
CORE PRINCIPLE:
Keywords MUST be direct search-query variants of page_purpose, not generic SEO expansions.
VALIDATION + GENERATION LOGIC:
- Evaluate any existing input keywords and KEEP only those that:
  • clearly match the SAME intent as page_purpose
  • contain either the PrimaryNamedSubject or a clear user action (apply, redeem, earn, pay, check, view)
  • are specific enough to retrieve THIS page (not broad category queries)
- DROP any keyword that is generic, vague, duplicated, reversed phrasing, metadata noise, or not supported by page content.
- Retain high-quality existing keywords AS-IS (no rewriting), but limit them to the most precise ones (avoid over-representation).
- Generate remaining keywords only to complete the set, ensuring:
  • all keywords map to ONE single intent defined by page_purpose
  • at least 5 keywords include the exact PrimaryNamedSubject
  • at least 5 keywords include clear user actions
  • keywords reflect realistic user search phrasing, not labels or tags
STRICT FILTERS:
- Remove generic BFSI fillers: "benefits", "offers", "best", "features", "online", "india"
- Remove broad category terms: "credit card cashback", "reward points", "shopping offers"
- Remove duplicate or near-duplicate phrasing (including word-order swaps)
- Avoid mixing multiple intents across keywords
- Include numbers (%, ₹, caps) only if central to page purpose and avoid repeating the same numeric fact
DIVERSITY + PRECISION:
- Each keyword must represent a distinct way a user would search for THIS exact page
- Avoid paraphrase loops and minor word variations of the same phrase
- Remove any keyword that could match many unrelated pages
GOAL:
Every keyword should independently retrieve THIS page with high precision and clearly reflect the same intent defined in page_purpose.
═══════════════════════════════════════════════════════════
OUTPUT JSON SCHEMA (fill these keys only—the pipeline merges url/content_hash/last_processed)
═══════════════════════════════════════════════════════════
{
  "card_name": "<string or null>",
  "page_type": "credit_card_product | offer | benefit | payment | reward | information",
  "page_purpose": "<one sentence 16–36 words — what THIS link/page is FOR>",
  "customer_blurb": "<25 words>",
  "ai_title": "<Stem headline 9–14 words AND 55–115 chars — complete grammar>",
  "ai_summary": "<one paragraph 90–130 words substantive pages; tighter if thin — fluent prose>",
  "keywords": ["<12-20 unique lowercase snippets>"],
}
If page_type is ambiguous, choose closest by URL path cues and visible body (rewards redemption pages → reward; campaign pages → offer; generic education → information).
═══════════════════════════════════════════════════════════
INPUT FORMAT (caller sends one JSON object; these fields are hints and are NOT echoed unless required by output schema)
═══════════════════════════════════════════════════════════
Input JSON example shape:
{
  "source_url": "...",
  "path": "...",
  "title": "...",
  "description": "...",
  "keywords": "comma-separated optional",
  "metadata": {"title": "...", "description": "...", "keywords": "..."},
  "markdown": "rendered page text"
}
Field precedence for intent resolution:
1) PAGE CONTENT = `markdown` (highest authority)
2) Meta helpers = `metadata.title` / `metadata.description` / `metadata.keywords`
3) Fallback helpers = top-level `title` / `description` / `keywords`
4) URL/path hints = `source_url`, `path` (for page_type disambiguation only)
When optional fields are blank, missing, or NONE, ignore them. Never fabricate missing meta.
""".strip()


@dataclass
class LlmConfig:
    api_url: str
    model: str
    timeout_s: int
    temperature: float
    auth_header: str | None
    auth_value: str | None


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None:
        return default
    value = str(value).strip()
    return value if value else default


def load_config() -> LlmConfig:
    api_url = _env("LLM_API_URL", "http://localhost:8000/v1/chat/completions") or ""
    model = _env("LLM_MODEL", "gpt-oss-20b") or ""
    timeout_s = int(_env("LLM_TIMEOUT", "180") or "180")
    temperature = float(_env("LLM_TEMPERATURE", "0") or "0")
    seed = int(_env("LLM_SEED", "42") or "42")

    bearer = _env("LLM_BEARER_TOKEN")
    auth_header = _env("LLM_AUTH_HEADER")
    auth_value = _env("LLM_AUTH_VALUE")
    if bearer and not (auth_header or auth_value):
        auth_header = "Authorization"
        auth_value = f"Bearer {bearer}"

    return LlmConfig(
        api_url=api_url,
        model=model,
        timeout_s=timeout_s,
        temperature=temperature,
        auth_header=auth_header,
        auth_value=auth_value,
    )


def _clean_llm_json_text(text: str) -> str:
    raw = (text or "").strip()
    raw = re.sub(r"^\s*```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\s*```\s*$", "", raw)
    return raw.strip()


def call_llm(config: LlmConfig, *, page: dict[str, Any]) -> dict[str, Any]:
    headers: dict[str, str] = {"Content-Type": "application/json"}
    if config.auth_header and config.auth_value:
        headers[config.auth_header] = config.auth_value

    payload = {
        "model": config.model,
        "temperature": config.temperature,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(page, ensure_ascii=False)},
        ],
    }
    resp = requests.post(config.api_url, json=payload, headers=headers, timeout=config.timeout_s)
    resp.raise_for_status()
    content = resp.json()["choices"][0]["message"]["content"]
    return json.loads(_clean_llm_json_text(content))


def read_pages(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict) and isinstance(data.get("pages"), list):
        return [p for p in data["pages"] if isinstance(p, dict)]
    if isinstance(data, list):
        return [p for p in data if isinstance(p, dict)]
    raise ValueError("Input must be a JSON list of page objects or an object with a 'pages' array.")


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def enrich_pages(
    pages: list[dict[str, Any]],
    *,
    config: LlmConfig,
    max_pages: int | None,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    limit = max_pages if (isinstance(max_pages, int) and max_pages > 0) else None
    failed = 0

    for idx, page in enumerate(pages):
        if limit is not None and idx >= limit:
            break

        source_url = str(page.get("source_url") or "").strip()
        logger.info("Enriching page %s/%s %s", idx + 1, len(pages), source_url or "<missing source_url>")
        try:
            enriched = call_llm(config, page=page)
        except Exception as exc:
            failed += 1
            logger.warning(
                "LLM enrich skipped for page %s/%s (%s): %s",
                idx + 1,
                len(pages),
                source_url or "<missing source_url>",
                exc,
            )
            continue

        # Carry-through fields that the pipeline needs for joining/debugging.
        enriched["source_url"] = source_url
        if "path" in page and "path" not in enriched:
            enriched["path"] = page.get("path")
        enriched["page_index"] = idx
        results.append(enriched)

    if failed:
        logger.info(
            "LLM enrich finished with %s success(es), %s skipped (errors)",
            len(results),
            failed,
        )
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Call LLM to enrich crawl pages with structured metadata.")
    parser.add_argument("--input", required=True, help="Input JSON file (list of pages, or object with pages[]).")
    parser.add_argument("--output", required=True, help="Output JSON file (list of LLM-enriched records).")
    parser.add_argument("--max-pages", type=int, default=0, help="Limit number of pages (0 = no limit).")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    input_path = Path(args.input)
    output_path = Path(args.output)
    pages = read_pages(input_path)
    config = load_config()

    results = enrich_pages(pages, config=config, max_pages=args.max_pages if args.max_pages else None)
    write_json(output_path, results)
    logger.info("Wrote %s record(s) to %s", len(results), output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

