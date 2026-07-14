import argparse
import json
import re
from pathlib import Path
from typing import Any, Iterable, Iterator
from urllib.parse import urlparse

from common.paths import OUTPUT_DIR

DEFAULT_INPUT_PATH = OUTPUT_DIR
DEFAULT_OUTPUT_PATH = OUTPUT_DIR / "embedding_payloads.jsonl"
COMMON_LANGUAGE_SEGMENTS = {
    "en",
    "hi",
    "fr",
    "de",
    "es",
    "it",
    "pt",
    "ja",
    "ko",
    "zh",
    "ar",
    "ru",
}
SKIP_METADATA_KEYS = {"embedding_text"}
LARGE_TEXT_KEYS = {"markdown", "html", "content", "body", "raw_html"}
NOISY_METADATA_PREFIXES = ("request_", "response_")


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        text = value
    else:
        text = str(value)
    return re.sub(r"\s+", " ", text).strip()


def clean_keywords(value: Any) -> str:
    return " ".join(split_keywords(value))


def split_keywords(value: Any) -> list[str]:
    """Normalize keywords from crawl/LLM records (list, comma-string, or scalar)."""
    if value is None:
        return []
    if isinstance(value, list):
        parts = [normalize_text(item) for item in value]
        return [item for item in parts if item]
    if isinstance(value, tuple):
        parts = [normalize_text(item) for item in value]
        return [item for item in parts if item]

    text = normalize_text(value)
    if not text:
        return []

    parts = [item.strip() for item in text.split(",")]
    return [item for item in parts if item]


def parse_url_path_text(value: Any) -> str:
    raw_value = normalize_text(value)
    if not raw_value:
        return ""

    parsed = urlparse(raw_value)
    path_only = parsed.path or raw_value
    segments = [segment for segment in path_only.split("/") if segment]

    cleaned_segments: list[str] = []
    for segment in segments:
        cleaned_segment = segment.rsplit(".", 1)[0]
        cleaned_segment = cleaned_segment.replace("-", " ").replace("_", " ")
        cleaned_segment = re.sub(r"[^A-Za-z0-9\s]", " ", cleaned_segment)
        cleaned_segment = normalize_text(cleaned_segment)
        if not cleaned_segment:
            continue
        if cleaned_segment.lower() in COMMON_LANGUAGE_SEGMENTS and len(segments) > 1:
            continue
        cleaned_segments.append(cleaned_segment)

    if not cleaned_segments:
        return ""

    final_segment = cleaned_segments[-1]
    if " " in final_segment:
        return final_segment

    return " ".join(cleaned_segments)


def extract_last_path_param(value: Any) -> str:
    raw_value = normalize_text(value)
    if not raw_value:
        return ""

    parsed = urlparse(raw_value)
    path_only = parsed.path or raw_value
    segments = [segment for segment in path_only.split("/") if segment]
    if not segments:
        return ""

    last_segment = segments[-1].rsplit(".", 1)[0]
    cleaned = last_segment.replace("-", " ").replace("_", " ")
    cleaned = re.sub(r"[^A-Za-z0-9\s]", " ", cleaned)
    return normalize_text(cleaned)


def is_login_redirect_url(value: Any) -> bool:
    redirect_url = normalize_text(value)
    if not redirect_url:
        return False
    parsed = urlparse(redirect_url)
    return "login" in (parsed.path or "").lower()


def prefix_text(text: Any, prefix: str) -> str:
    normalized_prefix = normalize_text(prefix)
    normalized_text = normalize_text(text)
    if not normalized_prefix:
        return normalized_text
    if not normalized_text:
        return normalized_prefix
    if normalized_text.lower() == normalized_prefix.lower():
        return normalized_text
    if normalized_text.lower().startswith(f"{normalized_prefix.lower()} | "):
        return normalized_text
    return f"{normalized_prefix} | {normalized_text}"


def prefix_keywords(value: Any, prefix: str) -> list[str]:
    normalized_prefix = normalize_text(prefix)
    keyword_parts = split_keywords(value)
    if not normalized_prefix:
        return keyword_parts
    if keyword_parts and keyword_parts[0].lower() == normalized_prefix.lower():
        return keyword_parts
    return [normalized_prefix, *keyword_parts]


def get_record_path(record: dict[str, Any]) -> str:
    raw_path = normalize_text(record.get("path"))
    if raw_path:
        return raw_path
    source_url = normalize_text(record.get("source_url"))
    if not source_url:
        return ""
    return urlparse(source_url).path or ""


def get_enriched_fields(record: dict[str, Any]) -> tuple[str, str, list[str], str]:
    path_value = get_record_path(record)
    title = normalize_text(record.get("title"))
    description = normalize_text(record.get("description"))
    keywords = split_keywords(record.get("keywords"))

    if is_login_redirect_url(record.get("redirected_url")):
        path_param = extract_last_path_param(path_value)
        if path_param:
            title = prefix_text(title, path_param)
            description = prefix_text(description, path_param)
            keywords = prefix_keywords(keywords, path_param)

    return title, description, keywords, path_value


def is_successful_page(record: dict[str, Any]) -> bool:
    return record.get("success") is True


def iter_input_files(input_path: Path) -> Iterator[Path]:
    if input_path.is_file():
        yield input_path
        return

    if not input_path.exists():
        raise FileNotFoundError(f"Input path does not exist: {input_path}")

    for suffix in ("*.json", "*.jsonl"):
        yield from sorted(input_path.glob(suffix))


def load_records_from_json(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))

    if isinstance(data, dict):
        pages = data.get("pages")
        if isinstance(pages, list):
            return [item for item in pages if isinstance(item, dict)]
        return [data]

    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]

    return []


def load_records_from_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        text = line.strip()
        if not text:
            continue
        item = json.loads(text)
        if isinstance(item, dict):
            records.append(item)
        else:
            raise ValueError(f"Expected object on line {line_number} in {path}")
    return records


def load_records(input_path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for file_path in iter_input_files(input_path):
        if file_path.suffix.lower() == ".jsonl":
            records.extend(load_records_from_jsonl(file_path))
        else:
            records.extend(load_records_from_json(file_path))
    return records


def should_include_extra_metadata(key: str, value: Any) -> bool:
    if key in SKIP_METADATA_KEYS:
        return False
    if key in LARGE_TEXT_KEYS:
        return False
    if key.startswith(NOISY_METADATA_PREFIXES):
        return False
    if value is None:
        return True
    if isinstance(value, (bool, int, float)):
        return True
    if isinstance(value, str):
        return len(normalize_text(value)) <= 500
    if isinstance(value, (dict, list)):
        return len(json.dumps(value, ensure_ascii=False)) <= 2000
    return False


def build_embedding_text(record: dict[str, Any]) -> str:
    title, description, keywords, path_value = get_enriched_fields(record)
    parts = [
        title,
        description,
        clean_keywords(keywords),
        parse_url_path_text(path_value),
    ]
    return "\n".join(part for part in parts if part)


def build_metadata(record: dict[str, Any]) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    title, description, keywords, path_value = get_enriched_fields(record)

    metadata["title"] = title
    metadata["description"] = description
    metadata["keywords"] = split_keywords(keywords)
    metadata["path"] = path_value
    if "source_url" in record:
        metadata["source_url"] = record.get("source_url")

    parsed_url_path_text = parse_url_path_text(path_value)
    if parsed_url_path_text:
        metadata["parsed_url_path_text"] = parsed_url_path_text

    for key, value in record.items():
        if key in metadata:
            continue
        if should_include_extra_metadata(key, value):
            metadata[key] = value

    return metadata


def build_output_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "embedding_text": build_embedding_text(record),
        "metadata": build_metadata(record),
    }


def build_output_records(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    output_records: list[dict[str, Any]] = []
    for record in records:
        if not is_successful_page(record):
            continue
        embedding_text = build_embedding_text(record)
        if not embedding_text:
            continue
        output_records.append(
            {
                "embedding_text": embedding_text,
                "metadata": build_metadata(record),
            }
        )
    return output_records


def write_output(records: list[dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(record, ensure_ascii=False) for record in records]
    output_path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build embedding payloads from scraped JSON or JSONL page data."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT_PATH,
        help="Input file or directory containing JSON/JSONL crawl output.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Output JSONL file for embedding payloads.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    records = load_records(args.input)
    output_records = build_output_records(records)
    write_output(output_records, args.output)
    print(f"Wrote {len(output_records)} embedding payloads to {args.output}")


if __name__ == "__main__":
    # Example path cleanup:
    # "/en/eapply/track-credit-card-application.page" -> "track credit card application"
    # "/creditcards/app/user/login#address-tab" -> "creditcards app user login"
    #
    # Example keywords cleanup:
    # "sbi card,sbi credit card,credit card services" ->
    # "sbi card sbi credit card credit card services"
    main()
