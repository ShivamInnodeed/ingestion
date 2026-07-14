import argparse
import json
import logging
from pathlib import Path
from typing import Any, Iterator

from sentence_transformers import SentenceTransformer

from common.paths import OUTPUT_DIR

DEFAULT_INPUT_PATH = OUTPUT_DIR / "embedding_payloads.jsonl"
DEFAULT_OUTPUT_PATH = OUTPUT_DIR / "embedding_vectors.jsonl"
DEFAULT_MODEL_NAME = "all-MiniLM-L6-v2"
DEFAULT_BATCH_SIZE = 32


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


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
        logging.info("Loading records from %s", file_path)
        if file_path.suffix.lower() == ".jsonl":
            records.extend(load_records_from_jsonl(file_path))
        else:
            records.extend(load_records_from_json(file_path))
    return records


def get_embedding_text(record: dict[str, Any]) -> str:
    return normalize_text(record.get("embedding_text"))


def is_valid_record(record: dict[str, Any]) -> bool:
    return bool(get_embedding_text(record))


def iter_batches(items: list[dict[str, Any]], batch_size: int) -> Iterator[list[dict[str, Any]]]:
    for index in range(0, len(items), batch_size):
        yield items[index : index + batch_size]


def build_output_record(record: dict[str, Any], embedding: list[float]) -> dict[str, Any]:
    return {
        "embedding_text": get_embedding_text(record),
        "embedding": embedding,
        "metadata": record.get("metadata", {}),
    }


def generate_embeddings(
    records: list[dict[str, Any]],
    model: SentenceTransformer,
    batch_size: int,
) -> list[dict[str, Any]]:
    valid_records = [record for record in records if is_valid_record(record)]
    skipped_empty = len(records) - len(valid_records)

    logging.info("Encoding %s valid records", len(valid_records))
    if skipped_empty:
        logging.info("Skipped %s records with empty embedding_text", skipped_empty)

    output_records: list[dict[str, Any]] = []

    for batch_number, batch in enumerate(iter_batches(valid_records, batch_size), start=1):
        texts = [get_embedding_text(record) for record in batch]
        logging.info("Encoding batch %s with %s records", batch_number, len(batch))
        embeddings = model.encode(
            texts,
            batch_size=batch_size,
            normalize_embeddings=True,
            show_progress_bar=False,
        )

        for record, embedding in zip(batch, embeddings):
            output_records.append(build_output_record(record, embedding.tolist()))

    return output_records


def write_output(records: list[dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(record, ensure_ascii=False) for record in records]
    output_path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build sentence-transformer embeddings from embedding payload JSON or JSONL."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT_PATH,
        help="Input file or directory containing embedding payload JSON/JSONL.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Output JSONL file containing embeddings.",
    )
    parser.add_argument(
        "--model-name",
        default=DEFAULT_MODEL_NAME,
        help="SentenceTransformer model name.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Batch size for embedding generation.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = parse_args()
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be greater than 0")

    records = load_records(args.input)
    logging.info("Loaded %s input records", len(records))

    model = SentenceTransformer(args.model_name)
    output_records = generate_embeddings(records, model, args.batch_size)
    write_output(output_records, args.output)

    logging.info("Wrote %s embedding records to %s", len(output_records), args.output)


if __name__ == "__main__":
    main()
