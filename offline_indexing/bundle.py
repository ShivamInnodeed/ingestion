from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from es_kb.config import AppSettings

DEFAULT_BUNDLE_DIRNAME = "offline_es_bundle"
BUNDLE_MANIFEST_FILENAME = "bundle_manifest.json"
BUNDLE_SETTINGS_FILENAME = "es_effective_config.json"
BUNDLE_EMBEDDINGS_FILENAME = "embedding_vectors.jsonl"
BUNDLE_DELETED_DOC_IDS_FILENAME = "deleted_doc_ids.jsonl"
BUNDLE_INDEXER_SCRIPT_FILENAME = "offline_index_to_es.py"
BUNDLE_README_FILENAME = "README_OFFLINE.md"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_deleted_doc_ids(path: Path, deleted_doc_ids: list[str]) -> None:
    lines = sorted({str(doc_id).strip() for doc_id in deleted_doc_ids if str(doc_id).strip()})
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def _copy_if_exists(src: Path, dst: Path) -> bool:
    if not src.is_file():
        return False
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return True


def _count_non_empty_lines(path: Path) -> int:
    if not path.is_file():
        return 0
    count = 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                count += 1
    return count


def _repo_offline_assets() -> tuple[Path, Path]:
    base = Path(__file__).resolve().parent
    return base / BUNDLE_INDEXER_SCRIPT_FILENAME, base / BUNDLE_README_FILENAME


def export_offline_bundle(
    *,
    bundle_dir: Path,
    embeddings_output_path: Path,
    deleted_doc_ids: list[str],
    settings: AppSettings,
    settings_overrides: dict[str, Any] | None = None,
    recreate_index_requested: bool = False,
    source_payload_path: Path | None = None,
    source_payload_delta_path: Path | None = None,
) -> Path:
    """Create a self-contained offline indexing bundle and return manifest path."""
    bundle_dir.mkdir(parents=True, exist_ok=True)

    embeddings_bundle_path = bundle_dir / BUNDLE_EMBEDDINGS_FILENAME
    if not embeddings_output_path.is_file():
        raise FileNotFoundError(f"Embeddings output not found for bundle export: {embeddings_output_path}")
    shutil.copy2(embeddings_output_path, embeddings_bundle_path)

    deleted_doc_ids_path = bundle_dir / BUNDLE_DELETED_DOC_IDS_FILENAME
    _write_deleted_doc_ids(deleted_doc_ids_path, deleted_doc_ids)

    settings_snapshot_path = bundle_dir / BUNDLE_SETTINGS_FILENAME
    settings_payload = settings.model_dump(mode="json")
    _write_json(settings_snapshot_path, settings_payload)

    indexer_script_src, readme_src = _repo_offline_assets()
    indexer_script_dst = bundle_dir / BUNDLE_INDEXER_SCRIPT_FILENAME
    readme_dst = bundle_dir / BUNDLE_README_FILENAME
    _copy_if_exists(indexer_script_src, indexer_script_dst)
    _copy_if_exists(readme_src, readme_dst)

    manifest_path = bundle_dir / BUNDLE_MANIFEST_FILENAME
    manifest = {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "index": {
            "index_name": settings.index_name,
            "embedding_dims": settings.embedding_dims,
            "es_delete_mode": settings.es_delete_mode,
            "bulk_chunk_size": settings.bulk_chunk_size,
            "bulk_refresh": settings.bulk_refresh,
        },
        "files": {
            "embeddings": BUNDLE_EMBEDDINGS_FILENAME,
            "deleted_doc_ids": BUNDLE_DELETED_DOC_IDS_FILENAME,
            "settings": BUNDLE_SETTINGS_FILENAME,
            "indexer_script": BUNDLE_INDEXER_SCRIPT_FILENAME,
            "readme": BUNDLE_README_FILENAME,
            "source_embeddings_path": str(embeddings_output_path),
            "source_payload_path": str(source_payload_path) if source_payload_path else None,
            "source_payload_delta_path": str(source_payload_delta_path) if source_payload_delta_path else None,
        },
        "runtime": {
            "recreate_index_requested": bool(recreate_index_requested),
            "settings_overrides": settings_overrides or {},
            "record_count_hint": _count_non_empty_lines(embeddings_bundle_path),
            "deleted_doc_id_count": _count_non_empty_lines(deleted_doc_ids_path),
        },
    }
    _write_json(manifest_path, manifest)
    return manifest_path
