from __future__ import annotations

from dataclasses import dataclass

from .normalize_hash import (
    compute_content_hash,
    normalize_markdown_for_hash,
    normalize_text_for_hash,
)

STATUS_NEW = "NEW"
STATUS_UPDATED = "UPDATED"
STATUS_UNCHANGED = "UNCHANGED"
STATUS_DELETED = "DELETED"


@dataclass
class ChangeDetectionResult:
    status: str
    current_hash: str
    previous_hash: str | None
    changed_fields: list[str]


def detect_changed_fields(
    *,
    title: str | None,
    description: str | None,
    markdown: str | None,
    previous_title: str | None,
    previous_description: str | None,
    previous_markdown: str | None,
) -> list[str]:
    changed_fields: list[str] = []
    if normalize_markdown_for_hash(markdown) != normalize_markdown_for_hash(previous_markdown):
        changed_fields.append("markdown")
    return changed_fields


def classify_change(
    *,
    title: str | None,
    description: str | None,
    markdown: str | None,
    previous_hash: str | None,
    previous_title: str | None = None,
    previous_description: str | None = None,
    previous_markdown: str | None = None,
) -> ChangeDetectionResult:
    current_hash = compute_content_hash(title, description, markdown)
    if not previous_hash:
        return ChangeDetectionResult(
            status=STATUS_NEW,
            current_hash=current_hash,
            previous_hash=None,
            changed_fields=["markdown"],
        )
    if current_hash == previous_hash:
        return ChangeDetectionResult(
            status=STATUS_UNCHANGED,
            current_hash=current_hash,
            previous_hash=previous_hash,
            changed_fields=[],
        )
    return ChangeDetectionResult(
        status=STATUS_UPDATED,
        current_hash=current_hash,
        previous_hash=previous_hash,
        changed_fields=detect_changed_fields(
            title=title,
            description=description,
            markdown=markdown,
            previous_title=previous_title,
            previous_description=previous_description,
            previous_markdown=previous_markdown,
        ),
    )
