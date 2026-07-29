"""Stable identity and version semantics shared by Recall evaluators."""

from __future__ import annotations

from typing import Any


def stable_boundary_identity(value: dict[str, Any]) -> tuple[str, str]:
    """Identify a source-level document independently of projection revision."""

    source_id = value.get("source_id")
    logical_document_id = value.get("logical_document_id")
    if (
        not isinstance(source_id, str)
        or not source_id
        or not isinstance(logical_document_id, str)
        or not logical_document_id
    ):
        raise ValueError("boundary identity is invalid")
    return source_id, logical_document_id


def boundary_revision(value: dict[str, Any]) -> int:
    """Return a validated monotonic logical-document revision."""

    revision = value.get("revision")
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
        raise ValueError("boundary revision is invalid")
    return revision


def revision_is_fresh(
    candidate: dict[str, Any],
    gold: dict[str, Any],
) -> bool:
    """A later projection revision is fresh, even when it is not exact."""

    return boundary_revision(candidate) >= boundary_revision(gold)
