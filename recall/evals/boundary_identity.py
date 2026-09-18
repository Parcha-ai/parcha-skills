"""Stable identity and version semantics shared by Recall evaluators."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
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


def native_family_id(kind: str, native_id: str) -> str:
    """Hash an explicit native identity, independently of source host.

    Claude callers supply the canonical sessionId UUID, not the collector's
    file parent. Codex callers supply the canonical collector native parent.
    These are the two existing evaluation family rules, not inferred lineage.
    Already qualified strings and family hashes are rejected, never stripped
    or re-normalized. Malformed/unresolved identities must stay unavailable.
    """
    patterns = {
        "claude-parent": r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}",
        "codex-native": r"codex-session-[0-9a-f]{24}",
    }
    if (
        not isinstance(kind, str)
        or kind not in patterns
        or not isinstance(native_id, str)
        or re.fullmatch(patterns[kind], native_id) is None
    ):
        raise ValueError("native family identity is invalid")
    return hashlib.sha256(f"{kind}|{native_id}".encode("utf-8")).hexdigest()


def protected_family_ids(
    memberships: Mapping[str, list[str] | tuple[str, ...] | set[str] | frozenset[str]],
) -> frozenset[str]:
    """Select hashed families with explicit optimize/test split membership.

    Empty entries (including defaultdict lookup side effects) and validation-
    only entries are not protected. Unknown splits or malformed inputs raise;
    callers must not turn those failures into permission to use a family.
    This does not resolve missing lineage or approve calibration use.
    """
    if not isinstance(memberships, Mapping):
        raise ValueError("family split membership is invalid")
    protected = set()
    for family, splits in memberships.items():
        if (
            not isinstance(family, str)
            or re.fullmatch(r"[0-9a-f]{64}", family) is None
            or not isinstance(splits, (list, tuple, set, frozenset))
            or any(
                not isinstance(split, str)
                or split not in {"optimize", "validation", "test"}
                for split in splits
            )
        ):
            raise ValueError("family split membership is invalid")
        if any(split in {"optimize", "test"} for split in splits):
            protected.add(family)
    return frozenset(protected)
