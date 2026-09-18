"""Explicit validation-only expansion of a pinned, frozen v2 truth set.

Inputs remain private and callers own file permission checks. Native family
mapping is a caller-reviewed source-lineage assertion, not inferred here.
Only aggregate counts, hashes, and scores leave this module.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from .agentic_truth import (
    _score_validated_cases,
    _validate_case_rows,
    _validate_cases,
)
from .boundary_identity import stable_boundary_identity
from .retrieval import EvaluationInputError


SCHEMA_VERSION = "recall.agentic-retrieval-truth.expanded.v1"
FAMILY_FIELDS = {"source_id", "logical_document_id", "family_id"}


def canonical_sha256(rows: list[dict[str, Any]]) -> str:
    """Hash ordered rows as sorted-key compact JSON, UTF-8, no ASCII escaping.

    Row/list order is significant; object key order is not. NaN is forbidden.
    Freeze this digest independently before admitting additions. It differs
    from the raw JSONL file digest, which callers should retain separately.
    """
    payload = json.dumps(
        rows,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def validate_expanded_truth_set(
    base_cases: list[dict[str, Any]],
    additions: list[dict[str, Any]],
    family_mapping: list[dict[str, Any]],
    *,
    expected_base_sha256: str,
) -> dict[str, Any]:
    """Require the unchanged frozen 60 plus approved, independent positives.

    Every gold boundary must have exactly one mapping row containing source_id,
    logical_document_id, and a nonempty family_id. Family IDs group native
    sessions and their copied/forwarded descendants across source boundaries.
    Multiple boundaries within one added case may share a family; added cases
    may not share any family with the base or with another added case.
    """
    _validate_cases(base_cases)
    base_digest = canonical_sha256(base_cases)
    if base_digest != expected_base_sha256:
        raise EvaluationInputError("expanded truth frozen base digest mismatch")
    _validate_case_rows(additions)
    for case in additions:
        if case["split"] != "validation":
            raise EvaluationInputError(
                "expanded truth additions must be validation-only"
            )
        if case["answerability"] != "answerable":
            raise EvaluationInputError("expanded truth additions must be answerable")
    cases = base_cases + additions
    receipt = _validate_case_rows(cases)
    identities = [
        stable_boundary_identity(boundary)
        for case in cases
        for boundary in case["gold_boundaries"]
    ]
    if len(identities) != len(set(identities)):
        raise EvaluationInputError("expanded truth gold boundaries must be unique")
    if not isinstance(family_mapping, list):
        raise EvaluationInputError("expanded truth family mapping is invalid")
    family_by_boundary: dict[tuple[str, str], str] = {}
    for row in family_mapping:
        if (
            not isinstance(row, dict)
            or set(row) != FAMILY_FIELDS
            or any(
                not isinstance(value, str)
                or not value
                or value != value.strip()
                or len(value) > 255
                for value in row.values()
            )
        ):
            raise EvaluationInputError("expanded truth family mapping is invalid")
        identity = stable_boundary_identity(row)
        if identity in family_by_boundary:
            raise EvaluationInputError("expanded truth family mapping is ambiguous")
        family_by_boundary[identity] = row["family_id"]
    if set(family_by_boundary) != set(identities):
        raise EvaluationInputError(
            "expanded truth family mapping must cover every boundary exactly"
        )
    base_families = {
        family_by_boundary[stable_boundary_identity(boundary)]
        for case in base_cases
        for boundary in case["gold_boundaries"]
    }
    added_families: set[str] = set()
    for case in additions:
        own_families = {
            family_by_boundary[stable_boundary_identity(boundary)]
            for boundary in case["gold_boundaries"]
        }
        if own_families & base_families:
            raise EvaluationInputError(
                "expanded truth addition overlaps a frozen family"
            )
        if own_families & added_families:
            raise EvaluationInputError("expanded truth family overlaps added cases")
        added_families.update(own_families)
    return {
        "schema_version": SCHEMA_VERSION,
        **receipt,
        "base_case_count": len(base_cases),
        "added_case_count": len(additions),
        "base_family_count": len(base_families),
        "added_family_count": len(added_families),
        "base_sha256": base_digest,
        "additions_sha256": canonical_sha256(additions),
        "family_mapping_sha256": canonical_sha256(family_mapping),
    }


def score_expanded_boundary_candidates(
    base_cases: list[dict[str, Any]],
    additions: list[dict[str, Any]],
    family_mapping: list[dict[str, Any]],
    results: list[dict[str, Any]],
    *,
    expected_base_sha256: str,
    split: str | None = None,
) -> dict[str, Any]:
    """Validate the expansion and use v2 metric math and result contracts."""
    validate_expanded_truth_set(
        base_cases,
        additions,
        family_mapping,
        expected_base_sha256=expected_base_sha256,
    )
    return _score_validated_cases(
        base_cases + additions,
        results,
        split=split,
        schema_version=SCHEMA_VERSION,
    )
