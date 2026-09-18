"""Load a private, pinned validation expansion before systems-card traffic."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..expanded_truth import validate_expanded_truth_set
from ..private_holdout import _load_jsonl, _private_path, _read_private
from ..retrieval import EvaluationInputError

EXPANSION_SCHEMA = "recall.systems-card.truth-expansion.v1"
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")


@dataclass(frozen=True)
class TruthExpansion:
    base: list[dict[str, Any]]
    additions: list[dict[str, Any]]
    families: list[dict[str, Any]]
    base_canonical_sha256: str
    pins: dict[str, str]


def _private(path: Path) -> Path:
    path = _private_path(path, exists=True)
    if any((parent / ".git").exists() for parent in path.parents):
        raise EvaluationInputError("truth expansion must remain outside git")
    return path


def load_truth_expansion(path: str | Path, *, split: str = "validation") -> TruthExpansion:
    """Read a closed manifest and three adjacent, hash-pinned private JSONLs.

    Relative artifact paths cannot escape the manifest directory or traverse
    symlinks. Inputs are retained in memory after validation; later file edits
    cannot change the questions sent by this run. The manifest carries raw file
    hashes plus the independent canonical digest of the frozen original truth.
    """
    try:
        if split != "validation":
            raise EvaluationInputError("truth expansion is validation-only")
        manifest_path = _private(Path(path))
        payload = _read_private(manifest_path)
        manifest = json.loads(payload)
        if (
            not isinstance(manifest, dict)
            or set(manifest) != {"schema_version", "base_canonical_sha256", "base", "additions", "families"}
            or manifest["schema_version"] != EXPANSION_SCHEMA
            or not isinstance(manifest["base_canonical_sha256"], str)
            or not _DIGEST.fullmatch(manifest["base_canonical_sha256"])
        ):
            raise EvaluationInputError("truth expansion manifest schema is invalid")
        artifacts = {}
        pins = {"manifest_sha256": hashlib.sha256(payload).hexdigest()}
        paths = set()
        for name in ("base", "additions", "families"):
            entry = manifest[name]
            if (
                not isinstance(entry, dict)
                or set(entry) != {"path", "sha256"}
                or not isinstance(entry["path"], str)
                or not entry["path"]
                or not isinstance(entry["sha256"], str)
                or not _DIGEST.fullmatch(entry["sha256"])
            ):
                raise EvaluationInputError("truth expansion artifact schema is invalid")
            relative = Path(entry["path"])
            if relative.is_absolute() or ".." in relative.parts:
                raise EvaluationInputError("truth expansion artifact path is invalid")
            artifact = _private(manifest_path.parent / relative)
            if artifact == manifest_path or artifact in paths:
                raise EvaluationInputError("truth expansion artifacts must be distinct")
            paths.add(artifact)
            rows, raw = _load_jsonl(artifact)
            actual = hashlib.sha256(raw).hexdigest()
            if actual != entry["sha256"]:
                raise EvaluationInputError("truth expansion artifact digest mismatch")
            artifacts[name] = rows
            pins[f"{name}_sha256"] = actual
        receipt = validate_expanded_truth_set(
            artifacts["base"], artifacts["additions"], artifacts["families"],
            expected_base_sha256=manifest["base_canonical_sha256"],
        )
        pins["base_canonical_sha256"] = receipt["base_sha256"]
        return TruthExpansion(**artifacts, base_canonical_sha256=receipt["base_sha256"], pins=pins)
    except (OSError, ValueError, TypeError, KeyError, RecursionError):
        # File errors and malformed JSON can contain private paths or contents.
        raise EvaluationInputError("truth expansion preflight failed") from None
