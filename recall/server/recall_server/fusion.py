"""Arm fusion policy for lossless-passage search (H2-b).

Two fusion modes rank the documents that the dense, passage-lexical, and
sparse-exact arms return:

- ``convex`` (default): each arm's best passage score per document is min-max
  normalised inside that arm, then the arms are combined as a convex sum
  ``Σ alpha_arm × normalised``. Arms that cannot be normalised meaningfully
  (fewer than ``MIN_MINMAX_DOCUMENTS`` documents, or a recent-first fallback
  whose scores are all ``0.0``) fall back to a rank-based score
  ``(k + 1) / (k + rank)`` so the arm still contributes and its members stay
  close together instead of being spread across ``[0, 1]`` arbitrarily.
- ``rrf``: the prior reciprocal-rank fusion ``Σ weight_arm / (k + rank)``.

``RECALL_SEARCH_FUSION`` selects the mode and ``RECALL_SEARCH_FUSION_ALPHAS``
(``dense:0.15,lexical:0.30,sparse:0.55``) sets the convex weights. Both are
validated once at startup; a malformed value is a deployment error, not a
silent fallback.
"""

from __future__ import annotations

import math
import os
from typing import Any, Iterable

FUSION_MODES = ("convex", "rrf")
DEFAULT_FUSION_MODE = "convex"
RRF_K = 60
# Below this many distinct documents min-max normalisation only spreads the
# leg's members to the extremes of [0, 1]; ranks say as much and stay bounded.
MIN_MINMAX_DOCUMENTS = 3
ARM_NAMES = ("dense", "passage-lexical", "sparse-exact")
ARM_ALIASES = {
    "dense": "dense",
    "lexical": "passage-lexical",
    "passage-lexical": "passage-lexical",
    "passage_lexical": "passage-lexical",
    "sparse": "sparse-exact",
    "sparse-exact": "sparse-exact",
    "sparse_exact": "sparse-exact",
}
# The RRF leg weights the convex defaults descend from: a document containing
# every informative query term is stronger evidence than a semantic neighbour.
RRF_LEG_WEIGHTS = {
    "dense": 0.15,
    "passage-lexical": 0.30,
    "sparse-exact": 0.55,
}
# Proportional to the RRF weights. On the unit fixtures this reproduces the
# RRF ordering exactly (see tests/central_brain/test_passage_fusion.py); the
# offline tuner (evals/fusion_tuning.py) is the way to move them.
DEFAULT_FUSION_ALPHAS = dict(RRF_LEG_WEIGHTS)
ALPHA_SUM_TOLERANCE = 1e-6
_FUSION_ALPHAS_ERROR = (
    "RECALL_SEARCH_FUSION_ALPHAS must name dense, lexical, and sparse once "
    "as name:weight pairs with non-negative finite weights summing to 1"
)


def parse_fusion_mode(value: str | None) -> str:
    text = (value if value is not None else DEFAULT_FUSION_MODE).strip().lower()
    if text not in FUSION_MODES:
        raise ValueError(
            "RECALL_SEARCH_FUSION must be one of " + ", ".join(FUSION_MODES)
        )
    return text


def parse_fusion_alphas(value: str | None) -> dict[str, float]:
    """Parse ``dense:0.15,lexical:0.30,sparse:0.55`` into canonical arm names."""

    if value is None or not value.strip():
        return dict(DEFAULT_FUSION_ALPHAS)
    if not isinstance(value, str) or len(value) > 256:
        raise ValueError(_FUSION_ALPHAS_ERROR)
    alphas: dict[str, float] = {}
    for item in value.split(","):
        name, separator, weight_text = item.strip().partition(":")
        arm = ARM_ALIASES.get(name.strip().lower())
        if not separator or arm is None or arm in alphas:
            raise ValueError(_FUSION_ALPHAS_ERROR)
        try:
            weight = float(weight_text.strip())
        except ValueError as exc:
            raise ValueError(_FUSION_ALPHAS_ERROR) from exc
        if not math.isfinite(weight) or weight < 0:
            raise ValueError(_FUSION_ALPHAS_ERROR)
        alphas[arm] = weight
    return validate_fusion_alphas(alphas)


def validate_fusion_alphas(alphas: dict[str, float]) -> dict[str, float]:
    if set(alphas) != set(ARM_NAMES):
        raise ValueError(_FUSION_ALPHAS_ERROR)
    for weight in alphas.values():
        if (
            isinstance(weight, bool)
            or not isinstance(weight, (int, float))
            or not math.isfinite(weight)
            or weight < 0
        ):
            raise ValueError(_FUSION_ALPHAS_ERROR)
    if abs(sum(alphas.values()) - 1.0) > ALPHA_SUM_TOLERANCE:
        raise ValueError(_FUSION_ALPHAS_ERROR)
    return {arm: float(alphas[arm]) for arm in ARM_NAMES}


def fusion_mode_from_env() -> str:
    return parse_fusion_mode(os.environ.get("RECALL_SEARCH_FUSION"))


def fusion_alphas_from_env() -> dict[str, float]:
    return parse_fusion_alphas(os.environ.get("RECALL_SEARCH_FUSION_ALPHAS"))


def rank_score(rank: int, *, k: int = RRF_K) -> float:
    """Rank fallback: 1.0 at the top, decaying gently with rank."""

    return (k + 1) / (k + rank)


def leg_document_scores(
    rows: Iterable[dict[str, Any]],
    *,
    mode: str,
) -> tuple[dict[str, dict[str, float | int]], bool]:
    """Score each distinct document of one arm.

    Returns ``({document_id: {"score", "rank", "normalized"}}, normalized)``
    where ``score`` is the arm's best raw passage score for the document,
    ``rank`` the document's first position in the arm, and ``normalized`` the
    value the fused score uses. ``normalized`` (the flag) says whether min-max
    was applied or the rank fallback was used.
    """

    documents: dict[str, dict[str, float | int]] = {}
    for rank, row in enumerate(rows, start=1):
        document_id = row["logical_document_id"]
        score = float(row["score"])
        entry = documents.get(document_id)
        if entry is None:
            documents[document_id] = {"score": score, "rank": rank}
        elif score > entry["score"]:
            entry["score"] = score
    if mode == "rrf":
        for entry in documents.values():
            entry["normalized"] = 1.0 / (RRF_K + int(entry["rank"]))
        return documents, False
    raw = [float(entry["score"]) for entry in documents.values()]
    recency_mode = bool(raw) and all(value == 0.0 for value in raw)
    use_minmax = (
        len(documents) >= MIN_MINMAX_DOCUMENTS
        and not recency_mode
        and all(math.isfinite(value) for value in raw)
    )
    if use_minmax:
        low, high = min(raw), max(raw)
        spread = high - low
        for entry in documents.values():
            entry["normalized"] = (
                (float(entry["score"]) - low) / spread if spread > 0 else 1.0
            )
        return documents, True
    for entry in documents.values():
        entry["normalized"] = rank_score(int(entry["rank"]))
    return documents, False


def fuse_arm_scores(
    arm_scores: dict[str, dict[str, Any]],
    alphas: dict[str, float],
) -> float:
    """Convex fused score from per-arm ``normalized`` values (offline replay)."""

    return sum(
        float(alphas.get(arm, 0.0)) * float(entry["normalized"])
        for arm, entry in arm_scores.items()
        if isinstance(entry, dict) and "normalized" in entry
    )
