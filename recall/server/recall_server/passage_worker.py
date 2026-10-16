"""Bounded worker loop for logical passage projection and embeddings."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

from .passage_index import CanonicalPassageProjector


LOG = logging.getLogger(__name__)


def run_passage_worker(
    projector: CanonicalPassageProjector,
    *,
    tenant_id: str | None,
    projection_batch_size: int,
    embedding_batch_size: int,
    max_batches_per_cycle: int,
    concurrency: int,
    interval_seconds: float,
    once: bool = False,
    sleep: Callable[[float], Any] = time.sleep,
) -> dict[str, int | str]:
    if not 0.1 <= interval_seconds <= 300:
        raise ValueError("passage worker interval is invalid")
    while True:
        projected = projector.project_pending(
            tenant_id=tenant_id,
            batch_size=projection_batch_size,
            max_batches=max_batches_per_cycle,
            concurrency=concurrency,
        )
        embedded = projector.embed_pending(
            tenant_id=tenant_id,
            batch_size=embedding_batch_size,
            max_batches=max_batches_per_cycle,
        )
        result: dict[str, int | str] = {
            "status": (
                "complete"
                if projected["status"] == "complete"
                and embedded["status"] in {"complete", "disabled"}
                else "pending"
            ),
            "documents": int(projected["documents"]),
            "passages": int(projected["passages"]),
            "embedded": int(embedded["processed"]),
            "stale": int(projected["stale"]),
            "membership_certified_passages": int(projected.get("membership_certified_passages", 0)),
            "membership_failed_documents": int(projected.get("membership_failed_documents", 0)),
            "membership_deferred_documents": int(projected.get("membership_deferred_documents", 0)),
            "membership_elapsed_ms": int(projected.get("membership_elapsed_ms", 0)),
        }
        LOG.info(
            "passage cycle status=%s documents=%s passages=%s "
            "embedded=%s stale=%s "
            "membership_certified_passages=%s membership_failed_documents=%s "
            "membership_deferred_documents=%s membership_elapsed_ms=%s",
            result["status"],
            result["documents"],
            result["passages"],
            result["embedded"],
            result["stale"],
            result["membership_certified_passages"],
            result["membership_failed_documents"],
            result["membership_deferred_documents"],
            result["membership_elapsed_ms"],
        )
        if once:
            return result
        if not any(
            int(result[key])
            for key in ("documents", "passages", "embedded", "stale")
        ):
            sleep(interval_seconds)
