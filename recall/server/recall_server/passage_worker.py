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
        }
        LOG.info(
            "passage cycle status=%s documents=%s passages=%s "
            "embedded=%s stale=%s",
            result["status"],
            result["documents"],
            result["passages"],
            result["embedded"],
            result["stale"],
        )
        if once:
            return result
        if not any(
            int(result[key])
            for key in ("documents", "passages", "embedded", "stale")
        ):
            sleep(interval_seconds)
