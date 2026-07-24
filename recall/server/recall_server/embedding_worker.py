from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

from .canonical_retrieval import (
    MAX_CANONICAL_EMBEDDING_BATCH,
    CanonicalRetrieval,
)

LOG = logging.getLogger(__name__)


def run_canonical_embedding_worker(
    retrieval: CanonicalRetrieval,
    *,
    tenant_id: str | None,
    batch_size: int,
    max_batches_per_cycle: int,
    interval_seconds: float,
    once: bool = False,
    sleep: Callable[[float], Any] = time.sleep,
) -> dict[str, int | str]:
    """Drain canonical embedding lag outside latency-sensitive ingest requests."""

    if not 1 <= batch_size <= MAX_CANONICAL_EMBEDDING_BATCH:
        raise ValueError(
            "embedding worker batch size must be between 1 and "
            f"{MAX_CANONICAL_EMBEDDING_BATCH}"
        )
    if not 1 <= max_batches_per_cycle <= 100:
        raise ValueError(
            "embedding worker max batches per cycle must be between 1 and 100"
        )
    if not 0.1 <= interval_seconds <= 300:
        raise ValueError(
            "embedding worker interval seconds must be between 0.1 and 300"
        )
    while True:
        result = retrieval.embed_pending(
            tenant_id=tenant_id,
            batch_size=batch_size,
            max_batches=max_batches_per_cycle,
        )
        LOG.info(
            "canonical embedding cycle status=%s processed=%s batches=%s",
            result["status"],
            result["processed"],
            result["batches"],
        )
        if once:
            return result
        if result["processed"] == 0:
            sleep(interval_seconds)
