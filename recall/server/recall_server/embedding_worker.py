from __future__ import annotations

import logging
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from .canonical_retrieval import (
    MAX_CANONICAL_EMBEDDING_BATCH,
    CanonicalRetrieval,
)

LOG = logging.getLogger(__name__)


def _tenant_ids(retrieval: CanonicalRetrieval) -> tuple[str, ...]:
    with retrieval.store.connect() as connection:
        rows = connection.execute(
            """SELECT DISTINCT tenant_id
               FROM canonical_sources
               ORDER BY tenant_id"""
        ).fetchall()
    return tuple(row["tenant_id"] for row in rows)


def _run_cycle(
    retrieval: CanonicalRetrieval,
    *,
    tenant_id: str | None,
    parallel_tenants: int,
    batch_size: int,
    max_batches_per_cycle: int,
) -> dict[str, int | str]:
    if parallel_tenants == 1:
        return retrieval.embed_pending(
            tenant_id=tenant_id,
            batch_size=batch_size,
            max_batches=max_batches_per_cycle,
        )
    tenants = _tenant_ids(retrieval)
    if not tenants:
        return {"status": "complete", "processed": 0, "batches": 0}

    def drain(bound_tenant_id: str) -> dict[str, int | str]:
        return retrieval.embed_pending(
            tenant_id=bound_tenant_id,
            batch_size=batch_size,
            max_batches=max_batches_per_cycle,
        )

    with ThreadPoolExecutor(
        max_workers=min(parallel_tenants, len(tenants)),
        thread_name_prefix="canonical-embedding",
    ) as executor:
        results = list(executor.map(drain, tenants))
    return {
        "status": (
            "complete"
            if any(result["status"] == "complete" for result in results)
            else "busy"
        ),
        "processed": sum(int(result["processed"]) for result in results),
        "batches": sum(int(result["batches"]) for result in results),
    }


def run_canonical_embedding_worker(
    retrieval: CanonicalRetrieval,
    *,
    tenant_id: str | None,
    parallel_tenants: int = 1,
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
    if not 1 <= parallel_tenants <= 8:
        raise ValueError(
            "embedding worker parallel tenants must be between 1 and 8"
        )
    if tenant_id is not None and parallel_tenants != 1:
        raise ValueError(
            "embedding worker cannot combine one tenant with parallel tenants"
        )
    if not 0.1 <= interval_seconds <= 300:
        raise ValueError(
            "embedding worker interval seconds must be between 0.1 and 300"
        )
    while True:
        result = _run_cycle(
            retrieval,
            tenant_id=tenant_id,
            parallel_tenants=parallel_tenants,
            batch_size=batch_size,
            max_batches_per_cycle=max_batches_per_cycle,
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
