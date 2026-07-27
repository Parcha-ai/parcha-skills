from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

from .evidence_projection import CanonicalEvidenceProjector
from .logical_evidence_projection import CanonicalLogicalEvidenceProjector

LOG = logging.getLogger(__name__)


def run_canonical_evidence_worker(
    projector: CanonicalEvidenceProjector,
    *,
    tenant_id: str | None,
    batch_size: int,
    max_batches_per_cycle: int,
    interval_seconds: float,
    once: bool = False,
    sleep: Callable[[float], Any] = time.sleep,
) -> dict[str, int | str]:
    if not 1 <= batch_size <= 1000:
        raise ValueError("evidence worker batch size must be between 1 and 1000")
    if not 1 <= max_batches_per_cycle <= 100:
        raise ValueError(
            "evidence worker max batches per cycle must be between 1 and 100"
        )
    if not 0.1 <= interval_seconds <= 300:
        raise ValueError(
            "evidence worker interval seconds must be between 0.1 and 300"
        )
    while True:
        result = projector.project_pending(
            tenant_id=tenant_id,
            batch_size=batch_size,
            max_batches=max_batches_per_cycle,
        )
        LOG.info(
            "canonical evidence cycle status=%s processed=%s batches=%s",
            result["status"],
            result["processed"],
            result["batches"],
        )
        if once:
            return result
        if result["processed"] == 0:
            sleep(interval_seconds)


def run_logical_evidence_worker(
    projector: CanonicalLogicalEvidenceProjector,
    *,
    tenant_id: str | None,
    batch_size: int,
    max_batches_per_cycle: int,
    upload_concurrency: int,
    interval_seconds: float,
    once: bool = False,
    sleep: Callable[[float], Any] = time.sleep,
) -> dict[str, int | str]:
    """Continuously materialize dirty source sessions as bounded S3 documents."""

    if not 1 <= batch_size <= 2_000:
        raise ValueError(
            "logical evidence worker batch size must be between 1 and 2000"
        )
    if not 1 <= max_batches_per_cycle <= 100:
        raise ValueError(
            "logical evidence worker max batches per cycle must be between 1 and 100"
        )
    if not 1 <= upload_concurrency <= 32:
        raise ValueError(
            "logical evidence worker upload concurrency must be between 1 and 32"
        )
    if not 0.1 <= interval_seconds <= 300:
        raise ValueError(
            "logical evidence worker interval seconds must be between 0.1 and 300"
        )
    seeded = projector.seed_backfill(tenant_id=tenant_id)
    while True:
        result = projector.project_pending(
            tenant_id=tenant_id,
            batch_size=batch_size,
            max_batches=max_batches_per_cycle,
            upload_concurrency=upload_concurrency,
        )
        LOG.info(
            "logical evidence cycle status=%s documents=%s records=%s "
            "batches=%s cleanup_failures=%s",
            result["status"],
            result["documents"],
            result["records"],
            result["batches"],
            result["cleanup_failures"],
        )
        if once:
            return {"seeded": seeded, **result}
        seeded = 0
        if result["documents"] == 0 and result["pruned"] == 0:
            sleep(interval_seconds)
