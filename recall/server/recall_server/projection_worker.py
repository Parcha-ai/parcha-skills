"""One durable loop for Recall's authoritative retrieval projections."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

from .logical_evidence_projection import CanonicalLogicalEvidenceProjector
from .passage_index import CanonicalPassageProjector


LOG = logging.getLogger(__name__)


def run_projection_worker(
    logical: CanonicalLogicalEvidenceProjector,
    passages: CanonicalPassageProjector,
    *,
    tenant_id: str,
    logical_batch_size: int,
    passage_batch_size: int,
    embedding_batch_size: int,
    max_batches_per_cycle: int,
    upload_concurrency: int,
    passage_concurrency: int,
    interval_seconds: float,
    once: bool = False,
    sleep: Callable[[float], Any] = time.sleep,
) -> dict[str, int | str]:
    """Service every projection stage without upstream backfill starvation."""

    if not 0.1 <= interval_seconds <= 300:
        raise ValueError("projection worker interval is invalid")
    while True:
        # Drain already-ready downstream work before an expensive logical
        # document batch. New upstream output becomes eligible next cycle;
        # dependency correctness stays in each projector while searchable
        # freshness no longer waits behind an unbounded backfill.
        embedded = passages.embed_pending(
            tenant_id=tenant_id,
            batch_size=embedding_batch_size,
            max_batches=max_batches_per_cycle,
        )
        projected = passages.project_pending(
            tenant_id=tenant_id,
            batch_size=passage_batch_size,
            max_batches=max_batches_per_cycle,
            concurrency=passage_concurrency,
        )
        documents = logical.project_pending(
            tenant_id=tenant_id,
            batch_size=logical_batch_size,
            max_batches=max_batches_per_cycle,
            upload_concurrency=upload_concurrency,
        )
        result: dict[str, int | str] = {
            "status": (
                "complete"
                if documents["status"] == "complete"
                and projected["status"] == "complete"
                and embedded["status"] in {"complete", "disabled"}
                and int(documents["documents"]) == 0
                and int(projected["passages"]) == 0
                else "pending"
            ),
            "documents": int(documents["documents"]),
            "records": int(documents["records"]),
            "passage_documents": int(projected["documents"]),
            "passages": int(projected["passages"]),
            "embedded": int(embedded["processed"]),
            "stale": int(projected["stale"]),
            "pruned": int(documents["pruned"]),
            "cleanup_failures": int(documents["cleanup_failures"]),
        }
        LOG.info(
            "projection cycle status=%s documents=%s records=%s "
            "passage_documents=%s passages=%s embedded=%s stale=%s pruned=%s "
            "cleanup_failures=%s",
            *(
                result[key]
                for key in (
                    "status",
                    "documents",
                    "records",
                    "passage_documents",
                    "passages",
                    "embedded",
                    "stale",
                    "pruned",
                    "cleanup_failures",
                )
            ),
        )
        if once:
            return result
        if not any(
            int(result[key])
            for key in (
                "documents",
                "passage_documents",
                "passages",
                "embedded",
                "stale",
                "pruned",
            )
        ):
            sleep(interval_seconds)
