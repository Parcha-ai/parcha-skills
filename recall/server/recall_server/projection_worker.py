"""One durable loop for Recall's authoritative retrieval projections."""

from __future__ import annotations

import logging
import time
import urllib.error
from collections.abc import Callable
from http.client import RemoteDisconnected
from typing import Any

from .logical_evidence_projection import CanonicalLogicalEvidenceProjector
from .passage_index import CanonicalPassageProjector
from .parquet_scan import CanonicalParquetScanProjector


LOG = logging.getLogger(__name__)


def run_projection_worker(
    logical: CanonicalLogicalEvidenceProjector,
    passages: CanonicalPassageProjector,
    scan: CanonicalParquetScanProjector | None = None,
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
        embedding_error = 0
        try:
            embedded = passages.embed_pending(
                tenant_id=tenant_id,
                batch_size=embedding_batch_size,
                max_batches=max_batches_per_cycle,
            )
        except (
            ConnectionError,
            RemoteDisconnected,
            TimeoutError,
            urllib.error.URLError,
        ) as error:
            # The embedding provider is an external dependency. Preserve the
            # durable worker and retry next cycle instead of restarting every
            # projection stage because one request was disconnected.
            embedding_error = 1
            embedded = {"status": "unavailable", "processed": 0}
            LOG.warning(
                "projection embedding unavailable type=%s",
                type(error).__name__,
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
        # Parquet shards are source/month materializations of the authoritative
        # logical documents. During a large retrofit, every logical batch can
        # dirty the same shards. Wait until that queue drains so each dirty
        # shard is rebuilt once instead of rewriting the corpus every cycle.
        scanned = (
            scan.project_pending(
                tenant_id=tenant_id,
                batch_size=min(4, logical_batch_size),
                max_batches=max_batches_per_cycle,
            )
            if (
                scan is not None
                and int(documents.get("pending", 0)) == 0
                and projected["status"] == "complete"
                and int(projected["documents"]) == 0
            )
            else {
                "status": "deferred" if scan is not None else "complete",
                "shards": 0,
                "rows": 0,
                "stale": 0,
                "contended": 0,
            }
        )
        result: dict[str, int | str] = {
            "status": (
                "complete"
                if documents["status"] == "complete"
                and projected["status"] == "complete"
                and embedded["status"] in {"complete", "disabled"}
                and scanned["status"] == "complete"
                and int(documents["documents"]) == 0
                and int(projected["passages"]) == 0
                and int(scanned["shards"]) == 0
                and int(scanned["stale"]) == 0
                and int(scanned["contended"]) == 0
                else "pending"
            ),
            "documents": int(documents["documents"]),
            "logical_pending": int(documents.get("pending", 0)),
            "records": int(documents["records"]),
            "passage_documents": int(projected["documents"]),
            "passage_requeued": int(projected.get("requeued", 0)),
            "passages": int(projected["passages"]),
            "embedded": int(embedded["processed"]),
            "embedding_error": embedding_error,
            "parquet_shards": int(scanned["shards"]),
            "parquet_rows": int(scanned["rows"]),
            "parquet_stale": int(scanned["stale"]),
            "parquet_contended": int(scanned["contended"]),
            "stale": int(projected["stale"]),
            "pruned": int(documents["pruned"]),
            "cleanup_failures": int(documents["cleanup_failures"]),
        }
        LOG.info(
            "projection cycle status=%s documents=%s logical_pending=%s records=%s "
            "passage_documents=%s passage_requeued=%s passages=%s embedded=%s "
            "embedding_error=%s "
            "parquet_shards=%s "
            "parquet_rows=%s parquet_stale=%s parquet_contended=%s "
            "stale=%s pruned=%s "
            "cleanup_failures=%s",
            *(
                result[key]
                for key in (
                    "status",
                    "documents",
                    "logical_pending",
                    "records",
                    "passage_documents",
                    "passage_requeued",
                    "passages",
                    "embedded",
                    "embedding_error",
                    "parquet_shards",
                    "parquet_rows",
                    "parquet_stale",
                    "parquet_contended",
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
                "parquet_shards",
                "parquet_stale",
                "stale",
                "pruned",
            )
        ):
            sleep(interval_seconds)
