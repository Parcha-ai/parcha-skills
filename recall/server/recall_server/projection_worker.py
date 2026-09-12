"""One durable loop for Recall's authoritative retrieval projections."""

from __future__ import annotations

import logging
import threading
import time
import urllib.error
from collections.abc import Callable
from http.client import RemoteDisconnected
from typing import Any

from .logical_evidence_projection import CanonicalLogicalEvidenceProjector
from .passage_index import CanonicalPassageProjector
from .parquet_scan import CanonicalParquetScanProjector


LOG = logging.getLogger(__name__)

# Per-process churn totals. The worker and the web service are separate
# processes, so these only appear on /metrics when a worker serves it; the
# DB-derived gauges in ``BrainStore.service_metrics`` cover the other case.
PROJECTION_TOTALS: dict[str, int] = {
    "passages_written": 0,
    "documents_projected": 0,
    "passages_embedded": 0,
    "parquet_rows_written": 0,
    "bodies_thinned": 0,
    # Wall-clock spent per phase, in milliseconds, so a slow cycle can be
    # attributed to embedding, passages, logical (including S3 cleanup),
    # parquet, or thinning from /metrics alone.
    "cycle_elapsed_ms": 0,
    "embed_elapsed_ms": 0,
    "passage_elapsed_ms": 0,
    "logical_elapsed_ms": 0,
    "parquet_elapsed_ms": 0,
    "thin_elapsed_ms": 0,
}
PROJECTION_TOTALS_LOCK = threading.Lock()
_CYCLE_TO_TOTAL = {
    "passages": "passages_written",
    "passage_documents": "documents_projected",
    "embedded": "passages_embedded",
    "parquet_rows": "parquet_rows_written",
    "canonical_bodies_thinned": "bodies_thinned",
}
PHASE_ELAPSED_KEYS = (
    "cycle_elapsed_ms",
    "embed_elapsed_ms",
    "passage_elapsed_ms",
    "logical_elapsed_ms",
    "parquet_elapsed_ms",
    "thin_elapsed_ms",
)
_CYCLE_TO_TOTAL.update({key: key for key in PHASE_ELAPSED_KEYS})


def record_cycle(result: dict[str, int | str]) -> None:
    """Fold one projection cycle into the process-lifetime totals."""

    with PROJECTION_TOTALS_LOCK:
        for cycle_key, total_key in _CYCLE_TO_TOTAL.items():
            PROJECTION_TOTALS[total_key] += max(0, int(result.get(cycle_key, 0)))


def projection_totals() -> dict[str, int]:
    with PROJECTION_TOTALS_LOCK:
        return dict(PROJECTION_TOTALS)


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
    body_thinner: Callable[[bool], dict[str, Any]] | None = None,
    quiet_seconds: float = 0.0,
    max_wait_seconds: float = 0.0,
    clock: Callable[[], float] = time.monotonic,
    parquet_every_cycles: int = 3,
    cleanup_concurrency: int = 8,
) -> dict[str, int | str]:
    """Service every projection stage without upstream backfill starvation."""

    if not 0.1 <= interval_seconds <= 300:
        raise ValueError("projection worker interval is invalid")
    if (
        isinstance(parquet_every_cycles, bool)
        or not isinstance(parquet_every_cycles, int)
        or not 1 <= parquet_every_cycles <= 1000
        or isinstance(cleanup_concurrency, bool)
        or not isinstance(cleanup_concurrency, int)
        or not 1 <= cleanup_concurrency <= 64
    ):
        raise ValueError("projection worker budget is invalid")
    cycles_since_parquet = 0

    def elapsed_ms(started: float) -> int:
        return max(0, int(round((clock() - started) * 1000)))

    while True:
        cycle_started = clock()
        # Drain already-ready downstream work before an expensive logical
        # document batch. New upstream output becomes eligible next cycle;
        # dependency correctness stays in each projector while searchable
        # freshness no longer waits behind an unbounded backfill.
        embedding_error = 0
        phase_started = clock()
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
        embed_elapsed_ms = elapsed_ms(phase_started)
        phase_started = clock()
        projected = passages.project_pending(
            tenant_id=tenant_id,
            batch_size=passage_batch_size,
            max_batches=max_batches_per_cycle,
            concurrency=passage_concurrency,
        )
        passage_elapsed_ms = elapsed_ms(phase_started)
        phase_started = clock()
        documents = logical.project_pending(
            tenant_id=tenant_id,
            batch_size=logical_batch_size,
            max_batches=max_batches_per_cycle,
            upload_concurrency=upload_concurrency,
            quiet_seconds=quiet_seconds,
            max_wait_seconds=max_wait_seconds,
            cleanup_concurrency=cleanup_concurrency,
        )
        logical_elapsed_ms = elapsed_ms(phase_started)
        phase_started = clock()
        # Parquet shards are source/month materializations of the authoritative
        # logical documents. Prefer to run them once the upstream queues have
        # drained so a dirty month is rebuilt once, but never starve them: with
        # steady ingestion plus debounce the logical queue is never empty, and
        # the scan plane went 4 days without a rebuild in production. Every
        # `parquet_every_cycles` cycles the (now fragment-level) rebuild runs
        # regardless of the backlog.
        cycles_since_parquet += 1
        parquet_due = (
            int(documents.get("pending", 0)) == 0
            and projected["status"] == "complete"
            and int(projected["documents"]) == 0
        ) or cycles_since_parquet >= parquet_every_cycles
        if scan is not None and parquet_due:
            cycles_since_parquet = 0
        scanned = (
            scan.project_pending(
                tenant_id=tenant_id,
                batch_size=min(4, logical_batch_size),
                max_batches=max_batches_per_cycle,
            )
            if scan is not None and parquet_due
            else {
                "status": "deferred" if scan is not None else "complete",
                "shards": 0,
                "rows": 0,
                "stale": 0,
                "contended": 0,
                "fragments_rewritten": 0,
                "fragments_total": 0,
                "documents_dirty": 0,
            }
        )
        parquet_elapsed_ms = elapsed_ms(phase_started)
        phase_started = clock()
        # The thinner has its own row-level authority gates: live S3 raw data,
        # an S3 logical manifest, retained searchable chunks, and no queued
        # reprojection for that source group. Run one bounded batch every cycle
        # so steady ingestion cannot permanently prevent safe rows from being
        # thinned merely because an unrelated global queue is non-empty. While
        # freshness work is queued the thinner is told it is busy so it takes
        # a small batch: measured in production, a 1000-body batch held the
        # cycle for ~12 minutes while the logical queue grew.
        thin_busy = (
            int(documents.get("pending", 0)) > 0
            or int(projected.get("pending", 0)) > 0
        )
        thinned = (
            body_thinner(thin_busy)
            if body_thinner is not None
            else {
                "status": "deferred" if body_thinner is not None else "complete",
                "documents": 0,
                "refused": 0,
                "document_bytes_removed": 0,
                "event_bytes_replaced": 0,
            }
        )
        thin_elapsed_ms = elapsed_ms(phase_started)
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
                and thinned["status"] == "complete"
                else "pending"
            ),
            "documents": int(documents["documents"]),
            "logical_repaired": int(documents.get("repaired", 0)),
            "logical_pending": int(documents.get("pending", 0)),
            "logical_waiting": int(documents.get("waiting", 0)),
            "records": int(documents["records"]),
            "passage_documents": int(projected["documents"]),
            "passage_pending": int(projected.get("pending", 0)),
            "passage_requeued": int(projected.get("requeued", 0)),
            "passage_unavailable": int(projected.get("unavailable", 0)),
            # ``passages`` stays the number of passage rows written
            # (inserted) for the churn probe and the idle check; the
            # differential commit (H1-T3) also reports what it deleted and
            # kept in place.
            "passages": int(projected["passages"]),
            "passages_inserted": int(projected.get("inserted", projected["passages"])),
            "passages_deleted": int(projected.get("deleted", 0)),
            "passages_retained": int(projected.get("retained", 0)),
            "embedded": int(embedded["processed"]),
            "embedding_error": embedding_error,
            "parquet_shards": int(scanned["shards"]),
            "parquet_rows": int(scanned["rows"]),
            "parquet_stale": int(scanned["stale"]),
            "parquet_contended": int(scanned["contended"]),
            "parquet_fragments_rewritten": int(scanned.get("fragments_rewritten", 0)),
            "parquet_fragments_total": int(scanned.get("fragments_total", 0)),
            "parquet_documents_dirty": int(scanned.get("documents_dirty", 0)),
            "canonical_bodies_thinned": int(thinned["documents"]),
            "thin_mode": "busy" if thin_busy else "idle",
            "canonical_bodies_refused": int(thinned["refused"]),
            "canonical_document_bytes_removed": int(
                thinned["document_bytes_removed"]
            ),
            "canonical_event_bytes_replaced": int(
                thinned["event_bytes_replaced"]
            ),
            "stale": int(projected["stale"]),
            "pruned": int(documents["pruned"]),
            "cleanup_failures": int(documents["cleanup_failures"]),
            "logical_cleanup_completed": int(documents.get("cleanup_completed", 0)),
            "logical_cleanup_pending": int(documents.get("cleanup_pending", 0)),
            "old_objects_deleted": int(documents.get("old_objects_deleted", 0)),
            "cycle_elapsed_ms": elapsed_ms(cycle_started),
            "embed_elapsed_ms": embed_elapsed_ms,
            "passage_elapsed_ms": passage_elapsed_ms,
            "logical_elapsed_ms": logical_elapsed_ms,
            "parquet_elapsed_ms": parquet_elapsed_ms,
            "thin_elapsed_ms": thin_elapsed_ms,
        }
        record_cycle(result)
        LOG.info(
            "projection cycle status=%s documents=%s logical_repaired=%s "
            "logical_pending=%s logical_waiting=%s records=%s "
            "passage_documents=%s passage_pending=%s "
            "passage_requeued=%s passage_unavailable=%s "
            "passages=%s passages_inserted=%s passages_deleted=%s "
            "passages_retained=%s embedded=%s "
            "embedding_error=%s "
            "parquet_shards=%s "
            "parquet_rows=%s parquet_stale=%s parquet_contended=%s "
            "parquet_fragments_rewritten=%s parquet_fragments_total=%s "
            "parquet_documents_dirty=%s "
            "canonical_bodies_thinned=%s canonical_bodies_refused=%s "
            "thin_mode=%s "
            "canonical_document_bytes_removed=%s "
            "canonical_event_bytes_replaced=%s "
            "stale=%s pruned=%s "
            "cleanup_failures=%s "
            "logical_cleanup_completed=%s logical_cleanup_pending=%s "
            "old_objects_deleted=%s "
            "cycle_elapsed_ms=%s embed_elapsed_ms=%s passage_elapsed_ms=%s "
            "logical_elapsed_ms=%s parquet_elapsed_ms=%s thin_elapsed_ms=%s",
            *(
                result[key]
                for key in (
                    "status",
                    "documents",
                    "logical_repaired",
                    "logical_pending",
                    "logical_waiting",
                    "records",
                    "passage_documents",
                    "passage_pending",
                    "passage_requeued",
                    "passage_unavailable",
                    "passages",
                    "passages_inserted",
                    "passages_deleted",
                    "passages_retained",
                    "embedded",
                    "embedding_error",
                    "parquet_shards",
                    "parquet_rows",
                    "parquet_stale",
                    "parquet_contended",
                    "parquet_fragments_rewritten",
                    "parquet_fragments_total",
                    "parquet_documents_dirty",
                    "canonical_bodies_thinned",
                    "canonical_bodies_refused",
                    "thin_mode",
                    "canonical_document_bytes_removed",
                    "canonical_event_bytes_replaced",
                    "stale",
                    "pruned",
                    "cleanup_failures",
                    "logical_cleanup_completed",
                    "logical_cleanup_pending",
                    "old_objects_deleted",
                    *PHASE_ELAPSED_KEYS,
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
                "canonical_bodies_thinned",
                "stale",
                "pruned",
            )
        ):
            sleep(interval_seconds)
