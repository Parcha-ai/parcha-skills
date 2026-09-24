"""One durable loop for Recall's authoritative retrieval projections."""

from __future__ import annotations

import logging
import threading
import time
import urllib.error
from collections.abc import Callable
from http.client import RemoteDisconnected
from typing import Any

from .embedding_ledger import (
    count_unembedded_passages,
    record_embedded,
    validate_daily_cap,
    window_total,
)
from .logical_evidence_projection import CanonicalLogicalEvidenceProjector
from .search_outbox import search_outbox_pending
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
    "search_plane_rows_written": 0,
    "bodies_thinned": 0,
    # Wall-clock spent per phase, in milliseconds, so a slow cycle can be
    # attributed to embedding, passages, logical (including S3 cleanup),
    # parquet, or thinning from /metrics alone.
    "cycle_elapsed_ms": 0,
    "embed_elapsed_ms": 0,
    "passage_elapsed_ms": 0,
    "logical_elapsed_ms": 0,
    "parquet_elapsed_ms": 0,
    "search_plane_elapsed_ms": 0,
    "thin_elapsed_ms": 0,
}
PROJECTION_TOTALS_LOCK = threading.Lock()
_CYCLE_TO_TOTAL = {
    "passages": "passages_written",
    "passage_documents": "documents_projected",
    "embedded": "passages_embedded",
    "parquet_rows": "parquet_rows_written",
    "search_plane_rows": "search_plane_rows_written",
    "canonical_bodies_thinned": "bodies_thinned",
}
PHASE_ELAPSED_KEYS = (
    "cycle_elapsed_ms",
    "embed_elapsed_ms",
    "passage_elapsed_ms",
    "logical_elapsed_ms",
    "parquet_elapsed_ms",
    "search_plane_elapsed_ms",
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
    parquet_max_wait_seconds: float = 900.0,
    cleanup_concurrency: int = 8,
    max_cycles: int | None = None,
    skip_embedding: bool = False,
    search_plane: Callable[[], dict[str, Any]] | None = None,
) -> dict[str, int | str]:
    """Service every projection stage without upstream backfill starvation.

    ``search_plane`` (H3-b) drains the search projection outbox into
    turbopuffer before logical work and on ready parent completions; ``None`` (no
    turbopuffer settings, or ``--search-plane off``) skips the phase and the
    cycle reports zeros for it. Each publication tick retains the configured
    passage batch/concurrency and one bounded search drain. Completions are
    coalesced by the logical coordinator; downstream calls never overlap.

    With ``skip_embedding`` (H5-2) the embedding phase is left to the
    dedicated ``embedding-worker`` process: the cycle reports
    ``embedded=0 embed_elapsed_ms=0``, the idle check ignores embedding, and
    every other phase is unchanged. On the turbopuffer search plane (H3-e')
    the phase is always skipped: turbopuffer embeds natively and the
    Postgres embeddings table is retired.
    """

    if not 0.1 <= interval_seconds <= 300:
        raise ValueError("projection worker interval is invalid")
    if getattr(getattr(passages, "store", None), "search_plane", "postgres") == "turbopuffer":
        skip_embedding = True
    if (
        isinstance(parquet_every_cycles, bool)
        or not isinstance(parquet_every_cycles, int)
        or not 1 <= parquet_every_cycles <= 1000
        or isinstance(parquet_max_wait_seconds, bool)
        or not isinstance(parquet_max_wait_seconds, (int, float))
        or not 1 <= parquet_max_wait_seconds <= 86_400
        or isinstance(cleanup_concurrency, bool)
        or not isinstance(cleanup_concurrency, int)
        or not 1 <= cleanup_concurrency <= 64
    ):
        raise ValueError("projection worker budget is invalid")
    cycles_since_parquet = 0
    last_parquet_started = clock()
    last_busy_thin_cycle: int | None = None
    last_busy_thin_started = 0.0

    def elapsed_ms(started: float) -> int:
        return max(0, int(round((clock() - started) * 1000)))

    cycles = 0
    while True:
        cycles += 1
        cycle_started = phase_started = clock()
        active_phase = "setup"
        phase_elapsed = {key: 0 for key in PHASE_ELAPSED_KEYS}
        try:
            # Drain already-ready downstream work before an expensive logical
            # document batch. New upstream output becomes eligible next cycle;
            # dependency correctness stays in each projector while searchable
            # freshness no longer waits behind an unbounded backfill.
            embedding_error = 0
            active_phase = "embed"
            phase_started = clock()
            try:
                embedded = (
                    {"status": "skipped", "processed": 0}
                    if skip_embedding
                    else passages.embed_pending(
                        tenant_id=tenant_id,
                        batch_size=embedding_batch_size,
                        max_batches=max_batches_per_cycle,
                    )
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
            embed_elapsed_ms = 0 if skip_embedding else elapsed_ms(phase_started)
            phase_elapsed["embed_elapsed_ms"] = embed_elapsed_ms
            projected: dict[str, Any] = {}
            searched: dict[str, Any] = {}
            passage_elapsed_ms = search_plane_elapsed_ms = 0

            def accumulate(total, current):
                # Queue depth/status describe the newest tick; work and elapsed
                # counters describe the whole cycle, including earlier ticks.
                for key, value in current.items():
                    if key in {"status", "pending"}:
                        total[key] = value
                    elif isinstance(value, int):
                        total[key] = int(total.get(key, 0)) + value

            search_ticks = 0

            def publish_search():
                nonlocal active_phase, phase_started, search_ticks
                nonlocal search_plane_elapsed_ms
                previous_phase, previous_started = active_phase, phase_started
                if previous_phase == "passage":
                    phase_elapsed["passage_elapsed_ms"] = passage_elapsed_ms + elapsed_ms(previous_started)
                elif previous_phase == "logical":
                    phase_elapsed["logical_elapsed_ms"] = elapsed_ms(previous_started)
                active_phase = "search_plane"
                phase_started = clock()
                tick = (
                    search_plane()
                    if search_plane is not None
                    else {
                        "status": "skipped", "months": 0, "rows": 0,
                        "deleted": 0, "failed": 0, "rate_limited": 0, "pending": 0,
                    }
                )
                search_plane_elapsed_ms += elapsed_ms(phase_started) if search_plane is not None else 0
                phase_elapsed["search_plane_elapsed_ms"] = search_plane_elapsed_ms
                accumulate(searched, tick)
                search_ticks += 1
                # Owner phase wall time excludes search callbacks, although
                # owners can keep preparing and committing during publication.
                if previous_phase in {"passage", "logical"}:
                    previous_started += clock() - phase_started
                if previous_phase == "passage":
                    # The snapshot above is needed if search fails. Once it
                    # succeeds, an eventual passage failure adds its whole
                    # current tick to completed ticks, not to that snapshot.
                    phase_elapsed["passage_elapsed_ms"] = passage_elapsed_ms
                elif previous_phase == "logical":
                    phase_elapsed["logical_elapsed_ms"] = 0
                active_phase, phase_started = previous_phase, previous_started

            def publish_ready():
                nonlocal active_phase, phase_started, passage_elapsed_ms
                previous_phase, previous_started = active_phase, phase_started
                publication_started = clock()
                if previous_phase == "logical":
                    phase_elapsed["logical_elapsed_ms"] = elapsed_ms(previous_started)
                active_phase = "passage"
                phase_started = clock()
                before_search_ticks = search_ticks
                tick = passages.project_pending(
                    tenant_id=tenant_id,
                    batch_size=passage_batch_size,
                    max_batches=max_batches_per_cycle,
                    concurrency=passage_concurrency,
                    on_progress=publish_search if search_plane is not None else None,
                )
                passage_elapsed_ms += elapsed_ms(phase_started)
                phase_elapsed["passage_elapsed_ms"] = passage_elapsed_ms
                accumulate(projected, tick)
                phase_started = clock()
                if search_ticks == before_search_ticks:
                    # Drain pre-existing outbox work even when passages were idle.
                    publish_search()
                if previous_phase == "logical":
                    previous_started += clock() - publication_started
                active_phase, phase_started = previous_phase, previous_started

            # Drain existing work first, then publish completed parents while
            # unrelated preparation continues in the bounded logical executor.
            publish_ready()
            active_phase = "logical"
            phase_started = clock()
            documents = logical.project_pending(
                tenant_id=tenant_id,
                batch_size=logical_batch_size,
                max_batches=max_batches_per_cycle,
                upload_concurrency=upload_concurrency,
                quiet_seconds=quiet_seconds,
                max_wait_seconds=max_wait_seconds,
                cleanup_concurrency=cleanup_concurrency,
                on_progress=publish_ready,
                on_heartbeat=publish_search if search_plane is not None else None,
            )
            logical_elapsed_ms = elapsed_ms(phase_started)
            phase_elapsed["logical_elapsed_ms"] = logical_elapsed_ms
            active_phase = "parquet"
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
            ) or (
                cycles_since_parquet >= parquet_every_cycles
                or clock() - last_parquet_started >= parquet_max_wait_seconds
            )
            if scan is not None and parquet_due:
                cycles_since_parquet = 0
                last_parquet_started = clock()
            # The extra maintenance sweep can rebuild a whole month after the
            # queued batch. Yield it while freshness work remains or just ran;
            # logical work can enqueue passages after their pending count above.
            # Queued builds still run on cadence and can themselves compact.
            freshness_busy = (
                int(documents.get("pending", 0)) > 0
                or int(documents.get("waiting", 0)) > 0
                or int(documents.get("documents", 0)) > 0
                or int(projected.get("pending", 0)) > 0
                or int(projected.get("documents", 0)) > 0
            )
            scanned = (
                scan.project_pending(
                    tenant_id=tenant_id,
                    batch_size=min(4, logical_batch_size),
                    # Give fresh work another turn before queued month catch-up.
                    max_batches=1 if freshness_busy else max_batches_per_cycle,
                    compaction_budget=0 if freshness_busy else 1,
                )
                if scan is not None and parquet_due
                else {
                    "status": "deferred" if scan is not None else "complete",
                    "shards": 0,
                    "rows": 0,
                    "stale": 0,
                    "contended": 0,
                    "requeued": 0,
                    "fragments_rewritten": 0,
                    "fragments_total": 0,
                    "documents_dirty": 0,
                }
            )
            parquet_elapsed_ms = elapsed_ms(phase_started)
            phase_elapsed["parquet_elapsed_ms"] = parquet_elapsed_ms
            active_phase = "thin"
            phase_started = clock()
            # Preserve the thinner's row-level authority gates. While busy,
            # yield two cycles between batches, but run after 30 seconds at the
            # next reachable boundary. Earlier phases can exceed that interval.
            # Idle thinning runs immediately; each new busy streak starts ready.
            thin_busy = (
                int(documents.get("pending", 0)) > 0
                or int(projected.get("pending", 0)) > 0
            )
            if not thin_busy:
                last_busy_thin_cycle = None
            thin_due = (
                last_busy_thin_cycle is None
                or cycles - last_busy_thin_cycle >= 3
                or phase_started - last_busy_thin_started >= 30.0
            )
            thinned = (
                body_thinner(thin_busy)
                if body_thinner is not None and thin_due
                else {
                    "status": "deferred" if body_thinner is not None else "complete",
                    "documents": 0,
                    "refused": 0,
                    "document_bytes_removed": 0,
                    "event_bytes_replaced": 0,
                }
            )
            if body_thinner is not None and thin_due and thin_busy:
                # Failed calls do not consume cadence; the existing failed-cycle
                # path can retry at its next reachable thinning boundary.
                last_busy_thin_cycle = cycles
                last_busy_thin_started = phase_started
            thin_elapsed_ms = elapsed_ms(phase_started)
            phase_elapsed["thin_elapsed_ms"] = thin_elapsed_ms
            active_phase = "outbox"
            phase_started = clock()
            # H3-a: source-months waiting for the Lance writer, one cheap
            # count per cycle (a projector double without a store reports 0).
            search_outbox_queued = 0
            store = getattr(passages, "store", None)
            if store is not None:
                with store.connect() as connection:
                    search_outbox_queued = search_outbox_pending(
                        connection, tenant_id=tenant_id or None,
                    )
            active_phase = "report"
            phase_started = clock()
            result: dict[str, int | str] = {
                "status": (
                    "complete"
                    if documents["status"] == "complete"
                    and projected["status"] == "complete"
                    and embedded["status"] in {"complete", "disabled", "skipped"}
                    and scanned["status"] == "complete"
                    and int(documents["documents"]) == 0
                    and int(projected["passages"]) == 0
                    and int(scanned["shards"]) == 0
                    and int(scanned["stale"]) == 0
                    and int(scanned["contended"]) == 0
                    and int(scanned.get("requeued", 0)) == 0
                    and thinned["status"] == "complete"
                    and int(searched["months"]) == 0
                    and int(searched["failed"]) == 0
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
                "parquet_requeued": int(scanned.get("requeued", 0)),
                "parquet_fragments_rewritten": int(scanned.get("fragments_rewritten", 0)),
                "parquet_fragments_total": int(scanned.get("fragments_total", 0)),
                "parquet_documents_dirty": int(scanned.get("documents_dirty", 0)),
                "search_plane_months": int(searched["months"]),
                "search_plane_rows": int(searched["rows"]),
                "search_plane_deleted": int(searched["deleted"]),
                "search_plane_failed": int(searched["failed"]),
                "search_plane_rate_limited": int(searched.get("rate_limited", 0)),
                "canonical_bodies_thinned": int(thinned["documents"]),
                "thin_mode": "busy" if thin_busy else "idle",
                "thin_deferred": int(body_thinner is not None and not thin_due),
                "thin_probe_timeouts": int(thinned.get("historical_probe_timeouts", 0)),
                "thin_window_size": int(thinned.get("historical_window_size", 0)),
                "thin_hints_pending": int(thinned.get("committed_hints_pending", 0)),
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
                "logical_source_races": int(documents.get("source_races", 0)),
                "logical_failed": int(documents.get("failed", 0)),
                "logical_backoff": int(documents.get("backoff", 0)),
                "logical_quarantined": int(documents.get("quarantined", 0)),
                "old_objects_deleted": int(documents.get("old_objects_deleted", 0)),
                "search_outbox_pending": search_outbox_queued,
                "cycle_elapsed_ms": elapsed_ms(cycle_started),
                "embed_elapsed_ms": embed_elapsed_ms,
                "passage_elapsed_ms": passage_elapsed_ms,
                "passage_warmup_ms": int(projected.get("warmup_ms", 0)),
                "passage_pending_ms": int(projected.get("pending_ms", 0)),
                "passage_prepare_ms": int(projected.get("prepare_ms", 0)),
                "passage_commit_ms": int(projected.get("commit_ms", 0)),
                "passage_count_ms": int(projected.get("count_ms", 0)),
                "logical_elapsed_ms": logical_elapsed_ms,
                "parquet_elapsed_ms": parquet_elapsed_ms,
                "search_plane_elapsed_ms": search_plane_elapsed_ms,
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
                "parquet_requeued=%s "
                "parquet_fragments_rewritten=%s parquet_fragments_total=%s "
                "parquet_documents_dirty=%s "
                "search_plane_months=%s search_plane_rows=%s "
                "search_plane_deleted=%s search_plane_failed=%s "
                "search_plane_rate_limited=%s "
                "canonical_bodies_thinned=%s canonical_bodies_refused=%s "
                "thin_mode=%s thin_probe_timeouts=%s thin_window_size=%s thin_hints_pending=%s "
                "canonical_document_bytes_removed=%s "
                "canonical_event_bytes_replaced=%s "
                "stale=%s pruned=%s "
                "cleanup_failures=%s "
                "logical_cleanup_completed=%s logical_cleanup_pending=%s "
                "logical_source_races=%s logical_failed=%s logical_backoff=%s logical_quarantined=%s "
                "old_objects_deleted=%s search_outbox_pending=%s "
                "cycle_elapsed_ms=%s embed_elapsed_ms=%s passage_elapsed_ms=%s "
                "logical_elapsed_ms=%s parquet_elapsed_ms=%s "
                "search_plane_elapsed_ms=%s thin_elapsed_ms=%s "
                "passage_warmup_ms=%s passage_pending_ms=%s passage_prepare_ms=%s "
                "passage_commit_ms=%s passage_count_ms=%s",
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
                        "parquet_requeued",
                        "parquet_fragments_rewritten",
                        "parquet_fragments_total",
                        "parquet_documents_dirty",
                        "search_plane_months",
                        "search_plane_rows",
                        "search_plane_deleted",
                        "search_plane_failed",
                        "search_plane_rate_limited",
                        "canonical_bodies_thinned",
                        "canonical_bodies_refused",
                        "thin_mode",
                        "thin_probe_timeouts",
                        "thin_window_size",
                        "thin_hints_pending",
                        "canonical_document_bytes_removed",
                        "canonical_event_bytes_replaced",
                        "stale",
                        "pruned",
                        "cleanup_failures",
                        "logical_cleanup_completed",
                        "logical_cleanup_pending",
                        "logical_source_races",
                        "logical_failed",
                        "logical_backoff",
                        "logical_quarantined",
                        "old_objects_deleted",
                        "search_outbox_pending",
                        *PHASE_ELAPSED_KEYS,
                        "passage_warmup_ms",
                        "passage_pending_ms",
                        "passage_prepare_ms",
                        "passage_commit_ms",
                        "passage_count_ms",
                    )
                ),
            )
        except Exception as error:  # noqa: BLE001 - one bad cycle must not stop the service
            # A data error in one cycle (a poisoned group, a provider outage)
            # is logged content-free and the worker keeps serving; the logical
            # projector already backs off the guilty group.
            # A late maintenance failure must not hide time already spent in
            # earlier independent transactions. Do not report unknown writes as
            # successful counters or a completed cycle.
            phase_elapsed["cycle_elapsed_ms"] = elapsed_ms(cycle_started)
            if active_phase + "_elapsed_ms" in phase_elapsed:
                key = active_phase + "_elapsed_ms"
                previous_ticks = phase_elapsed[key] if active_phase in {"passage", "search_plane"} else 0
                phase_elapsed[key] = previous_ticks + elapsed_ms(phase_started)
            LOG.exception(
                "projection cycle failed type=%s failed_phase=%s phase_elapsed_ms=%s "
                + " ".join(key + "=%s" for key in PHASE_ELAPSED_KEYS),
                type(error).__name__, active_phase, elapsed_ms(phase_started),
                *(phase_elapsed[key] for key in PHASE_ELAPSED_KEYS),
            )
            if once or (max_cycles is not None and cycles >= max_cycles):
                raise
            sleep(interval_seconds)
            continue
        if once or (max_cycles is not None and cycles >= max_cycles):
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
                "search_plane_months",
                "canonical_bodies_thinned",
                "stale",
                "pruned",
            )
        ):
            sleep(interval_seconds)


EMBEDDING_PROVIDER_ERRORS = (
    ConnectionError,
    RemoteDisconnected,
    TimeoutError,
    urllib.error.URLError,
)
# Anti-join rows examined per cycle before the lag field stops counting; the
# probe gate is 5000, so anything above this reads as "more than the gate".
EMBEDDING_LAG_SAMPLE_LIMIT = 10_000


def run_embedding_worker(
    passages: CanonicalPassageProjector,
    store: Any,
    *,
    tenant_id: str,
    batch_size: int,
    max_batches_per_cycle: int,
    interval_seconds: float,
    daily_cap: int,
    once: bool = False,
    sleep: Callable[[float], Any] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    max_cycles: int | None = None,
    lag_sample_limit: int = EMBEDDING_LAG_SAMPLE_LIMIT,
) -> dict[str, int | str]:
    """Embed pending passages in a process of its own, under a daily cap (H5-2/H5-3).

    Every cycle reads the tenant's ledger total for the last 24 hours, hands
    ``embed_pending`` the remaining budget as ``max_passages``, and upserts
    what was embedded back into the ledger. At the cap the worker logs
    ``embedding cap reached`` and idles until the window rolls; it never
    calls the provider with a zero budget.
    """

    if not isinstance(tenant_id, str) or not tenant_id:
        raise ValueError("embedding worker tenant is invalid")
    if not 0.1 <= interval_seconds <= 300:
        raise ValueError("embedding worker interval is invalid")
    if (
        isinstance(batch_size, bool)
        or not isinstance(batch_size, int)
        or batch_size < 1
        or isinstance(max_batches_per_cycle, bool)
        or not isinstance(max_batches_per_cycle, int)
        or not 1 <= max_batches_per_cycle <= 100
        or isinstance(lag_sample_limit, bool)
        or not isinstance(lag_sample_limit, int)
        or lag_sample_limit < 1
    ):
        raise ValueError("embedding worker budget is invalid")
    validate_daily_cap(daily_cap)
    if getattr(store, "search_plane", "postgres") == "turbopuffer":
        # H3-e': nothing to embed from this process; the ledger and the
        # embeddings table are retired by migration 067, so neither is read.
        LOG.warning(
            "embedding worker not applicable on the turbopuffer search plane "
            "tenant=%s; suspend this service",
            tenant_id,
        )
        return {
            "status": "not-applicable",
            "embedded": 0,
            "pending": 0,
            "lag": 0,
            "embedded_24h": 0,
            "cap": daily_cap,
            "cap_remaining": daily_cap,
            "embedding_error": 0,
            "elapsed_ms": 0,
            "plane": "turbopuffer",
        }

    def elapsed_ms(started: float) -> int:
        return max(0, int(round((clock() - started) * 1000)))

    def lag_sample(connection: Any) -> int:
        runtime = getattr(store, "semantic_runtime", None)
        fingerprint = getattr(runtime, "passage_fingerprint", None)
        if not isinstance(fingerprint, str) or not fingerprint:
            return -1
        return count_unembedded_passages(
            connection,
            passage_fingerprint=fingerprint,
            limit=lag_sample_limit,
            tenant_id=tenant_id,
        )

    cycles = 0
    while True:
        cycles += 1
        try:
            cycle_started = clock()
            with store.connect() as connection:
                embedded_window = window_total(connection, tenant_id=tenant_id)
                connection.commit()
            cap_remaining = max(0, daily_cap - embedded_window)
            embedding_error = 0
            processed = 0
            if cap_remaining == 0:
                status = "capped"
                LOG.warning(
                    "embedding cap reached tenant=%s embedded_24h=%s cap=%s",
                    tenant_id,
                    embedded_window,
                    daily_cap,
                )
            else:
                try:
                    result = passages.embed_pending(
                        tenant_id=tenant_id,
                        batch_size=batch_size,
                        max_batches=max_batches_per_cycle,
                        max_passages=cap_remaining,
                    )
                except EMBEDDING_PROVIDER_ERRORS as error:
                    # The provider is an external dependency: keep the durable
                    # loop and retry next cycle instead of crashing the service.
                    embedding_error = 1
                    result = {"status": "unavailable", "processed": 0}
                    LOG.warning(
                        "embedding unavailable type=%s", type(error).__name__
                    )
                status = str(result["status"])
                processed = int(result["processed"])
                if processed:
                    with store.connect() as connection:
                        record_embedded(
                            connection, tenant_id=tenant_id, embedded=processed
                        )
                        connection.commit()
                    embedded_window += processed
                    cap_remaining = max(0, daily_cap - embedded_window)
                    if cap_remaining == 0:
                        status = "capped"
                        LOG.warning(
                            "embedding cap reached tenant=%s embedded_24h=%s cap=%s",
                            tenant_id,
                            embedded_window,
                            daily_cap,
                        )
            # A completed drain means nothing is pending: no count needed.
            # Otherwise sample a bounded anti-join so the log carries the lag.
            if status == "complete":
                lag = 0
            else:
                with store.connect() as connection:
                    lag = lag_sample(connection)
                    connection.commit()
            cycle: dict[str, int | str] = {
                "status": status,
                "embedded": processed,
                "pending": 1 if status in {"pending", "capped", "busy", "unavailable"} else 0,
                "lag": lag,
                "embedded_24h": embedded_window,
                "cap": daily_cap,
                "cap_remaining": cap_remaining,
                "embedding_error": embedding_error,
                "elapsed_ms": elapsed_ms(cycle_started),
            }
            record_cycle({"embedded": processed, "embed_elapsed_ms": cycle["elapsed_ms"]})
            LOG.info(
                "embedding cycle status=%s embedded=%s pending=%s lag=%s "
                "embedded_24h=%s cap=%s cap_remaining=%s embedding_error=%s "
                "elapsed_ms=%s",
                *(
                    cycle[key]
                    for key in (
                        "status",
                        "embedded",
                        "pending",
                        "lag",
                        "embedded_24h",
                        "cap",
                        "cap_remaining",
                        "embedding_error",
                        "elapsed_ms",
                    )
                ),
            )
        except Exception as error:  # noqa: BLE001 - one bad cycle must not stop the service
            LOG.exception("embedding cycle failed type=%s", type(error).__name__)
            if once or (max_cycles is not None and cycles >= max_cycles):
                raise
            sleep(interval_seconds)
            continue
        if once or (max_cycles is not None and cycles >= max_cycles):
            return cycle
        if cycle["status"] != "pending" or processed == 0:
            # Idle, capped, busy, or provider-unavailable: wait before polling
            # again. Only a cycle that embedded work and left more behind loops
            # straight into the next batch.
            sleep(interval_seconds)
