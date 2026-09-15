"""turbopuffer search plane writer (H3-b): drains the search projection outbox.

One outbox row is one dirty (tenant, source, month). For each claimed row the
projector reads the live passages of that month from the Postgres catalog,
upserts them into the tenant namespace (turbopuffer embeds ``embed_text``
natively and indexes ``text`` for BM25), applies the month's tombstones as
deletes, and only then finishes the row: a compare-and-delete on the claimed
``generation`` (a month re-queued during the write stays queued) plus an
upsert of ``search_projection_shards``.

Incremental reasons (``logical-update``, ``forget``, ``header-change``) only
send passages created after the shard's last projection watermark;
``backfill`` sends every live passage of the month. The watermark stored as
``built_at`` is the earliest of the read time and the oldest transaction in
progress when the read started, so a passage-plane transaction that was
still open during the read (its ``created_at`` predates the read) is picked
up by the next incremental pass instead of being lost.

Rate limits (turbopuffer's native-embedding limit is 1024 requests and 2M
tokens per minute per organisation, HTTP 429 ``RateLimitError``) back off
1 s, 2 s, 4 s ... capped at 60 s and retry the same batch, for at most
``RATE_LIMIT_BUDGET_SECONDS`` per month; the batch is never dropped. Writes
are clamped to ``max_batch_bytes`` of row payload as well as ``batch_rows``
(the per-namespace ingest limit is 32 MB/s).

Nothing here logs passage text or the API key; failures carry the error
class only.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from datetime import date, datetime, timezone
from typing import Any

from .turbopuffer_plane import (
    TurbopufferSettings,
    build_client,
    namespace_schema,
    passage_row,
    EMBED_TEXT_ATTRIBUTE,
)

LOG = logging.getLogger(__name__)

DEFAULT_MONTHS_PER_CYCLE = 4
DEFAULT_MAX_BATCH_BYTES = 32 * 1024 * 1024
RATE_LIMIT_BACKOFF_SECONDS = 1.0
RATE_LIMIT_BACKOFF_CAP_SECONDS = 60.0
RATE_LIMIT_BUDGET_SECONDS = 300.0
INCREMENTAL_REASONS = frozenset({"logical-update", "forget", "header-change"})

# Copied from passage_retrieval._lexical_query: a passage is live only while
# every receipt it cites still has an undeleted canonical chunk.
_LIVE_PASSAGE_PREDICATE = """
    NOT EXISTS (
        SELECT 1
          FROM unnest(passage.receipts) AS passage_receipt(receipt)
          LEFT JOIN canonical_chunks live_chunk
            ON live_chunk.tenant_id=passage.tenant_id
           AND live_chunk.source_id=passage.source_id
           AND live_chunk.receipt=passage_receipt.receipt
           AND live_chunk.deleted_at IS NULL
         WHERE live_chunk.receipt IS NULL
    )
"""

_PASSAGE_PAGE_SQL = f"""
    SELECT passage.passage_id,passage.source_id,passage.logical_document_id,
           passage.policy_fingerprint,passage.ordinal,
           passage.first_occurred_at,passage.last_occurred_at,
           passage.roles,passage.receipts,passage.spans,
           passage.text_redacted,passage.text_sha256,passage.header_redacted,
           evidence.native_parent_id,evidence.revision,
           evidence.manifest_object_key,evidence.manifest_content_sha256,
           evidence.first_occurred_at AS doc_first_occurred_at,
           evidence.last_occurred_at AS doc_last_occurred_at,
           COALESCE(
               (SELECT jsonb_agg(
                           jsonb_build_array(actor.relation,actor.actor_id)
                           ORDER BY actor.relation,actor.actor_id
                       )
                  FROM canonical_passage_actors actor
                 WHERE actor.tenant_id=passage.tenant_id
                   AND actor.source_id=passage.source_id
                   AND actor.passage_id=passage.passage_id),
               '[]'::jsonb
           ) AS actors
      FROM canonical_passages passage
      JOIN canonical_passage_documents projected
        USING(tenant_id,source_id,logical_document_id,revision,policy_fingerprint)
      JOIN canonical_evidence_documents evidence
        USING(tenant_id,source_id,logical_document_id)
     WHERE passage.tenant_id=%s
       AND passage.source_id=%s
       AND passage.first_occurred_at>=%s
       AND passage.first_occurred_at<%s
       AND (%s::timestamptz IS NULL OR passage.created_at>%s::timestamptz)
       AND (passage.first_occurred_at,passage.passage_id)>(%s,%s)
       AND {_LIVE_PASSAGE_PREDICATE}
     ORDER BY passage.first_occurred_at,passage.passage_id
     LIMIT %s
"""


def _month_bounds(month: date) -> tuple[datetime, datetime]:
    start = datetime(month.year, month.month, 1, tzinfo=timezone.utc)
    end = (
        datetime(month.year + 1, 1, 1, tzinfo=timezone.utc)
        if month.month == 12
        else datetime(month.year, month.month + 1, 1, tzinfo=timezone.utc)
    )
    return start, end


def _chunks(values: list[Any], size: int) -> list[list[Any]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _row_bytes(row: dict[str, Any]) -> int:
    return len(json.dumps(row, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))


def byte_bounded_batches(rows: list[dict[str, Any]], *, max_rows: int, max_bytes: int) -> list[list[dict[str, Any]]]:
    """Split ``rows`` so no batch exceeds ``max_rows`` or ``max_bytes`` of JSON.

    A single row larger than ``max_bytes`` travels alone (the service, not
    the writer, decides whether it is too big).
    """

    batches: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    current_bytes = 0
    for row in rows:
        size = _row_bytes(row)
        if current and (len(current) >= max_rows or current_bytes + size > max_bytes):
            batches.append(current)
            current, current_bytes = [], 0
        current.append(row)
        current_bytes += size
    if current:
        batches.append(current)
    return batches


def is_rate_limit(error: BaseException) -> bool:
    """``turbopuffer.RateLimitError`` (HTTP 429), matched by class name so the
    SDK stays an optional import."""

    return any("RateLimit" in klass.__name__ for klass in type(error).__mro__)


def is_transient(error: BaseException) -> bool:
    """A write worth retrying with backoff: 429, 5xx (the embedding path
    answered 502 on large batches live), connection and timeout errors."""

    if is_rate_limit(error):
        return True
    names = {klass.__name__ for klass in type(error).__mro__}
    if names & {"InternalServerError", "APIConnectionError", "APITimeoutError", "ServiceUnavailableError"}:
        return True
    status = getattr(error, "status_code", None)
    return isinstance(status, int) and status >= 500


# Native embeddings are billed and rate-limited per token (2M tokens/min per
# org). Writes are paced under this estimate (chars / 4) so the drain never
# trips the limit and leaves headroom for query-time embeddings.
DEFAULT_TOKENS_PER_MINUTE = 1_000_000
TOKEN_CHARS = 4


class TokenPacer:
    """Sliding one-minute window of estimated tokens; ``wait_for(tokens)``
    sleeps until sending ``tokens`` keeps the window under the limit."""

    def __init__(self, tokens_per_minute: int, *, clock: Any = time.monotonic, sleep: Any = time.sleep) -> None:
        self.limit = int(tokens_per_minute)
        self.clock = clock
        self.sleep = sleep
        self.sent: list[tuple[float, int]] = []
        self.slept_seconds = 0.0

    def wait_for(self, tokens: int) -> None:
        if self.limit <= 0:
            return
        while True:
            now = self.clock()
            self.sent = [(at, count) for at, count in self.sent if now - at < 60.0]
            used = sum(count for _at, count in self.sent)
            if used + tokens <= self.limit or not self.sent:
                self.sent.append((now, tokens))
                return
            oldest_at = self.sent[0][0]
            delay = max(0.05, 60.0 - (now - oldest_at))
            self.slept_seconds += delay
            self.sleep(delay)


def estimated_tokens(rows: list[dict[str, Any]]) -> int:
    return sum(len(row.get(EMBED_TEXT_ATTRIBUTE) or "") for row in rows) // TOKEN_CHARS + len(rows)


class TurbopufferProjector:
    """Drains ``search_projection_outbox`` into turbopuffer namespaces."""

    def __init__(
        self,
        store: Any,
        settings: TurbopufferSettings,
        *,
        client: Any = None,
        batch_rows: int | None = None,
        max_batch_bytes: int = DEFAULT_MAX_BATCH_BYTES,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Any] = time.sleep,
        rate_limit_budget_seconds: float = RATE_LIMIT_BUDGET_SECONDS,
        tokens_per_minute: int | None = None,
    ) -> None:
        batch = settings.write_batch_rows if batch_rows is None else batch_rows
        if isinstance(batch, bool) or not isinstance(batch, int) or not 1 <= batch <= 5000:
            raise ValueError("search plane write batch is invalid")
        if isinstance(max_batch_bytes, bool) or not isinstance(max_batch_bytes, int) or not 1024 <= max_batch_bytes <= DEFAULT_MAX_BATCH_BYTES:
            raise ValueError("search plane batch bytes are invalid")
        self.store = store
        self.settings = settings
        self.batch_rows = batch
        self.max_batch_bytes = max_batch_bytes
        self.clock = clock
        self.sleep = sleep
        self.rate_limit_budget_seconds = float(rate_limit_budget_seconds)
        limit = (
            getattr(settings, "tokens_per_minute", DEFAULT_TOKENS_PER_MINUTE)
            if tokens_per_minute is None
            else tokens_per_minute
        )
        # Wall clock on purpose: the injected clock serves deadlines in tests.
        self.pacer = TokenPacer(int(limit), clock=time.monotonic, sleep=sleep)
        self._client = client

    @property
    def client(self) -> Any:
        if self._client is None:
            self._client = build_client(self.settings)
        return self._client

    # -- catalog reads -------------------------------------------------------

    def claim_months(self, connection: Any, *, tenant_id: str, limit: int) -> list[dict[str, Any]]:
        """Oldest-queued outbox rows of the tenant; the claim is the generation read."""

        return [
            dict(row)
            for row in connection.execute(
                """SELECT tenant_id,source_id,month,generation,reason
                     FROM search_projection_outbox
                    WHERE tenant_id=%s
                    ORDER BY queued_at,source_id,month
                    LIMIT %s""",
                (tenant_id, limit),
            ).fetchall()
        ]

    def shard_watermark(self, connection: Any, claim: dict[str, Any]) -> datetime | None:
        row = connection.execute(
            """SELECT built_at FROM search_projection_shards
                WHERE tenant_id=%s AND source_id=%s AND month=%s""",
            (claim["tenant_id"], claim["source_id"], claim["month"]),
        ).fetchone()
        return row["built_at"] if row else None

    def read_watermark(self, connection: Any) -> datetime:
        """Safe ``built_at`` for this projection: nothing committed later than
        this can carry a ``created_at`` at or before it."""

        return connection.execute(
            """SELECT LEAST(
                          clock_timestamp(),
                          COALESCE(
                              (SELECT min(xact_start) FROM pg_stat_activity
                                WHERE xact_start IS NOT NULL
                                  AND backend_type='client backend'),
                              clock_timestamp()
                          )
                      ) AS watermark"""
        ).fetchone()["watermark"]

    def passage_page(
        self,
        connection: Any,
        claim: dict[str, Any],
        *,
        since: datetime | None,
        after: tuple[datetime, str] | None,
    ) -> list[dict[str, Any]]:
        start, end = _month_bounds(claim["month"])
        cursor_time, cursor_id = after or (datetime(1, 1, 1, tzinfo=timezone.utc), "")
        return [
            dict(row)
            for row in connection.execute(
                _PASSAGE_PAGE_SQL,
                (
                    claim["tenant_id"], claim["source_id"], start, end,
                    since, since, cursor_time, cursor_id, self.batch_rows,
                ),
            ).fetchall()
        ]

    def tombstone_ids(self, connection: Any, claim: dict[str, Any]) -> list[str]:
        return [
            str(row["passage_id"])
            for row in connection.execute(
                """SELECT passage_id FROM search_projection_tombstones
                    WHERE tenant_id=%s AND source_id=%s AND month=%s
                    ORDER BY deleted_at,passage_id""",
                (claim["tenant_id"], claim["source_id"], claim["month"]),
            ).fetchall()
        ]

    # -- catalog writes ------------------------------------------------------

    def finish_month(
        self,
        connection: Any,
        claim: dict[str, Any],
        *,
        namespace: str,
        rows_written: int,
        tombstones: list[str],
        watermark: datetime,
    ) -> bool:
        """Retire the claim; ``False`` when the generation moved meanwhile."""

        with connection.transaction():
            if tombstones:
                connection.execute(
                    """DELETE FROM search_projection_tombstones
                        WHERE tenant_id=%s AND source_id=%s
                          AND passage_id=ANY(%s::text[])""",
                    (claim["tenant_id"], claim["source_id"], tombstones),
                )
            connection.execute(
                """INSERT INTO search_projection_shards(
                       tenant_id,source_id,month,generation,dataset_uri,
                       row_count,built_at
                   ) VALUES (%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT(tenant_id,source_id,month) DO UPDATE SET
                       generation=excluded.generation,
                       dataset_uri=excluded.dataset_uri,
                       row_count=excluded.row_count,
                       built_at=excluded.built_at""",
                (
                    claim["tenant_id"], claim["source_id"], claim["month"],
                    claim["generation"],
                    f"turbopuffer://{self.settings.region}/{namespace}",
                    rows_written, watermark,
                ),
            )
            retired = connection.execute(
                """DELETE FROM search_projection_outbox
                    WHERE tenant_id=%s AND source_id=%s AND month=%s
                      AND generation=%s""",
                (
                    claim["tenant_id"], claim["source_id"], claim["month"],
                    claim["generation"],
                ),
            )
        return max(0, retired.rowcount) == 1

    # -- turbopuffer writes --------------------------------------------------

    def _write(self, namespace: Any, budget: dict[str, float], **kwargs: Any) -> int:
        """One write call; a 429 backs off and retries the same batch.

        ``budget`` carries the month's remaining backoff seconds and the
        number of rate-limited attempts (``rate_limited``); when the budget
        is spent the error propagates and the month stays queued.
        """

        delay = RATE_LIMIT_BACKOFF_SECONDS
        attempts = 0
        while True:
            try:
                namespace.write(
                    distance_metric="cosine_distance",
                    schema=namespace_schema(self.settings),
                    **kwargs,
                )
                return attempts
            except Exception as error:  # noqa: BLE001 - transient errors are retried
                if not is_transient(error):
                    raise
                attempts += 1
                budget["rate_limited"] = budget.get("rate_limited", 0) + 1
                if budget["remaining"] < delay:
                    LOG.warning(
                        "search plane rate limit budget exhausted attempts=%s type=%s",
                        attempts, type(error).__name__,
                    )
                    raise
                budget["remaining"] -= delay
                self.sleep(delay)
                delay = min(delay * 2, RATE_LIMIT_BACKOFF_CAP_SECONDS)

    def project_month(self, claim: dict[str, Any]) -> dict[str, Any]:
        """Project one claimed source-month; raises on a turbopuffer failure."""

        tenant_id = claim["tenant_id"]
        name = self.settings.namespace(tenant_id)
        namespace = self.client.namespace(name)
        budget = {"remaining": self.rate_limit_budget_seconds, "rate_limited": 0}
        with self.store.connect() as connection:
            since = (
                self.shard_watermark(connection, claim)
                if claim["reason"] in INCREMENTAL_REASONS
                else None
            )
            watermark = self.read_watermark(connection)
            tombstones = self.tombstone_ids(connection, claim)
            connection.commit()
        # Deletes first: a tombstone for an id that is live again (the same
        # passage re-inserted) must not erase the row upserted below.
        deleted = 0
        for batch in _chunks(tombstones, self.batch_rows):
            try:
                self._write(namespace, budget, deletes=batch)
            except Exception as error:  # noqa: BLE001 - class checked below
                if type(error).__name__ != "NotFoundError":
                    raise
            deleted += len(batch)
        written = 0
        after: tuple[datetime, str] | None = None
        while True:
            with self.store.connect() as connection:
                page = self.passage_page(connection, claim, since=since, after=after)
                connection.commit()
            if not page:
                break
            rows = [
                passage_row({**passage, "actors": [tuple(actor) for actor in passage["actors"]]})
                for passage in page
            ]
            for batch in byte_bounded_batches(rows, max_rows=self.batch_rows, max_bytes=self.max_batch_bytes):
                self.pacer.wait_for(estimated_tokens(batch))
                self._write(namespace, budget, upsert_rows=batch)
                written += len(batch)
            last = page[-1]
            after = (last["first_occurred_at"], last["passage_id"])
            if len(page) < self.batch_rows:
                break
        with self.store.connect() as connection:
            retired = self.finish_month(
                connection, claim,
                namespace=name, rows_written=written,
                tombstones=tombstones, watermark=watermark,
            )
        return {
            "rows": written, "deleted": deleted, "retired": retired,
            "rate_limited": int(budget["rate_limited"]),
        }

    def drain(
        self,
        *,
        tenant_id: str,
        max_months: int,
        deadline_at: float | None = None,
    ) -> dict[str, int | str]:
        """One cycle: up to ``max_months`` outbox rows, oldest first."""

        if isinstance(max_months, bool) or not isinstance(max_months, int) or not 1 <= max_months <= 1000:
            raise ValueError("search plane months per cycle is invalid")
        with self.store.connect() as connection:
            claims = self.claim_months(connection, tenant_id=tenant_id, limit=max_months)
            connection.commit()
        result: dict[str, int | str] = {
            "status": "complete",
            "months": 0,
            "rows": 0,
            "deleted": 0,
            "failed": 0,
            "requeued": 0,
            "rate_limited": 0,
            "pending": 0,
        }
        for claim in claims:
            if deadline_at is not None and self.clock() >= deadline_at:
                break
            try:
                outcome = self.project_month(claim)
            except Exception as error:  # noqa: BLE001 - one month must not stop the cycle
                result["failed"] = int(result["failed"]) + 1
                LOG.warning(
                    "search plane month failed reason=%s generation=%s type=%s",
                    claim["reason"], claim["generation"], type(error).__name__,
                )
                continue
            result["months"] = int(result["months"]) + 1
            result["rows"] = int(result["rows"]) + int(outcome["rows"])
            result["deleted"] = int(result["deleted"]) + int(outcome["deleted"])
            result["rate_limited"] = int(result["rate_limited"]) + int(outcome["rate_limited"])
            if not outcome["retired"]:
                result["requeued"] = int(result["requeued"]) + 1
        with self.store.connect() as connection:
            result["pending"] = int(
                connection.execute(
                    """SELECT count(*) AS count FROM search_projection_outbox
                        WHERE tenant_id=%s""",
                    (tenant_id,),
                ).fetchone()["count"]
            )
            connection.commit()
        if int(result["pending"]) or int(result["failed"]):
            result["status"] = "pending"
        return result


def drain_search_outbox(
    store: Any,
    settings: TurbopufferSettings,
    *,
    tenant_id: str,
    max_months: int,
    client: Any = None,
    batch_rows: int | None = None,
    deadline_at: float | None = None,
    sleep: Callable[[float], Any] = time.sleep,
) -> dict[str, int | str]:
    """One drain cycle over the tenant's outbox (see ``TurbopufferProjector``)."""

    projector = TurbopufferProjector(store, settings, client=client, batch_rows=batch_rows, sleep=sleep)
    return projector.drain(tenant_id=tenant_id, max_months=max_months, deadline_at=deadline_at)
