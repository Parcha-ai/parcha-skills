"""H3-a: the search projection outbox that feeds the Lance-on-S3 search plane.

Every passage-plane write records, in its own transaction, which
(tenant, source, month) shards the Lance writer (H3-b) must rebuild and which
passage ids it must delete. The helpers here take an open connection so the
outbox row commits or rolls back with the write that caused it: a month is
never queued for a change that did not land, and a landed change is never
missed.

Reasons: ``logical-update`` (differential passage commit), ``forget``
(passages removed with their evidence document), ``header-change`` (a
contextual header was rendered for rows that had none, so their embedding key
changed), ``backfill`` (a full rebuild, sticky until built).
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Iterable

SEARCH_OUTBOX_REASONS = frozenset(
    {"backfill", "logical-update", "forget", "header-change"}
)


def _month(value: datetime | date | str) -> date:
    """First day of the UTC month of ``value`` (ISO-8601 strings accepted)."""

    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("search outbox timestamp must carry a timezone")
        value = parsed
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc)
        value = value.date()
    return value.replace(day=1)


def _next_month(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def outbox_months(
    spans: Iterable[tuple[datetime | date | str, datetime | date | str]],
) -> list[date]:
    """Every first-of-month between each span's first and last time, deduplicated.

    Mirrors ``generate_series(date_trunc('month',first), date_trunc('month',
    last), '1 month')`` in the parquet plane, so a passage or document that
    straddles a month boundary lands in both shards.
    """

    months: set[date] = set()
    for first, last in spans:
        cursor = _month(first)
        end = _month(last)
        if end < cursor:
            raise ValueError("search outbox span is inverted")
        while cursor <= end:
            months.add(cursor)
            cursor = _next_month(cursor)
    return sorted(months)


def enqueue_search_outbox(
    connection: Any,
    *,
    tenant_id: str,
    source_id: str,
    months: Iterable[date],
    reason: str,
) -> int:
    """Upsert one outbox row per month; a row already queued gets generation+1.

    ``backfill`` is sticky: an incremental reason never downgrades a month
    that is waiting for a full rebuild. Returns the number of rows touched.
    """

    if reason not in SEARCH_OUTBOX_REASONS:
        raise ValueError("search outbox reason is invalid")
    values = sorted({_month(month) for month in months})
    if not values:
        return 0
    result = connection.execute(
        """INSERT INTO search_projection_outbox(
               tenant_id,source_id,month,generation,reason,
               queued_at,first_queued_at
           )
           SELECT %s,%s,month.value,1,%s,clock_timestamp(),clock_timestamp()
             FROM unnest(%s::date[]) AS month(value)
           ON CONFLICT(tenant_id,source_id,month)
           DO UPDATE SET
               generation=search_projection_outbox.generation+1,
               reason=CASE
                   WHEN search_projection_outbox.reason='backfill'
                   THEN 'backfill' ELSE excluded.reason END,
               queued_at=clock_timestamp()""",
        (tenant_id, source_id, reason, values),
    )
    return max(0, result.rowcount)


def write_search_tombstones(
    connection: Any,
    *,
    tenant_id: str,
    source_id: str,
    passages: Iterable[dict[str, Any]],
) -> int:
    """Record one tombstone per deleted passage (keyed by its first month).

    ``passages`` rows carry ``passage_id`` and ``first_occurred_at``. A
    passage already tombstoned keeps its original ``deleted_at``.
    """

    ids: list[str] = []
    months: list[date] = []
    for row in passages:
        ids.append(str(row["passage_id"]))
        months.append(_month(row["first_occurred_at"]))
    if not ids:
        return 0
    result = connection.execute(
        """INSERT INTO search_projection_tombstones(
               tenant_id,source_id,passage_id,month,deleted_at
           )
           SELECT %s,%s,dead.passage_id,dead.month,clock_timestamp()
             FROM unnest(%s::text[],%s::date[]) AS dead(passage_id,month)
           ON CONFLICT(tenant_id,source_id,passage_id) DO NOTHING""",
        (tenant_id, source_id, ids, months),
    )
    return max(0, result.rowcount)


def record_passage_deletions(
    connection: Any,
    *,
    tenant_id: str,
    source_id: str,
    passages: Iterable[dict[str, Any]],
    reason: str,
) -> dict[str, int]:
    """Tombstone the given passages and enqueue every month they spanned.

    ``passages`` rows carry ``passage_id``, ``first_occurred_at`` and
    ``last_occurred_at``. The caller runs this before the rows are deleted
    (or on a snapshot of them) in the same transaction.
    """

    rows = list(passages)
    tombstones = write_search_tombstones(
        connection,
        tenant_id=tenant_id,
        source_id=source_id,
        passages=rows,
    )
    queued = enqueue_search_outbox(
        connection,
        tenant_id=tenant_id,
        source_id=source_id,
        months=outbox_months(
            (row["first_occurred_at"], row["last_occurred_at"]) for row in rows
        ),
        reason=reason,
    )
    return {"tombstones": tombstones, "queued": queued}


def seed_search_outbox(
    connection: Any,
    *,
    tenant_id: str,
    source_id: str | None = None,
) -> int:
    """Queue one ``backfill`` row per existing parquet shard month.

    Reads ``canonical_parquet_scan_shards`` only (the same trick as migration
    053). Idempotent: a month already waiting as ``backfill`` is untouched, so
    a second run touches zero rows; a month queued for an incremental reason
    is promoted to a full rebuild.
    """

    result = connection.execute(
        """INSERT INTO search_projection_outbox(
               tenant_id,source_id,month,generation,reason,
               queued_at,first_queued_at
           )
           SELECT shard.tenant_id,shard.source_id,shard.bucket_start,
                  1,'backfill',clock_timestamp(),clock_timestamp()
             FROM (
                   SELECT DISTINCT tenant_id,source_id,bucket_start
                     FROM canonical_parquet_scan_shards
                    WHERE tenant_id=%s
                      AND (%s::text IS NULL OR source_id=%s)
             ) shard
           ON CONFLICT(tenant_id,source_id,month)
           DO UPDATE SET
               generation=search_projection_outbox.generation+1,
               reason='backfill',
               queued_at=clock_timestamp()
            WHERE search_projection_outbox.reason<>'backfill'""",
        (tenant_id, source_id, source_id),
    )
    return max(0, result.rowcount)


def search_outbox_pending(connection: Any, *, tenant_id: str | None = None) -> int:
    """Cheap count of queued source-months for the worker cycle log."""

    return int(
        connection.execute(
            """SELECT count(*) AS count FROM search_projection_outbox
                WHERE (%s::text IS NULL OR tenant_id=%s)""",
            (tenant_id, tenant_id),
        ).fetchone()["count"]
    )
