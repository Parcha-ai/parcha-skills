"""Daily embedding ledger: the durable budget behind the embedding worker.

The ledger is one row per (tenant, UTC day) counting passages sent to the
embedding provider. The worker reads the window total before every cycle and
hands ``embed_pending`` the remaining budget, so the cap holds across
restarts and across several worker replicas. The window covers the UTC day
buckets that intersect the last 24 hours (today and, until 24 h have passed
since midnight UTC, yesterday), so it never under-counts the last 24 hours.
"""

from __future__ import annotations

import os
from typing import Any

DEFAULT_DAILY_CAP = 200_000
MAX_DAILY_CAP = 50_000_000
# Anti-join rows examined before the unembedded count stops counting.
UNEMBEDDED_COUNT_CAP = 250_000


def daily_cap_from_env(environment: dict[str, str] | None = None) -> int:
    """``RECALL_EMBEDDING_DAILY_CAP`` as a validated integer (default 200000)."""

    values = os.environ if environment is None else environment
    raw = (values.get("RECALL_EMBEDDING_DAILY_CAP") or "").strip()
    if not raw:
        return DEFAULT_DAILY_CAP
    try:
        cap = int(raw)
    except ValueError:
        raise ValueError("RECALL_EMBEDDING_DAILY_CAP is invalid") from None
    validate_daily_cap(cap)
    return cap


def validate_daily_cap(cap: Any) -> int:
    if isinstance(cap, bool) or not isinstance(cap, int) or not 1 <= cap <= MAX_DAILY_CAP:
        raise ValueError("embedding daily cap is invalid")
    return cap


def ledger_exists(connection: Any) -> bool:
    return bool(
        connection.execute(
            "SELECT to_regclass('public.canonical_embedding_ledger') AS value"
        ).fetchone()["value"]
    )


def window_total(
    connection: Any,
    *,
    tenant_id: str | None = None,
    search_plane: str = "postgres",
) -> int:
    """Passages embedded in the UTC day buckets that intersect the last 24 h.

    0 on the turbopuffer plane without a query: nothing embeds from this
    process and migration 067 drops the ledger (H3-e').
    """

    if search_plane == "turbopuffer":
        return 0
    scope = tenant_id or ""
    return int(
        connection.execute(
            """SELECT COALESCE(sum(embedded),0) AS n
                 FROM canonical_embedding_ledger
                WHERE day >= ((now() AT TIME ZONE 'UTC') - interval '24 hours')::date
                  AND (%s::text='' OR tenant_id=%s)""",
            (scope, scope),
        ).fetchone()["n"]
    )


def record_embedded(
    connection: Any,
    *,
    tenant_id: str,
    embedded: int,
    search_plane: str = "postgres",
) -> None:
    """Upsert one cycle's count into today's UTC bucket (no-op for zero).

    A no-op on the turbopuffer plane: the ledger is gone after migration 067.
    """

    if isinstance(embedded, bool) or not isinstance(embedded, int) or embedded < 0:
        raise ValueError("embedding ledger count is invalid")
    if not tenant_id or embedded == 0 or search_plane == "turbopuffer":
        return
    connection.execute(
        """INSERT INTO canonical_embedding_ledger(tenant_id,day,embedded)
           VALUES (%s,(now() AT TIME ZONE 'UTC')::date,%s)
           ON CONFLICT (tenant_id,day) DO UPDATE
              SET embedded=canonical_embedding_ledger.embedded+excluded.embedded,
                  updated_at=now()""",
        (tenant_id, embedded),
    )


def count_unembedded_passages(
    connection: Any,
    *,
    passage_fingerprint: str,
    limit: int = UNEMBEDDED_COUNT_CAP,
    tenant_id: str | None = None,
    search_plane: str = "postgres",
) -> int:
    """Passages without a vector for ``passage_fingerprint``, counted up to ``limit``.

    Shared by the ``/metrics`` gauge and the embedding worker's lag field so
    the embedding key (runtime fingerprint plus content hash) is defined in one
    place. H2-a (contextual headers) changes that key; update it here. 0 on
    the turbopuffer plane without a query (H3-e').
    """

    if search_plane == "turbopuffer":
        return 0
    scope = tenant_id or ""
    return int(
        connection.execute(
            """SELECT count(*) AS n FROM (
                   SELECT 1 FROM canonical_passages passage
                   WHERE (%s::text='' OR passage.tenant_id=%s)
                     AND NOT EXISTS (
                       SELECT 1 FROM canonical_passage_embeddings embedding
                       WHERE embedding.tenant_id=passage.tenant_id
                         AND embedding.source_id=passage.source_id
                         AND embedding.passage_id=passage.passage_id
                         AND embedding.runtime_fingerprint=%s
                         AND embedding.content_sha256=passage.text_sha256
                   )
                   LIMIT %s
               ) missing""",
            (scope, scope, passage_fingerprint, limit),
        ).fetchone()["n"]
    )
