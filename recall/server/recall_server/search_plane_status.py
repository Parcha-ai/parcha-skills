"""``search-plane-status``: is the turbopuffer namespace caught up with Postgres?

The drain gate before migration 067 (H3-e'). Content-free: live passages
under the current policy fingerprint in the catalog, the namespace's
``approx_row_count`` from turbopuffer, the outbox depth, the built
source-months, and ``drift = passages - rows``. A namespace that does not
exist yet counts as zero rows.
"""
from __future__ import annotations

import logging
from typing import Any

from .search_outbox import search_outbox_pending
from .turbopuffer_plane import TurbopufferSettings, build_client
from .turbopuffer_projection import _LIVE_PASSAGE_PREDICATE

LOG = logging.getLogger("recall.search_plane")

LIVE_PASSAGE_IDS_SQL = f"""
    SELECT passage.passage_id AS passage_id
      FROM canonical_passages passage
      JOIN canonical_passage_documents projected
        USING(tenant_id,source_id,logical_document_id,revision,policy_fingerprint)
     WHERE passage.tenant_id=%s
       AND projected.policy_fingerprint=%s
       AND {_LIVE_PASSAGE_PREDICATE}
"""
RECONCILE_PAGE_ROWS = 1000
RECONCILE_DELETE_ROWS = 500

LIVE_PASSAGES_SQL = f"""
    SELECT count(*) AS n
      FROM canonical_passages passage
      JOIN canonical_passage_documents projected
        USING(tenant_id,source_id,logical_document_id,revision,policy_fingerprint)
     WHERE passage.tenant_id=%s
       AND projected.policy_fingerprint=%s
       AND {_LIVE_PASSAGE_PREDICATE}
"""


def _approx_row_count(metadata: Any) -> int:
    if isinstance(metadata, dict):
        value = metadata.get("approx_row_count")
    else:
        value = getattr(metadata, "approx_row_count", None)
    if value is None:
        raise ValueError("turbopuffer namespace metadata has no approx_row_count")
    return int(value)


def namespace_row_count(client: Any, namespace_name: str) -> int:
    """``approx_row_count`` of one namespace; 0 when it was never written."""

    namespace = client.namespace(namespace_name)
    try:
        return _approx_row_count(namespace.metadata())
    except Exception as error:  # noqa: BLE001 - the SDK's NotFoundError, by class name
        if type(error).__name__ == "NotFoundError":
            return 0
        raise


def search_plane_status(
    store: Any,
    settings: TurbopufferSettings,
    *,
    tenant_id: str,
    policy_fingerprint: str,
    client: Any = None,
) -> dict[str, Any]:
    if not isinstance(tenant_id, str) or not tenant_id:
        raise ValueError("search plane status tenant is invalid")
    if client is None:
        client = build_client(settings)
    namespace_name = settings.namespace(tenant_id)
    with store.connect() as connection:
        passages = int(
            connection.execute(
                LIVE_PASSAGES_SQL, (tenant_id, policy_fingerprint)
            ).fetchone()["n"]
        )
        pending = search_outbox_pending(connection, tenant_id=tenant_id)
        shards = int(
            connection.execute(
                "SELECT count(*) AS n FROM search_projection_shards WHERE tenant_id=%s",
                (tenant_id,),
            ).fetchone()["n"]
        )
        connection.commit()
    rows = namespace_row_count(client, namespace_name)
    return {
        "status": "ok",
        "tenant_id": tenant_id,
        "search_plane": getattr(store, "search_plane", "postgres"),
        "policy_fingerprint": policy_fingerprint,
        "namespace": namespace_name,
        "passages": passages,
        "rows": rows,
        "outbox_pending": pending,
        "shards": shards,
        "drift": passages - rows,
    }


def _row_id(row: Any) -> str | None:
    value = row.get("id") if isinstance(row, dict) else getattr(row, "id", None)
    return value if isinstance(value, str) else None


def namespace_ids(namespace: Any, *, page_rows: int = RECONCILE_PAGE_ROWS) -> set[str]:
    """Every row id in the namespace, paged by id (no attributes over the wire)."""

    ids: set[str] = set()
    last: str | None = None
    while True:
        query: dict[str, Any] = {
            "rank_by": ("id", "asc"), "limit": page_rows, "include_attributes": [],
        }
        if last is not None:
            query["filters"] = ("id", "Gt", last)
        response = namespace.query(**query)
        rows = getattr(response, "rows", None)
        if rows is None and isinstance(response, dict):
            rows = response.get("rows")
        page = [row_id for row_id in (_row_id(row) for row in rows or ()) if row_id]
        if not page:
            return ids
        ids.update(page)
        last = page[-1]
        if len(page) < page_rows:
            return ids


def search_plane_reconcile(
    store: Any,
    settings: TurbopufferSettings,
    *,
    tenant_id: str,
    policy_fingerprint: str,
    client: Any = None,
    apply: bool = False,
    page_rows: int = RECONCILE_PAGE_ROWS,
    delete_rows: int = RECONCILE_DELETE_ROWS,
    projector: Any = None,
) -> dict[str, Any]:
    """Exact drift: namespace ids against the live passage ids in the catalog.

    With ``apply``: ``stale`` rows (in the namespace, not live: a forgotten
    or replaced passage whose tombstone never landed) are deleted, and
    ``missing`` passages (live, not in the namespace: inserted behind a
    running backfill's cursor, so no outbox row carries them) are written
    through ``projector.upsert_passages``. Counts only, never ids or text.
    """

    if not isinstance(tenant_id, str) or not tenant_id:
        raise ValueError("search plane reconcile tenant is invalid")
    if client is None:
        client = build_client(settings)
    namespace_name = settings.namespace(tenant_id)
    namespace = client.namespace(namespace_name)
    with store.connect() as connection:
        live = {
            row["passage_id"]
            for row in connection.execute(LIVE_PASSAGE_IDS_SQL, (tenant_id, policy_fingerprint)).fetchall()
            if isinstance(row.get("passage_id"), str)
        }
        connection.commit()
    LOG.info("search plane reconcile live_passages=%s", len(live))
    try:
        present = namespace_ids(namespace, page_rows=page_rows)
    except Exception as error:  # noqa: BLE001 - the SDK's NotFoundError, by class name
        if type(error).__name__ != "NotFoundError":
            raise
        present = set()
    stale = sorted(present - live)
    missing = sorted(live - present)
    LOG.info(
        "search plane reconcile namespace_rows=%s stale=%s missing=%s apply=%s",
        len(present), len(stale), len(missing), apply,
    )
    deleted = 0
    written = 0
    if apply:
        for start in range(0, len(stale), delete_rows):
            batch = stale[start:start + delete_rows]
            namespace.write(deletes=batch)
            deleted += len(batch)
            LOG.info("search plane reconcile deleted=%s/%s", deleted, len(stale))
        if missing:
            if projector is None:
                raise ValueError("search plane reconcile needs a projector to write missing passages")
            written = int(projector.upsert_passages(tenant_id, missing))
            LOG.info("search plane reconcile written=%s/%s", written, len(missing))
    return {
        "status": "ok",
        "tenant_id": tenant_id,
        "policy_fingerprint": policy_fingerprint,
        "namespace": namespace_name,
        "live_passages": len(live),
        "namespace_rows": len(present),
        "stale": len(stale),
        "missing": len(missing),
        "applied": bool(apply),
        "deleted": deleted,
        "written": written,
    }
