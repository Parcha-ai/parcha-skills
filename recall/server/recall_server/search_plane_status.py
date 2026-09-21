"""``search-plane-status``: is the turbopuffer namespace caught up with Postgres?

The drain gate before migration 067 (H3-e'). Content-free: live passages
under the current policy fingerprint in the catalog, the namespace's
``approx_row_count`` from turbopuffer, the outbox depth, the built
source-months, and ``drift = passages - rows``. A namespace that does not
exist yet counts as zero rows.
"""
from __future__ import annotations

from itertools import islice
import logging
import tempfile
from typing import Any

from .search_outbox import search_outbox_pending
from .turbopuffer_plane import TurbopufferSettings, build_client
from .turbopuffer_projection import _LIVE_PASSAGE_PREDICATE

LOG = logging.getLogger("recall.search_plane")

LIVE_PASSAGE_IDS_PAGE_SQL = f"""
    SELECT passage.passage_id AS passage_id
      FROM canonical_passages passage
      JOIN canonical_passage_documents projected
        USING(tenant_id,source_id,logical_document_id,revision,policy_fingerprint)
     WHERE passage.tenant_id=%s
       AND projected.policy_fingerprint=%s
       AND passage.passage_id>%s
       AND {_LIVE_PASSAGE_PREDICATE}
     ORDER BY passage.passage_id
     LIMIT %s
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


def namespace_id_pages(namespace: Any, *, page_rows: int = RECONCILE_PAGE_ROWS):
    """Yield sorted namespace ids a page at a time, without attributes."""

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
            return
        yield from page
        last = page[-1]
        if len(page) < page_rows:
            return


def namespace_ids(namespace: Any, *, page_rows: int = RECONCILE_PAGE_ROWS) -> set[str]:
    """Compatibility helper for callers that explicitly need the complete set."""

    return set(namespace_id_pages(namespace, page_rows=page_rows))


def live_passage_id_pages(
    store: Any,
    *,
    tenant_id: str,
    policy_fingerprint: str,
    page_rows: int = RECONCILE_PAGE_ROWS,
):
    """Yield unique live ids using short, restartable keyset transactions.

    A page is the unit of database work.  The strict id cursor makes a rerun
    safe after connection loss, and avoids the former unbounded query plus
    ``fetchall()``. Passage ids can occur under more than one source, so
    adjacent duplicates are collapsed.
    """

    last = ""
    while True:
        with store.connect() as connection:
            rows = connection.execute(
                LIVE_PASSAGE_IDS_PAGE_SQL,
                (tenant_id, policy_fingerprint, last, page_rows),
            ).fetchall()
            connection.commit()
        page = [
            row["passage_id"]
            for row in rows
            if isinstance(row.get("passage_id"), str)
        ]
        if not page:
            return
        seen = last
        for passage_id in page:
            if passage_id != seen:
                yield passage_id
                seen = passage_id
        last = page[-1]
        if len(page) < page_rows:
            return


def _next_or_none(values: Any) -> str | None:
    try:
        return next(values)
    except StopIteration:
        return None


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
    if isinstance(page_rows, bool) or not isinstance(page_rows, int) or page_rows < 1:
        raise ValueError("search plane reconcile page rows are invalid")
    if isinstance(delete_rows, bool) or not isinstance(delete_rows, int) or delete_rows < 1:
        raise ValueError("search plane reconcile delete rows are invalid")
    if client is None:
        client = build_client(settings)
    namespace_name = settings.namespace(tenant_id)
    namespace = client.namespace(namespace_name)
    present_ids = iter(namespace_id_pages(namespace, page_rows=page_rows))

    live_ids = iter(live_passage_id_pages(
        store,
        tenant_id=tenant_id,
        policy_fingerprint=policy_fingerprint,
        page_rows=page_rows,
    ))
    live_id = _next_or_none(live_ids)
    try:
        present_id = _next_or_none(present_ids)
    except Exception as error:  # noqa: BLE001 - the SDK's NotFoundError, by class name
        if type(error).__name__ != "NotFoundError":
            raise
        present_ids = iter(())
        present_id = None
    live_count = 0
    present_count = 0
    stale_count = 0
    missing_count = 0
    deleted = 0
    written = 0
    # Applying while the namespace is being paged would change the input and
    # corrupt the exact initial counts. Spool content-free ids to bounded disk,
    # finish the comparison, then replay idempotent batches.
    with tempfile.TemporaryFile(mode="w+t", encoding="ascii") as stale_ids, \
            tempfile.TemporaryFile(mode="w+t", encoding="ascii") as missing_ids:
        while live_id is not None or present_id is not None:
            if present_id is None or (live_id is not None and live_id < present_id):
                live_count += 1
                missing_count += 1
                if apply:
                    missing_ids.write(live_id + "\n")
                live_id = _next_or_none(live_ids)
            elif live_id is None or present_id < live_id:
                present_count += 1
                stale_count += 1
                if apply:
                    stale_ids.write(present_id + "\n")
                present_id = _next_or_none(present_ids)
            else:
                live_count += 1
                present_count += 1
                live_id = _next_or_none(live_ids)
                present_id = _next_or_none(present_ids)

        if apply:
            if missing_count and projector is None:
                raise ValueError("search plane reconcile needs a projector to write missing passages")
            stale_ids.seek(0)
            stale_lines = (value.rstrip("\n") for value in stale_ids)
            while batch := list(islice(stale_lines, delete_rows)):
                namespace.write(deletes=batch)
                deleted += len(batch)
            missing_ids.seek(0)
            missing_lines = (value.rstrip("\n") for value in missing_ids)
            while batch := list(islice(missing_lines, delete_rows)):
                written += int(projector.upsert_passages(tenant_id, batch))
    LOG.info(
        "search plane reconcile live_passages=%s namespace_rows=%s stale=%s missing=%s apply=%s",
        live_count, present_count, stale_count, missing_count, apply,
    )
    return {
        "status": "ok",
        "tenant_id": tenant_id,
        "policy_fingerprint": policy_fingerprint,
        "namespace": namespace_name,
        "live_passages": live_count,
        "namespace_rows": present_count,
        "stale": stale_count,
        "missing": missing_count,
        "applied": bool(apply),
        "deleted": deleted,
        "written": written,
    }
