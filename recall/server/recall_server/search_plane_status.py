"""``search-plane-status``: is the turbopuffer namespace caught up with Postgres?

The drain gate before migration 067 (H3-e'). Content-free: live passages
under the current policy fingerprint in the catalog, the namespace's
``approx_row_count`` from turbopuffer, the outbox depth, the built
source-months, and ``drift = passages - rows``. A namespace that does not
exist yet counts as zero rows.
"""
from __future__ import annotations

from typing import Any

from .search_outbox import search_outbox_pending
from .turbopuffer_plane import TurbopufferSettings, build_client
from .turbopuffer_projection import _LIVE_PASSAGE_PREDICATE

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
