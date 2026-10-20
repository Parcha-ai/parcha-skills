"""Bounded presence-first repair of retained passages after a native rebuild.

The existing publisher owns authority, publication leases and writes. This queue
only records which parent pages still need comparison; it is not a certificate.
A lost vendor ACK leaves the cursor before that page. Restart queries presence
again, but cannot promise exactly-once embedding across an unknown remote write.
"""
from __future__ import annotations

import hashlib
import time
from typing import Any

from psycopg.types.json import Jsonb

from .search_outbox import _month
from .turbopuffer_plane import namespace_schema

MAX_PAGE_ROWS = 128
MAX_METADATA_BYTES = 8 * 1024 * 1024


def available(connection: Any) -> bool:
    # Compatible readers/producers may be rolled out before optional schema094.
    return connection.execute(
        "SELECT to_regclass('public.search_parent_reconciliation') IS NOT NULL AS ready"
    ).fetchone()["ready"] is True


def enqueue(connection: Any, tenant_id: str, source_id: str, logical_document_id: str) -> None:
    if available(connection):
        connection.execute("""INSERT INTO search_parent_reconciliation
            (tenant_id,source_id,logical_document_id) VALUES (%s,%s,%s)
            ON CONFLICT(tenant_id,source_id,logical_document_id) DO UPDATE
            SET generation=search_parent_reconciliation.generation+1""",
            (tenant_id, source_id, logical_document_id))


def _scope(writer: Any, tenant_id: str) -> dict[str, Any]:
    return {
        "version": 1,
        "verified_singleton_receipts": getattr(writer.store, "verified_singleton_receipts", False),
        "destination": [writer.settings.region, writer.settings.namespace(tenant_id),
                        namespace_schema(writer.settings)],
        "credential_fingerprint": hashlib.sha256(
            ("search-publication-account-v1\0" + writer.settings.api_key).encode()).hexdigest(),
    }


def _checkpoint(value: Any, scope: dict[str, Any], generation: int) -> dict[str, Any]:
    if (isinstance(value, dict) and value.get("scope") == scope
            and type(value.get("generation")) is int and 0 < value["generation"] <= generation
            and type(value.get("after")) is int and value["after"] >= -1):
        return value
    return {"scope": scope, "generation": generation, "after": -1}


def pending(connection: Any, tenant_id: str) -> int:
    if not available(connection):
        return 0
    return int(connection.execute(
        "SELECT count(*) AS count FROM search_parent_reconciliation WHERE tenant_id=%s",
        (tenant_id,)).fetchone()["count"])


class ReconciliationMetadataBudgetExceeded(ValueError):
    """The parent remains owed; no oversized metadata page is transferred."""


def measure_metadata(writer: Any, connection: Any, tenant_id: str, ids: list[str],
                     deadline: float, byte_limit: int) -> None:
    from .turbopuffer_projection import _PASSAGE_SELECT_SQL, _PASSAGE_FROM_SQL
    # Original publication hydration shape, measured in the same RR snapshot as
    # the native read. Reserve certificate field overhead when that read is on.
    measured = writer.store._execute_bounded(connection,
        "SELECT coalesce(sum(octet_length(row_to_json(hydrated)::text)+128),0) AS bytes FROM ("
        + _PASSAGE_SELECT_SQL + _PASSAGE_FROM_SQL
        + " WHERE passage.tenant_id=%s AND passage.passage_id=ANY(%s::text[])) hydrated",
        (tenant_id, ids), deadline).fetchone()
    if measured["bytes"] > byte_limit:
        raise ReconciliationMetadataBudgetExceeded()


def metadata_prefix(writer: Any, connection: Any, tenant_id: str,
                    keys: list[dict[str, Any]], deadline: float) -> list[dict[str, Any]]:
    from .turbopuffer_projection import _PASSAGE_SELECT_SQL, _PASSAGE_FROM_SQL
    if not keys:
        return keys
    ids = [row["passage_id"] for row in keys]
    measured = writer.store._execute_bounded(connection,
        "SELECT hydrated.passage_id,octet_length(row_to_json(hydrated)::text)+128 AS bytes FROM ("
        + _PASSAGE_SELECT_SQL + _PASSAGE_FROM_SQL
        + " WHERE passage.tenant_id=%s AND passage.passage_id=ANY(%s::text[])) hydrated",
        (tenant_id, ids), deadline).fetchall()
    sizes = {row["passage_id"]: row["bytes"] for row in measured}
    if len(measured) != len(ids) or set(sizes) != set(ids):
        raise ValueError("search_parent_reconciliation_metadata_unavailable")
    total = 0
    prefix = []
    for key in keys:
        size = sizes[key["passage_id"]]
        if total + size > MAX_METADATA_BYTES:
            if not prefix:
                raise ReconciliationMetadataBudgetExceeded()
            break
        total += size
        prefix.append(key)
    return prefix


def step(writer: Any, tenant_id: str) -> dict[str, int]:
    """Attempt one parent page; leave all unacknowledged work durable.

    Generation changes preserve an active pass even across process restarts.
    At EOF, a changed generation starts one more pass for changes behind the
    cursor. Other parents never invalidate this parent's progress.
    """
    result = {"attempted": 0, "examined": 0, "written": 0}
    page_limit = min(MAX_PAGE_ROWS, writer.page_rows)
    deadline = time.monotonic() + writer.store.search_deadline_ms / 1000
    with writer.store.connect() as connection:
        if not available(connection):
            return result
        claim = writer.store._execute_bounded(connection, """SELECT *
            FROM search_parent_reconciliation WHERE tenant_id=%s
            ORDER BY attempted_at,source_id,logical_document_id
            LIMIT 1 FOR UPDATE SKIP LOCKED""", (tenant_id,), deadline).fetchone()
        if not claim:
            return result
        result["attempted"] = 1
        key = (tenant_id, claim["source_id"], claim["logical_document_id"])
        checkpoint = _checkpoint(claim["page_checkpoint"], _scope(writer, tenant_id), claim["generation"])
        claimed = writer.store._execute_bounded(connection, """UPDATE search_parent_reconciliation
            SET attempted_at=clock_timestamp(),page_checkpoint=%s
            WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s
            RETURNING attempted_at""",
            (Jsonb(checkpoint), *key), deadline).fetchone()
        attempted_at = claimed["attempted_at"]
        connection.commit()
    # Read keys and native authority together. Ordinal uses the existing parent
    # index; a later generation revisits any concurrent moves behind the cursor.
    with writer.store.connect() as connection:
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        keys = writer.store._execute_bounded(connection, """SELECT passage.passage_id,passage.ordinal
            FROM canonical_passage_documents projected
            JOIN canonical_passages passage USING(tenant_id,source_id,logical_document_id,revision,policy_fingerprint)
            WHERE projected.tenant_id=%s AND projected.source_id=%s
              AND projected.logical_document_id=%s AND passage.ordinal>%s
            ORDER BY passage.ordinal LIMIT %s""",
            (*key, checkpoint["after"], page_limit), deadline).fetchall()
        # A short byte-bounded prefix is not EOF. Measure only IDs and sizes
        # before transferring the original native metadata in this snapshot.
        selected_count = len(keys)
        keys = metadata_prefix(writer, connection, tenant_id, keys, deadline)
        has_more = selected_count == page_limit or len(keys) < selected_count
        ids = [row["passage_id"] for row in keys]
        page = writer.passages_by_id(connection, tenant_id, ids) if ids else []
        if {row["passage_id"] for row in page} != set(ids):
            raise ValueError("search_parent_reconciliation_authority_unavailable")
        connection.commit()
    result["examined"] = len(ids)
    if ids:
        namespace = writer.client.namespace(writer.settings.namespace(tenant_id))
        fields = ["source_id", "logical_document_id", "policy_fingerprint", "text_sha256", "header"]
        try:
            response = namespace.query(rank_by=("id", "asc"), filters=("id", "In", ids),
                                       limit=len(ids), include_attributes=fields)
            raw = response.get("rows", []) if isinstance(response, dict) else response.rows
        except Exception as error:
            if type(error).__name__ != "NotFoundError":
                raise
            raw = []
        present = {}
        for value in raw:
            row = value if isinstance(value, dict) else value.model_dump()
            identifier = row.get("id")
            if identifier not in ids or identifier in present:
                raise ValueError("search_parent_reconciliation_vendor_scope")
            present[identifier] = row
        missing = []
        groups = {}
        for row in page:
            expected = {field: row[field] for field in fields if field != "header"}
            expected["header"] = row.get("header_redacted") or ""
            current = present.get(row["passage_id"], {})
            if any(current.get(field) != value for field, value in expected.items()):
                missing.append(row["passage_id"])
                groups.setdefault((row["source_id"], _month(row["first_occurred_at"])), []).append(row["passage_id"])
        # Native write owner repeats authority under the existing source/month
        # lease. Reuse only routes from the authorized comparison, never bodies
        # or vendor metadata. Receipt/revision changes alone do not re-embed.
        result["written"] = writer._upsert_passage_groups(tenant_id, groups, namespace=namespace,
            budget={"remaining": writer.rate_limit_budget_seconds, "rate_limited": 0},
            metadata_byte_limit=MAX_METADATA_BYTES)
        if result["written"] != len(missing):
            raise ValueError("search_parent_reconciliation_write_incomplete")
    deadline = time.monotonic() + writer.store.search_deadline_ms / 1000
    with writer.store.connect() as connection:
        if has_more:
            next_checkpoint = {**checkpoint, "after": keys[-1]["ordinal"]}
            writer.store._execute_bounded(connection, """UPDATE search_parent_reconciliation
                SET page_checkpoint=%s WHERE tenant_id=%s AND source_id=%s
                  AND logical_document_id=%s AND page_checkpoint=%s AND attempted_at=%s""",
                (Jsonb(next_checkpoint), *key, Jsonb(checkpoint), attempted_at), deadline)
        else:
            # Delete only the fully observed generation. A concurrent enqueue
            # survives and is reset in the same transaction for its next pass.
            writer.store._execute_bounded(connection, """DELETE FROM search_parent_reconciliation
                WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s
                  AND generation=%s AND page_checkpoint=%s AND attempted_at=%s""",
                (*key, checkpoint["generation"], Jsonb(checkpoint), attempted_at), deadline)
            writer.store._execute_bounded(connection, """UPDATE search_parent_reconciliation
                SET page_checkpoint=NULL WHERE tenant_id=%s AND source_id=%s
                  AND logical_document_id=%s AND page_checkpoint=%s AND attempted_at=%s""",
                (*key, Jsonb(checkpoint), attempted_at), deadline)
        connection.commit()
    return result
