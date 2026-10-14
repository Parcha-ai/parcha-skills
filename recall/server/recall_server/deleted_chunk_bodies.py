"""Reclaim text from chunks that canonical deletion already made unreadable."""

import math
import time

from .chunk_retirement import (
    ChunkRetirementError,
    _try_parent_native_locks,
    require_retirement_owner,
)
from .logical_evidence import IDENTITY_RE

MAX_KEYS = 1000
_SNAPSHOT = """SELECT chunk.chunk_id,chunk.document_id,document.native_id,
        octet_length(chunk.text_redacted)::bigint AS body_bytes
    FROM canonical_chunks chunk JOIN canonical_documents document
      USING(tenant_id,source_id,document_id)
    WHERE chunk.tenant_id=%s AND chunk.source_id=%s AND chunk.chunk_id=ANY(%s)
      AND chunk.deleted_at IS NOT NULL AND document.deleted_at IS NOT NULL
      AND NOT document.is_current
    ORDER BY chunk.chunk_id"""
_DOCUMENT_SNAPSHOT = """SELECT document_id,octet_length(text_redacted)::bigint AS body_bytes
    FROM canonical_documents
    WHERE tenant_id=%s AND source_id=%s AND document_id=ANY(%s)
      AND deleted_at IS NOT NULL AND NOT is_current
    ORDER BY document_id"""


def clear_deleted_chunk_bodies(
    store,
    *,
    tenant_id,
    source_id,
    principal_id,
    chunk_ids,
    apply=False,
    include_document_bodies=False,
    deadline_at=None,
):
    """Clear only reviewed exact keys; dry-run by default, no archive dependency.

    Existing metadata remains authoritative for receipts, replay and forget.
    Duplicate document bodies require explicit opt-in; their bytes are counted
    separately once per document so chunk-only callers keep their budgets.
    Return counters only after COMMIT acknowledgment. An unknown commit can be
    reconciled by inspecting the same keys; retrying never clears live bodies.
    """
    if (
        any(
            not isinstance(value, str) or not IDENTITY_RE.fullmatch(value)
            for value in (tenant_id, source_id, principal_id)
        )
        or not isinstance(chunk_ids, tuple)
        or not 1 <= len(chunk_ids) <= MAX_KEYS
        or any(
            not isinstance(value, str) or not IDENTITY_RE.fullmatch(value)
            for value in chunk_ids
        )
        or len(set(chunk_ids)) != len(chunk_ids)
        or type(apply) is not bool
        or type(include_document_bodies) is not bool
    ):
        raise ChunkRetirementError("deleted_chunk_scope_invalid")
    if deadline_at is None:
        deadline_at = time.monotonic() + 5
    if type(deadline_at) not in (int, float) or not math.isfinite(deadline_at):
        raise ChunkRetirementError("deleted_chunk_deadline_invalid")
    params = (tenant_id, source_id, list(chunk_ids))
    with store.connect() as connection, connection.transaction():
        if not apply:
            connection.execute("SET TRANSACTION READ ONLY")
        connection.execute("SET LOCAL lock_timeout='250ms'")
        require_retirement_owner(
            store,
            connection,
            (tenant_id, source_id),
            principal_id,
            deadline_at,
            lock=apply,
        )

        def query(sql, args):
            return store._execute_bounded(connection, sql, args, deadline_at)

        rows = query(_SNAPSHOT, params).fetchall()
        if apply and rows:
            natives = [
                {"native_id": native} for native in {row["native_id"] for row in rows}
            ]
            if not _try_parent_native_locks(query, (tenant_id, source_id), natives):
                raise ChunkRetirementError("deleted_chunk_lock_busy")
            documents = sorted({row["document_id"] for row in rows})
            locked_docs = query(
                """SELECT document_id FROM canonical_documents
                WHERE tenant_id=%s AND source_id=%s AND document_id=ANY(%s)
                  AND deleted_at IS NOT NULL AND NOT is_current
                ORDER BY document_id FOR UPDATE NOWAIT""",
                (tenant_id, source_id, documents),
            ).fetchall()
            if {row["document_id"] for row in locked_docs} != set(documents):
                raise ChunkRetirementError("deleted_chunk_scope_changed")
            locked_rows = query(
                _SNAPSHOT + " FOR UPDATE OF chunk NOWAIT", params
            ).fetchall()
            if {(row["chunk_id"], row["document_id"]) for row in locked_rows} != {
                (row["chunk_id"], row["document_id"]) for row in rows
            }:
                raise ChunkRetirementError("deleted_chunk_scope_changed")
            rows = locked_rows
        eligible = sum(row["body_bytes"] > 0 for row in rows)
        body_bytes = sum(row["body_bytes"] for row in rows)
        document_rows = []
        if include_document_bodies and rows:
            document_rows = query(
                _DOCUMENT_SNAPSHOT,
                (tenant_id, source_id, sorted({row["document_id"] for row in rows})),
            ).fetchall()
        eligible_documents = sum(row["body_bytes"] > 0 for row in document_rows)
        document_bytes = sum(row["body_bytes"] for row in document_rows)
        if apply and eligible:
            changed = query(
                """UPDATE canonical_chunks chunk SET text_redacted=''
                FROM canonical_documents document
                WHERE chunk.tenant_id=%s AND chunk.source_id=%s
                  AND chunk.chunk_id=ANY(%s) AND chunk.text_redacted<>''
                  AND document.tenant_id=chunk.tenant_id AND document.source_id=chunk.source_id
                  AND document.document_id=chunk.document_id
                  AND chunk.deleted_at IS NOT NULL AND document.deleted_at IS NOT NULL
                  AND NOT document.is_current""",
                params,
            )
            if changed.rowcount != eligible:
                raise ChunkRetirementError("deleted_chunk_scope_changed")
        if apply and eligible_documents:
            changed = query(
                """UPDATE canonical_documents SET text_redacted=''
                WHERE tenant_id=%s AND source_id=%s AND document_id=ANY(%s)
                  AND deleted_at IS NOT NULL AND NOT is_current AND text_redacted<>''""",
                (tenant_id, source_id, [row["document_id"] for row in document_rows]),
            )
            if changed.rowcount != eligible_documents:
                raise ChunkRetirementError("deleted_chunk_scope_changed")
    return dict(
        status="cleared" if apply else "planned",
        requested_keys=len(chunk_ids),
        deleted_chunks=len(rows),
        eligible_chunks=eligible,
        eligible_utf8_bytes=body_bytes,
        cleared_chunks=eligible if apply else 0,
        cleared_utf8_bytes=body_bytes if apply else 0,
        eligible_documents=eligible_documents,
        eligible_document_utf8_bytes=document_bytes,
        cleared_documents=eligible_documents if apply else 0,
        cleared_document_utf8_bytes=document_bytes if apply else 0,
        physical_bytes_reclaimed=0,
    )


def discover_deleted_chunk_bodies(
    store,
    *,
    tenant_id,
    source_id,
    principal_id,
    after_chunk_id=None,
    through_chunk_id=None,
    limit=MAX_KEYS,
    include_document_bodies=False,
    deadline_at=None,
):
    """Advance one source-key window, including pages with no eligible bodies.

    Keep the returned upper key for the entire pass. A completed pass covers
    its traversed key range, not concurrent arrivals or global exhaustion.
    Returned candidates require the apply owner's fresh authority/deletion check.
    """
    if (
        any(
            not isinstance(value, str) or not IDENTITY_RE.fullmatch(value)
            for value in (tenant_id, source_id, principal_id)
        )
        or type(limit) is not int
        or not 1 <= limit <= MAX_KEYS
        or type(include_document_bodies) is not bool
        or any(
            value is not None
            and (not isinstance(value, str) or not IDENTITY_RE.fullmatch(value))
            for value in (after_chunk_id, through_chunk_id)
        )
        or after_chunk_id is not None
        and (through_chunk_id is None or after_chunk_id > through_chunk_id)
    ):
        raise ChunkRetirementError("deleted_chunk_scope_invalid")
    if deadline_at is None:
        deadline_at = time.monotonic() + 5
    if type(deadline_at) not in (int, float) or not math.isfinite(deadline_at):
        raise ChunkRetirementError("deleted_chunk_deadline_invalid")
    with store.connect() as connection, connection.transaction():
        connection.execute("SET TRANSACTION READ ONLY")
        connection.execute("SET LOCAL lock_timeout='250ms'")
        require_retirement_owner(
            store,
            connection,
            (tenant_id, source_id),
            principal_id,
            deadline_at,
            lock=False,
        )

        def query(sql, args):
            return store._execute_bounded(connection, sql, args, deadline_at)

        if through_chunk_id is None:
            upper = query(
                """SELECT chunk_id FROM canonical_chunks
                WHERE tenant_id=%s AND source_id=%s
                ORDER BY chunk_id DESC LIMIT 1""",
                (tenant_id, source_id),
            ).fetchone()
            through_chunk_id = upper["chunk_id"] if upper else None
        keys = []
        candidates = []
        documents = []
        if through_chunk_id is not None:
            lower = " AND chunk_id>%s" if after_chunk_id is not None else ""
            args = (
                (tenant_id, source_id, through_chunk_id)
                + ((after_chunk_id,) if after_chunk_id is not None else ())
                + (limit,)
            )
            keys = query(
                """SELECT chunk_id FROM canonical_chunks
                WHERE tenant_id=%s AND source_id=%s AND chunk_id<=%s"""
                + lower
                + " ORDER BY chunk_id LIMIT %s",
                args,
            ).fetchall()
            if keys:
                candidates = query(
                    _SNAPSHOT, (tenant_id, source_id, [row["chunk_id"] for row in keys])
                ).fetchall()
                if include_document_bodies and candidates:
                    documents = query(
                        _DOCUMENT_SNAPSHOT,
                        (
                            tenant_id,
                            source_id,
                            sorted({row["document_id"] for row in candidates}),
                        ),
                    ).fetchall()
    next_key = keys[-1]["chunk_id"] if keys else after_chunk_id
    document_ids = {row["document_id"] for row in documents if row["body_bytes"] > 0}
    return dict(
        candidate_chunk_ids=tuple(
            row["chunk_id"]
            for row in candidates
            if row["body_bytes"] > 0 or row["document_id"] in document_ids
        ),
        scanned_keys=len(keys),
        after_chunk_id=next_key,
        through_chunk_id=through_chunk_id,
        pass_complete=(len(keys) < limit or next_key == through_chunk_id),
        eligible_utf8_bytes=sum(row["body_bytes"] for row in candidates),
        eligible_document_utf8_bytes=sum(row["body_bytes"] for row in documents),
    )
