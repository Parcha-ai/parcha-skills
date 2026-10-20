"""Exact receipt metadata authority for physical and admitted singleton rows.

Profile admission requires the native SQL lifecycle owner. Historical singleton
metadata also requires its exact committed evidence catalog entry. Missing rows
or archive pointers alone do not prove membership. Callers retain source-grant
checks; this module neither hydrates bodies nor grants source access.
"""
from __future__ import annotations

import hashlib
from typing import Any

from .db import V2_AUTHORITY_RE
from .projectors import item_receipt


class ReceiptMembershipError(ValueError):
    """A receipt or admitted document contradicts its exact identity contract."""


def _identity(receipt: str) -> tuple[str, str, int, int]:
    try:
        if not isinstance(receipt, str) or not receipt.startswith('recall://'):
            raise ValueError()
        event, ordinal_text = receipt.rsplit('#item=', 1)
        base, revision_text = event.rsplit('?rev=', 1)
        source, native = base[len('recall://'):].split('/', 1)
        revision, ordinal = int(revision_text), int(ordinal_text)
        if (not V2_AUTHORITY_RE.fullmatch(source) or not native
                or not 1 <= revision <= 2147483647 or not 0 <= ordinal <= 2147483647
                or item_receipt(source, native, revision, ordinal) != receipt):
            raise ValueError()
        return source, native, revision, ordinal
    except (ValueError, TypeError):
        raise ReceiptMembershipError('receipt_membership_identity_invalid') from None


def admitted_singleton_metadata(row: dict[str, Any], *, include_deleted: bool = False) -> dict[str, Any]:
    """Derive metadata from an exact SQL authority snapshot, never an archive pointer."""
    if (row['profile'] != 1 or row['physical_chunks_remain']
            or (not row['is_current']
                and not row.get('historical_singleton_archive', False)
                and not (include_deleted and row['document_deleted_at'] is not None))):
        raise ReceiptMembershipError('receipt_membership_profile_invalid')
    receipt = item_receipt(row['source_id'], row['native_id'], row['revision'], 0)
    _identity(receipt)
    identity = '\x1f'.join((row['tenant_id'], row['source_id'], row['document_id'], '0'))
    return {**{key: row[key] for key in ('tenant_id', 'source_id', 'document_id',
              'native_id', 'revision', 'is_current')},
            'chunk_id': 'chk_' + hashlib.sha256(identity.encode()).hexdigest()[:32],
            'ordinal': 0, 'receipt': receipt, 'text_sha256': row['document_text_sha256']}


def lookup_current_search_membership(
    store: Any, *, tenant_id: str, authorized_sources: tuple[str, ...],
    receipts: tuple[str, ...], connection: Any, deadline_at: float,
) -> set[tuple[str, str]]:
    """Keep physical search proof first; derive only unresolved admitted receipts.

    Supported ingest/forget writers atomically invalidate current documents on
    edit or deletion. Current search therefore retains its existing live chunk
    and current-document authority, without historical event probes. The full
    metadata lookup below deliberately keeps its stronger historical fences.

    This opt-in boundary requires the separately admitted profile column. The
    admission owner must set the profile and remove its exact chunk atomically.
    A flag never overrides a retained deleted or different-ordinal chunk.
    """
    if (not isinstance(tenant_id, str) or not V2_AUTHORITY_RE.fullmatch(tenant_id)
            or not isinstance(authorized_sources, tuple)
            or any(not isinstance(source, str) or not V2_AUTHORITY_RE.fullmatch(source)
                   for source in authorized_sources)
            or not isinstance(receipts, tuple)
            or any(not isinstance(receipt, str) for receipt in receipts)):
        raise ReceiptMembershipError('receipt_membership_authority_invalid')
    requested = []
    for receipt in dict.fromkeys(receipts):
        source, native, revision, ordinal = _identity(receipt)
        if source in authorized_sources:
            requested.append((receipt, source, native, revision, ordinal))
    if not requested:
        return set()
    rows = store._execute_bounded(connection, """WITH requested AS MATERIALIZED (
        SELECT * FROM unnest(%s::text[],%s::text[],%s::text[],%s::integer[],%s::integer[])
          AS wanted(receipt,source_id,native_id,revision,ordinal)
    ), physical AS MATERIALIZED (
        SELECT requested.source_id,requested.receipt
          FROM requested
          JOIN canonical_chunks chunk ON chunk.tenant_id=%s
           AND chunk.source_id=requested.source_id AND chunk.receipt=requested.receipt
           AND chunk.deleted_at IS NULL
          JOIN canonical_documents document ON document.tenant_id=chunk.tenant_id
           AND document.source_id=chunk.source_id AND document.document_id=chunk.document_id
           AND document.is_current AND document.deleted_at IS NULL
    ), unresolved AS MATERIALIZED (
        SELECT requested.* FROM requested WHERE NOT EXISTS (
            SELECT 1 FROM physical WHERE physical.source_id=requested.source_id
              AND physical.receipt=requested.receipt
        )
    )
    SELECT physical.source_id,physical.receipt,true AS allowed FROM physical
    UNION ALL
    SELECT unresolved.source_id,unresolved.receipt,
           document.chunk_receipt_profile=1 AND NOT EXISTS (
               SELECT 1 FROM canonical_chunks remaining
                WHERE remaining.tenant_id=document.tenant_id
                  AND remaining.source_id=document.source_id
                  AND remaining.document_id=document.document_id
           ) AS allowed
      FROM unresolved
      JOIN canonical_documents document ON document.tenant_id=%s
       AND document.source_id=unresolved.source_id AND document.native_id=unresolved.native_id
       AND document.revision=unresolved.revision
     WHERE document.is_current AND document.deleted_at IS NULL
       AND document.chunk_receipt_profile IS NOT NULL AND unresolved.ordinal=0
    """, tuple([row[index] for row in requested] for index in range(5)) + (
        tenant_id, tenant_id), deadline_at).fetchall()
    if any(not row['allowed'] for row in rows):
        raise ReceiptMembershipError('receipt_membership_profile_invalid')
    return {(row['source_id'], row['receipt']) for row in rows}


def lookup_receipt_metadata(
    store: Any, *, tenant_id: str, authorized_sources: tuple[str, ...],
    receipts: tuple[str, ...], allow_verified_singletons: bool = False,
    current_only: bool = True, deadline_at: float | None = None,
    connection: Any = None, include_deleted: bool = False,
) -> dict[str, dict[str, Any]]:
    """Resolve a bounded caller-selected receipt set using exact indexed keys.

    Source grants remain the caller's authority. The optional existing SQL
    transaction allows lifecycle owners to retain their lock/snapshot boundary.
    The admitted profile column is queried only when singleton support is enabled.
    Only the forget owner uses include_deleted after its own authority/lock gate.
    """
    if (not isinstance(tenant_id, str) or not V2_AUTHORITY_RE.fullmatch(tenant_id)
            or not isinstance(authorized_sources, tuple)
            or any(not isinstance(s, str) or not V2_AUTHORITY_RE.fullmatch(s)
                   for s in authorized_sources)
            or not isinstance(receipts, tuple)
            or type(allow_verified_singletons) is not bool or type(current_only) is not bool
            or type(include_deleted) is not bool or (include_deleted and current_only)):
        raise ReceiptMembershipError('receipt_membership_authority_invalid')
    requested = []
    for receipt in dict.fromkeys(receipts):
        source, native, revision, ordinal = _identity(receipt)
        if source in authorized_sources:
            requested.append((receipt, source, native, revision, ordinal))
    if not requested:
        return {}
    profile = 'document.chunk_receipt_profile' if allow_verified_singletons else 'NULL::smallint'
    statement = f"""WITH requested AS (
        SELECT * FROM unnest(%s::text[],%s::text[],%s::text[],%s::integer[],%s::integer[])
          AS wanted(receipt,source_id,native_id,revision,ordinal)
    )
    SELECT requested.receipt AS requested_receipt,requested.ordinal AS requested_ordinal,
           document.tenant_id,document.source_id,document.document_id,document.native_id,
           document.revision,document.is_current,document.deleted_at AS document_deleted_at,
           document.text_sha256 AS document_text_sha256,
           {profile} AS profile,
           CASE WHEN {profile}=1 AND NOT document.is_current
                AND document.deleted_at IS NULL THEN EXISTS (
               SELECT 1 FROM canonical_evidence_objects evidence
                WHERE evidence.tenant_id=document.tenant_id
                  AND evidence.source_id=document.source_id
                  AND evidence.document_id=document.document_id
                  AND evidence.revision=document.revision
                  AND evidence.text_sha256=document.text_sha256
                  AND evidence.receipt_count=1
           ) ELSE false END AS historical_singleton_archive,
           CASE WHEN {profile} IS NOT NULL THEN EXISTS (
               SELECT 1 FROM canonical_chunks remaining
                WHERE remaining.tenant_id=document.tenant_id
                  AND remaining.source_id=document.source_id
                  AND remaining.document_id=document.document_id
           ) ELSE false END AS physical_chunks_remain,
           chunk.chunk_id,chunk.ordinal,chunk.receipt,chunk.text_sha256,chunk.deleted_at AS chunk_deleted_at
      FROM requested
      JOIN canonical_documents document ON document.tenant_id=%s
       AND document.source_id=requested.source_id AND document.native_id=requested.native_id
       AND document.revision=requested.revision
      JOIN canonical_events event ON event.tenant_id=document.tenant_id
       AND event.source_id=document.source_id AND event.event_id=document.event_id
      LEFT JOIN canonical_chunks chunk ON chunk.tenant_id=document.tenant_id
       AND chunk.source_id=document.source_id AND chunk.document_id=document.document_id
       AND chunk.ordinal=requested.ordinal
     WHERE (%s OR document.deleted_at IS NULL) AND (NOT %s OR document.is_current)
       AND NOT event.is_tombstone
       AND (%s OR NOT EXISTS (SELECT 1 FROM canonical_events later
            WHERE later.tenant_id=document.tenant_id AND later.source_id=document.source_id
              AND later.native_id=document.native_id AND later.revision>document.revision
              AND later.is_tombstone))
    """
    params = tuple([row[index] for row in requested] for index in range(5)) + (
        tenant_id, include_deleted, current_only, include_deleted)
    def read(opened):
        return store._execute_bounded(opened, statement, params, deadline_at).fetchall()
    if connection is None:
        with store.connect() as opened:
            rows = read(opened)
    else:
        rows = read(connection)
    return receipt_metadata_from_rows(rows, include_deleted=include_deleted)


def receipt_metadata_from_rows(rows, *, include_deleted=False):
    """Validate rows from the native SQL authority snapshot; never infer it."""
    result = {}
    for row in rows:
        receipt = row['requested_receipt']
        if row['profile'] is not None:
            metadata = admitted_singleton_metadata(row, include_deleted=include_deleted)
            if row['requested_ordinal'] != 0:
                continue
            row.update(metadata)
        elif row['chunk_id'] is None or (row['chunk_deleted_at'] is not None and not include_deleted):
            continue
        if row['receipt'] != receipt or receipt in result:
            raise ReceiptMembershipError('receipt_membership_identity_invalid')
        result[receipt] = {key: row[key] for key in (
            'tenant_id', 'source_id', 'chunk_id', 'document_id', 'native_id',
            'revision', 'ordinal', 'receipt', 'text_sha256', 'is_current',
        )}
    return result


def verified_receipt_relation(
    store: Any, connection: Any, *, tenant_id: str,
    receipts: tuple[str, ...] | list[str], sources: tuple[str, ...] | list[str],
    deadline_at: float,
) -> tuple[str, tuple[Any, ...]]:
    """Render proven identities for filters in the caller's read-only snapshot."""
    from .db import SearchDeadlineExceeded
    from .deep_inspection import DeepInspectionError

    try:
        members = lookup_receipt_metadata(store, tenant_id=tenant_id,
            authorized_sources=tuple(sources), receipts=tuple(receipts),
            allow_verified_singletons=True, connection=connection, deadline_at=deadline_at)
    except (ReceiptMembershipError, SearchDeadlineExceeded) as error:
        raise DeepInspectionError("deep_inspector_receipt_scope_violation") from error
    # Callers keep their event/actor/time/admitted-document SQL predicates
    # after the same full owner has proved physical and admitted identities.
    relation = """(SELECT %s::text AS tenant_id,member.*,
        NULL::timestamptz AS deleted_at FROM unnest(
            %s::text[],%s::text[],%s::integer[],%s::text[]
        ) AS member(source_id,document_id,ordinal,receipt)) chunk"""
    parameters = (tenant_id,
        [m["source_id"] for m in members.values()],
        [m["document_id"] for m in members.values()],
        [m["ordinal"] for m in members.values()], list(members))
    return relation, parameters


def verified_document_chunks(store, connection, *, tenant_id, source_id, documents, deadline_at):
    """Supply complete chunk metadata to an existing bounded proof or lock owner.

    Callers retain their physical row locks and byte measurements. Only the full
    membership owner may supply an absent admitted singleton; derived metadata
    contributes zero physical body bytes.
    """
    requested = document_chunk_requests(source_id, documents)
    members = lookup_receipt_metadata(store, tenant_id=tenant_id,
        authorized_sources=(source_id,),
        receipts=tuple(dict.fromkeys(receipt for receipts in requested.values() for receipt in receipts)),
        allow_verified_singletons=True, connection=connection, deadline_at=deadline_at)
    return verified_document_chunk_metadata(documents, requested, members)


def document_chunk_requests(source_id, documents):
    """Retain each document's exact physical receipts or admitted-singleton request."""
    requested = {}
    for row in documents:
        requested[row['document_id']] = tuple(chunk['receipt'] for chunk in row['chunks']) or (
            item_receipt(source_id, row['native_id'], row['revision'], 0),)
    return requested


def verified_document_chunk_metadata(documents, requested, members):
    """Compare captured chunks with metadata proved by the native receipt owner."""
    result = {}
    for row in documents:
        physical = {chunk['receipt']: chunk for chunk in row['chunks']}
        proven = []
        for receipt in requested[row['document_id']]:
            member = members.get(receipt)
            if member is None:
                if receipt in physical:
                    raise ReceiptMembershipError('receipt_membership_identity_invalid')
                continue
            captured = physical.get(receipt)
            if (member['document_id'] != row['document_id'] or captured is not None and any(
                    captured[key] != member[key] for key in ('ordinal', 'receipt', 'text_sha256'))):
                raise ReceiptMembershipError('receipt_membership_identity_invalid')
            proven.append({key: member[key] for key in ('ordinal', 'receipt', 'text_sha256')} |
                          {'pg_bytes': 0 if captured is None else captured.get('pg_bytes', 0)})
        result[row['document_id']] = proven
    return result
