"""Inactive exact receipt metadata lookup for admitted singleton documents.

No production caller enables this boundary yet. Profile support requires a
separate SQL admission owner; archive pointers or missing chunk rows are never
proof of admission. This module does not hydrate bodies or grant source access.
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
            or (not row['is_current'] and not (include_deleted and row['document_deleted_at'] is not None))):
        raise ReceiptMembershipError('receipt_membership_profile_invalid')
    receipt = item_receipt(row['source_id'], row['native_id'], row['revision'], 0)
    _identity(receipt)
    identity = '\x1f'.join((row['tenant_id'], row['source_id'], row['document_id'], '0'))
    return {**{key: row[key] for key in ('tenant_id', 'source_id', 'document_id',
              'native_id', 'revision', 'is_current')},
            'chunk_id': 'chk_' + hashlib.sha256(identity.encode()).hexdigest()[:32],
            'ordinal': 0, 'receipt': receipt, 'text_sha256': row['document_text_sha256']}


def lookup_receipt_metadata(
    store: Any, *, tenant_id: str, authorized_sources: tuple[str, ...],
    receipts: tuple[str, ...], allow_verified_singletons: bool = False,
    current_only: bool = True, deadline_at: float | None = None,
    connection: Any = None, include_deleted: bool = False,
) -> dict[str, dict[str, Any]]:
    """Resolve a bounded caller-selected receipt set using exact indexed keys.

    Source grants remain the caller's authority. The optional existing SQL
    transaction allows lifecycle owners to retain their lock/snapshot boundary.
    The future profile column is never queried by the default path.
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
