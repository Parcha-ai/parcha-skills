"""Explicit offline membership enrollment and comparison; no search caller uses it.

The certificate columns exist only in the disposable-PG proof fixture for now.
This owner never changes the native verifier or trusts a certificate in a response.
"""
from __future__ import annotations

import hashlib
import json
import math
import time
from typing import Any

from .db import V2_AUTHORITY_RE
from .receipt_membership import _identity, lookup_current_search_membership

_BINDING_FIELDS = (
    'tenant_id', 'source_id', 'native_parent_id', 'logical_document_id',
    'passage_id', 'policy_fingerprint', 'text_sha256', 'receipts',
    'manifest_content_sha256', 'manifest_revision', 'projected_revision',
    'passage_revision', 'source_document_sha256',
)
_SELECT = """SELECT passage.tenant_id,passage.source_id,passage.passage_id,
    passage.logical_document_id,passage.policy_fingerprint,passage.text_sha256,
    passage.receipts,passage.revision AS passage_revision,
    passage.membership_generation,passage.membership_binding_sha256,
    evidence.native_parent_id,evidence.manifest_content_sha256,
    evidence.revision AS manifest_revision,
    projected.revision AS projected_revision,projected.source_document_sha256,
    authority.generation
  FROM canonical_passages passage
  JOIN canonical_passage_documents projected
    USING(tenant_id,source_id,logical_document_id,policy_fingerprint)
  JOIN canonical_evidence_documents evidence USING(tenant_id,source_id,logical_document_id)
  LEFT JOIN canonical_parent_authority authority
    ON authority.tenant_id=evidence.tenant_id AND authority.source_id=evidence.source_id
   AND authority.native_parent_id=evidence.native_parent_id
 WHERE passage.tenant_id=%s AND passage.source_id=%s
   AND evidence.native_parent_id=%s AND passage.passage_id=ANY(%s)
 ORDER BY passage.passage_id"""

# Physical membership uses the actual chunk owner, not the native ID parsed
# from its receipt. A foreign physical owner cannot be certified under this parent.
_OWNERS = """WITH requested AS MATERIALIZED (
    SELECT * FROM unnest(%s::text[],%s::text[],%s::integer[],%s::integer[])
      AS wanted(receipt,native_id,revision,ordinal)
), physical AS MATERIALIZED (
    SELECT requested.receipt,coalesce(event.native_parent_id,event.native_id) AS parent
      FROM requested JOIN canonical_chunks chunk ON chunk.tenant_id=%s
       AND chunk.source_id=%s AND chunk.receipt=requested.receipt AND chunk.deleted_at IS NULL
      JOIN canonical_documents document USING(tenant_id,source_id,document_id)
      JOIN canonical_events event USING(tenant_id,source_id,event_id)
     WHERE document.is_current AND document.deleted_at IS NULL
), owned AS (
    SELECT receipt,bool_and(parent=%s) AS owned FROM physical GROUP BY receipt
    UNION ALL
    SELECT requested.receipt,coalesce(event.native_parent_id,event.native_id)=%s AS owned
      FROM requested JOIN canonical_documents document ON document.tenant_id=%s
       AND document.source_id=%s AND document.native_id=requested.native_id
       AND document.revision=requested.revision
      JOIN canonical_events event USING(tenant_id,source_id,event_id)
     WHERE document.is_current AND document.deleted_at IS NULL
       AND document.chunk_receipt_profile=1 AND requested.ordinal=0
       AND NOT EXISTS(SELECT 1 FROM physical WHERE physical.receipt=requested.receipt)
       AND NOT EXISTS(SELECT 1 FROM canonical_chunks remaining
           WHERE remaining.tenant_id=document.tenant_id AND remaining.source_id=document.source_id
             AND remaining.document_id=document.document_id)
)
SELECT receipt,bool_and(owned) AS owned FROM owned GROUP BY receipt"""


def _scope(*, tenant_id, principal_id, source_id, native_parent_id, passage_ids, deadline_at):
    if (any(not isinstance(value,str) or not V2_AUTHORITY_RE.fullmatch(value)
            for value in (tenant_id,principal_id,source_id))
            or not isinstance(native_parent_id,str) or not native_parent_id
            or not isinstance(passage_ids,tuple) or not passage_ids
            or any(not isinstance(value,str) or not value for value in passage_ids)
            or not isinstance(deadline_at,(int,float)) or not math.isfinite(deadline_at)):
        raise ValueError('parent_membership_scope_invalid')
    return dict(tenant_id=tenant_id,principal_id=principal_id,source_id=source_id,
                native_parent_id=native_parent_id,passage_ids=tuple(sorted(set(passage_ids))))


def _query(store, connection, statement, parameters, deadline):
    return store._execute_bounded(connection,statement,parameters,deadline)


def _authorized(store, connection, scope, deadline):
    row=_query(store,connection,"""SELECT EXISTS(
        SELECT 1 FROM brain_access_grants WHERE tenant_id=%s AND principal_id=%s
         AND permission IN ('owner','admin','read')) AND EXISTS(
        SELECT 1 FROM canonical_source_grants WHERE tenant_id=%s AND principal_id=%s
         AND source_id=%s AND permission IN ('owner','read')) AS allowed""",
        (scope['tenant_id'],scope['principal_id'],scope['tenant_id'],scope['principal_id'],scope['source_id']),deadline).fetchone()
    return bool(row['allowed'])


def _rows(store, connection, scope, deadline, *, lock=False):
    return _query(store,connection,_SELECT+(' FOR UPDATE OF passage' if lock else ''),
                  (scope['tenant_id'],scope['source_id'],scope['native_parent_id'],list(scope['passage_ids'])),deadline).fetchall()


def _binding(row):
    payload=['recall.passage-membership-binding.v1',*[row[field] for field in _BINDING_FIELDS]]
    return hashlib.sha256(json.dumps(payload,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def _complete(rows, scope):
    return len(rows)==len(scope['passage_ids']) and all(row['receipts'] for row in rows)


def _capture(store, scope, deadline, *, prove_ownership):
    with store.connect() as connection, connection.transaction():
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        if not _authorized(store,connection,scope,deadline):
            return dict(status='unauthorized')
        rows=_rows(store,connection,scope,deadline)
        if not _complete(rows,scope):
            return dict(status='incomplete')
        receipts=tuple(dict.fromkeys(receipt for row in rows for receipt in row['receipts']))
        members=set()
        for start in range(0,len(receipts),512):
            batch=receipts[start:start+512]
            current=lookup_current_search_membership(store,tenant_id=scope['tenant_id'],
                authorized_sources=(scope['source_id'],),receipts=batch,
                connection=connection,deadline_at=deadline)
            members.update(current)
            if prove_ownership:
                if current!={(scope['source_id'],receipt) for receipt in batch}:
                    return dict(status='membership-unproved')
                identities=[_identity(receipt) for receipt in batch]
                owned=_query(store,connection,_OWNERS,
                    (list(batch),[row[1] for row in identities],[row[2] for row in identities],
                     [row[3] for row in identities],scope['tenant_id'],scope['source_id'],
                     scope['native_parent_id'],scope['native_parent_id'],scope['tenant_id'],scope['source_id']),deadline).fetchall()
                if {row['receipt'] for row in owned if row['owned']}!=set(batch):
                    return dict(status='ownership-unproved')
        return dict(status='captured',rows=rows,members=members,unique_receipts=len(receipts))


def enroll_parent_passages(store: Any, *, deadline_at: float, **arguments) -> dict[str, Any]:
    """Prove a fixed explicit passage set, then publish only its captured generation."""
    scope=_scope(**arguments,deadline_at=deadline_at)
    with store.connect() as connection, connection.transaction():
        if not _authorized(store,connection,scope,deadline_at):
            return dict(status='unauthorized')
        # Initialize only a real, never-certified publication. Recreating a lost
        # authority row could otherwise make an older certificate match again.
        _query(store,connection,"""INSERT INTO canonical_parent_authority
            (tenant_id,source_id,native_parent_id,generation)
            SELECT evidence.tenant_id,evidence.source_id,evidence.native_parent_id,1
              FROM canonical_evidence_documents evidence
             WHERE evidence.tenant_id=%s AND evidence.source_id=%s AND evidence.native_parent_id=%s
               AND NOT EXISTS(SELECT 1 FROM canonical_parent_authority authority
                   WHERE authority.tenant_id=evidence.tenant_id AND authority.source_id=evidence.source_id
                     AND authority.native_parent_id=evidence.native_parent_id)
               AND EXISTS(SELECT 1 FROM canonical_passages passage
                   WHERE passage.tenant_id=evidence.tenant_id AND passage.source_id=evidence.source_id
                     AND passage.logical_document_id=evidence.logical_document_id
                     AND passage.passage_id=ANY(%s))
               AND NOT EXISTS(SELECT 1 FROM canonical_passages passage
                   WHERE passage.tenant_id=evidence.tenant_id AND passage.source_id=evidence.source_id
                     AND passage.logical_document_id=evidence.logical_document_id
                     AND passage.membership_generation IS NOT NULL)
            ON CONFLICT DO NOTHING""",(scope['tenant_id'],scope['source_id'],scope['native_parent_id'],
                                      list(scope['passage_ids'])),deadline_at)
    captured=_capture(store,scope,deadline_at,prove_ownership=True)
    if captured['status']!='captured':
        return captured
    rows=captured['rows']
    if any(row['generation'] is None for row in rows):
        return dict(status='unknown-generation')
    bindings={row['passage_id']:(_binding(row),row['generation']) for row in rows}
    logical_ids={row['logical_document_id'] for row in rows}
    if len(logical_ids)!=1:
        raise ValueError('parent_membership_publication_ambiguous')
    with store.connect() as connection, connection.transaction():
        # Reuse the native passage publisher's lock; never lock authority rows.
        _query(store,connection,'SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',
               ('lossless-passages\x1f'+next(iter(logical_ids)),),deadline_at)
        if not _authorized(store,connection,scope,deadline_at):
            return dict(status='unauthorized')
        current=_rows(store,connection,scope,deadline_at,lock=True)
        if (not _complete(current,scope) or any(
                bindings[row['passage_id']]!=(_binding(row),row['generation']) for row in current)):
            return dict(status='changed')
        updated=_query(store,connection,"""UPDATE canonical_passages passage
            SET membership_generation=certificate.generation,
                membership_binding_sha256=certificate.binding
            FROM unnest(%s::text[],%s::bigint[],%s::text[]) AS certificate(passage_id,generation,binding)
            WHERE passage.tenant_id=%s AND passage.source_id=%s
              AND passage.passage_id=certificate.passage_id""",
            (list(bindings),[value[1] for value in bindings.values()],[value[0] for value in bindings.values()],
             scope['tenant_id'],scope['source_id']),deadline_at)
        if updated.rowcount!=len(bindings):
            raise ValueError('parent_membership_publication_changed')
    return dict(status='enrolled',passages=len(rows),unique_receipts=captured['unique_receipts'])


def compare_parent_membership(store: Any, *, deadline_at: float, **arguments) -> dict[str, Any]:
    """Compare the candidate certificate branch with the unchanged native owner."""
    scope=_scope(**arguments,deadline_at=deadline_at)
    started=time.monotonic()
    captured=_capture(store,scope,deadline_at,prove_ownership=False)
    if captured['status']!='captured':
        return captured
    rows=captured['rows']
    certified={row['passage_id'] for row in rows if row['generation'] is not None
               and row['membership_generation']==row['generation']
               and row['membership_binding_sha256']==_binding(row)}
    native={row['passage_id'] for row in rows if all(
        (row['source_id'],receipt) in captured['members'] for receipt in row['receipts'])}
    return dict(status='compared',selected_passages=len(rows),unique_receipts=captured['unique_receipts'],
                certified_passages=len(certified),fallback_passages=len(rows)-len(certified),
                native_allowed_passages=len(native),disagreements=len(certified-native),
                elapsed_ms=round((time.monotonic()-started)*1000,3))
