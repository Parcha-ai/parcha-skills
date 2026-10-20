"""Explicit membership enrollment and comparison; no search caller trusts it.

Certification requires separately installed nullable certificate columns.
This owner never changes the native verifier or trusts a certificate in a response.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import dataclass
import time
from typing import Any

from .db import SearchDeadlineExceeded, V2_AUTHORITY_RE
from .receipt_membership import _identity, lookup_current_search_membership

_CERTIFICATION_LIMITS = dict(passages=128,receipt_references=20000,metadata_bytes=8*1024*1024)

_BINDING_FIELDS = (
    'tenant_id', 'source_id', 'native_parent_id', 'logical_document_id',
    'passage_id', 'policy_fingerprint', 'text_sha256', 'receipts',
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


class _BudgetExceeded(ValueError):
    pass


def _scope(*, tenant_id, principal_id, source_id, native_parent_id, passage_ids, deadline_at, limits, binding_epoch):
    if (not isinstance(limits,dict) or set(limits)!={'passages','receipt_references','metadata_bytes'}
            or any(type(value) is not int or value<=0 for value in limits.values())):
        raise ValueError('parent_membership_budget_invalid')
    if isinstance(passage_ids,tuple) and len(passage_ids)>limits['passages']:
        raise _BudgetExceeded()
    if (any(not isinstance(value,str) or not V2_AUTHORITY_RE.fullmatch(value)
            for value in (tenant_id,principal_id,source_id))
            or not isinstance(binding_epoch,str) or not binding_epoch.strip()
            or not isinstance(native_parent_id,str) or not native_parent_id
            or not isinstance(passage_ids,tuple) or not passage_ids
            or any(not isinstance(value,str) or not value for value in passage_ids)
            or not isinstance(deadline_at,(int,float)) or not math.isfinite(deadline_at)):
        raise ValueError('parent_membership_scope_invalid')
    return dict(tenant_id=tenant_id,principal_id=principal_id,source_id=source_id,
                native_parent_id=native_parent_id,binding_epoch=binding_epoch,passage_ids=tuple(sorted(set(passage_ids))),limits=dict(limits))


def _connect(store, deadline):
    remaining=deadline-time.monotonic()
    if remaining<=0:
        raise SearchDeadlineExceeded('parent membership deadline exceeded')
    return store.connect(timeout=remaining)


def _query(store, connection, statement, parameters, deadline):
    return store._execute_bounded_pipeline(connection,statement,parameters,deadline)


def _authorized(store, connection, scope, deadline):
    row=_query(store,connection,"""SELECT EXISTS(
        SELECT 1 FROM brain_access_grants WHERE tenant_id=%s AND principal_id=%s
         AND permission IN ('owner','admin','read')) AND EXISTS(
        SELECT 1 FROM canonical_source_grants WHERE tenant_id=%s AND principal_id=%s
         AND source_id=%s AND permission IN ('owner','read')) AS allowed""",
        (scope['tenant_id'],scope['principal_id'],scope['tenant_id'],scope['principal_id'],scope['source_id']),deadline).fetchone()
    return bool(row['allowed'])


def _rows(store, connection, scope, deadline, *, lock=False):
    # Measure metadata inside PostgreSQL before transferring it to Python.
    # An excess returns one empty header, never a truncated set of passages.
    selection=_SELECT+(' FOR UPDATE OF passage' if lock else '')
    statement="""WITH selected AS MATERIALIZED ("""+selection+"""), measured AS (
        SELECT count(*) AS selected_count,
               coalesce(sum(cardinality(receipts)),0) AS receipt_references,
               coalesce(sum(octet_length(row_to_json(selected)::text)),0) AS metadata_bytes
          FROM selected
    ) SELECT selected.*,measured.selected_count,measured.receipt_references,measured.metadata_bytes
        FROM measured LEFT JOIN selected ON measured.selected_count<=%s
         AND measured.receipt_references<=%s AND measured.metadata_bytes<=%s"""
    limits=scope['limits']
    rows=_query(store,connection,statement,
        (scope['tenant_id'],scope['source_id'],scope['native_parent_id'],list(scope['passage_ids']),
         limits['passages'],limits['receipt_references'],limits['metadata_bytes']),deadline).fetchall()
    measured=rows[0]
    if (measured['selected_count']>limits['passages']
            or measured['receipt_references']>limits['receipt_references']
            or measured['metadata_bytes']>limits['metadata_bytes']):
        raise _BudgetExceeded()
    return rows if measured['selected_count'] else []


def _binding(row, scope):
    payload=['recall.passage-membership-binding.v3',scope['binding_epoch'],*[row[field] for field in _BINDING_FIELDS]]
    return hashlib.sha256(json.dumps(payload,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def _complete(rows, scope):
    return len(rows)==len(scope['passage_ids']) and all(row['receipts'] for row in rows)


def _capture(store, scope, deadline, *, prove_ownership):
    with _connect(store,deadline) as connection, connection.transaction():
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


# Both explicit enrollment and historical admission initialize only a real
# publication that has never carried a certificate. Missing authority beside
# an old certificate must remain unknown, never reset to generation one.
_INITIALIZE_GENERATION = """INSERT INTO canonical_parent_authority
            (tenant_id,source_id,native_parent_id,generation)
            SELECT evidence.tenant_id,evidence.source_id,evidence.native_parent_id,1
              FROM canonical_evidence_documents evidence
             WHERE {selection}
               AND NOT EXISTS(SELECT 1 FROM canonical_parent_authority authority
                   WHERE authority.tenant_id=evidence.tenant_id AND authority.source_id=evidence.source_id
                     AND authority.native_parent_id=evidence.native_parent_id)
               AND EXISTS(SELECT 1 FROM canonical_passages passage
                   WHERE passage.tenant_id=evidence.tenant_id AND passage.source_id=evidence.source_id
                     AND passage.logical_document_id=evidence.logical_document_id
                     {passage_selection})
               AND NOT EXISTS(SELECT 1 FROM canonical_passages passage
                   WHERE passage.tenant_id=evidence.tenant_id AND passage.source_id=evidence.source_id
                     AND passage.logical_document_id=evidence.logical_document_id
                     AND passage.membership_generation IS NOT NULL)
            {ordering}ON CONFLICT DO NOTHING"""


def initialize_published_parent_generation(store, *, tenant_id, source_id,
                                         logical_document_id, deadline_at):
    """Initialize scheduling metadata without holding durable queue locks.

    This is the original never-certified initializer, not membership proof.
    Its transaction must finish before the queue cursor or publisher is locked.
    """
    with _connect(store, deadline_at) as connection, connection.transaction():
        owner = _query(store, connection, """SELECT source.owner_principal_id,evidence.native_parent_id
            FROM canonical_evidence_documents evidence
            JOIN canonical_sources source USING(tenant_id,source_id)
            WHERE evidence.tenant_id=%s AND evidence.source_id=%s
              AND evidence.logical_document_id=%s""",
            (tenant_id, source_id, logical_document_id), deadline_at).fetchone()
        if owner is None:
            return 'unavailable'
        scope = dict(tenant_id=tenant_id, source_id=source_id,
                     principal_id=owner['owner_principal_id'])
        if not _authorized(store, connection, scope, deadline_at):
            return 'unauthorized'
        _query(store, connection, _INITIALIZE_GENERATION.format(
            selection='evidence.tenant_id=%s AND evidence.source_id=%s AND evidence.native_parent_id=%s',
            passage_selection='', ordering=''),
            (tenant_id, source_id, owner['native_parent_id']), deadline_at)
        generation = _query(store, connection, """SELECT generation FROM canonical_parent_authority
            WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s""",
            (tenant_id, source_id, owner['native_parent_id']), deadline_at).fetchone()
    return 'initialized' if generation is not None else 'unknown-generation'


def _enroll(store, scope, deadline_at):
    with _connect(store,deadline_at) as connection, connection.transaction():
        if not _authorized(store,connection,scope,deadline_at):
            return dict(status='unauthorized')
        # Initialize only a real, never-certified publication. Recreating a lost
        # authority row could otherwise make an older certificate match again.
        _query(store,connection,_INITIALIZE_GENERATION.format(
            selection='evidence.tenant_id=%s AND evidence.source_id=%s AND evidence.native_parent_id=%s',
            passage_selection='AND passage.passage_id=ANY(%s)', ordering=''),
            (scope['tenant_id'],scope['source_id'],scope['native_parent_id'],list(scope['passage_ids'])),
            deadline_at)
    captured=_capture(store,scope,deadline_at,prove_ownership=True)
    if captured['status']!='captured':
        return captured
    rows=captured['rows']
    if any(row['generation'] is None for row in rows):
        return dict(status='unknown-generation')
    bindings={row['passage_id']:(_binding(row,scope),row['generation']) for row in rows}
    logical_ids={row['logical_document_id'] for row in rows}
    if len(logical_ids)!=1:
        raise ValueError('parent_membership_publication_ambiguous')
    with _connect(store,deadline_at) as connection, connection.transaction():
        # Reuse the native passage publisher's lock; never lock authority rows.
        _query(store,connection,'SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',
               ('lossless-passages\x1f'+next(iter(logical_ids)),),deadline_at)
        if not _authorized(store,connection,scope,deadline_at):
            return dict(status='unauthorized')
        current=_rows(store,connection,scope,deadline_at,lock=True)
        if (not _complete(current,scope) or any(
                bindings[row['passage_id']]!=(_binding(row,scope),row['generation']) for row in current)):
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


def _compare(store, scope, deadline_at):
    started=time.monotonic()
    captured=_capture(store,scope,deadline_at,prove_ownership=False)
    if captured['status']!='captured':
        return captured
    rows=captured['rows']
    certified={row['passage_id'] for row in rows if row['generation'] is not None
               and row['membership_generation']==row['generation']
               and row['membership_binding_sha256']==_binding(row,scope)}
    native={row['passage_id'] for row in rows if all(
        (row['source_id'],receipt) in captured['members'] for receipt in row['receipts'])}
    return dict(status='compared',selected_passages=len(rows),unique_receipts=captured['unique_receipts'],
                certified_passages=len(certified),fallback_passages=len(rows)-len(certified),
                native_allowed_passages=len(native),disagreements=len(certified-native),
                elapsed_ms=round((time.monotonic()-started)*1000,3))


def enroll_parent_passages(store: Any, *, deadline_at: float, limits: dict[str,int], **arguments) -> dict[str,Any]:
    """Prove an explicit fixed set within caller-declared job metadata budgets.

    An excess enrolls no part of the scope. It remains on native verification;
    callers can later select smaller batches without dropping captured history.
    """
    try:
        return _enroll(store,_scope(**arguments,deadline_at=deadline_at,limits=limits),deadline_at)
    except _BudgetExceeded:
        return dict(status='budget-exceeded',enrolled_passages=0)


def compare_parent_membership(store: Any, *, deadline_at: float, limits: dict[str,int], **arguments) -> dict[str,Any]:
    """Compare certificates with the complete unchanged native proof, or refuse."""
    try:
        return _compare(store,_scope(**arguments,deadline_at=deadline_at,limits=limits),deadline_at)
    except _BudgetExceeded:
        return dict(status='budget-exceeded',compared_passages=0)


@dataclass(frozen=True)
class CertificationSettings:
    """Optional writer work; no reader trusts these certificates yet."""

    enabled: bool = False
    binding_epoch: str = ''

    def __post_init__(self):
        if type(self.enabled) is not bool or not isinstance(self.binding_epoch,str):
            raise ValueError('parent_membership_settings_invalid')
        if self.enabled and not self.binding_epoch.strip():
            raise ValueError('parent_membership_epoch_required')

    @classmethod
    def from_env(cls):
        value=os.environ.get('RECALL_PARENT_MEMBERSHIP_CERTIFICATION','0')
        if value not in ('0','1'):
            raise ValueError('parent_membership_certification_invalid')
        return cls(enabled=value=='1',binding_epoch=os.environ.get('RECALL_PARENT_MEMBERSHIP_EPOCH',''))


def certification_failure(error, *, source_id=None, logical_document_id=None,
                          native_parent_id=None):
    """Retain attempted scope and exception metadata, never exception text."""
    state = None
    # BrainStore wraps QueryCanceled in SearchDeadlineExceeded with a cause.
    # Match the existing retirement diagnostic's bounded direct/cause lookup.
    for candidate in (error, error.__cause__):
        try:
            value = getattr(candidate, 'sqlstate', None)
            if isinstance(value, str) and len(value) == 5 and all(
                    char in '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ' for char in value):
                state = value
                break
        except Exception:
            pass
    return dict(error_class=type(error).__name__, sqlstate=state,
                source_id=source_id, logical_document_id=logical_document_id,
                native_parent_id=native_parent_id)


def certify_published_passages(store, *, tenant_id, source_id, logical_document_id,
                               passage_ids, settings, deadline_at):
    """Enroll committed publication metadata within one finite caller budget.

    Batches are job resource limits, not parent or capture limits. Unprocessed
    passages retain native verification and can be explicitly enrolled later.
    """
    if not settings.enabled or not passage_ids:
        return dict(status='disabled',certified_passages=0)
    with _connect(store,deadline_at) as connection:
        owner=_query(store,connection,"""SELECT source.owner_principal_id,evidence.native_parent_id
          FROM canonical_evidence_documents evidence
          JOIN canonical_sources source USING(tenant_id,source_id)
         WHERE evidence.tenant_id=%s AND evidence.source_id=%s
           AND evidence.logical_document_id=%s""",
          (tenant_id,source_id,logical_document_id),deadline_at).fetchone()
    if owner is None:
        return dict(status='unavailable',certified_passages=0)
    certified=0
    limits=dict(_CERTIFICATION_LIMITS)
    for start in range(0,len(passage_ids),limits['passages']):
        if time.monotonic()>=deadline_at:
            return dict(status='deadline',certified_passages=certified)
        try:
            outcome=enroll_parent_passages(store,tenant_id=tenant_id,source_id=source_id,
                principal_id=owner['owner_principal_id'],native_parent_id=owner['native_parent_id'],
                passage_ids=passage_ids[start:start+limits['passages']],limits=limits,
                binding_epoch=settings.binding_epoch,deadline_at=deadline_at)
        except Exception as error:
            # Earlier batches have committed. Report those acknowledged writes
            # even when a later batch fails; they are not a current-coverage claim.
            return dict(status='failed',certified_passages=certified,
                failure=certification_failure(error, source_id=source_id,
                    logical_document_id=logical_document_id, native_parent_id=owner['native_parent_id']))
        if outcome['status']=='enrolled':
            certified+=outcome['passages']
        else:
            return dict(status=outcome['status'],certified_passages=certified)
    return dict(status='enrolled',certified_passages=certified)


def certify_retired_parent(store, *, tenant_id, source_id, native_parent_id,
                           logical_document_id, principal_id, settings, deadline_at,
                           should_stop=None):
    """Certify committed passages after retirement, within the caller's budget.

    Keyset pages bound metadata transfer without a parent-size ceiling. Each
    page uses the original native proof and captured generation; a later edit
    invalidates earlier pages normally. Counts describe acknowledged writes.
    """
    if not settings.enabled:
        return dict(status='disabled', certified_passages=0)
    certified, after = 0, ''
    limits = dict(_CERTIFICATION_LIMITS)
    try:
        while True:
            if time.monotonic() >= deadline_at:
                return dict(status='deadline', certified_passages=certified)
            if should_stop is not None and should_stop():
                return dict(status='stopped', certified_passages=certified)
            with _connect(store, deadline_at) as connection:
                rows = _query(store, connection, """SELECT passage.passage_id
                    FROM canonical_passages passage
                    JOIN canonical_evidence_documents evidence
                      USING(tenant_id,source_id,logical_document_id)
                   WHERE passage.tenant_id=%s AND passage.source_id=%s
                     AND passage.logical_document_id=%s AND evidence.native_parent_id=%s
                     AND passage.passage_id>%s
                   ORDER BY passage.passage_id LIMIT %s""",
                    (tenant_id, source_id, logical_document_id, native_parent_id,
                     after, limits['passages']), deadline_at).fetchall()
            if not rows:
                return dict(status='enrolled' if certified else 'empty',
                            certified_passages=certified)
            ids = tuple(row['passage_id'] for row in rows)
            outcome = enroll_parent_passages(store, tenant_id=tenant_id,
                source_id=source_id, native_parent_id=native_parent_id,
                principal_id=principal_id, passage_ids=ids, limits=limits,
                binding_epoch=settings.binding_epoch, deadline_at=deadline_at)
            if outcome['status'] != 'enrolled':
                return dict(status=outcome['status'], certified_passages=certified)
            certified += outcome['passages']
            after = ids[-1]
    except Exception:
        # Earlier pages may have committed. Stop without replaying the unknown
        # page, and never relabel already committed retirement as a failure.
        return dict(status='failed', certified_passages=certified)
