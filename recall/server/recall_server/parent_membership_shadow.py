"""Optional same-snapshot certificate comparison; native results always win."""
from dataclasses import dataclass
import os
import time

from .db import SearchDeadlineExceeded
from .parent_membership_enrollment import _binding

# Receipts and passage text hashes already came from the native catalog query
# in this snapshot. Fetch only the extra binding and generation metadata.
_METADATA_SQL = """WITH selected AS MATERIALIZED (
    SELECT passage.source_id,passage.passage_id,passage.policy_fingerprint,
           passage.revision AS passage_revision,
           passage.membership_generation,passage.membership_binding_sha256,
           evidence.native_parent_id,evidence.revision AS manifest_revision,
           projected.revision AS projected_revision,projected.source_document_sha256,
           authority.generation
      FROM canonical_passages passage
      JOIN canonical_passage_documents projected
        USING(tenant_id,source_id,logical_document_id,policy_fingerprint)
      JOIN canonical_evidence_documents evidence USING(tenant_id,source_id,logical_document_id)
      LEFT JOIN canonical_parent_authority authority
        ON authority.tenant_id=evidence.tenant_id AND authority.source_id=evidence.source_id
       AND authority.native_parent_id=evidence.native_parent_id
     WHERE passage.tenant_id=%s
       AND (passage.source_id,passage.passage_id) IN (
           SELECT * FROM unnest(%s::text[],%s::text[]))
), measured AS (
    SELECT count(*) AS selected_count,
           coalesce(sum(octet_length(row_to_json(selected)::text)),0) AS metadata_bytes
      FROM selected
) SELECT selected.*,measured.selected_count,measured.metadata_bytes
    FROM measured LEFT JOIN selected ON measured.metadata_bytes<=8388608"""


@dataclass(frozen=True)
class ShadowSettings:
    enabled: bool = False
    binding_epoch: str = ''
    budget_ms: int = 100
    serving_reserve_ms: int = 1000

    def __post_init__(self):
        if (type(self.enabled) is not bool or not isinstance(self.binding_epoch,str)
                or (self.enabled and not self.binding_epoch.strip())
                or type(self.budget_ms) is not int or not 1<=self.budget_ms<=1000
                or type(self.serving_reserve_ms) is not int or not 1<=self.serving_reserve_ms<=60000):
            raise ValueError('parent_membership_shadow_settings_invalid')

    @classmethod
    def from_env(cls):
        enabled=os.environ.get('RECALL_PARENT_MEMBERSHIP_SHADOW','0')
        if enabled not in ('0','1'):
            raise ValueError('parent_membership_shadow_settings_invalid')
        return cls(enabled=enabled=='1',binding_epoch=os.environ.get('RECALL_PARENT_MEMBERSHIP_EPOCH',''),
                   budget_ms=int(os.environ.get('RECALL_PARENT_MEMBERSHIP_SHADOW_BUDGET_MS','100')),
                   serving_reserve_ms=int(os.environ.get('RECALL_PARENT_MEMBERSHIP_SHADOW_RESERVE_MS','1000')))


def observe_membership(store,connection,*,selected,members,settings,deadline_at):
    """Compare metadata with completed native proof inside its existing snapshot.

    The savepoint contains optional SQL errors. No native result is replaced,
    even if a certificate disagrees. Refused work reports unknown, not zero error.
    """
    started=time.monotonic()
    deadline=min(deadline_at-settings.serving_reserve_ms/1000,
                 started+settings.budget_ms/1000)
    if not settings.enabled:
        return dict(status='disabled',elapsed_ms=0)
    if deadline<=started:
        return dict(status='serving-reserve',elapsed_ms=0)
    if not selected:
        return dict(status='empty',elapsed_ms=0)
    try:
        # All selected keys came from the current native authorized-source query.
        # Query and binding use the same RR snapshot, including concurrent edits.
        tenant=selected[0]['tenant_id']
        with connection.transaction():
            rows=store._execute_bounded(connection,_METADATA_SQL,
                (tenant,[r['source_id'] for r in selected],[r['passage_id'] for r in selected]),deadline).fetchall()
            if rows[0]['metadata_bytes']>8388608:
                return dict(status='metadata-budget',elapsed_ms=round((time.monotonic()-started)*1000,3))
            metadata={(r['source_id'],r['passage_id']):r for r in rows if r['selected_count']}
            if len(metadata)!=len(selected):
                return dict(status='incomplete',elapsed_ms=round((time.monotonic()-started)*1000,3))
            certified=set();native=set();fallback_receipts=set();all_receipts=set();parents=set()
            for row in selected:
                if time.monotonic()>=deadline:
                    raise SearchDeadlineExceeded()
                key=(row['source_id'],row['passage_id'])
                extra=metadata[key]
                parents.add((row['source_id'],extra['native_parent_id']))
                receipts={(row['source_id'],receipt) for receipt in row['receipts']}
                all_receipts.update(receipts)
                valid=(extra['generation'] is not None
                       and extra['membership_generation']==extra['generation']
                       and extra['membership_binding_sha256']==_binding({**row,**extra},
                           {'binding_epoch':settings.binding_epoch}))
                if valid:
                    certified.add(key)
                else:
                    fallback_receipts.update(receipts)
                if receipts<=members:
                    native.add(key)
            if time.monotonic()>=deadline:
                raise SearchDeadlineExceeded()
        return dict(status='compared',selected_passages=len(selected),selected_parents=len(parents),
                    certified_passages=len(certified),fallback_passages=len(selected)-len(certified),
                    native_allowed_passages=len(native),disagreements=len(certified-native),
                    unique_receipts=len(all_receipts),avoidable_receipts=len(all_receipts-fallback_receipts),
                    elapsed_ms=round((time.monotonic()-started)*1000,3))
    except SearchDeadlineExceeded:
        return dict(status='deadline',elapsed_ms=round((time.monotonic()-started)*1000,3))
    except Exception:
        return dict(status='unavailable',elapsed_ms=round((time.monotonic()-started)*1000,3))
