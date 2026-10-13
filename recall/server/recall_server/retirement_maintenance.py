"""One bounded retirement round in the existing projection worker."""
import time

import orjson
import psycopg

from .chunk_retirement import ChunkRetirementError, ParentRetirementLimits
from .db import SearchDeadlineExceeded
from .retirement_policy import enroll_published_parent
from .parent_chunk_proof import read_parent_catalog
from .retirement_runner import RetirementScope, RunnerLimits, _authorize, _cohort_entry, run_retirement


class RetirementMaintenance:
    def __init__(self, store, archive, *, tenant_id=None, enabled=False, seconds=10,
                 reconcile_parents=25, max_parents=10, max_clear_bytes=64*1024**2,
                 max_archive_bytes=256*1024**2, catalog_archive_budget=False):
        if (type(catalog_archive_budget) is not bool or type(enabled) is not bool or type(seconds) not in (int, float)
                or not 0 < seconds <= 300 or type(reconcile_parents) is not int
                or not 1 <= reconcile_parents <= 100):
            raise ValueError('invalid retirement maintenance budget')
        self.store, self.archive, self.tenant_id, self.enabled = store, archive, tenant_id, enabled
        self.seconds, self.reconcile_parents = seconds, reconcile_parents
        self.catalog_archive_budget = catalog_archive_budget
        self.limits = RunnerLimits(max_parents=max_parents,
            max_clear_bytes=max_clear_bytes, max_archive_bytes=max_archive_bytes)

    def _execute(self, c, sql, args=()):
        return self.store._execute_bounded(c, sql, args, self.deadline)

    def _claim_source(self):
        with self.store.connect() as c, c.transaction():
            if self._execute(c, 'SELECT 1 FROM schema_migrations WHERE version=78').fetchone() is None:
                return None
            row = self._execute(c, '''SELECT * FROM canonical_retirement_source_policy
                WHERE capture_complete AND (%s::text IS NULL OR tenant_id=%s)
                  AND maintenance_scheduled_at < clock_timestamp()-interval '30 seconds'
                ORDER BY maintenance_scheduled_at,tenant_id,source_id LIMIT 1
                FOR UPDATE SKIP LOCKED''', (self.tenant_id, self.tenant_id)).fetchone()
            if row is None:
                return None
            return self._execute(c, '''UPDATE canonical_retirement_source_policy
                SET maintenance_epoch=maintenance_epoch+1,maintenance_scheduled_at=clock_timestamp()
                WHERE tenant_id=%s AND source_id=%s RETURNING *''',
                (row['tenant_id'], row['source_id'])).fetchone()

    def _parent_limits(self, source, candidate):
        key = source['tenant_id'], candidate['source_id'], candidate['native_parent_id']
        deadline = min(self.deadline, time.monotonic() + 5)
        with self.store.connect() as c, c.transaction():
            c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            current = read_parent_catalog(self.store, c, key, deadline)['manifest']
            if _cohort_entry(current) != _cohort_entry(candidate['manifest']):
                raise ChunkRetirementError('retirement_budget_catalog_changed')
            parts = self.store._execute_bounded(c, """SELECT count(*) AS count,
                COALESCE(sum(size_bytes),0)::bigint AS bytes
                FROM canonical_evidence_document_parts
                WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s AND revision=%s""",
                (*key[:2], current['logical_document_id'], current['revision']), deadline).fetchone()
        needed = current['manifest_size_bytes'] + parts['bytes']
        if parts['count'] != current['part_count'] or not 0 < needed <= self.limits.max_archive_bytes:
            raise ChunkRetirementError('retirement_budget_catalog_invalid')
        return ParentRetirementLimits(batch_documents=256, max_archive_bytes=needed)

    def _record_intent(self, source, candidate):
        intent = dict(parent=candidate['native_parent_id'], scope_epoch=candidate['scope_epoch'],
            catalog=_cohort_entry(candidate['manifest']), before={name: candidate['cumulative_'+name]
                for name in ('cleared_documents', 'cleared_chunks', 'cleared_utf8_bytes')})
        with self.store.connect() as c, c.transaction():
            changed = self._execute(c, '''UPDATE canonical_retirement_source_policy
                SET maintenance_intent=%s::jsonb
                WHERE tenant_id=%s AND source_id=%s AND maintenance_epoch=%s''',
                (orjson.dumps(intent).decode(), source['tenant_id'], source['source_id'],
                 source['maintenance_epoch'])).rowcount
            if changed != 1:
                raise ChunkRetirementError('retirement_maintenance_superseded')

    def _reconcile_intent(self, source):
        intent = source['maintenance_intent']
        if intent is None:
            return dict(reconciled_intents=0, observed_previous_cleared_documents=0)
        if (not isinstance(intent, dict) or not isinstance(intent.get('parent'), str)
                or type(intent.get('scope_epoch')) is not int
                or not isinstance(intent.get('before'), dict)):
            raise ChunkRetirementError('retirement_maintenance_intent_invalid')
        with self.store.connect() as c, c.transaction():
            catalog = self._execute(c, """SELECT to_jsonb(evidence) AS manifest
                FROM canonical_evidence_documents evidence
                WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s FOR SHARE NOWAIT""",
                (source['tenant_id'], source['source_id'], intent['parent'])).fetchone()
            same_catalog = catalog is not None and _cohort_entry(catalog['manifest']) == intent.get('catalog')
            progress = self._execute(c, '''SELECT * FROM canonical_chunk_retirement_progress
                WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s FOR SHARE NOWAIT''',
                (source['tenant_id'], source['source_id'], intent['parent'])).fetchone()
            # Reading under the progress lock observes a completed clear transaction
            # or refuses while it is active. A later fresh claim fences proofs
            # still outside their transaction; the old outcome stays unasserted.
            if same_catalog and progress is not None and progress['scope_epoch'] < intent['scope_epoch']:
                raise ChunkRetirementError('retirement_maintenance_epoch_regressed')
            before = intent['before'].get('cleared_documents')
            if type(before) is not int or before < 0:
                raise ChunkRetirementError('retirement_maintenance_intent_invalid')
            observed = 0 if progress is None or not same_catalog else progress['cumulative_cleared_documents'] - before
            if observed < 0:
                raise ChunkRetirementError('retirement_maintenance_counter_regressed')
        return dict(reconciled_intents=1, observed_previous_cleared_documents=observed,
            reconciled_state='observed' if same_catalog and progress is not None else 'replaced')

    def _reconcile_publications(self, source):
        if not source['enabled']:
            return 0
        key = source['tenant_id'], source['source_id']
        enrolled = 0
        reconcile_deadline = min(time.monotonic()+1, self.deadline-0.25)
        with self.store.connect() as c, c.transaction():
            parents = self._execute(c, '''SELECT native_parent_id FROM canonical_evidence_documents
                WHERE tenant_id=%s AND source_id=%s
                  AND (%s::text IS NULL OR native_parent_id>%s)
                ORDER BY native_parent_id LIMIT %s''',
                (*key, source['maintenance_cursor'], source['maintenance_cursor'], self.reconcile_parents)).fetchall()
            cursor = None
            for parent in parents:
                if time.monotonic() >= reconcile_deadline:
                    break
                cursor = parent['native_parent_id']
                outcome = enroll_published_parent(self.store, c, (*key, cursor))
                enrolled += outcome in {'enrolled', 'rebound'}
            self._execute(c, '''UPDATE canonical_retirement_source_policy SET maintenance_cursor=%s
                WHERE tenant_id=%s AND source_id=%s AND maintenance_epoch=%s''',
                (cursor, *key, source['maintenance_epoch']))
        return enrolled

    def cycle(self, *, busy):
        report = dict(status='disabled', cleared_documents=0, cleared_utf8_bytes=0,
            archive_bytes=0, enrolled_parents=0, reconciled_intents=0,
            observed_previous_cleared_documents=0, commit_outcome_unknown=False)
        if not self.enabled:
            return report
        budget = min(self.seconds, 5 if busy else self.seconds)
        self.deadline = time.monotonic() + budget
        # Leave one bounded transaction for the final batch and another for
        # intent bookkeeping. Short synchronous rounds divide their budget;
        # the independent 180-second round retains five seconds for each.
        reserve = min(5, budget / 4)
        owner_deadline = self.deadline - reserve
        stop_admitting_at = owner_deadline - reserve
        try:
            source = self._claim_source()
            if source is None:
                return dict(report, status='no_ready_sources')
            scope = RetirementScope(source['tenant_id'], source['principal_id'], (source['source_id'],))
            with self.store.connect() as c, c.transaction():
                _authorize(self.store, c, scope, self.deadline)
            report.update(self._reconcile_intent(source))
            report['enrolled_parents'] = self._reconcile_publications(source)
            result = run_retirement(self.store, self.archive, scope=scope, apply=True,
                limits=self.limits, parent_limits=ParentRetirementLimits(batch_documents=256),
                deadline_at=owner_deadline,
                should_stop=lambda: time.monotonic() >= stop_admitting_at,
                before_parent=lambda candidate: self._record_intent(source, candidate),
                parent_limits_for=(lambda candidate: self._parent_limits(source, candidate))
                    if self.catalog_archive_budget else None)
            report.update(result)
            if not result['commit_outcome_unknown']:
                with self.store.connect() as c, c.transaction():
                    self._execute(c, '''UPDATE canonical_retirement_source_policy SET maintenance_intent=NULL
                        WHERE tenant_id=%s AND source_id=%s AND maintenance_epoch=%s''',
                        (source['tenant_id'], source['source_id'], source['maintenance_epoch']))
            return report
        except ChunkRetirementError as error:
            return dict(report, status='owner_refused' if error.error_code == 'retirement_owner_required'
                        else 'deferred', error=error.error_code)
        except (SearchDeadlineExceeded, psycopg.Error):
            return dict(report, status='deferred')
