"""Empty asynchronous logical commits must retain remote deletion IDs."""
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase, mock
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'server'))
from recall_server import logical_evidence_projection as module


class EmptyLogicalTombstoneTest(TestCase):
    def run_commit(self, stale=False, failure=False):
        at = datetime.now(timezone.utc)
        candidate = module.LogicalGroupCandidate('tenant:t', 'source:s', 'parent:p', at, 3, 2)
        calls = []
        passage = dict(passage_id='passage:old', first_occurred_at=at, last_occurred_at=at)
        class Connection:
            def transaction(self):
                return nullcontext()
            def execute(self, sql, args):
                calls.append((sql, args))
                if 'SELECT generation,changed_at' in sql:
                    return SimpleNamespace(fetchone=lambda: dict(generation=2 if stale else 3, changed_at=at))
                if 'SELECT logical_document_id,first_occurred_at' in sql:
                    return SimpleNamespace(fetchone=lambda: dict(logical_document_id='logical:l', first_occurred_at=at, last_occurred_at=at))
                return SimpleNamespace(fetchall=lambda: [passage], rowcount=1)
        connection=Connection()
        projector=module.CanonicalLogicalEvidenceProjector(SimpleNamespace(connect=lambda:nullcontext(connection)), None)
        def tombstones(conn, **kwargs):
            self.assertIs(conn, connection)
            self.assertEqual(kwargs, dict(tenant_id='tenant:t', source_id='source:s', passages=[passage], reason='forget'))
            self.assertFalse(any('DELETE FROM canonical_evidence_documents' in sql for sql, _ in calls))
            if failure:
                raise RuntimeError('write failed')
        with (mock.patch.object(projector, '_publish_body_locators'),
              mock.patch.object(projector, '_old_references', return_value=(None, ())),
              mock.patch.object(projector, '_enqueue_cleanup'),
              mock.patch.object(projector, '_queue_parquet_scan'),
              mock.patch.object(module, 'record_passage_deletions', side_effect=tombstones) as record):
            if failure:
                with self.assertRaisesRegex(RuntimeError, 'write failed'):
                    projector._commit_empty(candidate)
                self.assertFalse(any('DELETE FROM canonical_evidence_documents' in sql for sql, _ in calls))
            else:
                self.assertEqual(projector._commit_empty(candidate), 'stale' if stale else 'pruned')
            self.assertEqual(record.call_count, 0 if stale else 1)
        return calls

    def test_tombstones_precede_cascade_in_same_transaction(self):
        self.run_commit()

    def test_stale_generation_does_not_publish_deletions(self):
        self.run_commit(stale=True)

    def test_tombstone_failure_prevents_cascade(self):
        self.run_commit(failure=True)
