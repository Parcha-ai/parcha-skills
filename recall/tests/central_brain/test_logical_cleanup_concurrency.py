"""Actual final cleanup executor, with transaction-scoped blocked archive deletes."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector, LogicalGroupCandidate


class Store:
    pool_max_size = 4

    def __init__(self):
        self.rows, self.acknowledged = {}, []
        self.in_transaction = False

    def connect(self):
        return nullcontext(self)

    def prepare_pool(self, _size):
        pass

    @contextmanager
    def transaction(self):
        self.in_transaction = True
        try:
            yield
        finally:
            self.in_transaction = False

    def execute(self, query, values=()):
        if 'SELECT queue.*' in query:
            assert self.in_transaction and 'FOR UPDATE SKIP LOCKED' in query
            assert all(name in query for name in ('canonical_evidence_documents', 'canonical_evidence_document_parts', 'canonical_parquet_scan_shards'))
            excluded = set(zip(*values[2:5])) if len(values) == 6 else set()
            rows = [row for row in self.rows.values()
                    if (row['tenant_id'], row['source_id'], row['artifact_id']) not in excluded]
            return SimpleNamespace(fetchall=lambda: rows[:values[-1]])
        if 'WITH completed(' in query:
            for key in values[2]:
                self.acknowledged.append(key)
                del self.rows[key]
        elif 'WITH failed(' in query:
            for key in values[2]:
                self.rows[key]['attempts'] += 1
        elif 'SELECT count(*) AS count' in query:
            return SimpleNamespace(fetchone=lambda: {'count': len(self.rows)})
        elif 'SELECT count(*) AS queued' in query:
            return SimpleNamespace(fetchone=lambda: dict(queued=0, waiting=0, quarantined=0, backoff=0))
        else:
            raise AssertionError('unexpected SQL')
        return None


class Archive:
    def __init__(self, root, store, expected, fail):
        self.root, self.store, self.expected, self.fail = root, store, expected, fail
        self.entered, self.release, self.lock = threading.Event(), threading.Event(), threading.Lock()
        self.active = self.maximum = 0
        self.calls = []

    def delete_reference(self, reference):
        assert self.store.in_transaction
        key = reference['artifact_id']
        with self.lock:
            self.calls.append(key)
            self.active += 1
            self.maximum = max(self.maximum, self.active)
            if self.active == self.expected:
                self.entered.set()
        try:
            if not self.release.wait(5):
                raise RuntimeError('test release timeout')
            if key == self.fail:
                raise OSError('synthetic archive unavailable')
            p = self.root/key
            existed = p.exists()
            p.unlink(missing_ok=True)
            return existed
        finally:
            with self.lock:
                self.active -= 1


class Projector(CanonicalLogicalEvidenceProjector):
    def __init__(self, store, archive, rows):
        super().__init__(store, archive, bound_tenant_id='tenant:synthetic')
        self.rows = rows
        self.prepared = self.committed = 0

    def _pending(self, **kwargs):
        return [LogicalGroupCandidate('tenant:synthetic', 'source:synthetic', 'parent', datetime.now(timezone.utc), 1, 1)]

    def _prepare_batch_and_upload(self, candidates, **kwargs):
        self.prepared += len(candidates)
        assert not self.store.rows
        return [SimpleNamespace(prepared=SimpleNamespace(record_count=1, receipt_count=1), all_references=()) for _ in candidates]

    def _commit_upload(self, candidate, upload):
        self.committed += 1
        self.store.rows.update({r['artifact_id']: r for r in self.rows})
        return 'committed'


class FinalCleanupConcurrencyTests(unittest.TestCase):
    def exercise(self, *, upload, cleanup, expected, fail=None, protected=False):
        with tempfile.TemporaryDirectory() as tmp:
            root, store = Path(tmp), Store()
            archive = Archive(root, store, expected, fail)
            rows = []
            for key in ('one', 'two', 'three', 'four'):
                (root/key).write_text('synthetic immutable archive')
                rows.append(dict(tenant_id='tenant:synthetic', source_id='source:synthetic', artifact_id=key,
                    storage_backend='s3', object_key=key, content_sha256='a'*64, size_bytes=1,
                    media_type='application/json', encryption='none', version_id='v1', created_at=datetime.now(timezone.utc),
                    removable=not (protected and key == 'four'), attempts=0))
            projector = Projector(store, archive, rows)
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(projector.project_pending, batch_size=1, max_batches=1,
                    upload_concurrency=upload, cleanup_concurrency=cleanup)
                try:
                    self.assertTrue(archive.entered.wait(2), f'final cleanup reached {archive.maximum}, expected {expected}')
                    self.assertEqual(archive.maximum, expected)
                    self.assertFalse(future.done())
                    self.assertEqual(projector.committed, 1)
                finally:
                    archive.release.set()
                result = future.result(timeout=5)
            self.assertEqual(archive.maximum, expected)
            self.assertEqual(projector.prepared, 1)
            self.assertEqual(result['documents'], 1)
            if protected:
                self.assertNotIn('four', archive.calls)
                self.assertTrue((root/'four').exists())
            if fail:
                self.assertEqual(set(store.rows), {fail})
                self.assertEqual(store.rows[fail]['attempts'], 1)
                self.assertTrue((root/fail).exists())
                self.assertEqual(result['cleanup_failures'], 1)
                self.assertEqual(result['cleanup_pending'], 1)
                archive.fail = None
                retry = projector.drain_cleanup(concurrency=expected)
                self.assertEqual(retry['deleted'], 1)
                self.assertEqual(retry['pending'], 0)
                self.assertEqual(store.acknowledged.count(fail), 1)
            else:
                self.assertEqual(result['old_objects_deleted'], 4)
                self.assertEqual(result['cleanup_failures'], 0)
                self.assertFalse(store.rows)
            self.assertFalse(store.in_transaction)

    def test_final_cleanup_uses_distinct_cleanup_budget(self):
        self.exercise(upload=1, cleanup=2, expected=2)

    def test_smaller_cleanup_budget_remains_cap(self):
        self.exercise(upload=2, cleanup=1, expected=1)

    def test_default_cleanup_keeps_upload_budget(self):
        self.exercise(upload=2, cleanup=None, expected=2)

    def test_protected_reference_and_failed_delete_survive_parallel_drain(self):
        self.exercise(upload=1, cleanup=2, expected=2, fail='two', protected=True)
