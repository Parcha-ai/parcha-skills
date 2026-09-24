"""Repair comparison overlaps read-only work without changing reports or admission."""
from contextlib import contextmanager, nullcontext
from datetime import datetime, timezone
import json
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import io
from contextlib import redirect_stdout
from recall_server.passage_index import CanonicalPassageProjector
from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY


class ParallelEmptyRepair(unittest.TestCase):
    def fixture(self, count=8):
        policy = DEFAULT_PASSAGE_POLICY
        rows = []
        for i in range(1, count + 1):
            row = dict(tenant_id='tenant:test', source_id='source:test',
                logical_document_id='ldoc_' + f'{i:032x}', revision=1,
                evidence_revision=1, source_document_sha256='a' * 64,
                document_content_sha256='a' * 64, passage_count=0,
                policy_fingerprint=policy.fingerprint, target_tokens=policy.target_tokens,
                overlap_tokens=policy.overlap_tokens, part_ordinal=0)
            for prefix in ('manifest_', 'part_'):
                row.update({prefix + key: value for key, value in dict(
                    artifact_id='art_' + '0' * 32, storage_backend='filesystem',
                    object_key='objects/00/example', content_sha256='b' * 64,
                    size_bytes=100, media_type='application/x-ndjson', encryption='none',
                    version_id='v1', created_at=datetime(2026, 9, 24, tzinfo=timezone.utc)).items()})
            rows.append(row)
        state = SimpleNamespace(leases=0, writes=[], rows=rows)
        def execute(sql, args=None):
            if 'FROM canonical_passage_documents sampled' in sql:
                return SimpleNamespace(fetchall=lambda: list(reversed(rows)))
            if 'SELECT logical_document_id,passage_id,receipts' in sql:
                return SimpleNamespace(fetchall=lambda: [])
            if sql.startswith('INSERT INTO canonical_passage_projection_queue'):
                state.writes.append(json.loads(args[0]))
                return SimpleNamespace(rowcount=len(state.writes[-1]))
            raise AssertionError(sql)
        connection = SimpleNamespace(execute=execute, transaction=nullcontext)
        @contextmanager
        def connect():
            state.leases += 1
            try:
                yield connection
            finally:
                state.leases -= 1
        projector = CanonicalPassageProjector(SimpleNamespace(connect=connect), Mock(), policy=policy)
        def prepare(candidate, *, policy):
            self.assertEqual(state.leases, 0, 'archive read holds database lease')
            self.assertEqual(policy, DEFAULT_PASSAGE_POLICY)
            i = int(candidate.logical_document_id[5:], 16)
            passage = SimpleNamespace(passage_id='passage-' + str(i), text='private synthetic text',
                                      receipts=('recall://synthetic/' + str(i),))
            return SimpleNamespace(passages=() if i == 2 else (passage,))
        projector._prepare = prepare
        return projector, state

    def test_four_comparisons_overlap_bounded_and_report_in_id_order(self):
        p, state = self.fixture(8)
        original = p._prepare
        barrier = threading.Barrier(4, timeout=3)
        gates = [threading.Event() for _ in range(8)]
        gates[3].set()
        gates[7].set()
        lock = threading.Lock()
        active = maximum = 0
        finished = []
        def prepare(candidate, *, policy):
            nonlocal active, maximum
            i = int(candidate.logical_document_id[5:], 16) - 1
            with lock:
                active += 1
                maximum = max(maximum, active)
            try:
                barrier.wait()
                self.assertTrue(gates[i].wait(3))
                result = original(candidate, policy=policy)
                with lock:
                    finished.append(i)
                if i % 4:
                    gates[i - 1].set()
                return result
            finally:
                with lock:
                    active -= 1
        p._prepare = prepare
        report = p.repair_empty(tenant_id='tenant:test', source_id='source:test', concurrency=4)
        self.assertEqual((maximum, active), (4, 0))
        self.assertEqual(finished, [3, 2, 1, 0, 7, 6, 5, 4])
        self.assertEqual([d['logical_document_id'] for d in report['documents']],
                         [row['logical_document_id'] for row in state.rows])
        self.assertEqual(state.writes, [])

    def test_parallel_plan_and_enqueue_equal_sequential_including_empty_and_stale(self):
        reports, admitted = [], []
        for concurrency in (1, 4):
            p, state = self.fixture()
            state.rows[-1]['evidence_revision'] = 2
            state.rows[-2]['policy_fingerprint'] = 'c' * 64
            reports.append(p.repair_empty(tenant_id='tenant:test', source_id='source:test',
                                         concurrency=concurrency, apply=True, price_per_mtoken=1.0))
            admitted.append(state.writes)
        self.assertEqual(reports[0], reports[1])
        self.assertEqual(admitted[0], admitted[1])
        self.assertEqual(reports[0]['queued'], 5)
        self.assertEqual([d['status'] for d in reports[0]['documents']][-2:], ['policy_mismatch', 'stale'])
        self.assertNotIn('private synthetic text', repr(reports))
        self.assertNotIn('recall://', repr(reports))

    def test_any_failed_archive_queues_nothing_and_threads_finish_before_return(self):
        p, state = self.fixture(4)
        original = p._prepare
        barrier = threading.Barrier(4, timeout=3)
        done = []
        def prepare(candidate, *, policy):
            try:
                barrier.wait()
                if candidate.logical_document_id.endswith('1'):
                    raise ValueError('synthetic archive hash mismatch')
                return original(candidate, policy=policy)
            finally:
                done.append(candidate.logical_document_id)
        p._prepare = prepare
        with self.assertRaisesRegex(ValueError, 'archive hash mismatch'):
            p.repair_empty(tenant_id='tenant:test', source_id='source:test', concurrency=4, apply=True)
        self.assertEqual(len(done), 4)
        self.assertEqual(state.writes, [])
        self.assertEqual(state.leases, 0)

    def test_bad_concurrency_refused_before_catalog_reads(self):
        for value in (True, 0, 33, 1.5):
            p, _ = self.fixture()
            p.store.connect = Mock(side_effect=AssertionError('unexpected read'))
            with self.subTest(value=value), self.assertRaises(ValueError):
                p.repair_empty(tenant_id='tenant:test', source_id='source:test', concurrency=value)
            p.store.connect.assert_not_called()

    def test_cli_preserves_sequential_default_and_forwards_opt_in(self):
        from recall_server import cli
        for flags, expected in (([], 1), (['--concurrency', '4'], 4)):
            projector = Mock()
            projector.repair_empty.return_value = {'status': 'planned'}
            argv = ['recall-server', '--dsn', 'postgresql://synthetic', 'repair-empty-passages',
                    '--tenant', 'tenant:test', '--source', 'source:test', *flags]
            with patch('sys.argv', argv), patch.object(cli, 'BrainStore'), \
                    patch.object(cli.SemanticRuntime, 'from_env'), \
                    patch.object(cli, 'build_rerank_runtime'), \
                    patch.object(cli, 'build_evidence_archive_store'), \
                    patch.object(cli, 'CanonicalPassageProjector', return_value=projector), \
                    redirect_stdout(io.StringIO()):
                cli.main()
            self.assertEqual(projector.repair_empty.call_args.kwargs['concurrency'], expected)
