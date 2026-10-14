"""The real worker command keeps retirement opt-in and deadline-bounded."""
import contextlib
import io
import unittest
from unittest.mock import patch

from recall_server import cli


class RetirementCliTests(unittest.TestCase):
    def invoke(self, extra, *, once=True, inspect_worker=None):
        with contextlib.ExitStack() as stack:
            names = ('BrainStore', 'SemanticRuntime', 'build_rerank_runtime',
                'CanonicalLogicalEvidenceProjector', 'CanonicalPassageProjector',
                'CanonicalParquetScanProjector', 'CanonicalBodyThinner',
                'build_evidence_archive_store', 'build_archive_store', 'RetirementMaintenance', 'RetirementSupervisor')
            mocked = {name: stack.enter_context(patch.object(cli, name)) for name in names}
            run = stack.enter_context(patch.object(cli, 'run_projection_worker', return_value={'status': 'pending'}))
            if inspect_worker is not None:
                run.side_effect = lambda *args, **kwargs: inspect_worker(mocked, kwargs)
            stack.enter_context(patch.object(cli, '_search_plane_settings', return_value=None))
            stack.enter_context(patch('sys.argv', ['recall-server', '--dsn', 'postgresql://synthetic.invalid/recall',
                'projection-worker', '--tenant', 'tenant:company:test', *(['--once'] if once else []), *extra]))
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            cli.main()
            callback = run.call_args.kwargs['retirement_maintainer']
            if callback is not None:
                callback(True)
        return mocked, run

    def test_default_off_does_not_construct_a_retirement_archive(self):
        mocked, run = self.invoke([])
        mocked['RetirementMaintenance'].assert_not_called()
        self.assertIsNone(run.call_args.kwargs['retirement_maintainer'])
        self.assertFalse(any(call.kwargs.get('deadline_reads') for call in
            mocked['build_evidence_archive_store'].call_args_list))

    def test_on_connects_existing_worker_to_deadline_archive_and_budget(self):
        mocked, _ = self.invoke(['--retirement', 'on', '--retirement-seconds', '7'])
        mocked['build_evidence_archive_store'].assert_any_call(deadline_reads=True)
        constructor = mocked['RetirementMaintenance']
        self.assertEqual(constructor.call_args.kwargs,
            dict(tenant_id='tenant:company:test', enabled=True, seconds=7.0))
        constructor.return_value.cycle.assert_called_once_with(busy=True)

    def test_continuous_worker_supervises_retirement_without_parent_archive_client(self):
        mocked, _ = self.invoke(['--retirement', 'on', '--retirement-seconds', '180'], once=False)
        mocked['RetirementMaintenance'].assert_not_called()
        self.assertFalse(any(call.kwargs.get('deadline_reads') for call in
            mocked['build_evidence_archive_store'].call_args_list))
        constructor = mocked['RetirementSupervisor']
        self.assertEqual(constructor.call_args.kwargs,
            dict(tenant_id='tenant:company:test', seconds=180.0))
        constructor.return_value.cycle.assert_called_once_with(busy=True)
        constructor.return_value.__enter__.assert_called_once()
        constructor.return_value.__exit__.assert_called_once()


    def test_continuous_default_allows_measured_hosted_parent_proof(self):
        mocked, _ = self.invoke(['--retirement', 'on'], once=False)
        self.assertEqual(mocked['RetirementSupervisor'].call_args.kwargs['seconds'],180)

    def test_continuous_thinning_snapshots_hints_on_coordinator_and_closes_slot(self):
        import threading
        import time
        prepared = []
        def inspect_worker(mocked, kwargs):
            thinner = mocked['CanonicalBodyThinner'].return_value
            thinner.thin.return_value = dict(status='pending', documents=1, refused=0,
                document_bytes_removed=10, event_bytes_replaced=20)
            hints = (('tenant:company:test', 'source:test', 'doc:test'),)
            logical = mocked['CanonicalLogicalEvidenceProjector'].return_value
            logical._take_committed_body_keys.side_effect = lambda: (prepared.append(threading.get_ident()) or hints)
            scheduler = kwargs['background_thinner']
            self.assertIsNone(kwargs['body_thinner'])
            self.assertFalse(scheduler.closed)
            scheduler.tick(True)
            deadline = time.monotonic() + 2
            result = None
            while result is None and time.monotonic() < deadline:
                result = scheduler.tick(True)
                time.sleep(0.005)
            self.assertEqual(result['documents'], 1)
            self.assertEqual(prepared, [threading.get_ident()])
            thinner.thin.assert_called_once_with(committed_keys=hints, batch_size=100)
            return {'status': 'pending'}
        _, run = self.invoke([], once=False, inspect_worker=inspect_worker)
        self.assertTrue(run.call_args.kwargs['background_thinner'].closed)

    def test_once_keeps_original_synchronous_thinner(self):
        _, run = self.invoke([], once=True)
        self.assertIsNone(run.call_args.kwargs['background_thinner'])
        self.assertTrue(callable(run.call_args.kwargs['body_thinner']))
