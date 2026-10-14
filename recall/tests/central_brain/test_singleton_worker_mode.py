"""Explicit metadata deletion mode follows the existing worker command/child."""
import unittest
from unittest.mock import patch
from tests.central_brain import test_retirement_maintenance_cli as cli_tests
from recall_server.retirement_supervisor import RetirementSupervisor
from recall_server.retirement_maintenance import RetirementMaintenance


class SingletonWorkerMode(unittest.TestCase):
    def test_existing_completion_log_reports_only_acknowledged_metadata_count(self):
        import json
        from io import BytesIO
        from unittest.mock import Mock
        owner = RetirementSupervisor('synthetic', metadata_mode='singleton')
        result = dict(status='commit_outcome_unknown', cleared_documents=1,
            cleared_utf8_bytes=99, retired_metadata_chunks=1, commit_outcome_unknown=True)
        owner.child = Mock(returncode=0, stdout=BytesIO(json.dumps(result).encode()))
        owner.child.poll.return_value = 0
        with self.assertLogs('recall_server.retirement_supervisor', level='INFO') as logs:
            observed = owner.cycle()
        self.assertEqual(observed, result)
        self.assertIn('retired_metadata_chunks=1', logs.output[0])
        self.assertIn('unknown=True', logs.output[0])

    def test_command_selects_same_mode_for_once_and_continuous(self):
        for once, owner in ((True, 'RetirementMaintenance'), (False, 'RetirementSupervisor')):
            with self.subTest(once=once):
                mocked, _ = cli_tests.RetirementCliTests().invoke(
                    ['--retirement', 'on', '--retirement-metadata', 'singleton'], once=once)
                self.assertEqual(mocked[owner].call_args.kwargs['metadata_mode'], 'singleton')

    def test_supervisor_default_child_configuration_is_unchanged(self):
        default = RetirementSupervisor('synthetic', tenant_id='tenant', seconds=10)
        self.assertEqual(default.configuration, dict(dsn='synthetic', tenant_id='tenant', seconds=10))
        enabled = RetirementSupervisor('synthetic', tenant_id='tenant', seconds=10, metadata_mode='singleton')
        self.assertEqual(enabled.configuration, dict(default.configuration, metadata_mode='singleton'))
        for value in ('all', True, None):
            with self.assertRaises(ValueError):
                RetirementSupervisor('synthetic', metadata_mode=value)

    def test_maintenance_passes_only_explicit_mode_to_existing_runner(self):
        from recall_server import retirement_maintenance as maintenance
        from unittest.mock import Mock, MagicMock
        store = MagicMock(verified_singleton_receipts=True, search_plane='turbopuffer')
        source = dict(tenant_id='tenant', principal_id='principal', source_id='source')
        owner = RetirementMaintenance(store, Mock(), enabled=True, metadata_mode='singleton')
        with patch.object(owner, '_claim_source', return_value=source), \
                patch.object(maintenance, '_authorize'), \
                patch.object(owner, '_reconcile_intent', return_value={}), \
                patch.object(owner, '_reconcile_publications', return_value=0), \
                patch.object(maintenance, 'run_retirement', return_value={'commit_outcome_unknown': True}) as run:
            store.connect.return_value.__enter__.return_value.transaction.return_value.__enter__.return_value = None
            owner.cycle(busy=False)
        self.assertEqual(run.call_args.kwargs['metadata_mode'], 'singleton')
