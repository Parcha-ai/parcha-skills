"""The optional Slack raw-owner relation does not relax the serving contract."""
import unittest

from tests.central_brain.test_core_deployment import healthy_snapshot
from recall_server import SCHEMA_VERSION
from recall_server.capabilities import CapabilityError, assess_snapshot


class SlackDeletionSchemaCompatibility(unittest.TestCase):
    def test_old_runtime_schema_and_77_are_both_supported(self):
        for optional in ([], [77], [70, 72, 73, 74, 77]):
            with self.subTest(optional=optional):
                snapshot = healthy_snapshot()
                snapshot['migration_versions'] = [*range(1, 70), *optional]
                snapshot['postgres_vector_plane_present'] = False
                self.assertEqual(assess_snapshot(snapshot)['status'], 'ready')

    def test_77_cannot_hide_a_missing_mandatory_or_unknown_version(self):
        for versions in ([*range(1, 69), 77], [*range(1, 70), 77, SCHEMA_VERSION + 1], [*range(1, 70), 77, 77]):
            with self.subTest(versions=versions):
                snapshot = healthy_snapshot()
                snapshot['migration_versions'] = versions
                snapshot['postgres_vector_plane_present'] = False
                with self.assertRaises(CapabilityError) as caught:
                    assess_snapshot(snapshot)
                self.assertEqual(caught.exception.code, 'schema_drift')
