"""Retained schema90 compatibility through Codex extraction activation."""
import unittest
from recall_server import SCHEMA_VERSION
from recall_server.capabilities import assess_snapshot, CapabilityError
from tests.central_brain.test_core_deployment import healthy_snapshot

class CodexExtractionSchemaCompatibilityTests(unittest.TestCase):
    def test_known_90_preserves_both_search_planes(self):
        self.assertEqual(SCHEMA_VERSION, 90)
        for retired in (False, True):
            snapshot = healthy_snapshot()
            snapshot['migration_versions'] = [n for n in range(1, 91) if retired or n != 67]
            snapshot['postgres_vector_plane_present'] = not retired
            self.assertEqual(assess_snapshot(snapshot)['schema_version'], 90)

    def test_known_90_does_not_hide_unknown_or_incomplete_schema(self):
        for versions in (list(range(1, 92)), [n for n in range(1, 91) if n != 89],
                         [n for n in range(1, 91) if n != 69], list(range(1, 91)) + [90]):
            with self.subTest(versions=versions):
                snapshot = healthy_snapshot()
                snapshot['migration_versions'] = versions
                with self.assertRaisesRegex(CapabilityError, 'schema_drift'):
                    assess_snapshot(snapshot)

    def test_known_90_preserves_runtime_privilege_checks(self):
        snapshot = healthy_snapshot()
        snapshot['migration_versions'] = list(range(1, 91))
        snapshot['privileges']['schema_migrations_readonly'] = False
        with self.assertRaisesRegex(CapabilityError, 'role_privilege_insufficient'):
            assess_snapshot(snapshot)

if __name__=='__main__':unittest.main()
