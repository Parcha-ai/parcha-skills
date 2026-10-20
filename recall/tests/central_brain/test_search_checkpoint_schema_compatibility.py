"""Serving accepts explicit091 before publisher activation; future/gap fences remain."""
import unittest
from recall_server import SCHEMA_VERSION, SEARCH_PUBLICATION_CHECKPOINT_VERSION
from recall_server.capabilities import assess_snapshot, CapabilityError
from tests.central_brain.test_core_deployment import healthy_snapshot


class SearchCheckpointSchemaCompatibility(unittest.TestCase):
    def test_known_90_and_91_preserve_both_planes(self):
        self.assertEqual(SCHEMA_VERSION, 95)
        self.assertEqual(SEARCH_PUBLICATION_CHECKPOINT_VERSION, 91)
        for latest in (90, 91):
            for retired in (False, True):
                snapshot = healthy_snapshot()
                snapshot['migration_versions'] = [n for n in range(1, latest + 1) if retired or n != 67]
                snapshot['postgres_vector_plane_present'] = not retired
                self.assertEqual(assess_snapshot(snapshot)['schema_version'], latest)

    def test_unknown_gap_duplicate_and_privilege_still_refuse(self):
        for versions in (list(range(1, 97)), [n for n in range(1, 92) if n != 69],
                         list(range(1, 92)) + [91]):
            snapshot = healthy_snapshot()
            snapshot['migration_versions'] = versions
            with self.assertRaisesRegex(CapabilityError, 'schema_drift'):
                assess_snapshot(snapshot)
        snapshot = healthy_snapshot()
        snapshot['privileges']['schema_migrations_readonly'] = False
        with self.assertRaisesRegex(CapabilityError, 'role_privilege_insufficient'):
            assess_snapshot(snapshot)
