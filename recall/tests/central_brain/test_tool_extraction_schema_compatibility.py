"""Schema89 compatibility precedes the separately deployed typed publisher."""
import unittest

from recall_server.capabilities import CapabilityError, assess_snapshot
from tests.central_brain.test_core_deployment import healthy_snapshot


class ToolExtractionSchemaCompatibility(unittest.TestCase):
    def test_original_runtime_accepts_88_and_explicit_89(self):
        for retired in (False, True):
            for latest in (88, 89):
                snapshot = healthy_snapshot()
                snapshot['migration_versions'] = [n for n in range(1, latest + 1) if retired or n != 67]
                snapshot['postgres_vector_plane_present'] = not retired
                self.assertEqual(assess_snapshot(snapshot)['schema_version'], latest)

    def test_89_does_not_hide_unknown_version_missing_marker_or_privilege_failure(self):
        for versions in (list(range(1, 91)), [n for n in range(1, 90) if n != 69],
                         list(range(1, 90)) + [89]):
            snapshot = healthy_snapshot()
            snapshot['migration_versions'] = versions
            with self.assertRaisesRegex(CapabilityError, 'schema_drift'):
                assess_snapshot(snapshot)
        snapshot = healthy_snapshot()
        snapshot['migration_versions'] = list(range(1, 90))
        snapshot['privileges']['schema_migrations_readonly'] = False
        with self.assertRaisesRegex(CapabilityError, 'role_privilege_insufficient'):
            assess_snapshot(snapshot)
