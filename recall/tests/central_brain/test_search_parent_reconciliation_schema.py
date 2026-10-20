"""Optional094 accepts both deployed093 and the new queue, never future versions."""
import unittest
from recall_server.capabilities import assess_snapshot, CapabilityError
from tests.central_brain.test_core_deployment import healthy_snapshot


class ParentReconciliationSchema(unittest.TestCase):
    def test_pre_and_post_migration_preserve_both_planes(self):
        for latest in (93, 94):
            for retired in (False, True):
                snapshot = healthy_snapshot()
                snapshot['migration_versions'] = [v for v in range(1, latest+1) if retired or v != 67]
                snapshot['postgres_vector_plane_present'] = not retired
                self.assertEqual(assess_snapshot(snapshot)['schema_version'], latest)

    def test_future_duplicate_and_missing_core_remain_refused(self):
        for versions in (list(range(1,97)), list(range(1,95))+[94],
                         [v for v in range(1,95) if v != 69]):
            snapshot = healthy_snapshot()
            snapshot['migration_versions'] = versions
            with self.assertRaisesRegex(CapabilityError, 'schema_drift'):
                assess_snapshot(snapshot)
