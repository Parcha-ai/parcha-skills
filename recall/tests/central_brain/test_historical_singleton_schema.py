"""Historical singleton readers can precede their irreversible compatibility floor."""
import unittest

from recall_server.capabilities import assess_snapshot, CapabilityError
from tests.central_brain.test_core_deployment import healthy_snapshot


class HistoricalSingletonSchema(unittest.TestCase):
    def test_compatible_reader_accepts_094_and_095_in_both_planes(self):
        for latest in (94, 95):
            for retired in (False, True):
                with self.subTest(latest=latest, retired=retired):
                    snapshot = healthy_snapshot()
                    snapshot['migration_versions'] = [
                        v for v in range(1, latest + 1) if retired or v != 67
                    ]
                    snapshot['postgres_vector_plane_present'] = not retired
                    self.assertEqual(assess_snapshot(snapshot)['schema_version'], latest)

    def test_floor_without_singleton_profile_and_future_schema_refuse(self):
        for versions in (
            [v for v in range(1, 96) if v != 80],
            list(range(1, 97)),
            list(range(1, 96)) + [95],
        ):
            snapshot = healthy_snapshot()
            snapshot['migration_versions'] = versions
            with self.assertRaisesRegex(CapabilityError, 'schema_drift'):
                assess_snapshot(snapshot)
