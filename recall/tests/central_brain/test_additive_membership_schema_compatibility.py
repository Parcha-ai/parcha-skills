"""Explicit 092 compatibility preserves the previous schema and refusal fences."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'server'))

from recall_server import SCHEMA_VERSION  # noqa: E402
from recall_server.capabilities import assess_snapshot, CapabilityError  # noqa: E402
from tests.central_brain.test_core_deployment import healthy_snapshot  # noqa: E402


class AdditiveMembershipSchemaCompatibility(unittest.TestCase):
    def test_known_91_and_92_preserve_both_planes(self):
        self.assertEqual(SCHEMA_VERSION, 93)
        for latest in (91, 92):
            for retired in (False, True):
                snapshot = healthy_snapshot()
                snapshot['migration_versions'] = [n for n in range(1, latest + 1)
                                                  if retired or n != 67]
                snapshot['postgres_vector_plane_present'] = not retired
                self.assertEqual(assess_snapshot(snapshot)['schema_version'], latest)

    def test_92_requires_existing_profile_generation_certificate_and_remainder(self):
        for prerequisite in (80, 82, 83, 85):
            snapshot = healthy_snapshot()
            snapshot['migration_versions'] = [n for n in range(1, 93) if n != prerequisite]
            with self.subTest(prerequisite=prerequisite):
                with self.assertRaisesRegex(CapabilityError, 'schema_drift'):
                    assess_snapshot(snapshot)

    def test_future_gap_duplicate_and_privilege_still_refuse(self):
        for versions in (list(range(1, 95)), [n for n in range(1, 93) if n != 69],
                         list(range(1, 93)) + [92]):
            snapshot = healthy_snapshot()
            snapshot['migration_versions'] = versions
            with self.assertRaisesRegex(CapabilityError, 'schema_drift'):
                assess_snapshot(snapshot)
        snapshot = healthy_snapshot()
        snapshot['privileges']['schema_migrations_readonly'] = False
        with self.assertRaisesRegex(CapabilityError, 'role_privilege_insufficient'):
            assess_snapshot(snapshot)
