"""Optional cleanup indexes do not relax schema or application-role authority."""
import itertools
import unittest

from recall_server.capabilities import CapabilityError, assess_snapshot
from tests.central_brain.test_core_deployment import healthy_snapshot


class CleanupIndexSchemaTests(unittest.TestCase):
    def test_optional_markers_preserve_policy_detachment_dependency(self):
        for retired, *optional in itertools.product((False, True), repeat=9):
            with self.subTest(retired=retired, optional=optional):
                snapshot = healthy_snapshot()
                versions = [n for n in range(1, 70) if retired or n != 67]
                versions += [version for enabled, version in zip(optional, range(70, 78)) if enabled]
                snapshot['migration_versions'] = versions
                snapshot['postgres_vector_plane_present'] = not retired
                if 76 in versions and 75 not in versions:
                    with self.assertRaises(CapabilityError):
                        assess_snapshot(snapshot)
                    continue
                result = assess_snapshot(snapshot)
                self.assertEqual(result['schema_version'], max(versions))
                self.assertEqual(result['status'], 'ready')

    def test_77_cannot_hide_unknown_gaps_duplicates_or_reordering(self):
        complete = list(range(1, 78))
        for versions in (complete + [78], [n for n in complete if n != 68],
                         [n for n in complete if n != 69], complete + [74],
                         complete[:-2] + [74, 73]):
            with self.subTest(versions=versions):
                snapshot = healthy_snapshot()
                snapshot['migration_versions'] = versions
                with self.assertRaises(CapabilityError) as error:
                    assess_snapshot(snapshot)
                self.assertEqual(error.exception.code, 'schema_drift')


if __name__ == '__main__':
    unittest.main()
