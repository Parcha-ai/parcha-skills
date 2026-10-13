"""Optional cleanup indexes do not relax schema or application-role authority."""
import itertools
import unittest

from recall_server.capabilities import CapabilityError, assess_snapshot
from tests.central_brain.test_core_deployment import healthy_snapshot


class CleanupIndexSchemaTests(unittest.TestCase):
    def test_74_is_optional_and_independent_of_other_known_optionals(self):
        for retired, *optional in itertools.product((False, True), repeat=6):
            with self.subTest(retired=retired, optional=optional):
                snapshot = healthy_snapshot()
                versions = [n for n in range(1, 70) if retired or n != 67]
                versions += [version for enabled, version in zip(optional, range(70, 75)) if enabled]
                snapshot['migration_versions'] = versions
                snapshot['postgres_vector_plane_present'] = not retired
                result = assess_snapshot(snapshot)
                self.assertEqual(result['schema_version'], max(versions))
                self.assertEqual(result['status'], 'ready')

    def test_74_cannot_hide_unknown_gaps_duplicates_or_reordering(self):
        complete = list(range(1, 75))
        for versions in (complete + [75], [n for n in complete if n != 68],
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
