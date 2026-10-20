"""Optional093 is explicit; schema92 and all original refusal fences remain."""
import unittest

from recall_server.capabilities import assess_snapshot, CapabilityError
from tests.central_brain.test_core_deployment import healthy_snapshot


class CodexTextBlockStorageCompatibility(unittest.TestCase):
    def test_92_and_93_preserve_both_planes(self):
        for latest in (92, 93):
            for retired in (False, True):
                snapshot = healthy_snapshot()
                snapshot['migration_versions'] = [n for n in range(1, latest+1)
                                                  if retired or n != 67]
                snapshot['postgres_vector_plane_present'] = not retired
                self.assertEqual(assess_snapshot(snapshot)['schema_version'], latest)

    def test_missing_codex_contract_future_and_duplicate_remain_refused(self):
        for versions in ([n for n in range(1, 94) if n != 90],
                         list(range(1, 95)), list(range(1, 94))+[93]):
            with self.subTest(versions=versions):
                snapshot = healthy_snapshot()
                snapshot['migration_versions'] = versions
                with self.assertRaisesRegex(CapabilityError, 'schema_drift'):
                    assess_snapshot(snapshot)
