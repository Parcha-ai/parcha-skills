"""The optional vector boundary refuses unsafe callers before database access."""
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'server'))
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.storage_retirement import retire_chunk_search_vector


class VectorRetirementTests(unittest.TestCase):
    def test_postgres_refused_without_io(self):
        store = Mock(search_plane='postgres')
        with self.assertRaisesRegex(ValueError, 'turbopuffer'):
            retire_chunk_search_vector(store, apply=True)
        store.connect.assert_not_called()

    def test_apply_must_be_boolean(self):
        store = Mock(search_plane='turbopuffer')
        for value in (1, 'true', None):
            with self.assertRaises(ValueError):
                retire_chunk_search_vector(store, apply=value)
        store.connect.assert_not_called()

    def test_private_postgres_evaluator_refuses_turbopuffer(self):
        store = Mock(search_plane='turbopuffer')
        bound = BoundCanonicalRetrieval(store, tenant_id='tenant:test', principal_id='principal:test',
                                        authorized_sources=('source:test',))
        with self.assertRaisesRegex(ValueError, 'postgres'):
            bound._legacy_chunk_search_for_eval('search phrase')
        store.connect.assert_not_called()


if __name__ == '__main__':
    unittest.main()
