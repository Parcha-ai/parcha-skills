"""The catalog boundary never hands unsafe immutable objects to the sandbox."""
from contextlib import nullcontext
from types import SimpleNamespace
import unittest
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "server"))
from recall_server.canonical_retrieval import BoundCanonicalRetrieval


class ScanForgetFenceTest(unittest.TestCase):
    def test_unsafe_or_unclassified_shards_are_withheld_and_reported_pending(self):
        rows = [dict(artifact_id='safe', scan_safe=True),
                dict(artifact_id='forgotten', scan_safe=False),
                dict(artifact_id='unknown')]
        class Connection:
            def execute(self, sql, args):
                if ' AS count' in sql:
                    return SimpleNamespace(fetchone=lambda: {'count': 2})
                return SimpleNamespace(fetchall=lambda: rows)
        retrieval = BoundCanonicalRetrieval(
            SimpleNamespace(connect=lambda: nullcontext(Connection())),
            tenant_id='tenant:t', principal_id='principal:p',
            authorized_sources=('source:s',),
        )
        selected, pending = retrieval._parquet_shards(['source:s'], since=None, until=None)
        self.assertEqual([r['artifact_id'] for r in selected], ['safe'])
        self.assertEqual(pending, 4)
        self.assertNotIn('scan_safe', selected[0])
