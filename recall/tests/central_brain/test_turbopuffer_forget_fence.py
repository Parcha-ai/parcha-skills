"""Search must distrust a lagging search projection after canonical forget."""
from contextlib import nullcontext
from types import SimpleNamespace
import time
import unittest

from recall_server.db import SearchDeadlineExceeded
from recall_server.passage_retrieval import collapse_document_candidates
from recall_server.turbopuffer_retrieval import arm_row
from tests.central_brain.test_turbopuffer_retrieval import (
    SETTINGS, TENANT, catalog_passage,
)
from tests.central_brain import test_turbopuffer_retrieval as fixtures
from tests.central_brain.fake_turbopuffer import FakeTurbopuffer
from recall_server.turbopuffer_plane import passage_row


class ForgetAuthorityTests(unittest.TestCase):
    def fixture(self, *, live=True, body='', foreign=False, error=None):
        client = FakeTurbopuffer()
        sample = catalog_passage(1, 'forgotten synthetic deployment phrase')
        ns = client.namespace(SETTINGS.namespace(TENANT))
        vendor = passage_row(sample)
        ns.write(upsert_rows=[vendor])
        retrieval, store = fixtures.ArmTests()._retrieval(client)
        authoritative = dict(sample, tenant_id=TENANT, chunk_text=body)
        if foreign:
            authoritative['source_id'] = 'foreign:source'
        calls = []
        store.connect = lambda: nullcontext(object())
        def execute(connection, sql, values, deadline):
            calls.append((sql, values, deadline))
            self.assertGreater(deadline, time.monotonic())
            if error:
                raise error
            return SimpleNamespace(fetchall=lambda: [authoritative] if live else [])
        store._execute_bounded = execute
        row = arm_row(vendor, 1.0)
        legs = (('dense', 1.0, [row]),)
        results = collapse_document_candidates(legs, limit=10)
        return retrieval, ns, legs, results, calls, authoritative

    def test_forgotten_vendor_row_is_dropped_before_body_or_rerank(self):
        retrieval, ns, legs, results, calls, _ = self.fixture(live=False)
        diagnostics = retrieval._hydrate_ranges(results, legs, deadline_at=time.monotonic()+5)
        self.assertEqual(results, [])
        self.assertEqual(legs[0][2], [])
        self.assertEqual(ns.queries, [])
        self.assertEqual(len(calls), 1)
        self.assertEqual(diagnostics['authority_status'], 'ok')
        self.assertEqual(diagnostics['authority_rejected'], 1)

    def test_retired_empty_body_metadata_remains_live(self):
        retrieval, ns, legs, results, calls, _ = self.fixture(body='')
        # Catalog-only arms do not carry bodies until after authority check.
        for row in legs[0][2]:
            row['text_redacted'] = ''
        for result in results:
            for span in result['matching_ranges']:
                span['text'] = ''
        retrieval._hydrate_ranges(results, legs, deadline_at=time.monotonic()+5)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['matching_ranges'][0]['text'], 'forgotten synthetic deployment phrase')
        self.assertEqual(len(calls), 1)
        self.assertNotIn("text_redacted<>''", calls[0][0])

    def test_foreign_source_or_mismatched_hash_is_rejected(self):
        for change in ('source', 'hash', 'tenant'):
            with self.subTest(change=change):
                retrieval, ns, legs, results, _, authority = self.fixture(foreign=change=='source')
                if change == 'hash':
                    authority['text_sha256'] = 'f'*64
                if change == 'tenant':
                    authority['tenant_id'] = 'tenant:foreign'
                retrieval._hydrate_ranges(results, legs, deadline_at=time.monotonic()+5)
                self.assertEqual(results, [])
                self.assertEqual(legs[0][2], [])
                self.assertEqual(ns.queries, [])

    def test_deadline_and_db_failure_fail_closed(self):
        for error, expected in ((SearchDeadlineExceeded(), 'deadline-exceeded'), (RuntimeError('synthetic failure'), 'unavailable')):
            with self.subTest(error=type(error).__name__):
                retrieval, ns, legs, results, _, _ = self.fixture(error=error)
                diagnostics = retrieval._hydrate_ranges(results, legs, deadline_at=time.monotonic()+5)
                self.assertEqual(results, [])
                self.assertEqual(legs[0][2], [])
                self.assertEqual(ns.queries, [])
                self.assertEqual(diagnostics['authority_status'], expected)

    def test_stale_upsert_after_forget_cannot_resurrect_search(self):
        retrieval, ns, _, _, calls, _ = self.fixture(live=False)
        ns.write(upsert_rows=list(ns.rows.values()))
        response = retrieval.search('deployment phrase', lexical_query='deployment phrase', since=None, until=None, limit=10, include_arms=True)
        self.assertEqual(response['results'], [])
        self.assertTrue(all(not rows for rows in response['arms'].values()))
        self.assertEqual(len(calls), 1)
        self.assertFalse(any(query['rank_by'] == ('id', 'asc') for query in ns.queries))
