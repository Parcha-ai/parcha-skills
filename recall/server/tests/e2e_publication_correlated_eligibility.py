#!/usr/bin/env python3
"""Publication nomination retains the original full-row and cursor contract."""
import json
import time
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from e2e_search_publication_page_keys import BASELINE_SQL, PublicationPageKeys, nodes
from recall_server import turbopuffer_projection as publication


class CorrelatedEligibility(PublicationPageKeys):
    def install_time_index(self):
        with self.store.connect() as c:
            exists = c.execute("SELECT to_regclass('canonical_passages_page_time_id_idx') AS name").fetchone()['name']
            if exists:
                return
            c.execute('CREATE INDEX canonical_passages_page_time_id_idx '
                      'ON canonical_passages(tenant_id,source_id,first_occurred_at,passage_id)')

        def remove_fixture_index():
            with self.store.connect() as c:
                c.execute('DROP INDEX canonical_passages_page_time_id_idx')

        self.addCleanup(remove_fixture_index)

    def drain(self, connection, owner, claim, statement, since):
        rows, after, pages = [], None, 0
        started = time.monotonic()
        with patch.object(publication, '_PASSAGE_PAGE_SQL', statement):
            while True:
                page = owner.passage_page(connection, claim, since=since, after=after,
                                          deadline_at=time.monotonic() + 5)
                pages += 1
                rows.extend(page)
                if len(page) < owner.page_rows:
                    break
                after = (page[-1]['first_occurred_at'], page[-1]['passage_id'])
                self.assertLess(pages, 1000)
        return rows, pages, (time.monotonic() - started) * 1000

    def test_dense_sparse_and_stale_full_drain_matches_original(self):
        owner, claim = self.tied_fixture()
        candidate = publication._PASSAGE_PAGE_SQL
        self.install_time_index()
        for case in ('dense', 'sparse', 'stale'):
            if case != 'dense':
                with self.store.connect() as c:
                    if case == 'sparse':
                        c.execute("UPDATE canonical_passages SET created_at=CASE WHEN ordinal>=19990 "
                                  "THEN '2026-10-19'::timestamptz ELSE '2020-01-01'::timestamptz END "
                                  'WHERE tenant_id=%s', (self.tenant,))
                    else:
                        c.execute('UPDATE canonical_passages SET revision=revision+1 '
                                  'WHERE tenant_id=%s AND ordinal<19990', (self.tenant,))
                    c.execute('ANALYZE canonical_passages')
            since = datetime(2026, 10, 1, tzinfo=timezone.utc) if case == 'sparse' else None
            with self.subTest(case=case), patch.object(self.store, 'verified_singleton_receipts', True), \
                    self.store.connect() as c, c.transaction():
                c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                outcomes = []
                for label, statement in (('oracle', BASELINE_SQL), ('candidate', candidate),
                                         ('candidate', candidate), ('oracle', BASELINE_SQL)):
                    rows, pages, elapsed = self.drain(c, owner, claim, statement, since)
                    print(json.dumps(dict(case=case, variant=label, rows=len(rows), pages=pages,
                                          elapsed_ms=elapsed)), flush=True)
                    outcomes.append((rows, pages))
                for outcome in outcomes[1:]:
                    self.assertEqual(outcome, outcomes[0])
                self.assertEqual(len({row['passage_id'] for row in outcomes[0][0]}), len(outcomes[0][0]))
                if case != 'dense':
                    self.assertEqual(outcomes[0][1], 1, 'sparse pages must not introduce empty cursor steps')

    def test_evidence_revision_ahead_and_projected_mismatch_match_original(self):
        owner, claim = self.tied_fixture()
        with self.store.connect() as c:
            c.execute('UPDATE canonical_evidence_documents SET revision=revision+1 WHERE tenant_id=%s',
                      (self.tenant,))
            c.execute('UPDATE canonical_passages SET revision=revision+1 '
                      'WHERE tenant_id=%s AND mod(ordinal,11)=0', (self.tenant,))
            c.execute('UPDATE canonical_passages SET policy_fingerprint=%s '
                      'WHERE tenant_id=%s AND mod(ordinal,13)=0', ('f' * 64, self.tenant))
        candidate = publication._PASSAGE_PAGE_SQL
        for verified in (False, True):
            with self.subTest(verified=verified), patch.object(self.store, 'verified_singleton_receipts', verified), \
                    self.store.connect() as c, c.transaction():
                c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                expected, expected_pages, _ = self.drain(c, owner, claim, BASELINE_SQL, None)
                actual, pages, _ = self.drain(c, owner, claim, candidate, None)
                self.assertTrue(actual)
                self.assertEqual(actual, expected)
                self.assertEqual(pages, expected_pages)
                self.assertTrue(all(row['ordinal'] % 11 and row['ordinal'] % 13 for row in actual))

    def test_native_nomination_plan_and_full_first_page(self):
        owner, claim = self.tied_fixture()
        captured = []
        execute = self.store._execute_bounded

        def capture(c, query, values, deadline, **options):
            captured.append((query, values))
            return execute(c, query, values, deadline, **options)

        self.install_time_index()
        with patch.object(self.store, 'verified_singleton_receipts', True), self.store.connect() as c, c.transaction():
            c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            with patch.object(self.store, '_execute_bounded', side_effect=capture):
                actual = owner.passage_page(c, claim, since=None, after=None, deadline_at=time.monotonic() + 5)
            query, values = captured[-1]
            plan = execute(c, 'EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) ' + query,
                           values, time.monotonic() + 5).fetchone()['QUERY PLAN'][0]
            with patch.object(publication, '_PASSAGE_PAGE_SQL', BASELINE_SQL):
                expected = owner.passage_page(c, claim, since=None, after=None, deadline_at=time.monotonic() + 5)
            self.assertEqual(actual, expected)
            self.assertEqual(len(actual), 128)
            eligible = next(n for n in nodes(plan['Plan']) if n.get('Subplan Name') == 'CTE eligible_page')
            print(json.dumps(dict(native_plan=plan, nomination_plan=eligible), default=str), flush=True)


if __name__ == '__main__':
    suite = unittest.TestSuite(CorrelatedEligibility(name) for name in CorrelatedEligibility.__dict__
                               if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
