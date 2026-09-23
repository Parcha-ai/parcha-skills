"""Authorize every exposable range, retaining the existing ranked candidate pool."""
from contextlib import nullcontext
from types import SimpleNamespace
import copy
import time
import unittest

from recall_server.passage_retrieval import collapse_document_candidates
from recall_server.turbopuffer_plane import passage_row
from recall_server.turbopuffer_retrieval import arm_row, CATALOG_ATTRIBUTES
from tests.central_brain.fake_turbopuffer import FakeTurbopuffer
from tests.central_brain import test_turbopuffer_retrieval as fixtures


class SelectedAuthorityTests(unittest.TestCase):
    def fixture(self, *, forgotten=()):
        client = FakeTurbopuffer()
        retrieval, store = fixtures.ArmTests()._retrieval(client)
        ns = client.namespace(fixtures.SETTINGS.namespace(fixtures.TENANT))
        vendors = {}
        for index in range(1, 5):
            vendors[index] = passage_row(fixtures.catalog_passage(index,
                'deployment evidence number ' + str(index), ldoc='ldoc_' + str(index)*32))
        ns.write(upsert_rows=list(vendors.values()))
        def catalog(index, score):
            vendor = vendors[index]
            return arm_row({k:v for k,v in vendor.items() if k in CATALOG_ATTRIBUTES or k=='id'}, score)
        legs = (
            ('dense', 1.0, [catalog(1,10),catalog(2,9),catalog(4,0.5)]),
            ('passage-lexical', 1.0, [catalog(1,10),catalog(2,9),catalog(4,0.5)]),
            ('sparse-exact', 0.01, [catalog(3,100)]),
        )
        results = collapse_document_candidates(legs, limit=2, fusion='convex',
            alphas={'dense':1.0,'passage-lexical':1.0,'sparse-exact':0.01}, nominate_per_arm=1)
        self.assertEqual([row['logical_document_id'] for row in results], ['ldoc_'+str(i)*32 for i in (1,2,3)])
        self.assertTrue(results[-1]['nominated'])
        queried = []
        store.connect = lambda: nullcontext(object())
        def execute(connection, sql, values, deadline):
            queried.append(set(values[-1]))
            return SimpleNamespace(fetchall=lambda:[dict(v, tenant_id=fixtures.TENANT, passage_id=v['id'])
                for index,v in vendors.items() if index not in forgotten and v['id'] in values[-1]])
        store._execute_bounded = execute
        return retrieval, store, ns, vendors, legs, results, queried

    def test_every_selected_range_including_nomination_is_checked_without_unused_tail(self):
        retrieval, store, ns, vendors, legs, results, queried = self.fixture()
        selected = {span['passage_id'] for row in results for span in row['matching_ranges']}
        retrieval._hydrate_ranges(results, legs, deadline_at=time.monotonic()+5)
        self.assertEqual(queried, [selected])
        self.assertNotIn(vendors[4]['id'], {row['passage_id'] for _,_,rows in legs for row in rows})
        self.assertIn(vendors[3]['id'], queried[0])
        bodies = [query['filters'][2] for query in ns.queries if query['rank_by']==('id','asc')]
        self.assertEqual(set(bodies[0]), selected)
        runtime = fixtures.RerankWiringTests._FakeRerank({})
        retrieval._rerank_fused('deployment evidence', results, legs, runtime=runtime,
            deadline_at=time.monotonic()+5, arm_elapsed_ms={})
        texts = runtime.calls[-1]['documents']
        self.assertTrue(any('number 3' in text for text in texts))
        self.assertFalse(any('number 4' in text for text in texts))

    def test_forgotten_nominee_and_unchecked_tail_never_reach_output_or_provider(self):
        retrieval, _, ns, vendors, legs, results, queried = self.fixture(forgotten=(3,4))
        retrieval._hydrate_ranges(results, legs, deadline_at=time.monotonic()+5)
        self.assertEqual(queried, [{vendors[i]['id'] for i in (1,2,3)}])
        self.assertEqual([row['logical_document_id'] for row in results], ['ldoc_'+str(i)*32 for i in (1,2)])
        self.assertTrue(all(row['passage_id'] in {vendors[1]['id'],vendors[2]['id']}
                            for _,_,rows in legs for row in rows))
        self.assertNotIn(vendors[4]['id'], str(results))
        self.assertFalse(any(vendors[3]['id'] in query['filters'][2] for query in ns.queries))

    def test_debug_arms_validate_all_candidates(self):
        retrieval, _, _, vendors, legs, results, queried = self.fixture(forgotten=(4,))
        retrieval._hydrate_ranges(results, legs, deadline_at=time.monotonic()+5, include_arms=True)
        self.assertEqual(queried, [{row['id'] for row in vendors.values()}])
        self.assertNotIn(vendors[4]['id'], {row['passage_id'] for _,_,rows in legs for row in rows})

    def test_existing_selected_results_identical_to_full_arm_validation(self):
        snapshots = []
        for debug in (False,True):
            retrieval, _, _, _, legs, results, _ = self.fixture()
            retrieval._hydrate_ranges(results, legs, deadline_at=time.monotonic()+5, include_arms=debug)
            snapshots.append(copy.deepcopy(results))
        self.assertEqual(*snapshots)

    def test_multiple_ranges_of_each_prererank_document_are_all_checked(self):
        retrieval, _, ns, vendors, legs, _, queried = self.fixture()
        vendors[5] = passage_row(fixtures.catalog_passage(5, 'deployment evidence extra selected range',
            ldoc='ldoc_'+'1'*32))
        ns.write(upsert_rows=[vendors[5]])
        legs[0][2].append(arm_row({k:v for k,v in vendors[5].items()
            if k in CATALOG_ATTRIBUTES or k=='id'}, 9.5))
        results = collapse_document_candidates(legs, limit=2, fusion='convex',
            alphas={'dense':1.0,'passage-lexical':1.0,'sparse-exact':0.01}, nominate_per_arm=1)
        selected = {span['passage_id'] for row in results for span in row['matching_ranges']}
        self.assertGreater(len(selected), len(results))
        retrieval._hydrate_ranges(results, legs, deadline_at=time.monotonic()+5)
        self.assertEqual(queried, [selected])
        self.assertIn(vendors[5]['id'], queried[0])
        self.assertIn(vendors[3]['id'], queried[0])

    def test_search_propagates_explicit_debug_arm_authority(self):
        for debug in (False,True):
            with self.subTest(debug=debug):
                retrieval, _, _, vendors, _, _, queried = self.fixture(forgotten=(4,))
                response = retrieval.search('deployment evidence', lexical_query='deployment evidence',
                    since=None, until=None, limit=1, include_arms=debug)
                if debug:
                    self.assertEqual(queried, [{v['id'] for v in vendors.values()}])
                    self.assertIn('arms', response)
                else:
                    self.assertEqual(len(queried[0]), 1)
                    self.assertNotIn('arms', response)
                self.assertNotIn(vendors[4]['logical_document_id'], str(response))
