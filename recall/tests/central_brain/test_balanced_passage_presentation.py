"""Keep paid-for evidence available under the native MCP serialization bound."""
import copy
import unittest
from recall_server import mcp
from tests.central_brain.test_rerank_matched_ranges import MatchedRangeRerankTests,RecordingProvider

class DescendingProvider(RecordingProvider):
    def post(self, **kwargs):
        self.observer(); self.calls.append((copy.deepcopy(kwargs['body']),kwargs['timeout']))
        return {'data':[{'index':i,'relevance_score':1-i/100} for i in range(len(kwargs['body']['documents']))]}

class PresentationTests(unittest.TestCase):
    def fixture(self, *, duplicate=False):
        ranges=[dict(passage_id=f'p{i}',receipts=[f'receipt:{i}'],text=f'evidence {i}',score=1-i/10,kind='dense') for i in range(5)]
        rows=[dict(passage_id=r['passage_id'],text_redacted=r['text']) for r in ranges]
        if duplicate:
            for i in (1,2):rows[i]['text_redacted']=rows[0]['text_redacted'];ranges[i]['text']=ranges[0]['text']
        return [dict(source_id='source-a',logical_document_id='parent',rank=1.,matching_ranges=ranges)],(('dense',1.,rows),)

    def native(self,pool,legs,provider,*,remaining=5):
        owner=MatchedRangeRerankTests();owner.setUp();self.addCleanup(owner.doCleanups)
        return owner.run_rerank(provider,pool,legs,remaining=remaining)

    def test_rank_four_evidence_survives_actual_native_rerank_and_fitter(self):
        for duplicate in (False,True):
            with self.subTest(duplicate=duplicate):
                pool,legs=self.fixture(duplicate=duplicate);provider=DescendingProvider()
                output,diag,_,_=self.native(pool,legs,provider)
                self.assertEqual(diag['rerank_status'],'ok');self.assertEqual(len(provider.calls),1)
                self.assertEqual(len(provider.calls[0][0]['documents']),3 if duplicate else 5)
                result=mcp.fit_search_result(mcp.clip_search_snippets(dict(results=output,diagnostics={}),256))
                returned=result['results'][0]['matching_ranges']
                self.assertEqual([r['passage_id']for r in returned],['p0','p1','p2','p3','p4'])
                self.assertIn('receipt:3',{v for r in returned for v in r['receipts']})
                self.assertLessEqual(mcp._encoded_result_size(result),mcp.SEARCH_RESULT_BUDGET_BYTES)

    def test_native_failure_and_deadline_fallback_preserve_selected_ranges(self):
        for remaining,provider in ((0,DescendingProvider()),(5,RecordingProvider(error=TimeoutError()))):
            pool,legs=self.fixture()
            output,diag,_,_=self.native(pool,legs,provider,remaining=remaining)
            self.assertEqual(output,pool)
            self.assertEqual(len(provider.calls),0 if remaining==0 else 1)
            self.assertIn(diag['rerank_status'],('unavailable','skipped-budget'))

    def test_existing_response_fitter_bounds_large_selected_pool_without_range_drop(self):
        ranges=[dict(passage_id=f'p{i}',text='x'*20000,receipts=[f'receipt:{i}:{j}' for j in range(30)],spans=[{'large':'z'*2000}]) for i in range(85)]
        value=dict(results=[dict(source_id='source-a',logical_document_id='parent',matching_ranges=ranges)],diagnostics={})
        self.assertGreater(mcp._encoded_result_size(value),mcp.SEARCH_RESULT_BUDGET_BYTES)
        fitted=mcp.fit_search_result(value)
        self.assertLessEqual(mcp._encoded_result_size(fitted),mcp.SEARCH_RESULT_BUDGET_BYTES)
        self.assertEqual(len(fitted['results'][0]['matching_ranges']),85)
        self.assertTrue(all(r['receipts']==ranges[i]['receipts'] for i,r in enumerate(fitted['results'][0]['matching_ranges'])))
        # Existing transport refuses any still-oversize result; no response cap increase.
        impossible=dict(results=[dict(matching_ranges=[dict(text='x',unbounded_metadata='x'*(mcp.MAX_MCP_RESPONSE_BYTES+1))])])
        response=mcp.bound_response(mcp._tool_result(mcp.fit_search_result(impossible)),1)
        self.assertIn('error',response)

if __name__=='__main__':unittest.main()
