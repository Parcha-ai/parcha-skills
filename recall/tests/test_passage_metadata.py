"""Exact passage metadata recovery without passage prose."""
import copy
import json
import sys
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'server'))
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server import mcp
from recall_server.authorization import allowed_tools
from evals import candidate_capture as capture
from evals.boundary_identity import native_family_id
from evals.systems_card.mcp_client import CallOutcome
from test_candidate_capture import mounted, LocalClient, SOURCE, SID, RECEIPT, digest


def arguments(hit):
    return {k: hit[k] for k in ('source_id', 'logical_document_id', 'revision', 'manifest_content_sha256')} | {
        'passage_ids': [p['passage_id'] for p in hit['matching_ranges'][:2]]}


class MetadataStore:
    semantic_runtime = None

    def __init__(self, hit):
        self.hit, self.calls, self.live = hit, [], True
        self.rows = [dict(passage_id=p['passage_id'], ordinal=i, policy_fingerprint='b'*64,
                          text_sha256=digest(t.encode()), spans=p['spans'], receipts=p['receipts'])
                     for i, (p,t) in enumerate(zip(hit['matching_ranges'], ['Café 🧠 shipped.', 'Next action.']))]

    @contextmanager
    def connect(self):
        yield self

    def _execute_bounded(self, connection, sql, values, deadline_at):
        self.calls.append((sql, values, deadline_at))
        tenant, source, doc, rev, manifest, ids = values
        valid = (tenant == 'tenant-test' and source == self.hit['source_id'] and doc == self.hit['logical_document_id']
                 and rev == self.hit['revision'] and manifest == self.hit['manifest_content_sha256'] and self.live)
        rows = [copy.deepcopy(r) for r in self.rows if valid and r['passage_id'] in ids]
        return type('Rows', (), {'fetchall': lambda self: rows})()

    def bound(self, **kwargs):
        return BoundCanonicalRetrieval(self, tenant_id=kwargs.get('tenant_id', 'tenant-test'),
            principal_id='reviewer', authorized_sources=kwargs.get('authorized_sources', (SOURCE,)))


class MetadataClient(LocalClient):
    def __init__(self, root, store, mutation=None):
        super().__init__(root)
        self.store, self.mutation = store, mutation

    def call_tool(self, name, args, **kwargs):
        if name != 'recall_passage_metadata':
            return super().call_tool(name, args, **kwargs)
        self.calls.append((name, args, kwargs))
        result = mcp._call_tool(self.store.bound(), {}, name, args)
        if self.mutation:
            self.mutation(result)
        return CallOutcome(name, True, 1, result=result)


class PassageMetadataTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name); self.mount = self.root/'mount'; self.mount.mkdir()
        self.hit = mounted(self.mount); self.store = MetadataStore(self.hit)

    def pages(self):
        pages, cursor = [], None
        for _ in range(100):
            page=self.store.bound().passage_metadata(**arguments(self.hit),cursor=cursor)
            pages.append(page)
            self.assertLessEqual(mcp._encoded_result_size(page),16384)
            self.assertNotIn('text',page['passage']);self.assertNotIn('opened_receipts',page)
            cursor=page['next_cursor']
            if page['complete']:
                self.assertIsNone(cursor);return pages
        self.fail('pagination did not terminate')

    def test_exact_order_and_authorized_metadata_query(self):
        pages=self.pages()
        self.assertEqual([p['passage']['passage_id'] for p in pages],arguments(self.hit)['passage_ids'])
        self.assertEqual(pages[0]['passage']['receipts'],[RECEIPT])
        self.assertEqual(pages[0]['passage']['spans'],self.hit['matching_ranges'][0]['spans'])
        sql,values,deadline=self.store.calls[0]
        self.assertNotIn('text_redacted',sql)
        for part in ['canonical_passages','canonical_passage_documents','canonical_evidence_documents',
                     'canonical_chunks','canonical_documents','canonical_events','is_current',
                     'deleted_at IS NULL','is_tombstone','manifest_content_sha256','tenant_id=%s','source_id=%s']:
            self.assertIn(part,sql)
        self.assertEqual(values[:2],('tenant-test',SOURCE));self.assertIsInstance(deadline,float)

    def test_paging_lossless_and_cursor_bound_to_selection_and_metadata(self):
        self.store.rows[0]['spans']*=180
        self.store.rows[0]['receipts']=[f'recall://{SOURCE}/event-{i}?rev=1#item=0' for i in range(140)]
        pages=self.pages();self.assertGreater(len(pages),2)
        first=[p['passage'] for p in pages if p['passage']['passage_id']==self.store.rows[0]['passage_id']]
        for key in ['spans','receipts']:
            self.assertEqual([x for p in first for x in p[key]],self.store.rows[0][key])
        args=arguments(self.hit);args['passage_ids'].reverse()
        with self.assertRaises(ValueError):self.store.bound().passage_metadata(**args,cursor=pages[0]['next_cursor'])
        self.store.rows[0]['text_sha256']='c'*64
        with self.assertRaises(ValueError):self.store.bound().passage_metadata(**arguments(self.hit),cursor=pages[0]['next_cursor'])

    def test_pins_scope_missing_deleted_invalid_fail_closed(self):
        for field,value in [('source_id','claude:other:host'),('logical_document_id','ldoc_'+'f'*32),
                            ('revision',999),('manifest_content_sha256','f'*64),('passage_ids',['psg_'+'f'*32]),
                            ('passage_ids',[]),('passage_ids',['broken']),('passage_ids',arguments(self.hit)['passage_ids']*2)]:
            with self.subTest(field=field,value=value):
                args=arguments(self.hit);args[field]=value
                with self.assertRaises(ValueError):self.store.bound().passage_metadata(**args)
        for bound in [self.store.bound(tenant_id='other'),self.store.bound(authorized_sources=())]:
            with self.assertRaises(ValueError):bound.passage_metadata(**arguments(self.hit))
        self.store.live=False
        with self.assertRaises(ValueError):self.store.bound().passage_metadata(**arguments(self.hit))
        self.store.live=True;self.store.rows.pop()
        with self.assertRaises(ValueError):self.store.bound().passage_metadata(**arguments(self.hit))

    def test_mcp_read_policy_closed_schema(self):
        principal=dict(credential_kind='mcp',tenant_id='tenant-test',principal_kind='human',role='member',scopes=['read'],audience='recall-mcp')
        self.assertIn('recall_passage_metadata',allowed_tools(principal))
        self.assertNotIn('recall_passage_metadata',allowed_tools({**principal,'scopes':[]}))
        self.assertNotIn('recall_passage_metadata',{t['name'] for t in mcp._tools_for({})})
        result=mcp._call_tool(self.store.bound(),principal,'recall_passage_metadata',arguments(self.hit))
        self.assertEqual(result['passage']['passage_id'],self.store.rows[0]['passage_id'])
        with self.assertRaises(mcp.McpProtocolError):mcp._call_tool(self.store.bound(),principal,'recall_passage_metadata',{**arguments(self.hit),'include_text':True})
        for denied in [{}, {**principal, 'scopes': []}]:
            with self.assertRaises(mcp.McpProtocolError):
                mcp.dispatch(self.store.bound(), denied, {
                    'jsonrpc': '2.0', 'id': 1, 'method': 'tools/call',
                    'params': {'name': 'recall_passage_metadata', 'arguments': arguments(self.hit)},
                }, protocol_version='2025-11-25')

    def missing(self):
        hit=copy.deepcopy(self.hit)
        for p in hit['matching_ranges']:p.update(spans=[],spans_omitted=True,receipts_truncated=1)
        return hit

    def test_capture_recovery_preserves_input_and_checks_authoritative_hash(self):
        hit=self.missing();before=copy.deepcopy(hit);client=MetadataClient(self.mount,self.store)
        cap=capture.CandidateCapture(self.root/'capture',client_factory=lambda:client,protected_families=set())
        row=cap.capture_case('case','What shipped?',{'results':[hit]})['candidates'][0]
        self.assertTrue(row['complete_selected_passages']);self.assertEqual(hit,before)
        self.assertEqual(row['text'],'Café 🧠 shipped.\nNext action.')
        frozen=json.loads((cap.output/'case-000-search.json').read_text())
        self.assertTrue(frozen['slots'][0]['selected_passages'][0]['spans_omitted'])
        self.assertEqual([c[0] for c in client.calls],['recall_passage_metadata']*2+['recall_exec']*2)
        self.store.rows[0]['text_sha256']='f'*64
        other=capture.CandidateCapture(self.root/'wronghash',client_factory=lambda:client,protected_families=set())
        failed=other.capture_case('case','What shipped?',{'results':[hit]})['candidates'][0]
        self.assertFalse(failed['complete_selected_passages']);self.assertEqual(failed['error'],'capture_passage_hash_mismatch')

    def test_protected_stays_prose_free_after_metadata_recovery(self):
        client=MetadataClient(self.mount,self.store)
        cap=capture.CandidateCapture(self.root/'capture',client_factory=lambda:client,protected_families={native_family_id('claude-parent',SID)})
        row=cap.capture_case('case','What shipped?',{'results':[self.missing()]})['candidates'][0]
        self.assertEqual(row['capture_status'],'withheld_protected')
        self.assertEqual([c[0] for c in client.calls],['recall_passage_metadata']*2+['recall_exec'])
        self.assertNotIn('Café',''.join(p.read_text() for p in cap.output.glob('*.json')))

    def test_bad_recovery_budget_keep_slots_no_prose(self):
        for mode in ['budget','bad_pin','bad_cursor','extra_text']:
            def mutate(page):
                if mode=='bad_pin':page['revision']=99
                if mode=='bad_cursor':page['next_cursor']='bad'
                if mode=='extra_text':page['text']='must never persist'
            client=MetadataClient(self.mount,self.store,mutate)
            cap=capture.CandidateCapture(self.root/mode,client_factory=lambda:client,protected_families=set(),max_calls=1 if mode=='budget' else 10)
            rows=cap.capture_case('case','What shipped?',{'results':[self.missing(),self.missing()]})['candidates']
            self.assertEqual([r['candidate_index'] for r in rows],[0,1])
            self.assertTrue(all(not r['complete_selected_passages'] for r in rows))
            self.assertTrue(all(c[0]=='recall_passage_metadata' for c in client.calls))
            self.assertNotIn('must never persist',''.join(p.read_text() for p in cap.output.glob('*.json')))

    def test_metadata_timeout_and_manifest_advancement_never_substitute_text(self):
        class TimeoutClient:
            def call_tool(self, *args, **kwargs):
                raise TimeoutError('private upstream detail must not persist')
        cap=capture.CandidateCapture(self.root/'timeout',client_factory=TimeoutClient,protected_families=set())
        rows=cap.capture_case('case','What shipped?',{'results':[self.missing(),self.missing()]})['candidates']
        self.assertEqual(len(rows),2)
        self.assertTrue(all(not r['complete_selected_passages'] for r in rows))
        self.assertEqual(cap.calls,2)
        self.assertNotIn('private upstream detail',''.join(p.read_text() for p in cap.output.glob('*.json')))

        def advance(page):
            if page['complete']:
                manifest=self.mount/'manifest.json'
                manifest.write_bytes(manifest.read_bytes()+b' ')
        client=MetadataClient(self.mount,self.store,advance)
        cap=capture.CandidateCapture(self.root/'advanced',client_factory=lambda:client,protected_families=set())
        row=cap.capture_case('case','What shipped?',{'results':[self.missing()]})['candidates'][0]
        self.assertEqual(row['error'],'manifest_advanced_or_mismatch')
        self.assertNotIn('text',row)
