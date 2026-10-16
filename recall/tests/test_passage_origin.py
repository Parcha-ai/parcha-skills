"""Preserve native copied-message identity without inferring it from text."""
import hashlib
import json
import unittest
from recall_server.logical_evidence import LogicalEvidenceRecord
from recall_server.passage_projection import PassagePolicy, build_passages, canonical_spans_json, visible_messages


def record(text='The prior deployment completed.', *, message_id='message-original', turn_id='turn-original', ordinal=0, role='user'):
    payload = dict(type='message', role=role, id=message_id,
        content=[dict(type='input_text', text=text)],
        internal_chat_message_metadata_passthrough=dict(turn_id=turn_id))
    return LogicalEvidenceRecord(ordinal=ordinal, event_native_id=f'native:{ordinal}',
        event_kind='response_item', occurred_at='2026-10-01T12:00:00Z', roles=(role,),
        receipts=(f'recall://codex:test/event-{ordinal}?rev=1#item=0',),
        segment_ordinal=0, segment_count=1,
        text=json.dumps(dict(type='response_item',payload=payload)))


def passages(records):
    return build_passages(tenant_id='tenant:test',source_id='codex:test',
        logical_document_id='ldoc_'+'a'*32,revision=1,messages=visible_messages(records),
        policy=PassagePolicy(target_tokens=8,overlap_tokens=2))


class OriginProjection(unittest.TestCase):
    def test_typed_native_origin_survives_projection_with_exact_message_hash(self):
        source=record()
        before=source.encode(source_id='codex:test')
        built=passages((source,))
        span=json.loads(canonical_spans_json(built[0].spans))[0]
        self.assertEqual(span.get('origin'),dict(message_id='message-original',turn_id='turn-original',role='user',
            text_sha256=hashlib.sha256(b'The prior deployment completed.').hexdigest()))
        self.assertEqual(source.encode(source_id='codex:test'),before)
        self.assertEqual(built[0].text,'The prior deployment completed.')

class OriginFolding(unittest.TestCase):
    def test_copied_ranges_keep_all_pointers_and_the_unique_continuation(self):
        from recall_server import passage_retrieval as retrieval
        source=record();built=passages((source,))[0]
        spans=json.loads(canonical_spans_json(built.spans))
        def row(parent,score,unique=False):
            ranges=[dict(passage_id='psg-'+parent,text=built.text,receipts=[f'recall://codex:test/{parent}?rev=1#item=0'],
                rerank_score=score,_origin_spans=spans,_origin_text_sha256=built.text_sha256)]
            if unique:ranges.append(dict(passage_id='continuation',text='Unique continuation',receipts=['recall://codex:test/new?rev=1#item=0'],rerank_score=.3))
            return dict(source_id='codex:test',logical_document_id=parent,native_parent_id=parent,
                matching_ranges=ranges,rerank_score=score,rank=score)
        result,diagnostics=retrieval.fold_copied_evidence([row('original',.9),row('fork',.8,True),row('copy',.7)])
        self.assertEqual([r['logical_document_id']for r in result],['original','fork'])
        self.assertEqual([r['passage_id']for r in result[1]['matching_ranges']],['continuation'])
        self.assertEqual(result[1]['rerank_score'],.3)
        copies=result[0]['matching_ranges'][0]['copied_evidence']
        self.assertEqual({p['logical_document_id']for p in copies},{'fork','copy'})
        self.assertEqual({r for p in copies for r in p['receipts']},{'recall://codex:test/fork?rev=1#item=0','recall://codex:test/copy?rev=1#item=0'})
        self.assertEqual(diagnostics['copied_ranges_folded'],2)
        self.assertFalse(any(k.startswith('_origin')for r in result for x in r['matching_ranges']for k in x))

class OriginCompatibility(unittest.TestCase):
    def test_same_identity_and_text_with_different_roles_stay_distinct(self):
        from recall_server.passage_retrieval import fold_copied_evidence
        rows=[]
        for role in ('user','assistant'):
            p=passages((record(role=role),))[0]
            rows.append(dict(source_id='codex:test',logical_document_id=role,matching_ranges=[dict(passage_id=role,
                text=p.text,receipts=list(p.receipts),_origin_spans=json.loads(canonical_spans_json(p.spans)),_origin_text_sha256=p.text_sha256)]))
        out,d=fold_copied_evidence(rows);self.assertEqual(len(out),2);self.assertEqual(d['copied_ranges_folded'],0)

    def test_origin_annotation_preserves_existing_passage_identity(self):
        built=passages((record(),))[0]
        self.assertEqual(built.passage_id,'psg_80675167c4bf964ecacc02da1376a8ef')
        self.assertIn('origin',json.loads(canonical_spans_json(built.spans))[0])

    def test_missing_origin_keeps_literal_prechange_passage_id(self):
        built=passages((record(message_id=None),))[0]
        self.assertEqual(built.passage_id,'psg_80675167c4bf964ecacc02da1376a8ef')
        self.assertNotIn('origin',json.loads(canonical_spans_json(built.spans))[0])

    def test_independent_equal_text_and_edited_origin_are_distinct(self):
        from recall_server.passage_retrieval import fold_copied_evidence
        rows=[]
        for i,source in enumerate((record(),record(message_id='independent'),record(text='Edited source report.'))):
            p=passages((source,))[0]
            rows.append(dict(source_id='codex:test',logical_document_id=str(i),matching_ranges=[dict(
                passage_id=p.passage_id,text=p.text,receipts=list(p.receipts),
                _origin_spans=json.loads(canonical_spans_json(p.spans)),_origin_text_sha256=p.text_sha256)]))
        folded,diagnostics=fold_copied_evidence(rows)
        self.assertEqual(len(folded),3);self.assertEqual(diagnostics['copied_ranges_folded'],0)

    def test_untrusted_vendor_metadata_and_changed_visible_bytes_cannot_fold(self):
        from recall_server.passage_retrieval import fold_copied_evidence
        p=passages((record(),))[0];span=json.loads(canonical_spans_json(p.spans))
        for altered in ('missing','body','malformed','role','source'):
            with self.subTest(altered=altered):
                import copy
                first=dict(source_id='codex:test',logical_document_id='one',matching_ranges=[dict(passage_id='one',text=p.text,receipts=['first'],spans=span,_origin_spans=span,_origin_text_sha256=p.text_sha256)])
                second=copy.deepcopy(first);second['logical_document_id']='two';item=second['matching_ranges'][0];item['passage_id']='two'
                if altered=='missing':item.pop('_origin_spans')
                if altered=='body':item['text']='Changed after hydration'
                if altered=='malformed':item['_origin_spans'][0]['origin']['turn_id']=None
                if altered=='role':item['_origin_spans'][0]['origin']['role']=[]
                if altered=='source':second['source_id']='codex:other'
                out,d=fold_copied_evidence([first,second]);self.assertEqual(len(out),2);self.assertEqual(d['copied_ranges_folded'],0)


class CopiedPointerTransport(unittest.TestCase):
    def test_existing_response_fitter_keeps_every_alternate_parent_pointer(self):
        from recall_server.mcp import fit_search_result, _tool_result, bound_response
        copies=[dict(source_id='codex:test',logical_document_id=f'parent-{i}',passage_id=f'passage-{i}',
            receipts=[f'recall://codex:test/{i}-'+('a'*100)+f'-{n}?rev=1#item=0'for n in range(400)])for i in range(30)]
        value=dict(results=[dict(logical_document_id='primary',matching_ranges=[dict(text='source evidence',receipts=['primary-receipt'],copied_evidence=copies)])])
        fitted=fit_search_result(value)
        response=bound_response(dict(jsonrpc='2.0',id=1,result=_tool_result(fitted)),1)
        self.assertIn('result',response)
        actual=fitted['results'][0]['matching_ranges'][0]['copied_evidence']
        self.assertEqual([(c['logical_document_id'],c['passage_id'])for c in actual],[(c['logical_document_id'],c['passage_id'])for c in copies])
        self.assertTrue(all(len(c['receipts'])+c['receipts_truncated']==400 for c in actual))
        self.assertTrue(all(len(c['receipts'])==400 for c in copies),'fitting mutated the authoritative result')


if __name__=='__main__':unittest.main()
