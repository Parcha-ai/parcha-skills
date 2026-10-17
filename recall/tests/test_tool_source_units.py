"""Independent source fixtures exercise the existing mounted reader/authority.

Expected pointers and bytes are authored here, not obtained from the resolver.
No production extractor creates these spans; they are contract fixtures only.
"""
import copy
import base64
import gzip
import inspect
import json
import re
import tempfile
import unittest
from pathlib import Path

from contracts.source_units import resolve_claude_tool_source_unit as resolve
from evals import candidate_capture as capture
from test_candidate_capture import LocalClient, SOURCE, SID, RECEIPT, DOC, PARENT, digest, mounted
from test_passage_metadata import MetadataStore, MetadataClient, arguments

INPUT = 'claude.tool-input.string.v1#/message/content/0/input/message'
RESULT = 'claude.tool-result.string.v1#/message/content/0/content'
TEXT = 'Café 🧠 was observed.\nOnly two supplied keys were checked.'


def envelope(kind='input', value=TEXT):
    role = 'assistant' if kind == 'input' else 'user'
    block = ({'type': 'tool_use', 'name': 'ArbitraryUnrecognizedTool', 'input': {'message': value}}
             if kind == 'input' else {'type': 'tool_result', 'content': value})
    return {'type': role, 'sessionId': SID, 'message': {'role': role, 'content': [block]}}


def tool_mount(root, *, kind='input', content=None, raw=None, split=False, ref=None, expected=TEXT):
    value = envelope(kind) if content is None else content
    serialized = json.dumps(value, ensure_ascii=False, separators=(',', ':')) if raw is None else raw
    pieces = [serialized[:17], serialized[17:]] if split else [serialized]
    records = []
    for index, piece in enumerate(pieces):
        record = dict(ordinal=index, segment_ordinal=index, segment_count=len(pieces),
                      event_native_id='a' * 24 + '-000001', occurred_at='2026-09-18T00:00:00Z',
                      roles=['assistant' if kind == 'input' else 'user'],
                      receipts=[RECEIPT] if index == 0 else [])
        # Raw malformed and segmented fixtures remain literal fragments so JSON
        # parsing cannot normalize away duplicate source keys in the fixture.
        record['content_fragment' if split or raw is not None else 'content'] = piece if split or raw is not None else value
        records.append(record)
    part = b''.join((json.dumps(row, ensure_ascii=False) + '\n').encode() for row in records)
    (root / 'part-00000.jsonl').write_bytes(part)
    manifest = dict(logical_document_id=DOC, revision=2, native_parent_sha256=digest(PARENT.encode()),
                    record_count=len(records), parts=[dict(ordinal=0, content_sha256=digest(part),
                    first_record_ordinal=0, last_record_ordinal=len(records)-1)])
    raw_manifest = json.dumps(manifest).encode(); (root / 'manifest.json').write_bytes(raw_manifest)
    span = dict(record_ordinal=0, record_count=len(records), source_byte_start=0,
                source_byte_end=len(expected.encode()), passage_byte_start=0, passage_byte_end=len(expected.encode()),
                source_unit=ref or (INPUT if kind == 'input' else RESULT))
    return dict(source_id=SOURCE, logical_document_id=DOC, native_parent_id=PARENT, revision=2,
                manifest_content_sha256=digest(raw_manifest), first_occurred_at='2026-09-18T00:00:00Z',
                last_occurred_at='2026-09-18T00:00:00Z', matching_ranges=[dict(passage_id='psg_'+'1'*32,
                text=expected[:3], receipts=[RECEIPT], spans=[span])])


class ToolUnitContractTest(unittest.TestCase):
    def test_declared_inputs_and_results_share_exact_leaf_contract(self):
        for kind, ref in [('input', INPUT), ('result', RESULT)]:
            value = envelope(kind)
            got = resolve(json.dumps(value), ref)
            self.assertEqual(got, (TEXT, 'tool_'+kind))
            self.assertEqual(value, envelope(kind))
        # A tool name changes neither source coordinates nor their meaning.
        value = envelope(); value['message']['content'][0]['name'] = 'SendMessage'
        self.assertEqual(resolve(json.dumps(value), INPUT), (TEXT,'tool_input'))

    def test_escaped_unicode_empty_keys_and_identical_leaves_keep_identity(self):
        value = envelope(); value['message']['content'][0]['input'] = {'a/b~🙂': [TEXT, TEXT], '': TEXT}
        base = 'claude.tool-input.string.v1#/message/content/0/input/'
        for pointer in [base,base+'a~1b~0🙂/0',base+'a~1b~0🙂/1']:
            self.assertEqual(resolve(json.dumps(value), pointer),(TEXT,'tool_input'))

    def test_json_looking_string_is_literal_not_a_second_document(self):
        literal = '{"message":"quoted \\u00e9"}'
        self.assertEqual(resolve(json.dumps(envelope(value=literal)),INPUT),(literal,'tool_input'))
        with self.assertRaises(ValueError):resolve(json.dumps(envelope(value=literal)),INPUT+'/message')
        value = envelope(value={'message': 'quoted é'})
        self.assertEqual(resolve(json.dumps(value),INPUT+'/message'),('quoted é','tool_input'))
        # Keys inside a tool argument are ordinary data, not provider roles.
        value = envelope(value={'type':'image','channel':'analysis','text':'literal argument'})
        for key,text in [('type','image'),('channel','analysis'),('text','literal argument')]:
            self.assertEqual(resolve(json.dumps(value),INPUT+'/'+key),(text,'tool_input'))

    def test_duplicate_keys_unknown_envelopes_roles_and_hidden_blocks(self):
        duplicate = json.dumps(envelope()).replace('"message": "Caf', '"message": "first", "message": "Caf')
        bad = [duplicate, '[]', '{', json.dumps({'type':'response_item','payload':{'type':'function_call','arguments':'text'}})]
        for mutate in [lambda x:x.update(type='user'), lambda x:x['message'].update(role='analysis'),
                       lambda x:x['message'].update(channel='analysis')]:
            value=envelope();mutate(value);bad.append(json.dumps(value))
        for text in bad:
            with self.subTest(text=text[:25]), self.assertRaisesRegex(ValueError, '^tool_source_unit_invalid$'):
                resolve(text,INPUT)
        value=envelope();value['message']['content'][0]['type']='text'
        with self.assertRaises(ValueError):resolve(json.dumps(value),INPUT)
        for kind in ['thinking','reasoning','redacted_thinking','image','audio','document']:
            value=envelope('result', [{'type':kind,'text':'hidden','data':'encoded'}])
            with self.assertRaises(ValueError):resolve(json.dumps(value),RESULT+'/0/text')

    def test_unrelated_leaf_count_long_pointer_and_depth_do_not_limit_resolution(self):
        value=envelope(value=['x']*300)
        self.assertEqual(resolve(json.dumps(value),INPUT+'/299'),('x','tool_input'))
        value=envelope(value={'x'*2101:'data'})
        self.assertEqual(resolve(json.dumps(value),INPUT+'/'+'x'*2101),('data','tool_input'))
        nested='deep'
        for _ in range(40):nested={'child':nested}
        self.assertEqual(resolve(json.dumps(envelope(value=nested)),INPUT+'/child'*40),('deep','tool_input'))

    def test_provider_text_block_excludes_metadata_but_preserves_literal_business_json(self):
        business='{"nested":{"type":"document","title":"business data"}}'
        value=envelope('result',[{'type':'text','text':business,'id':'not-text'},
                                 {'type':'image','source':{'data':'not-text'}}])
        self.assertEqual(resolve(json.dumps(value),RESULT+'/0/text'),(business,'tool_result'))
        for tail in ['/0/type','/0/id','/0/text/nested/title','/1/source/data']:
            with self.assertRaises(ValueError):resolve(json.dumps(value),RESULT+tail)
        # Nonstandard bare object results have no declared provider text field.
        with self.assertRaises(ValueError):resolve(json.dumps(envelope('result',{'type':'document','title':'business'})),RESULT+'/title')


class ToolUnitCaptureTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.mount=self.root/'mount';self.mount.mkdir();self.number=0

    def run_capture(self, hit, *, client=None):
        self.number+=1;client=client or LocalClient(self.mount)
        cap=capture.CandidateCapture(self.root/f'capture-{self.number}',client_factory=lambda:client,protected_families=set())
        row=cap.capture_case('fixture','Read exact selected text',{'results':[hit]})['candidates'][0]
        return row,client

    def test_both_origins_exact_bytes_full_source_retained_and_program_bound(self):
        for kind,ref in [('input',INPUT),('result',RESULT)]:
            for split in [False,True]:
                with self.subTest(kind=kind,split=split):
                    hit=tool_mount(self.mount,kind=kind,split=split)
                    before={p.name:p.read_bytes() for p in self.mount.iterdir()}
                    row,client=self.run_capture(hit)
                    self.assertTrue(row['complete_selected_passages'],row)
                    self.assertEqual(row['text'],TEXT)
                    span=row['source_evidence'][0]['spans'][0]
                    self.assertEqual(span['source_unit'],ref);self.assertEqual(span['source_origin'],'tool_'+kind)
                    self.assertEqual(span['receipts'],[RECEIPT])
                    self.assertFalse(row['complete_document'])
                    self.assertEqual(before,{p.name:p.read_bytes() for p in self.mount.iterdir()})
                    self.assertTrue(all(len(args['program'].encode())<=16000 for _,args,_ in client.calls))
                    for _,args,_ in client.calls:
                        encoded=re.search(r"TOOL_UNIT_RESOLVER = '([^']+)'",args['program']).group(1)
                        self.assertEqual(gzip.decompress(base64.b64decode(encoded)),(inspect.getsource(resolve)+'\n'+inspect.getsource(capture._read_page)+'\n'+inspect.getsource(capture._page_stdout)).encode())

    def test_two_passages_maximum_supported_spans_preserve_program_bound(self):
        original=''.join(chr(65+i%26)for i in range(256))
        for new in [False,True]:
            with self.subTest(new_unit=new):
                hit=(tool_mount(self.mount,content=envelope(value=original),expected=original)
                     if new else mounted(self.mount,text=original))
                hit['matching_ranges']=[]
                for p in range(2):
                    spans=[dict(record_ordinal=0,record_count=1,source_byte_start=p*128+i,
                                source_byte_end=p*128+i+1,passage_byte_start=i*2,passage_byte_end=i*2+1,
                                **({'source_unit':INPUT}if new else {}))for i in range(128)]
                    text='\n'.join(original[p*128:p*128+128])
                    hit['matching_ranges'].append(dict(passage_id='psg_'+str(p)*32,text=text[:3],
                        spans=spans,receipts=[RECEIPT]))
                row,client=self.run_capture(hit)
                self.assertTrue(row['complete_selected_passages'],row)
                self.assertEqual(row['text'],'\n'.join(original))
                self.assertTrue(all(len(args['program'].encode())<=16000 for _,args,_ in client.calls))
                if not new:
                    self.assertTrue(all('TOOL_UNIT_RESOLVER =' not in args['program'] for _,args,_ in client.calls))

    def test_unknown_version_wrong_origin_forged_and_alias_pointers_fail(self):
        for ref in [INPUT.replace('.v1#','.v2#'), INPUT.replace('tool-input','tool-result'),
                    INPUT.replace('/input/message','/name'), INPUT.replace('/0/','/00/'),
                    INPUT.replace('/message/content/0/input/message','/sessionId'),
                    INPUT+'/message', INPUT.replace('/message','/~2message',1)]:
            with self.subTest(ref=ref):
                row,_=self.run_capture(tool_mount(self.mount,ref=ref))
                self.assertFalse(row['complete_selected_passages']);self.assertNotIn('text',row)
                self.assertEqual(row['error'],'tool_source_unit_invalid')

    def test_maximum_spans_with_distinct_ordinary_input_keys(self):
        values={f'field_{i:03d}':chr(65+i%26)for i in range(256)}
        content=envelope();content['message']['content'][0]['input']=values
        hit=tool_mount(self.mount,content=content)
        hit['matching_ranges']=[]
        for p in range(2):
            spans=[dict(record_ordinal=0,record_count=1,source_byte_start=0,
                        source_byte_end=1,passage_byte_start=i*2,passage_byte_end=i*2+1,
                        source_unit=f'claude.tool-input.string.v1#/message/content/0/input/field_{p*128+i:03d}')for i in range(128)]
            text='\n'.join(values[f'field_{p*128+i:03d}']for i in range(128))
            hit['matching_ranges'].append(dict(passage_id='psg_'+str(p)*32,text=text[:3],
                spans=spans,receipts=[RECEIPT]))
        row,client=self.run_capture(hit)
        self.assertTrue(row['complete_selected_passages'],row)
        self.assertEqual(row['text'],'\n'.join(values.values()))
        self.assertTrue(all(len(args['program'].encode())<=16000 for _,args,_ in client.calls))

    def test_wrong_provider_record_role_duplicate_keys_and_bounds_fail(self):
        duplicate=json.dumps(envelope()).replace('"message": "Caf','"message": "first", "message": "Caf')
        for change in ['duplicate','role','envelope','end','utf8','receipt']:
            with self.subTest(change=change):
                value=envelope()
                if change=='role':value['type']='user';value['message']['role']='user'
                if change=='envelope':value={'type':'response_item','payload':value}
                hit=tool_mount(self.mount,content=value,raw=duplicate if change=='duplicate' else None)
                span=hit['matching_ranges'][0]['spans'][0]
                if change=='end':span['source_byte_end']=9999
                if change=='utf8':span['source_byte_start']=4
                if change=='receipt':hit['matching_ranges'][0]['receipts']=['recall://'+SOURCE+'/wrong?rev=1#item=0']
                row,_=self.run_capture(hit)
                self.assertFalse(row['complete_selected_passages']);self.assertNotIn('text',row)

    def test_original_metadata_authority_pins_grants_and_forget(self):
        hit=tool_mount(self.mount);store=MetadataStore(hit)
        store.rows[0]['text_sha256']=digest(TEXT.encode())
        omitted=copy.deepcopy(hit);omitted['matching_ranges'][0].update(spans=[],spans_omitted=True)
        row,client=self.run_capture(omitted,client=MetadataClient(self.mount,store))
        self.assertTrue(row['complete_selected_passages'],row)
        self.assertEqual(row['text'],TEXT)
        self.assertEqual([call[0]for call in client.calls],['recall_passage_metadata','recall_exec','recall_exec'])
        with self.assertRaises(ValueError):store.bound(authorized_sources=()).passage_metadata(**arguments(hit))
        with self.assertRaises(ValueError):store.bound().passage_metadata(**dict(arguments(hit),revision=3))
        store.live=False
        with self.assertRaises(ValueError):store.bound().passage_metadata(**arguments(hit))
        for file in ['manifest.json','part-00000.jsonl']:
            saved=(self.mount/file).read_bytes();(self.mount/file).write_bytes(saved+b' ')
            failed,_=self.run_capture(hit)
            self.assertFalse(failed['complete_selected_passages']);self.assertNotIn('text',failed)
            (self.mount/file).write_bytes(saved)

    def test_unit_and_unused_origin_remain_inside_authoritative_metadata_hash(self):
        hit=tool_mount(self.mount);store=MetadataStore(hit)
        store.rows[0]['text_sha256']=digest(TEXT.encode())
        store.rows[0]['spans'][0]['origin']={'message_id':'fixture','role':'assistant'}
        omitted=copy.deepcopy(hit);omitted['matching_ranges'][0].update(spans=[],spans_omitted=True)
        good,_=self.run_capture(omitted,client=MetadataClient(self.mount,store))
        self.assertTrue(good['complete_selected_passages'],good)
        for field in ['source_unit','origin']:
            def mutate(page):
                page['passage']['spans'][0][field] = INPUT+'/forged' if field=='source_unit' else {'role':'user'}
            client=MetadataClient(self.mount,store,mutation=mutate)
            failed,_=self.run_capture(omitted,client=client)
            self.assertEqual(failed['error'],'capture_metadata_hash_mismatch')
            self.assertEqual([call[0]for call in client.calls],['recall_passage_metadata'])

    def test_origin_cannot_change_between_verified_reader_and_assembly(self):
        def mutate(spec,response):
            if spec['phase']=='text':
                rows=[json.loads(line)for line in response['stdout'].splitlines()]
                rows[0]['source_origin']='tool_result'
                response['stdout']='\n'.join(json.dumps(row)for row in rows)
        row,_=self.run_capture(tool_mount(self.mount),client=LocalClient(self.mount,mutate))
        self.assertFalse(row['complete_selected_passages']);self.assertNotIn('text',row)
        self.assertEqual(row['error'],'capture_fragment_identity')


if __name__=='__main__':unittest.main()
