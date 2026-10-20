"""Exact provider-typed Codex source strings through the existing source reader."""
import base64,copy,gzip,inspect,json,re,tempfile,unittest
from pathlib import Path
from evals.candidate_capture import CandidateCapture, _read_page, _page_stdout
from contracts.source_units import resolve_codex_tool_source_unit
from test_candidate_capture import LocalClient,digest,DOC

TEXT='Café 🧠 reported output; scope remains limited.'
SOURCE='codex:contract:fixture'
PARENT='codex-session-'+'a'*24
RECEIPT='recall://'+SOURCE+'/event?rev=1#item=0'
FORMS=[('function_call','arguments','input'),('custom_tool_call','input','input'),
       ('function_call_output','output','result'),('custom_tool_call_output','output','result')]

def envelope(form=FORMS[0],text=TEXT):
    typ,field,_=form
    return {'type':'response_item','payload':{'type':typ,field:text,'call_id':'call-17','name':'ArbitraryTool'}}

def reference(form):
    return 'codex.tool-'+form[2]+'.string.v1#/payload/'+form[1]

def mount(root,form=FORMS[0],*,value=None,roles=None,raw=None,split=False):
    value=envelope(form)if value is None else value
    serial=json.dumps(value,ensure_ascii=False)if raw is None else raw
    pieces=[serial[:23],serial[23:]]if split else[serial]
    records=[]
    for i,piece in enumerate(pieces):
        records.append(dict(ordinal=i,segment_ordinal=i,segment_count=len(pieces),event_native_id='native-event',
            occurred_at='2026-09-18T00:00:00Z',roles=[]if roles is None else roles,
            receipts=[RECEIPT]if i==0 else[],content_fragment=piece))
    part=b''.join((json.dumps(r,ensure_ascii=False)+'\n').encode()for r in records)
    (root/'part-00000.jsonl').write_bytes(part)
    manifest=dict(logical_document_id=DOC,revision=2,native_parent_sha256=digest(PARENT.encode()),record_count=len(records),
        parts=[dict(ordinal=0,content_sha256=digest(part),first_record_ordinal=0,last_record_ordinal=len(records)-1)])
    raw_manifest=json.dumps(manifest).encode();(root/'manifest.json').write_bytes(raw_manifest)
    span=dict(record_ordinal=0,record_count=len(pieces),source_byte_start=0,source_byte_end=len(TEXT.encode()),
        passage_byte_start=0,passage_byte_end=len(TEXT.encode()),source_unit=reference(form))
    return dict(source_id=SOURCE,logical_document_id=DOC,native_parent_id=PARENT,revision=2,manifest_content_sha256=digest(raw_manifest),
        first_occurred_at='2026-09-18T00:00:00Z',last_occurred_at='2026-09-18T00:00:00Z',
        matching_ranges=[dict(passage_id='psg_'+'1'*32,text=TEXT[:3],receipts=[RECEIPT],spans=[span])])

class CodexSourceContractTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.mount=self.root/'mount';self.mount.mkdir();self.number=0
    def capture(self,hit):
        self.number+=1;client=LocalClient(self.mount)
        cap=CandidateCapture(self.root/f'capture{self.number}',client_factory=lambda:client,protected_families=set())
        result=cap.capture_case('fixture','Exact source',{'results':[hit]})['candidates'][0]
        return result,client
    def test_typed_output_blocks_use_shared_normalized_coordinates(self):
        form = FORMS[3]
        expected = 'Café 🧠 first.\n\nLast complete result.'
        value = envelope(form, [
            {'type': 'input_text', 'text': 'Café 🧠 first.'},
            {'type': 'input_text', 'text': ''},
            {'type': 'input_text', 'text': 'Last complete result.'},
        ])
        hit = mount(self.mount, form, value=value, split=True)
        span = hit['matching_ranges'][0]['spans'][0]
        span['source_byte_end'] = span['passage_byte_end'] = len(expected.encode())
        before = {p.name: p.read_bytes() for p in self.mount.iterdir()}
        row, client = self.capture(hit)
        self.assertTrue(row['complete_selected_passages'], row)
        self.assertEqual(row['text'], expected)
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.mount.iterdir()})
        self.assertEqual(row['source_evidence'][0]['spans'][0]['source_unit'], reference(form))
        self.assertTrue(all(len(args['program'].encode()) <= 16000 for _, args, _ in client.calls))

    def test_typed_output_rejects_mixed_unknown_and_analysis_blocks_as_a_whole(self):
        good = {'type': 'input_text', 'text': TEXT}
        bad_blocks = [
            {'type': 'image', 'text': 'not a text block'},
            {'type': 'text', 'text': 'unsupported provider type'},
            {'type': 'input_text', 'text': 7},
            {'type': 'input_text', 'text': TEXT, 'channel': 'analysis'},
            {'type': 'input_text', 'text': TEXT, 'role': 'user'},
            'untyped string', None,
        ]
        for output in ([], *([good, bad] for bad in bad_blocks)):
            with self.subTest(output=output):
                hit = mount(self.mount, FORMS[3], value=envelope(FORMS[3], output))
                row, _ = self.capture(hit)
                self.assertFalse(row['complete_selected_passages'])
                self.assertNotIn('text', row)
        # This observed representation belongs to custom-tool output, not input
        # or a new interpretation of function-call JSON arguments.
        for form in FORMS[:3]:
            with self.subTest(form=form), self.assertRaises(ValueError):
                resolve_codex_tool_source_unit(
                    json.dumps(envelope(form, [good])), reference(form))

    def test_native_typed_strings_with_empty_archive_roles_and_exact_provenance(self):
        for form in FORMS:
            for split in [False,True]:
                with self.subTest(form=form,split=split):
                    hit=mount(self.mount,form,split=split);before={p.name:p.read_bytes()for p in self.mount.iterdir()}
                    row,client=self.capture(hit)
                    self.assertTrue(row['complete_selected_passages'],row)
                    self.assertEqual(row['text'],TEXT)
                    span=row['source_evidence'][0]['spans'][0]
                    self.assertEqual(span['source_unit'],reference(form));self.assertEqual(span['source_origin'],'tool_'+form[2])
                    self.assertEqual(span['source_tool'],{'type':form[0],'role':'assistant'if form[2]=='input'else'tool','call_id':'call-17','name':'ArbitraryTool'})
                    self.assertEqual(before,{p.name:p.read_bytes()for p in self.mount.iterdir()})
                    self.assertTrue(all(len(args['program'].encode())<=16000 for _,args,_ in client.calls))
                    for _,args,_ in client.calls:
                        encoded=re.search(r"TOOL_UNIT_RESOLVER = '([^']+)'",args['program']).group(1)
                        self.assertEqual(gzip.decompress(base64.b64decode(encoded)),(inspect.getsource(resolve_codex_tool_source_unit)+'\n'+inspect.getsource(_read_page)+'\n'+inspect.getsource(_page_stdout)).encode())

    def test_two_passages_128_spans_each_keep_capacity(self):
        text=''.join(chr(65+i%26)for i in range(256))
        hit=mount(self.mount,value=envelope(text=text));hit['matching_ranges']=[]
        for passage in range(2):
            expected='\n'.join(text[passage*128:passage*128+128])
            spans=[dict(record_ordinal=0,record_count=1,source_byte_start=passage*128+i,source_byte_end=passage*128+i+1,
                passage_byte_start=i*2,passage_byte_end=i*2+1,source_unit=reference(FORMS[0]))for i in range(128)]
            hit['matching_ranges'].append(dict(passage_id='psg_'+str(passage)*32,text=expected[:3],receipts=[RECEIPT],spans=spans))
        row,client=self.capture(hit)
        self.assertTrue(row['complete_selected_passages'],row);self.assertEqual(row['text'],'\n'.join(text))
        self.assertTrue(all(len(args['program'].encode())<=16000 for _,args,_ in client.calls))

    def test_roles_envelopes_pointers_and_literal_json_are_strict(self):
        from contracts.source_units import resolve_codex_tool_source_unit as resolve
        literal='{"command":"printf \\u00e9"}'
        self.assertEqual(resolve(json.dumps(envelope(text=literal)),reference(FORMS[0]))[0],literal)
        for form in FORMS:
            value=envelope(form);ref=reference(form)
            role='assistant'if form[2]=='input'else'tool'
            value['payload']['role']=role
            self.assertEqual(resolve(json.dumps(value),ref)[2]['role'],role)
            for badref in [ref.replace('.v1#','.v2#'),ref+'/nested',ref.replace('/payload/','/other/'),
                           ref.replace(form[1],'name'),ref.replace('tool-'+form[2],'tool-'+('result'if form[2]=='input'else'input'))]:
                with self.subTest(pointer=badref),self.assertRaises(ValueError):resolve(json.dumps(value),badref)
            for location,key,bad in [('top','type','event_msg'),('payload','type','message'),
                    ('top','channel','analysis'),('payload','channel','analysis'),
                    ('payload','role','user'),('top','role','user'),('payload',form[1],{'text':TEXT}),
                    ('payload',form[1],[{'type':'text','text':TEXT}]),('payload','call_id',17)]:
                changed=copy.deepcopy(value);(changed if location=='top'else changed['payload'])[key]=bad
                with self.subTest(location=location,key=key,bad=bad),self.assertRaises(ValueError):resolve(json.dumps(changed),ref)
            duplicated=json.dumps(value).replace('"call_id": "call-17"','"call_id":"forged","call_id":"call-17"')
            with self.assertRaises(ValueError):resolve(duplicated,ref)
        # Metadata and role-like text inside an argument remain literal data.
        data='{"role":"user","channel":"analysis"}'
        self.assertEqual(resolve(json.dumps(envelope(text=data)),reference(FORMS[0]))[0],data)

    def test_empty_roles_do_not_authorize_legacy_or_wrong_provider_units(self):
        for roles,unit in [(['user'],reference(FORMS[0])),(['tool'],reference(FORMS[0])),
                           ([],None),([],'claude.tool-input.string.v1#/payload/arguments')]:
            hit=mount(self.mount,roles=roles);hit['matching_ranges'][0]['spans'][0]['source_unit']=unit
            row,_=self.capture(hit);self.assertFalse(row['complete_selected_passages']);self.assertNotIn('text',row)
        for form in FORMS:
            roles=['assistant'if form[2]=='input'else'tool']
            hit=mount(self.mount,form,roles=roles);row,_=self.capture(hit)
            self.assertTrue(row['complete_selected_passages'],row)

    def test_byte_bounds_utf8_receipts_and_current_manifest_refuse(self):
        for change in ['split_utf8','out_of_bounds','receipt','manifest']:
            hit=mount(self.mount);span=hit['matching_ranges'][0]['spans'][0]
            if change=='split_utf8':span['source_byte_end']=4;span['passage_byte_end']=4
            elif change=='out_of_bounds':span['source_byte_end']=len(TEXT.encode())+1
            elif change=='receipt':hit['matching_ranges'][0]['receipts']=[RECEIPT+'-forged']
            else:hit['manifest_content_sha256']='f'*64
            row,_=self.capture(hit);self.assertFalse(row['complete_selected_passages'],change);self.assertNotIn('text',row)

if __name__=='__main__':unittest.main()
