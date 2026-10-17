"""Typed tool outputs keep their provider role through the ordinary builder."""
import json,unittest
from dataclasses import replace
from contracts.source_units import resolve_codex_tool_source_unit,codex_tool_source_unit_role
from recall_server.passage_projection import PassageMessage,PassagePolicy,build_passages

UNIT='codex.tool-result.string.v1#/payload/output'
SOURCE='codex:fixture:contract'
RECEIPT='recall://'+SOURCE+'/native-record?rev=1#item=0'

class CodexPassageRoleContractTests(unittest.TestCase):
    def test_literal_tool_result_keeps_tool_role_and_source_coordinates(self):
        message=PassageMessage(record_ordinal=7,occurred_at='2026-10-17T00:00:00Z',roles=('tool',),
            receipts=(RECEIPT,),text='Observed failure: the local test returned code 1.',source_unit=UNIT)
        passages=build_passages(tenant_id='tenant:fixture',source_id=SOURCE,logical_document_id='ldoc_'+'a'*32,
            revision=1,messages=(message,),policy=PassagePolicy(1024,128))
        self.assertEqual(len(passages),1)
        passage=passages[0]
        self.assertEqual(passage.roles,('tool',));self.assertEqual(passage.text,message.text)
        self.assertEqual(passage.spans[0].source_unit,UNIT)
        self.assertEqual(passage.text.encode()[passage.spans[0].passage_byte_start:passage.spans[0].passage_byte_end],message.text.encode())
        self.assertEqual(passage.receipts,(RECEIPT,))

    def message(self,unit=UNIT,roles=('tool',)):
        return PassageMessage(record_ordinal=0,occurred_at='2026-10-17T00:00:00Z',roles=roles,
            receipts=(RECEIPT,),text='Literal provider text.',source_unit=unit)

    def test_declared_role_agrees_with_shared_resolver_for_all_forms(self):
        for kind,field,origin,role in [('function_call','arguments','input','assistant'),
                ('custom_tool_call','input','input','assistant'),('function_call_output','output','result','tool'),
                ('custom_tool_call_output','output','result','tool')]:
            unit=f'codex.tool-{origin}.string.v1#/payload/{field}'
            resolved=resolve_codex_tool_source_unit(json.dumps({'type':'response_item','payload':{'type':kind,field:'Literal provider text.'}}),unit)
            self.assertEqual(codex_tool_source_unit_role(unit),role)
            self.assertEqual(resolved[2]['role'],role)
            self.message(unit,(role,)).validate()

    def test_tool_role_never_expands_ordinary_or_wrong_provider_messages(self):
        for unit in [None,'assistant_final.complete.v1','claude.tool-result.string.v1#/message/content/0/content',
                     'codex.tool-input.string.v1#/payload/arguments',UNIT.replace('.v1#','.v2#'),UNIT+'/nested']:
            with self.subTest(unit=unit),self.assertRaises(ValueError):self.message(unit).validate()
        for roles in [('user',),('assistant',),('system',),('user','tool'),('assistant','tool'),()]:
            with self.subTest(roles=roles),self.assertRaises(ValueError):self.message(roles=roles).validate()
        for role in ['user','assistant']:
            self.message(None,(role,)).validate()

    def test_typed_builder_requires_same_codex_source_and_receipts(self):
        for source,message in [('claude:fixture:contract',self.message()),
                               (SOURCE,replace(self.message(),receipts=('recall://codex:other:fixture/event?rev=1#item=0',)))]:
            with self.subTest(source=source),self.assertRaisesRegex(ValueError,'source does not match'):
                build_passages(tenant_id='tenant:fixture',source_id=source,logical_document_id='ldoc_'+'a'*32,
                    revision=1,messages=(message,),policy=PassagePolicy(1024,128))

if __name__=='__main__':unittest.main()
