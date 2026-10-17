"""Codex provider strings remain literal, attributed and source-resolvable."""
import json
import unittest
from dataclasses import replace

from contracts.source_units import resolve_codex_tool_source_unit
from recall_server.logical_evidence import LogicalEvidenceRecord
from recall_server.passage_projection import PassagePolicy, build_passages

SOURCE = 'codex:publication:fixture'
RECEIPT = 'recall://' + SOURCE + '/record-one?rev=1#item=0'


class CodexToolPassageProjectionTests(unittest.TestCase):
    def test_first_projection_default_and_explicit_old_requests(self):
        from recall_server.passage_index import _document_extraction_version, PassageCandidate
        self.assertEqual(_document_extraction_version(SOURCE,None),'codex_tool_groups_v1')
        self.assertIn('codex_tool_groups_v1',PassageCandidate.supported_extraction_versions)
        for version in ('legacy','tool_groups_v1','codex_tool_groups_v1'):
            self.assertEqual(_document_extraction_version(SOURCE,version),version)
        self.assertEqual(_document_extraction_version('claude:fixture',None),'tool_groups_v1')
        self.assertEqual(_document_extraction_version('slack:fixture',None),'legacy')

    def records(self, event, *, roles=(), segmented=False):
        raw = json.dumps(event, ensure_ascii=False)
        chunks = (raw[:50], raw[50:]) if segmented else (raw,)
        return tuple(LogicalEvidenceRecord(ordinal=index, event_native_id='record-one',
            event_kind='codex.response_item', occurred_at='2026-10-17T00:00:00Z',
            roles=roles, receipts=(RECEIPT,) if index==0 else (), text=chunk,
            segment_ordinal=index, segment_count=len(chunks)) for index,chunk in enumerate(chunks))

    def test_four_native_forms_preserve_literal_text_roles_and_exact_coordinates(self):
        from recall_server.tool_passage_projection import extract_codex_tool_event_groups
        for form, field, kind, role in [('function_call','arguments','input','assistant'),
                ('custom_tool_call','input','input','assistant'),
                ('function_call_output','output','result','tool'),
                ('custom_tool_call_output','output','result','tool')]:
            with self.subTest(form=form):
                text = '{"report":"Café checks failed; deployment untested.","options":["Approve"]}'
                event = dict(type='response_item',payload=dict(type=form,call_id='call-one',name='UnknownTool',**{field:text}))
                records = self.records(event, segmented=True)
                groups, coverage = extract_codex_tool_event_groups(records)
                self.assertEqual(len(groups),1)
                group = groups[0]
                self.assertEqual(group.full_event_text,''.join(record.text for record in records))
                self.assertEqual(group.provider_role,role)
                self.assertEqual(group.evidence_kind,'tool_'+kind)
                self.assertEqual(group.tool_call_id,'call-one')
                self.assertEqual(len(group.messages),1)
                self.assertEqual(group.messages[0].text,text)
                self.assertEqual(group.messages[0].roles,(role,))
                self.assertEqual(group.messages[0].origin['source_tool']['name'],'UnknownTool')
                passages = build_passages(tenant_id='tenant:fixture',source_id=SOURCE,
                    logical_document_id='ldoc_'+'a'*32,revision=1,messages=group.messages,policy=PassagePolicy(4,1))
                self.assertGreater(len(passages),1)
                for passage in passages:
                    self.assertEqual(passage.roles,(role,))
                    for span in passage.spans:
                        leaf,origin,provenance = resolve_codex_tool_source_unit(group.full_event_text,span.source_unit)
                        self.assertEqual(leaf.encode()[span.source_byte_start:span.source_byte_end],
                            passage.text.encode()[span.passage_byte_start:span.passage_byte_end])
                        self.assertEqual(span.record_count,2)
                        self.assertEqual(span.origin['source_tool'],provenance)
                self.assertEqual(coverage['included_string_leaves'],1)
                self.assertEqual(coverage['dispositions'],{})

    def test_unsupported_and_empty_items_stay_raw_with_explicit_dispositions(self):
        from recall_server.tool_passage_projection import extract_codex_tool_event_groups
        cases = [
            (dict(type='response_item',payload=dict(type='function_call_output',output={'text':'nested'})),(), 'source_unit_refused'),
            (dict(type='response_item',payload=dict(type='function_call_output',output='failed',role='assistant')),(), 'source_unit_refused'),
            (dict(type='response_item',channel='analysis',payload=dict(type='function_call',arguments='secret')),(), 'source_unit_refused'),
            (dict(type='response_item',payload=dict(type='function_call',arguments='request')),('user',), 'provider_role_or_receipts_mismatch'),
            (dict(type='response_item',payload=dict(type='function_call',arguments='')),(), 'empty_string'),
            (dict(type='response_item',payload=dict(type='function_call',arguments=' \n')),(), 'no_token_content'),
            (dict(type='response_item',payload=dict(type='future_tool',output='unknown')),(), 'unsupported_provider_event'),
        ]
        for event,roles,reason in cases:
            with self.subTest(reason=reason,event=event):
                records=self.records(event,roles=roles)
                before=tuple(records)
                groups,coverage=extract_codex_tool_event_groups(records)
                self.assertEqual(groups,())
                self.assertEqual(coverage['dispositions'],{reason:1})
                self.assertEqual(records,before)

    def test_typed_attribution_never_turns_options_into_approval(self):
        from recall_server.tool_passage_projection import extract_codex_tool_event_groups,tool_evidence_context
        event=dict(type='response_item',payload=dict(type='custom_tool_call',input='Options: Approve deployment; Hold off'))
        groups,_=extract_codex_tool_event_groups(self.records(event))
        passages=build_passages(tenant_id='tenant:fixture',source_id=SOURCE,
            logical_document_id='ldoc_'+'a'*32,revision=1,messages=groups[0].messages,policy=PassagePolicy(100,10))
        self.assertIn('does not prove execution, selection, or human approval',tool_evidence_context(passages[0].spans))
        self.assertEqual(groups[0].messages[0].origin['semantic_role'],'unknown')

    def test_duplicate_keys_are_raw_only_without_deleting_original_bytes(self):
        from recall_server.tool_passage_projection import extract_codex_tool_event_groups
        raw='{"type":"response_item","payload":{"type":"function_call_output","output":"first","output":"second"}}'
        record=replace(self.records({})[0],text=raw)
        groups,coverage=extract_codex_tool_event_groups((record,))
        self.assertEqual(groups,())
        self.assertEqual(coverage['dispositions'],{'invalid_json':1})
        self.assertEqual(record.text,raw)


if __name__ == '__main__':
    unittest.main()
