"""Typed builder obligations for explicitly enrolled document extraction."""
import sys
import json
from dataclasses import replace
import unittest
from pathlib import Path

sys.path[:0] = [str(Path(__file__).resolve().parents[1]), str(Path(__file__).resolve().parents[1] / 'server')]
from recall_server.passage_projection import (  # noqa: E402
    COMPLETE_SOURCE_UNITS, PassageMessage, PassagePolicy, build_passages,
    reconstruct_passage,
)

UNIT = 'claude.tool-input.string.v1#/message/content/0/input/message'
RECEIPT = 'recall://claude:integration:fixture/event-1?rev=1#item=0'


class ToolPassageIntegrationTest(unittest.TestCase):
    def test_ordinary_windows_retain_unit_coordinates_without_report_marker(self):
        message = PassageMessage(
            record_ordinal=0, occurred_at='2026-10-17T00:00:00Z',
            roles=('assistant',), receipts=(RECEIPT,),
            text='Maya kept the restart control available. Cleanup remained scoped to previews.',
            source_unit=UNIT, origin={'role': 'assistant'},
        )
        messages = (message,)
        passages = build_passages(
            tenant_id='tenant:fixture', source_id='claude:integration:fixture',
            logical_document_id='ldoc_'+'a'*32, revision=1,
            messages=messages, policy=PassagePolicy(4, 1),
        )
        self.assertGreater(len(passages), 1)
        for passage in passages:
            self.assertEqual(reconstruct_passage(passage, messages), passage.text)
            for span in passage.spans:
                self.assertEqual(span.source_unit, UNIT)
                self.assertNotIn(span.source_unit, COMPLETE_SOURCE_UNITS)
                self.assertEqual(
                    message.text.encode()[span.source_byte_start:span.source_byte_end],
                    passage.text.encode()[span.passage_byte_start:span.passage_byte_end],
                )


class ToolEventGroupingTest(unittest.TestCase):
    def test_raw_only_dispositions_are_explicit_for_unreadable_or_unprojectable_fields(self):
        from recall_server.tool_passage_projection import extract_tool_event_groups
        event = dict(type='assistant', message=dict(role='assistant', content=[
            dict(type='tool_use', input={'empty': '', 'whitespace': ' \t\n', 'id': 2})]))
        records = self.records(event)
        groups, coverage = extract_tool_event_groups(records)
        self.assertEqual(groups, ())
        self.assertEqual(coverage['dispositions'], {'empty_string': 1, 'no_token_content': 1})
        self.assertEqual(coverage['string_leaves'], 2)
        duplicate = records[0].text.replace('"empty": ""', '"empty": "first", "empty": "second"')
        groups, coverage = extract_tool_event_groups((replace(records[0], text=duplicate),))
        self.assertEqual(groups, ())
        self.assertEqual(coverage['dispositions'], {'invalid_json': 1})
        wrong_role = (replace(records[0], roles=('user',)),)
        self.assertEqual(extract_tool_event_groups(wrong_role)[1]['dispositions'], {'provider_role_mismatch': 1})

    def records(self, event, *, segmented=False):
        from recall_server.logical_evidence import LogicalEvidenceRecord
        text=json.dumps(event,ensure_ascii=False)
        chunks=(text[:50],text[50:])if segmented else(text,)
        return tuple(LogicalEvidenceRecord(ordinal=i,event_native_id='event-1',
            event_kind='claude.message',occurred_at='2026-10-17T00:00:00Z',
            roles=(event['type'],),receipts=(RECEIPT,)if i==0 else(),
            text=chunk,segment_ordinal=i,segment_count=len(chunks))
            for i,chunk in enumerate(chunks))

    def groups(self,event,**kwargs):
        from recall_server.tool_passage_projection import tool_event_groups
        return tool_event_groups(self.records(event,**kwargs))

    def test_sibling_fields_pack_together_without_semantic_relabeling(self):
        event=dict(type='assistant',message=dict(role='assistant',content=[
            dict(type='tool_use',id='call-1',name='AnyProviderTool',input={
                'actions':[{'message':'I approve this decision.'},{'message':'Hold off.'}],
                'report':'Observed failure remains unresolved.', 'empty':''})]))
        group,=self.groups(event,segmented=True)
        self.assertEqual(group.provider_role,'assistant')
        self.assertEqual(group.evidence_kind,'tool_input')
        self.assertEqual(len(group.messages),3)
        self.assertEqual(json.loads(group.full_event_text),event)
        passages=build_passages(tenant_id='tenant:fixture',source_id='claude:integration:fixture',
            logical_document_id='ldoc_'+'a'*32,revision=1,messages=group.messages,policy=PassagePolicy(1024,128))
        self.assertEqual(len(passages),1)
        self.assertEqual(len(passages[0].spans),3)
        self.assertTrue(all(s.origin['semantic_role']=='unknown' and s.record_count==2 for s in passages[0].spans))
        self.assertTrue(all(m.roles==('assistant',)for m in group.messages))
        self.assertNotIn('complete', ' '.join(s.source_unit for s in passages[0].spans))
        with self.assertRaises(ValueError):
            reconstruct_passage(passages[0],(replace(group.messages[0],source_unit=UNIT),)+group.messages[1:])

    def test_results_are_observations_not_human_speech_and_skip_provider_metadata(self):
        event=dict(type='user',message=dict(role='user',content=[dict(type='tool_result',
            tool_use_id='call-1',content=[{'type':'text','text':'Checks failed: 2.'},
                                       {'type':'image','source':{'data':'not text'}}])]))
        group,=self.groups(event)
        self.assertEqual([m.text for m in group.messages],['Checks failed: 2.'])
        self.assertEqual(group.evidence_kind,'tool_result')
        self.assertEqual(group.provider_role,'user')
        self.assertEqual(group.messages[0].origin['semantic_role'],'unknown')
        self.assertEqual(group.tool_call_id,'call-1')

    def test_blocks_do_not_merge_and_no_tool_name_special_case(self):
        event=dict(type='assistant',message=dict(role='assistant',content=[
            dict(type='tool_use',id='a',name='UnknownTool',input={'message':'First task'}),
            dict(type='tool_use',id='b',name='UnknownTool',input={'message':'Unrelated second task'})]))
        groups=self.groups(event)
        self.assertEqual(len(groups),2)
        with self.assertRaises(ValueError):
            build_passages(tenant_id='tenant:fixture',source_id='claude:integration:fixture',
                logical_document_id='ldoc_'+'a'*32,revision=1,
                messages=groups[0].messages+groups[1].messages,policy=PassagePolicy(1024,128))

    def test_existing_128_span_reader_capacity_splits_without_dropping_siblings(self):
        from contracts.source_units import resolve_claude_tool_source_unit
        event=dict(type='assistant',message=dict(role='assistant',content=[
            dict(type='tool_use',id='many',name='UnknownTool',
                 input={f'field_{i:03d}':f'é{i}'for i in range(257)})]))
        group,=self.groups(event)
        passages=build_passages(tenant_id='tenant:fixture',source_id='claude:integration:fixture',
            logical_document_id='ldoc_'+'a'*32,revision=1,messages=group.messages,policy=PassagePolicy(1024,128))
        self.assertEqual([len(p.spans)for p in passages],[128,128,1])
        seen=set()
        for passage in passages:
            self.assertEqual(reconstruct_passage(passage,group.messages),passage.text)
            for span in passage.spans:
                text,kind=resolve_claude_tool_source_unit(group.full_event_text,span.source_unit)
                self.assertEqual(kind,'tool_input')
                self.assertEqual(text.encode()[span.source_byte_start:span.source_byte_end],
                    passage.text.encode()[span.passage_byte_start:span.passage_byte_end])
                seen.add(span.source_unit)
        self.assertEqual(seen,{m.source_unit for m in group.messages})

    def test_capacity_split_progress_with_257_tiny_fields_and_long_leaf(self):
        from contracts.source_units import resolve_claude_tool_source_unit
        fields={f'field_{i:03d}':f'é{i}'for i in range(257)}
        fields['long']='long '*2048
        event=dict(type='assistant',message=dict(role='assistant',content=[
            dict(type='tool_use',id='many',name='UnknownTool',input=fields)]))
        group,=self.groups(event)
        passages=build_passages(tenant_id='tenant:fixture',source_id='claude:integration:fixture',
            logical_document_id='ldoc_'+'a'*32,revision=1,messages=group.messages,policy=PassagePolicy(1024,128))
        self.assertEqual(len(passages),5)
        covered={m.source_unit:set()for m in group.messages}
        for passage in passages:
            self.assertLessEqual(len(passage.spans),128)
            self.assertEqual(reconstruct_passage(passage,group.messages),passage.text)
            for span in passage.spans:
                text,_=resolve_claude_tool_source_unit(group.full_event_text,span.source_unit)
                self.assertEqual(text.encode()[span.source_byte_start:span.source_byte_end],
                    passage.text.encode()[span.passage_byte_start:span.passage_byte_end])
                covered[span.source_unit].update(range(span.source_byte_start,span.source_byte_end))
        self.assertTrue(all(covered[m.source_unit]==set(range(len(m.text.encode())))for m in group.messages))

    def test_unknown_business_keys_remain_literal_and_legacy_visibility_unchanged(self):
        from recall_server.passage_projection import visible_messages
        event=dict(type='assistant',message=dict(role='assistant',content=[
            dict(type='text',text='Visible authored explanation.'),
            dict(type='tool_use',id='a',name='UnknownTool',input={
                'type':'document','actions':{'a/b~é':'literal {"message":"not reparsed"}'}})]))
        records=self.records(event)
        self.assertEqual([m.text for m in visible_messages(records)],['Visible authored explanation.'])
        group,=self.groups(event)
        self.assertEqual([m.text for m in group.messages],['document','literal {"message":"not reparsed"}'])
        self.assertTrue(group.messages[1].source_unit.endswith('/a~1b~0é'))
        self.assertTrue(all(m.origin['semantic_role']=='unknown' for m in group.messages))


if __name__ == '__main__':
    unittest.main()
