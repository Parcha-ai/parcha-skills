import dataclasses,json,random,unittest
from types import SimpleNamespace
from recall_server.passage_projection import PassageMessage,PassagePolicy,build_passages,COMPLETE_REPORT_UNIT,COMPLETE_ASSISTANT_FINAL_UNIT
from recall_server.passage_append import make_checkpoint,resume_passages,reuse_enabled
from recall_server.actor_attribution import ActorLink

def resumed(*,prior_messages,prior_passages,messages,arguments):
 if not prior_passages or messages[:len(prior_messages)]!=prior_messages:
  return build_passages(messages=messages,**arguments),dict(fallback=True)
 candidate=SimpleNamespace(revision=1,source_document_sha256='a'*64,manifest_reference=dict(content_sha256='b'*64))
 checkpoint=make_checkpoint(candidate,dict(parts=[dict(ordinal=0)]),prior_messages,prior_passages,arguments['policy'])
 start=checkpoint['message_index'];tail=messages[start:]
 actual=tuple(dataclasses.replace(p,revision=arguments['revision'])for p in prior_passages[:checkpoint['passage_ordinal']])+resume_passages(tail,checkpoint,arguments)
 return actual,dict(fallback=False,skipped_message_bytes=sum(len(m.text.encode())for m in messages[:start])+checkpoint['source_byte_start'])
A=dict(tenant_id='tenant:test',source_id='claude:test',logical_document_id='ldoc_'+'a'*32,revision=2,policy=PassagePolicy(target_tokens=16,overlap_tokens=4))
def message(i,text,unit=None,count=1):return PassageMessage(record_ordinal=i,record_count=count,occurred_at='2026-01-01T00:00:00Z',roles=('assistant',),receipts=(f'recall://claude:test/event{i}?rev=1#item=0',),text=text,source_unit=unit,actor_links=(ActorLink('actor_0123456789abcdef0123456789abcdef','contributor'),))
class Equivalence(unittest.TestCase):
 def compare(self,old,new):
  prior=build_passages(messages=old,**dict(A,revision=1))if old else()
  expected=build_passages(messages=new,**A)
  actual,work=resumed(prior_messages=old,prior_passages=prior,messages=new,arguments=A)
  self.assertEqual(actual,expected);return work
 def test_long_append_skips_tokenizing_prefix(self):
  old=tuple(message(i,'words '+('alpha beta gamma '*20))for i in range(500));new=old+(message(500,'new answer here'),)
  work=self.compare(old,new);self.assertGreater(work['skipped_message_bytes'],.99*sum(len(m.text.encode())for m in new));print(json.dumps(work))
 def test_boundary_offsets_reports_and_utf8(self):
  for unit in(None,COMPLETE_REPORT_UNIT,COMPLETE_ASSISTANT_FINAL_UNIT):
   for count in(1,15,16,17,28,32,50):
    old=(message(0,'prefix '*40),message(1,'évidence '*count,unit,count=3));new=old+(message(4,'tail answer '*10),)
    self.compare(old,new)
 def test_edits_and_empty_fall_back(self):
  old=(message(0,'hello '*40),);new=(message(0,'changed '*40),message(1,'tail'))
  self.assertTrue(self.compare(old,new)['fallback']);self.assertTrue(self.compare((),new)['fallback'])
 def test_many_random_appends(self):
  random.seed(42)
  for _ in range(100):
   old=tuple(message(i,' '.join(random.choice(('aa',' bb','éé','x'*100))for _ in range(random.randint(1,60))),random.choice((None,None,None,COMPLETE_REPORT_UNIT)))for i in range(random.randint(1,15)))
   self.compare(old,old+(message(len(old),'new '*random.randint(1,80)),))
 def test_actual_typed_tool_segments_and_report_extraction(self):
  from recall_server.logical_evidence import LogicalEvidenceRecord
  from recall_server.passage_projection import visible_messages,decode_logical_record
  def record(ordinal,native,text,segment=0,count=1,role='assistant'):
   return LogicalEvidenceRecord(ordinal=ordinal,event_native_id=native,event_kind='claude.message',occurred_at='2026-01-01T00:00:00Z',roles=(role,),receipts=(f'recall://claude:test/{native}?rev=1#item=0',)if segment==0 else(),segment_ordinal=segment,segment_count=count,text=text)
  tool=json.dumps(dict(type='user',message=dict(role='user',content=[dict(type='tool_result',content='internal tool bytes '*50)])))
  visible=json.dumps(dict(type='assistant',message=dict(role='assistant',content=[dict(type='text',text='real retained text '*40)])))
  final=json.dumps(dict(type='response_item',payload=dict(type='message',role='assistant',phase='final_answer',content=[dict(type='output_text',text='completed finding '*30)])))
  records=(record(0,'tool',tool,role='user'),record(1,'split',visible[:50],0,2),record(2,'split',visible[50:],1,2),record(3,'final',final))
  # Place each physical segment in a different immutable part: native decoder
  # and grouping must retain one complete source message across that boundary.
  parts=[r.encode(source_id='claude:test')+b'\n'for r in records]
  decoded=tuple(decode_logical_record(part[:-1],source_id='claude:test')for part in parts)
  old=visible_messages(decoded);self.assertEqual(len(old),2);self.assertEqual(old[0].record_count,2);self.assertEqual(old[0].record_ordinal,1)
  appended=record(4,'new','human-readable new fact');new=visible_messages(decoded+(appended,));self.compare(old,new)
 def test_claude_completed_envelope_append_matches_full_without_api_id_join(self):
  from tests.test_assistant_final_units import claude_records
  from recall_server.passage_projection import visible_messages
  records=claude_records(text='New complete qualification café λ. '*10)
  old=visible_messages(records[:1]);completed=visible_messages(records[:2])
  self.assertEqual(old,completed[:1])
  self.assertIsNone(old[0].source_unit)
  self.assertEqual(completed[1].source_unit,COMPLETE_ASSISTANT_FINAL_UNIT)
  self.compare(old,completed)
  before=build_passages(messages=old,**dict(A,revision=1))
  after=build_passages(messages=completed,**A)
  self.assertEqual((before[0].passage_id,before[0].text,before[0].receipts),
                   (after[0].passage_id,after[0].text,after[0].receipts))
  self.compare(completed,visible_messages(records))
 def test_explicit_reuse_switch_rejects_unknown_values(self):
  import os
  from unittest.mock import patch
  with patch.dict(os.environ,{},clear=True):self.assertTrue(reuse_enabled())
  for value,expected in [('0',False),('1',True)]:
   with patch.dict(os.environ,{'RECALL_PASSAGE_APPEND_REUSE':value}):self.assertEqual(reuse_enabled(),expected)
  with patch.dict(os.environ,{'RECALL_PASSAGE_APPEND_REUSE':'maybe'}):
   with self.assertRaises(ValueError):reuse_enabled()
 def test_retained_metadata_cannot_be_inserted_as_a_body(self):
  from recall_server.passage_index import classify_passages
  from recall_server.passage_append import RetainedPassage
  from recall_server.logical_evidence import LogicalEvidenceError
  retained=RetainedPassage('psg_'+'1'*32,0,('recall://source:test/a?rev=1#item=0',))
  with self.assertRaisesRegex(LogicalEvidenceError,'passage_retained_prefix_missing'):
   classify_passages([], (retained,), revision=2)
  diff=classify_passages([dict(passage_id=retained.passage_id,ordinal=0,revision=1)],(retained,),revision=2)
  self.assertEqual(diff.to_insert,());self.assertEqual(diff.retained,(retained.passage_id,))
if __name__=='__main__':unittest.main()
