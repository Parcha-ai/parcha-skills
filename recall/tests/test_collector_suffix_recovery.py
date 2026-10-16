"""Recover only reviewed complete Claude suffixes with a following native chain link."""
import hashlib
import json
import tempfile
import threading
import unittest
from pathlib import Path
from http.server import ThreadingHTTPServer
from unittest import mock
from collector.collector import Collector, CollectorRuntimeError
from collector.suffix_recovery import inspect_suffix, recover_suffix
from test_collector import AckServer


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


class VerifiedSuffixRecovery(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)/'claude';self.root.mkdir()
        self.path=self.root/'session.jsonl'
        self.message=dict(type='user',uuid='11111111-1111-4111-8111-111111111111',
            sessionId='22222222-2222-4222-8222-222222222222',timestamp='2026-10-01T00:00:00Z',
            parentUuid=None,message=dict(role='user',content='Recover complete history.'))
        self.next=dict(type='assistant',uuid='33333333-3333-4333-8333-333333333333',
            sessionId=self.message['sessionId'],parentUuid=self.message['uuid'],
            timestamp='2026-10-01T00:00:01Z',message=dict(role='assistant',content=[dict(type='text',text='Following record.')]))
        self.prefix=b'{"message":{"content":"interrupted '
        self.write()
        AckServer.batches={};AckServer.requests=0;AckServer.received=[];AckServer.drop_after_first_commit=False
        self.server=ThreadingHTTPServer(('127.0.0.1',0),AckServer)
        thread=threading.Thread(target=self.server.serve_forever,daemon=True);thread.start()
        self.addCleanup(self.server.server_close);self.addCleanup(self.server.shutdown)
        self.collector=Collector(root=self.root,harness='claude',source_id='claude:test',
            spool_path=Path(self.tmp.name)/'spool.db',endpoint=f'http://127.0.0.1:{self.server.server_port}',token='test-only')
        self.addCleanup(self.collector.close)
        self.collector.scan();self.collector.flush()

    def write(self):
        suffix=json.dumps(self.message).encode()+b'\n';following=json.dumps(self.next).encode()+b'\n'
        line=self.prefix+suffix;self.path.write_bytes(line+following)
        self.spec=dict(path=str(self.path),line_start=0,suffix_start=len(self.prefix),line_end=len(line),
            next_end=len(line)+len(following),line_sha256=digest(line),suffix_sha256=digest(suffix),next_sha256=digest(following))

    def test_exact_suffix_dry_read_has_no_side_effects(self):
        before=self.path.read_bytes();count=self.collector.db.total_changes
        value=inspect_suffix(self.root,self.spec)
        self.assertEqual(value['content'],self.message)
        self.assertEqual(value['provenance']['original_line_start'],0)
        self.assertEqual(value['provenance']['suffix_start'],len(self.prefix))
        self.assertEqual(self.path.read_bytes(),before);self.assertEqual(self.collector.db.total_changes,count)

    def test_enqueue_and_lost_ack_replay_keep_original_error_and_cursor(self):
        before=self.path.read_bytes();cursor=dict(self.collector.db.execute('select * from files').fetchone())
        original_error=dict(self.collector.db.execute('select * from dead_letters').fetchone())
        first=recover_suffix(self.collector,self.spec)
        self.assertEqual(first['status'],'queued')
        self.assertEqual(recover_suffix(self.collector,self.spec)['status'],'already_pending')
        self.assertEqual(len(self.collector.pending_envelopes()),1)
        envelope=self.collector.pending_envelopes()[0]
        self.assertTrue(envelope['native_id'].endswith(f'-{len(self.prefix):016x}'))
        self.assertEqual(envelope['provenance']['recovered_suffix']['original_line_sha256'],digest(before[:self.spec['line_end']]))
        with mock.patch.object(self.collector,'_after_remote_commit',side_effect=RuntimeError('lost local ACK')):
            with self.assertRaises(RuntimeError):self.collector.flush()
        self.assertEqual(recover_suffix(self.collector,self.spec)['status'],'already_pending')
        self.assertEqual(self.collector.flush()['acked'],1)
        self.assertEqual(recover_suffix(self.collector,self.spec)['status'],'already_acked')
        self.assertEqual(dict(self.collector.db.execute('select * from dead_letters').fetchone()),original_error)
        self.assertEqual(dict(self.collector.db.execute('select * from files').fetchone()),cursor)
        self.assertEqual(self.path.read_bytes(),before)
        self.assertEqual(len(AckServer.batches),2,'following event plus one recovered native identity')

    def test_incomplete_nested_and_mismatched_chain_are_refused(self):
        for kind in ('incomplete','nested','session','parent','nonvisible','hash'):
            with self.subTest(kind=kind):
                self.write();spec=dict(self.spec)
                if kind=='incomplete':
                    raw=self.path.read_bytes();line=raw[:self.spec['line_end']-2]+b'\n';following=raw[self.spec['line_end']:]
                    self.path.write_bytes(line+following)
                    spec.update(line_end=len(line),next_end=len(line)+len(following),line_sha256=digest(line),suffix_sha256=digest(line[len(self.prefix):]))
                elif kind=='nested':
                    self.prefix=b'{"wrapper":';self.write();raw=self.path.read_bytes();line=raw[:self.spec['line_end']-1]+b'}\n';following=raw[self.spec['line_end']:]
                    self.path.write_bytes(line+following)
                    spec=dict(self.spec,line_end=len(line),next_end=len(line)+len(following),line_sha256=digest(line),suffix_sha256=digest(line[len(self.prefix):]))
                elif kind=='session':self.next['sessionId']='44444444-4444-4444-8444-444444444444';self.write();spec=dict(self.spec)
                elif kind=='parent':self.next['parentUuid']='44444444-4444-4444-8444-444444444444';self.write();spec=dict(self.spec)
                elif kind=='nonvisible':self.message['message']['content']=[dict(type='tool_result',content='tool output')];self.write();spec=dict(self.spec)
                elif kind=='hash':spec['suffix_sha256']='0'*64
                with self.assertRaises(ValueError):recover_suffix(self.collector,spec)
                self.assertEqual(self.collector.pending_envelopes(),[])
                self.prefix=b'{"message":{"content":"interrupted '
                self.next['sessionId']=self.message['sessionId'];self.next['parentUuid']=self.message['uuid'];self.message['message']['content']='Recover complete history.'

    def test_missing_dead_letter_is_not_generic_import(self):
        self.collector.db.execute('delete from dead_letters');self.collector.db.commit()
        with self.assertRaisesRegex(ValueError,'dead_letter'):recover_suffix(self.collector,self.spec)
        self.assertEqual(self.collector.pending_envelopes(),[])

    def test_native_attachment_chain_is_valid_and_forged_existing_identity_is_not(self):
        self.next['type']='attachment';self.write()
        self.assertEqual(recover_suffix(self.collector,self.spec)['status'],'queued')
        self.collector.db.execute("update record_generations set base_content_sha256='changed'")
        self.collector.db.commit()
        with self.assertRaisesRegex(ValueError,'existing_identity_mismatch'):recover_suffix(self.collector,self.spec)
        self.assertEqual(len(self.collector.pending_envelopes()),1)

    def test_privacy_drop_does_not_archive_or_enqueue(self):
        from privacy.policy import PrivacyPolicy
        self.message['password']='synthetic-password';self.write()
        self.collector.privacy=PrivacyPolicy(mode='drop')
        with mock.patch.object(self.collector,'_archive_raw')as archive:
            self.assertEqual(recover_suffix(self.collector,self.spec)['status'],'privacy_dropped')
        archive.assert_not_called()
        self.assertEqual(self.collector.pending_envelopes(),[])

    def test_archive_failure_and_source_change_never_enqueue(self):
        with mock.patch.object(self.collector,'_archive_raw',side_effect=CollectorRuntimeError('archive_unavailable')):
            with self.assertRaises(CollectorRuntimeError):recover_suffix(self.collector,self.spec)
        self.assertEqual(self.collector.pending_envelopes(),[])
        def changed(**kwargs):
            self.path.write_bytes(self.path.read_bytes().replace(b'Recover complete',b'Changed complete'))
        with mock.patch.object(self.collector,'_archive_raw',side_effect=changed):
            with self.assertRaisesRegex(ValueError,'source_changed'):recover_suffix(self.collector,self.spec)
        self.assertEqual(self.collector.pending_envelopes(),[])

    def test_native_archive_repairs_preserve_original_line_provenance(self):
        writes=[]
        class Archive:
            def put_raw(self,**kwargs):
                writes.append(kwargs)
                return dict(content_sha256=digest(kwargs['payload']),size_bytes=len(kwargs['payload']),media_type=kwargs['media_type'])
        self.collector.archive=Archive();self.collector.tenant_id='tenant:test'
        recover_suffix(self.collector,self.spec)
        original=self.collector.pending_envelopes()[0]['provenance']['recovered_suffix']
        self.assertEqual(json.loads(writes[0]['payload']),self.message)
        row=self.collector.db.execute("select * from outbox where state='pending'").fetchone()
        envelope=json.loads(row['envelope_json']);envelope['provenance'].pop('artifact_ref')
        self.collector.db.execute('update outbox set envelope_json=? where id=?',(json.dumps(envelope),row['id']));self.collector.db.commit()
        row=self.collector.db.execute('select * from outbox where id=?',(row['id'],)).fetchone()
        repaired=self.collector._repair_pending_envelope(row);self.collector.db.commit()
        self.assertEqual(json.loads(repaired['envelope_json'])['provenance'].get('recovered_suffix'),original)
        self.collector.db.execute("update outbox set state='dead' where id=?",(row['id'],));self.collector.db.commit()
        self.assertEqual(self.collector.recover_dead_payloads()['recovered'],1)
        self.assertEqual(self.collector.pending_envelopes()[0]['provenance'].get('recovered_suffix'),original)

    def test_dead_payload_retry_never_attaches_old_proof_to_changed_suffix(self):
        recover_suffix(self.collector,self.spec)
        self.collector.db.execute("update outbox set state='dead' where state='pending'");self.collector.db.commit()
        self.path.write_bytes(self.path.read_bytes().replace(b'Recover complete',b'Changed complete'))
        result=self.collector.recover_dead_payloads()
        self.assertEqual(result,dict(recovered=0,unrecoverable=1))
        self.assertEqual(self.collector.pending_envelopes(),[])
        row=self.collector.db.execute("select envelope_json from outbox where state='dead'").fetchone()
        self.assertEqual(json.loads(row['envelope_json'])['content']['message']['content'],'Recover complete history.')


if __name__=='__main__':unittest.main()
