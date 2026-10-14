"""Operator journal survives uncertainty and cannot silently replay a write."""
from pathlib import Path
import runpy
import sys
import tempfile
import unittest
from unittest.mock import patch

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / 'server')]
from recall_server.retirement_campaign import digest

OPERATOR = runpy.run_path(str(RECALL / 'scripts/retire_ingest_audits.py'))


class OperatorTests(unittest.TestCase):
    def config(self):
        return dict(commit='a' * 40, service_id='srv-audit',
                    scope=dict(tenant_id='tenant:test', source_id='source:test', cutoff='2026-10-13T00:00:00+00:00'),
                    cutoff_sealed=True, owner_modules={
                        'recall_server.' + name: 'b' * 64
                        for name in ('audit_retirement','archive','archive_runtime','db')})

    def test_unknown_post_keeps_durable_intent_and_refuses_reopening(self):
        config, payload = self.config(), dict(operation='archive', after=None, limit=100)
        spec = dict(config=config, payload=payload)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'operation'
            def uncertain(body, pin, emit):
                self.assertTrue((path / 'intent.json').is_file())
                self.assertEqual(pin, digest(spec))
                emit('launch_intent', dict(pin=pin))
                raise TimeoutError('synthetic lost response')
            with patch.object(OPERATOR['RenderJob'], 'run', side_effect=uncertain) as launch:
                with self.assertRaises(TimeoutError):
                    OPERATOR['run_operation'](config,payload,path,digest(spec))
                self.assertTrue((path/'failure.json').is_file())
                self.assertFalse((path/'result.json').exists())
                with self.assertRaises(FileExistsError):
                    OPERATOR['run_operation'](config,payload,path,digest(spec))
                self.assertEqual(launch.call_count,1)

    def test_review_digest_cutoff_and_receipt_scope_refuse_before_launch(self):
        from copy import deepcopy
        config, payload = self.config(), dict(operation='archive',after=None,limit=100)
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(OPERATOR['RenderJob'],'run') as launch:
                with self.assertRaisesRegex(ValueError,'reviewed_operation_digest'):
                    OPERATOR['run_operation'](config,payload,Path(directory)/'wrong','0'*64)
                self.assertFalse((Path(directory)/'wrong').exists())
                unsealed = deepcopy(config)
                unsealed['cutoff_sealed'] = False
                with self.assertRaisesRegex(ValueError,'writer_cutoff_not_sealed'):
                    OPERATOR['package_body'](unsealed,payload)
                receipt = dict(config['scope'],source_id='source:other')
                with self.assertRaisesRegex(ValueError,'receipt_scope'):
                    OPERATOR['package_body'](config,dict(operation='clear',receipt=receipt))
                self.assertEqual(launch.call_count,0)

    def test_reconciliation_is_get_only_and_binds_the_original_job(self):
        from recall_server.retirement_campaign import _mkdir,_write
        config,payload = self.config(),dict(operation='archive',after=None,limit=100)
        pin = digest(dict(config=config,payload=payload))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'operation'
            _mkdir(path)
            _write(path/'intent.json',dict(config=config,payload=payload,sha256=pin))
            _write(path/'event-0000.json',dict(name='launch_intent',value=dict(
                pin=pin,config_sha256=digest(config),owner_id='owner')))
            _write(path/'event-0001.json',dict(name='job',value=dict(id='job-original')))
            def inspect(job_id,result_pin,owner,emit):
                self.assertEqual((job_id,result_pin,owner),('job-original',pin,'owner'))
                return dict(status='cleared',rows=5)
            with patch.object(OPERATOR['RenderJob'],'reconcile',side_effect=inspect) as read:
                with patch.object(OPERATOR['RenderJob'],'run') as launch:
                    self.assertEqual(OPERATOR['reconcile_operation'](path,'job-original',pin),
                                     dict(status='cleared',rows=5))
                    with self.assertRaisesRegex(ValueError,'reconciliation_job_changed'):
                        OPERATOR['reconcile_operation'](path,'job-other',pin)
                    self.assertEqual(read.call_count,1)
                    self.assertEqual(launch.call_count,0)

    def batch_receipts(self):
        rows = []
        for after,end in [(None,'a'),('a','b')]:
            value = dict(self.config()['scope'],contract='recall.ingest-audit-retirement.v1',
                         after=after,next_after=end,scanned=1,row_count=1,raw_bytes=100,exhausted=False)
            rows.append(dict(value,sha256=digest(value)))
        return rows

    def test_batch_budget_chain_duplicates_scope_and_digest_refuse_before_launch(self):
        from copy import deepcopy
        receipts = self.batch_receipts()
        for mutation,error in [('duplicate','duplicate_receipt'),('chain','receipt_chain'),
                               ('scope','receipt_scope'),('hash','receipt_digest'),('count','batch_page_budget')]:
            changed = deepcopy(receipts)
            if mutation == 'duplicate':
                changed[1] = changed[0]
            elif mutation == 'count':
                changed *= 6
            else:
                changed[1][{'chain':'after','scope':'source_id','hash':'sha256'}[mutation]] = 'wrong'
                if mutation != 'hash':
                    changed[1]['sha256'] = digest({k:v for k,v in changed[1].items() if k != 'sha256'})
            with self.subTest(mutation=mutation), self.assertRaisesRegex(ValueError,error):
                OPERATOR['package_body'](self.config(),dict(operation='inspect_batch',receipts=changed))
        for count in [0,11,True]:
            with self.assertRaisesRegex(ValueError,'batch_page_budget'):
                OPERATOR['package_body'](self.config(),dict(operation='archive_batch',after=None,limit=50000,max_pages=count))

    def test_clear_requires_durable_verified_archive_and_uncertainty_keeps_all_receipts(self):
        import json
        from recall_server.retirement_campaign import _mkdir,_write
        config,receipts = self.config(),self.batch_receipts()
        archive_payload = dict(operation='archive_batch',after=None,limit=1,max_pages=2)
        archive_pin = digest(dict(config=config,payload=archive_payload))
        archived = dict(status='archived',receipts=receipts,next_after='b',exhausted=False)
        with tempfile.TemporaryDirectory() as temporary:
            archive = Path(temporary)/'archive'
            _mkdir(archive)
            _write(archive/'intent.json',dict(config=config,payload=archive_payload,sha256=archive_pin))
            payload = dict(operation='clear_batch',receipts=receipts,archive_operation=str(archive))
            pin = digest(dict(config=config,payload=payload))
            directory = Path(temporary)/'clear'
            with patch.object(OPERATOR['RenderJob'],'run',return_value=archived) as launch:
                with self.assertRaisesRegex(ValueError,'durable_archive_receipts'):
                    OPERATOR['run_operation'](config,payload,directory,pin)
                _write(archive/'result.json',archived)
                with self.assertRaisesRegex(ValueError,'archive_verified_result'):
                    OPERATOR['run_operation'](config,payload,directory,pin)
                self.assertEqual(launch.call_count,0)
            _write(archive/'event-0000.json',dict(name='verified_job_result',value=dict(
                pin=archive_pin,result_sha256=digest(archived),result=archived)))
            def uncertain(body,pin,emit):
                saved = json.loads((directory/'intent.json').read_bytes())
                self.assertEqual(saved['payload']['receipts'],receipts)
                emit('launch_intent',dict(pin=pin))
                raise TimeoutError('unknown_clear')
            with patch.object(OPERATOR['RenderJob'],'run',side_effect=uncertain) as launch:
                with self.assertRaises(TimeoutError):
                    OPERATOR['run_operation'](config,payload,directory,pin)
                with self.assertRaises(FileExistsError):
                    OPERATOR['run_operation'](config,payload,directory,pin)
                self.assertEqual(launch.call_count,1)
                self.assertTrue((directory/'failure.json').is_file())
            # Delayed logs can be recovered by GET without rerunning the archive.
            recovered = archive/'reconcile-original'
            _mkdir(recovered)
            (archive/'result.json').rename(recovered/'result.json')
            (archive/'event-0000.json').rename(recovered/'event-0000.json')
            OPERATOR['require_durable_archive'](config,payload)
            evidence = json.loads((recovered/'event-0000.json').read_bytes())
            evidence['value']['pin'] = 'f'*64
            (recovered/'event-0000.json').write_text(json.dumps(evidence))
            with self.assertRaisesRegex(ValueError,'archive_verified_result'):
                OPERATOR['require_durable_archive'](config,payload)


if __name__ == '__main__':
    unittest.main()
