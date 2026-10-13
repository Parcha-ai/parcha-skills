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


if __name__ == '__main__':
    unittest.main()
