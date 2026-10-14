#!/usr/bin/env python3
"""Execute the complete audit batch package against real PostgreSQL and archives."""
import hashlib
import importlib
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import unittest

import e2e_audit_retirement as original
from e2e_audit_retirement import RECALL
from recall_server.retirement_jobs import decode_result


class AuditBatchTests(unittest.TestCase):
    setUpClass = classmethod(original.AuditRetirementTests.setUpClass.__func__)
    tearDownClass = classmethod(original.AuditRetirementTests.tearDownClass.__func__)
    setUp = original.AuditRetirementTests.setUp
    add = original.AuditRetirementTests.add
    rows = original.AuditRetirementTests.rows

    def package(self, payload, fault=''):
        operator = runpy.run_path(str(RECALL/'scripts/retire_ingest_audits.py'))
        config = dict(commit='a'*40,service_id='srv:synthetic',cutoff_sealed=True,
                      scope=dict(tenant_id=self.tenant,source_id=self.source,cutoff=self.cutoff),
                      owner_modules={})
        for name in ('audit_retirement','archive','archive_runtime','db'):
            module = importlib.import_module('recall_server.'+name)
            config['owner_modules'][module.__name__] = hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
        environment = dict(PATH=os.environ['PATH'],PYTHONPATH=str(RECALL)+os.pathsep+str(RECALL/'server'),
                           RECALL_DATABASE_URL=os.environ['RECALL_DATABASE_URL'],
                           RENDER_GIT_COMMIT=config['commit'],RENDER_SERVICE_ID=config['service_id'],
                           RECALL_SEARCH_PLANE='turbopuffer',RECALL_TPUF_API_KEY='synthetic-ci-key')
        bootstrap = ('from pathlib import Path\n'
                     'from recall_server.archive import FilesystemArchiveStore\n'
                     'import recall_server.archive_runtime as runtime\n'
                     'runtime.build_evidence_archive_store=lambda **kwargs: FilesystemArchiveStore('
                     + 'Path(' + repr(str(self.archive.root)) + '),namespace_key=b"a"*32)\n')
        body,pin = operator['package_body'](config,payload)
        result = subprocess.run([sys.executable,'-c',bootstrap+fault+body],env=environment,
                                capture_output=True,text=True,timeout=30)
        events = [json.loads(line) for line in result.stdout.splitlines()]
        return result, decode_result(events,pin) if result.returncode == 0 else None

    def run_package(self,payload):
        process,result = self.package(payload)
        self.assertEqual(process.returncode,0,process.stderr)
        return result

    def test_archive_then_separate_clear_preserves_retained_rows_and_stops_at_cap(self):
        for key in ['01','02','03','04','05']:
            self.add(key,operation='forget.commit' if key in ['01','04'] else 'ingest.commit')
        before = self.rows()
        archived = self.run_package(dict(operation='archive_batch',after=None,limit=2,max_pages=2))
        self.assertEqual([r['row_count'] for r in archived['receipts']],[1,1])
        self.assertEqual(archived['next_after'],'04')
        self.assertFalse(archived['exhausted'])
        self.assertEqual(self.rows(),before)
        clear = self.run_package(dict(operation='clear_batch',receipts=archived['receipts'],archive_operation='/host/operation'))
        self.assertEqual([r['outcome']['rows'] for r in clear['results']],[1,1])
        self.assertEqual([r['audit_id'] for r in self.rows()],['01','04','05'])
        inspected = self.run_package(dict(operation='inspect_batch',receipts=archived['receipts']))
        self.assertEqual([r['outcome']['status'] for r in inspected['results']],['absent','absent'])
        tail = self.run_package(dict(operation='archive_batch',after='04',limit=2,max_pages=10))
        self.assertEqual(len(tail['receipts']),1)
        self.assertTrue(tail['exhausted'])
        for receipt in archived['receipts']:
            self.owner.restore(receipt)
        self.assertEqual(self.rows(),before)

    def test_interrupted_archive_preserves_rows_and_does_not_issue_clear_receipts(self):
        self.add('01')
        self.add('02')
        before = self.rows()
        fault = """from recall_server.audit_retirement import AuditRetirement
original = AuditRetirement.archive_page
calls = 0
def interrupted(self, **kwargs):
    global calls
    calls += 1
    if calls == 2:
        raise TimeoutError('synthetic_second_archive_interrupted')
    return original(self, **kwargs)
AuditRetirement.archive_page = interrupted
"""
        process,result = self.package(dict(operation='archive_batch',after=None,limit=1,max_pages=2),fault)
        self.assertNotEqual(process.returncode,0)
        self.assertIn('synthetic_second_archive_interrupted',process.stderr)
        self.assertIsNone(result)
        self.assertEqual(self.rows(),before)
        self.assertEqual(len(list(self.archive.root.rglob('data'))),1)

    def test_lost_clear_ack_stops_after_committed_page_and_inspection_finds_exact_state(self):
        self.add('01')
        self.add('02')
        archived = self.run_package(dict(operation='archive_batch',after=None,limit=1,max_pages=2))
        fault = """from recall_server.audit_retirement import AuditRetirement
original = AuditRetirement.clear
def lost_ack(self, receipt):
    original(self, receipt)
    raise ConnectionError('synthetic_ack_lost_after_real_commit')
AuditRetirement.clear = lost_ack
"""
        process,result = self.package(dict(operation='clear_batch',receipts=archived['receipts'],archive_operation='/host/operation'),fault)
        self.assertNotEqual(process.returncode,0)
        self.assertIn('synthetic_ack_lost_after_real_commit',process.stderr)
        self.assertIsNone(result)
        self.assertEqual([r['audit_id'] for r in self.rows()],['02'])
        inspected = self.run_package(dict(operation='inspect_batch',receipts=archived['receipts']))
        self.assertEqual([r['outcome']['status'] for r in inspected['results']],['absent','present'])
        self.owner.restore(archived['receipts'][0])
        self.assertEqual([r['audit_id'] for r in self.rows()],['01','02'])


if __name__ == '__main__':
    unittest.main()
