#!/usr/bin/env python3
"""The existing parent proof/cursor owns optional singleton metadata retirement."""
import os
import unittest
from unittest.mock import patch

from e2e_singleton_metadata_admission import SingletonMetadataAdmission
from recall_server import retirement_runner as runner
from recall_server.chunk_retirement import ParentRetirementLimits


class SingletonWorkerConvergence(SingletonMetadataAdmission):
    def run_parent(self, **kwargs):
        scope = runner.RetirementScope(self.tenant, self.principal, (self.source,))
        page = runner.plan_cohort(self.store, scope=scope, limit=1)
        runner.enroll_cohort(self.store, scope=scope, reviewed_plan=page)
        with self.store.connect() as connection:
            connection.execute("UPDATE canonical_chunk_retirement_progress SET updated_at=now()-interval '2 minutes' WHERE tenant_id=%s", (self.tenant,))
        with patch.object(self.store, 'verified_singleton_receipts', True), \
                patch.object(self.store, 'search_plane', 'turbopuffer'), \
                patch.dict(os.environ, {'RECALL_CHUNK_BODY_READS': 'archive'}):
            return runner.run_retirement(self.store, self.archive, scope=scope, apply=True,
                limits=runner.RunnerLimits(max_parents=1),
                parent_limits=kwargs.pop('parent_limits', ParentRetirementLimits(batch_documents=2)), **kwargs)

    def test_worker_default_retains_metadata_even_with_reader_capability(self):
        original = self.prepare(retire_body=False)
        result = self.run_parent()
        self.assertEqual(result['failed_parents'], 0)
        self.assertEqual(result['cleared_documents'], 1)
        self.assertEqual(self.baseline(original['receipt']), original)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT text_redacted FROM canonical_chunks WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['text_redacted'], '')

    def test_worker_explicit_singleton_mode_reuses_parent_proof_and_progress(self):
        original = self.prepare(retire_body=False)
        with patch.object(self.archive, 'read_raw', wraps=self.archive.read_raw) as read:
            result = self.run_parent(metadata_mode='singleton')
        self.assertEqual(result['failed_parents'], 0)
        self.assertEqual(result['completed_parent_proofs'], 1)
        self.assertEqual(result['cleared_documents'], 1)
        self.assertEqual(result['retired_metadata_chunks'], 1)
        self.assertIsNone(self.baseline(original['receipt']))
        references = [call.args[0]['object_key'] for call in read.call_args_list]
        self.assertGreater(len(references), 0)
        self.assertEqual(len(references), len(set(references)), 'archive proof was repeated')
        with self.store.connect() as connection:
            progress = connection.execute('SELECT * FROM canonical_chunk_retirement_progress WHERE tenant_id=%s',
                (self.tenant,)).fetchone()
        self.assertEqual(progress['status'], 'complete')
        self.assertEqual(progress['cumulative_cleared_chunks'], 1)
        with patch.object(self.store, 'verified_singleton_receipts', True):
            self.assertEqual(self.store.resolve(original['receipt'], tenant_id=self.tenant,
                authorized_sources=(self.source,), chunk_body_archive=self.archive), self.public_response)


    def test_worker_embedding_dependent_row_stays_physical_without_cascade(self):
        original = self.prepare(retire_body=False)
        with self.store.connect() as connection:
            connection.execute("""INSERT INTO canonical_chunk_embeddings
                (tenant_id,source_id,chunk_id,model,dimensions,content_sha256,runtime_fingerprint,embedding)
                VALUES(%s,%s,%s,'synthetic',512,%s,'synthetic',%s::halfvec)""",
                (self.tenant,self.source,original['chunk_id'],original['text_sha256'],'['+','.join(['0']*512)+']'))
        result = self.run_parent(metadata_mode='singleton')
        self.assertEqual(result['cleared_documents'], 1)
        self.assertEqual(result['retired_metadata_chunks'], 0)
        self.assertEqual(self.baseline(original['receipt']), original)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_chunk_embeddings WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['n'], 1)

    def test_worker_mixed_multichunk_parent_admits_only_singleton(self):
        original = self.prepare(retire_body=False)
        multiple = self.ingest('turn/multiple', 'Synthetic multi chunk passage evidence ' * 2000)
        self.projector.project_pending(tenant_id=self.tenant,batch_size=2,max_batches=1,upload_concurrency=1)
        before = self.baseline(multiple['receipt'])
        result = self.run_parent(metadata_mode='singleton')
        self.assertEqual(result['retired_metadata_chunks'], 1)
        self.assertEqual(result['cleared_documents'], 2)
        self.assertIsNone(self.baseline(original['receipt']))
        self.assertEqual(self.baseline(multiple['receipt']), before)

    def test_worker_failure_after_delete_rolls_back_body_profile_and_cursor(self):
        from recall_server.chunk_retirement import ChunkRetirementError
        original = self.prepare(retire_body=False)
        execute = self.store._execute_bounded
        def failed(connection, sql, values, deadline):
            result = execute(connection, sql, values, deadline)
            if sql.lstrip().startswith('DELETE FROM canonical_chunks'):
                self.assertEqual(result.rowcount, 1)
                raise ChunkRetirementError('synthetic_delete_failure')
            return result
        with patch.object(self.store, '_execute_bounded', failed):
            result = self.run_parent(metadata_mode='singleton')
        self.assertEqual(result['retired_metadata_chunks'], 0)
        self.assertEqual(result['cleared_documents'], 0)
        self.assertEqual(self.baseline(original['receipt']), original)
        with self.store.connect() as connection:
            self.assertIsNone(connection.execute('SELECT chunk_receipt_profile FROM canonical_documents WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['chunk_receipt_profile'])
            self.assertNotEqual(connection.execute('SELECT text_redacted FROM canonical_chunks WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['text_redacted'], '')
            progress = connection.execute('SELECT * FROM canonical_chunk_retirement_progress WHERE tenant_id=%s',
                (self.tenant,)).fetchone()
        self.assertEqual(progress['last_record_ordinal'], -1)
        self.assertEqual(progress['cumulative_cleared_chunks'], 0)

    def test_worker_actual_commit_lost_ack_retains_unknown_and_zero_counters(self):
        import psycopg
        from contextlib import contextmanager
        original = self.prepare(retire_body=False)
        execute, transaction = self.store._execute_bounded, psycopg.Connection.transaction
        armed = False
        def arm(connection, sql, values, deadline):
            nonlocal armed
            result = execute(connection, sql, values, deadline)
            if sql.lstrip().startswith('DELETE FROM canonical_chunks'):
                armed = True
            return result
        @contextmanager
        def lost(connection, *args, **kwargs):
            nonlocal armed
            with transaction(connection, *args, **kwargs) as value:
                yield value
            if armed:
                armed = False
                raise psycopg.OperationalError('synthetic lost acknowledgement')
        with patch.object(self.store, '_execute_bounded', arm), patch.object(psycopg.Connection, 'transaction', lost):
            result = self.run_parent(metadata_mode='singleton')
        self.assertTrue(result['commit_outcome_unknown'])
        self.assertEqual(result['retired_metadata_chunks'], 0)
        self.assertEqual(result['cleared_documents'], 0)
        self.assertIsNone(self.baseline(original['receipt']))
        with self.store.connect() as connection:
            state = connection.execute('SELECT * FROM canonical_chunk_retirement_progress WHERE tenant_id=%s',
                (self.tenant,)).fetchone()
        self.assertEqual(state['status'], 'complete')
        later = self.run_parent(metadata_mode='singleton')
        self.assertEqual(later['attempted_parents'], 0)
        self.assertEqual(later['retired_metadata_chunks'], 0)

    def test_worker_empty_metadata_reverse_preserves_progress_and_original_reader(self):
        from recall_server.singleton_metadata_restore import restore_singleton_metadata
        original = self.prepare()
        with self.store.connect() as connection:
            before = connection.execute('SELECT to_jsonb(chunk) AS row FROM canonical_chunks chunk WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['row']
        result = self.run_parent(metadata_mode='singleton')
        self.assertEqual(result['retired_metadata_chunks'], 1)
        self.assertEqual(result['cleared_utf8_bytes'], 0)
        with self.store.connect() as connection:
            progress = connection.execute('SELECT * FROM canonical_chunk_retirement_progress WHERE tenant_id=%s',
                (self.tenant,)).fetchone()
        with patch.object(self.store,'verified_singleton_receipts',True):
            scope=dict(tenant_id=self.tenant,source_id=self.source,principal_id=self.principal,
                document_ids=(original['document_id'],))
            plan=restore_singleton_metadata(self.store,self.archive,**scope)
            restored=restore_singleton_metadata(self.store,self.archive,**scope,apply=True,reviewed_plan=plan['plan'])
        self.assertEqual(restored['restored_body_bytes'],0)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT to_jsonb(chunk) AS row FROM canonical_chunks chunk WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['row'],before)
            self.assertEqual(connection.execute('SELECT * FROM canonical_chunk_retirement_progress WHERE tenant_id=%s',
                (self.tenant,)).fetchone(),progress)
        self.assertEqual(self.store.resolve(original['receipt'],tenant_id=self.tenant,
            authorized_sources=(self.source,),chunk_body_archive=self.archive),self.public_response)

    def test_worker_pending_publication_cannot_mark_metadata_work_complete(self):
        from recall_server.logical_evidence_projection import mark_logical_evidence_dirty
        original=self.prepare(retire_body=False)
        with self.store.connect() as connection:
            mark_logical_evidence_dirty(connection,tenant_id=self.tenant,source_id=self.source,
                native_ids=[original['native_id']],reason='ingest')
        result=self.run_parent(metadata_mode='singleton')
        self.assertEqual(result['retired_metadata_chunks'],0)
        self.assertEqual(result['cleared_documents'],0)
        with self.store.connect() as connection:
            self.assertNotEqual(connection.execute('SELECT status FROM canonical_chunk_retirement_progress WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['status'],'complete')
        self.assertEqual(self.baseline(original['receipt']),original)


    def test_worker_partial_batches_resume_without_recounting_admitted_rows(self):
        original = self.prepare(retire_body=False)
        second = self.ingest('turn/second', 'Second singleton evidence')
        self.projector.project_pending(tenant_id=self.tenant,batch_size=2,max_batches=1,upload_concurrency=1)
        limits=ParentRetirementLimits(batch_documents=1,max_batches=1)
        first=self.run_parent(metadata_mode='singleton',parent_limits=limits)
        self.assertEqual(first['retired_metadata_chunks'],1)
        self.assertEqual(first['partial_parents'],1)
        second_result=self.run_parent(metadata_mode='singleton',parent_limits=limits)
        self.assertEqual(second_result['retired_metadata_chunks'],1)
        self.assertEqual(second_result['batches'],1)
        self.assertEqual(second_result['partial_parents'],1)
        self.assertEqual(second_result['completed_parent_proofs'],0)
        self.assertIsNone(self.baseline(original['receipt']))
        self.assertIsNone(self.baseline(second['receipt']))
        # The selected tail consumes this invocation's one batch. With no
        # physical rows left, the next claim completes scheduling without a full proof.
        completion=self.run_parent(metadata_mode='singleton',parent_limits=limits)
        self.assertEqual(completion['completed_parent_proofs'],0)
        self.assertEqual(completion['completed_noop_parents'],1)
        self.assertEqual(completion['retired_metadata_chunks'],0)
        self.assertEqual(completion['cleared_documents'],0)
        final=self.run_parent(metadata_mode='singleton')
        self.assertEqual(final['attempted_parents'],0)
        self.assertEqual(final['retired_metadata_chunks'],0)

    def test_worker_complete_body_only_parent_requires_explicit_existing_reset(self):
        from recall_server.chunk_retirement import set_parent_retirement_enabled
        original=self.prepare(retire_body=False)
        self.run_parent()
        no_reset=self.run_parent(metadata_mode='singleton')
        self.assertEqual(no_reset['attempted_parents'],0)
        self.assertIsNotNone(self.baseline(original['receipt']))
        set_parent_retirement_enabled(self.store,tenant_id=self.tenant,source_id=self.source,
            native_parent_id='session',enabled=True)
        result=self.run_parent(metadata_mode='singleton')
        self.assertEqual(result['retired_metadata_chunks'],1)
        self.assertEqual(result['cleared_utf8_bytes'],0)

    def test_worker_revocation_after_proof_refuses_all_mutation(self):
        from recall_server import chunk_retirement
        original=self.prepare(retire_body=False)
        batch=chunk_retirement._retire_parent_batch
        def revoke(*args,**kwargs):
            with self.store.connect() as connection:
                connection.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s AND principal_id=%s',
                    (self.tenant,self.source,self.principal))
            return batch(*args,**kwargs)
        with patch.object(chunk_retirement,'_retire_parent_batch',revoke):
            result=self.run_parent(metadata_mode='singleton')
        self.assertEqual(result['retired_metadata_chunks'],0)
        self.assertEqual(result['cleared_utf8_bytes'],0)
        self.assertIn('retirement_owner_required',result['errors'])
        self.assertEqual(self.baseline(original['receipt']),original)

    def test_worker_tombstone_after_proof_prevents_metadata_admission(self):
        from recall_server import chunk_retirement
        original=self.prepare(retire_body=False)
        batch=chunk_retirement._retire_parent_batch
        def tombstone(*args,**kwargs):
            self.ingest(original['native_id'],'',tombstone=True)
            return batch(*args,**kwargs)
        with patch.object(chunk_retirement,'_retire_parent_batch',tombstone):
            result=self.run_parent(metadata_mode='singleton')
        self.assertEqual(result['retired_metadata_chunks'],0)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_documents WHERE tenant_id=%s AND chunk_receipt_profile IS NOT NULL',
                (self.tenant,)).fetchone()['n'],0)
        self.assertIsNone(self.store.resolve(original['receipt'],tenant_id=self.tenant,
            authorized_sources=(self.source,),chunk_body_archive=self.archive))

    def test_worker_existing_child_runs_source_policy_and_clears_intent(self):
        import json,subprocess,sys
        from pathlib import Path
        from recall_server import retirement_policy as policy
        original=self.prepare(retire_body=False)
        scope=runner.RetirementScope(self.tenant,self.principal,(self.source,))
        policy.start_policy(self.store,scope=scope)
        self.assertTrue(policy.capture_baseline(self.store,scope=scope)['complete'])
        policy.set_policy_enabled(self.store,scope=scope,reviewed_plan=policy.policy_plan(self.store,scope=scope),enabled=True)
        with self.store.connect() as connection:
            epoch=connection.execute('SELECT scope_epoch FROM canonical_chunk_retirement_progress WHERE tenant_id=%s',(self.tenant,)).fetchone()['scope_epoch']
        runner.enroll_baseline_parent(self.store,scope=scope,reviewed_plan=runner.plan_cohort(self.store,scope=scope),expected_scope_epoch=epoch)
        with self.store.connect() as connection:
            connection.execute("UPDATE canonical_chunk_retirement_progress SET updated_at=now()-interval '2 minutes' WHERE tenant_id=%s",(self.tenant,))
        script="""import sys
from pathlib import Path
from recall_server.archive import FilesystemArchiveStore
from recall_server import retirement_process as process
class DurableArchiveFixture(FilesystemArchiveStore):
    storage_backend='s3'
    encryption='sse-s3'
process.build_evidence_archive_store=lambda **kw:DurableArchiveFixture(Path(sys.argv[1]),namespace_key=b'm'*32)
process.main()
"""
        root=Path(__file__).resolve().parents[2]
        env=dict(os.environ,RECALL_VERIFIED_SINGLETON_RECEIPTS='1',RECALL_CHUNK_BODY_READS='archive',
            RECALL_SEARCH_PLANE='turbopuffer',PYTHONPATH=str(root)+os.pathsep+str(root/'server'))
        finished=subprocess.run([sys.executable,'-c',script,str(self.archive.root)],
            input=json.dumps(dict(dsn=self.store.dsn,tenant_id=self.tenant,seconds=30,metadata_mode='singleton')),
            text=True,capture_output=True,timeout=40,env=env)
        self.assertEqual(finished.returncode,0,finished.stdout+finished.stderr)
        result=json.loads(finished.stdout)
        self.assertEqual(result['retired_metadata_chunks'],1,result)
        self.assertFalse(result['commit_outcome_unknown'])
        self.assertIsNone(self.baseline(original['receipt']))
        with self.store.connect() as connection:
            self.assertIsNone(connection.execute('SELECT maintenance_intent FROM canonical_retirement_source_policy WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['maintenance_intent'])
            self.assertEqual(connection.execute('SELECT status FROM canonical_chunk_retirement_progress WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['status'],'complete')


from e2e_singleton_receipt_integration import SingletonIntegrationTests


class WorkerAdmissionLifecycle(SingletonIntegrationTests):
    setUp = SingletonMetadataAdmission.setUp
    run_parent = SingletonWorkerConvergence.run_parent

    def admit_fixture(self, document_id):
        self.projector.project_pending(tenant_id=self.tenant,batch_size=2,max_batches=1,upload_concurrency=1)
        result=self.run_parent(metadata_mode='singleton')
        self.assertEqual(result['retired_metadata_chunks'],1)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT chunk_receipt_profile FROM canonical_documents WHERE tenant_id=%s AND document_id=%s',
                (self.tenant,document_id)).fetchone()['chunk_receipt_profile'],1)


if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(
        [SingletonWorkerConvergence(name) for name in SingletonWorkerConvergence.__dict__ if name.startswith('test_worker_')]
        + [WorkerAdmissionLifecycle(name) for name in dir(WorkerAdmissionLifecycle) if name.startswith('test_integration_')]))
    raise SystemExit(not result.wasSuccessful())
