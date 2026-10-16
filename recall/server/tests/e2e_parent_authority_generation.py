#!/usr/bin/env python3
"""Shadow parent membership generations; native search authority is unchanged."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
import json
import os
from pathlib import Path
import threading
import time

import psycopg
import unittest
from unittest.mock import patch

from e2e_singleton_search_authority import SingletonSearchTests
from recall_server.chunk_retirement import retire_current_chunks, ChunkRetirementError
from recall_server.receipt_membership import lookup_current_search_membership


class ParentAuthorityGenerationTests(SingletonSearchTests):
    def generation(self, parent='session', connection=None):
        sql='''SELECT generation FROM canonical_parent_authority
               WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s'''
        args=(self.tenant,self.source,parent)
        if connection is not None:
            row=connection.execute(sql,args).fetchone()
        else:
            with self.store.connect() as c:
                row=c.execute(sql,args).fetchone()
        return row['generation'] if row else 0

    def test_runtime_role_grants_and_reused_backend_flush_each_transaction(self):
        role='runtime_generation_'+self.tenant.rsplit(':',1)[-1]
        with self.store.connect() as admin:
            admin.execute(psycopg.sql.SQL('CREATE ROLE {} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS').format(psycopg.sql.Identifier(role)))
            admin.execute(psycopg.sql.SQL('GRANT USAGE ON SCHEMA public TO {}').format(psycopg.sql.Identifier(role)))
            admin.execute(psycopg.sql.SQL('GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO {}').format(psycopg.sql.Identifier(role)))
            admin.execute(psycopg.sql.SQL('REVOKE INSERT,UPDATE,DELETE ON schema_migrations FROM {}').format(psycopg.sql.Identifier(role)))
            admin.execute(psycopg.sql.SQL('GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA public TO {}').format(psycopg.sql.Identifier(role)))
            database=admin.execute('SELECT current_database() AS name').fetchone()['name']
            admin.execute(psycopg.sql.SQL('GRANT TEMP ON DATABASE {} TO {}').format(psycopg.sql.Identifier(database),psycopg.sql.Identifier(role)))
        try:
            with psycopg.connect(self.store.dsn,row_factory=psycopg.rows.dict_row) as runtime:
                @contextmanager
                def role_connection(*,timeout=None):
                    with runtime.transaction():
                        runtime.execute(psycopg.sql.SQL('SET LOCAL ROLE {}').format(psycopg.sql.Identifier(role)))
                        runtime.execute('SET LOCAL search_path=pg_catalog,public')
                        yield runtime
                with patch.object(self.store,'connect',side_effect=role_connection):
                    self.ingest('turn/search','Synthetic runtime original')
                    self.assertEqual(self.generation(),1)
                    with psycopg.connect(self.store.dsn) as admin:
                        admin.execute(psycopg.sql.SQL('REVOKE UPDATE ON canonical_parent_authority FROM {}').format(psycopg.sql.Identifier(role)))
                    with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                        self.edit()
                    self.assertEqual(self.generation(),1)
                    with psycopg.connect(self.store.dsn) as admin:
                        admin.execute(psycopg.sql.SQL('GRANT UPDATE ON canonical_parent_authority TO {}').format(psycopg.sql.Identifier(role)))
                    self.edit()
                    self.assertEqual(self.generation(),2)
                    with self.store.connect() as c:
                        privileges=c.execute("""SELECT current_user AS role,
                          has_schema_privilege(current_user,'public','CREATE') AS schema_create,
                          has_table_privilege(current_user,'schema_migrations','INSERT') AS migration_write,
                          has_database_privilege(current_database(),'TEMP') AS temp,
                          (SELECT count(*) FROM pg_temp.recall_parent_authority_pending) AS pending""").fetchone()
                        self.assertEqual(privileges,dict(role=role,schema_create=False,migration_write=False,temp=True,pending=0))
                    # Simulate backend temp cleanup between transactions. The
                    # next writer must recreate its queue, retaining durable G.
                    with self.store.connect() as c:
                        c.execute('DROP TABLE pg_temp.recall_parent_authority_pending')
                    self.edit(batch=True,tombstone=True,native='session')
                    self.assertEqual(self.generation(),3)
                    with self.store.connect() as c:
                        self.assertEqual(c.execute('SELECT count(*) AS n FROM pg_temp.recall_parent_authority_pending').fetchone()['n'],0)
        finally:
            with self.store.connect() as admin:
                admin.execute(psycopg.sql.SQL('DROP OWNED BY {}').format(psycopg.sql.Identifier(role)))
                admin.execute(psycopg.sql.SQL('DROP ROLE {}').format(psycopg.sql.Identifier(role)))

    def test_ingest_replay_edit_and_batch_advance_once(self):
        self.receipt=self.ingest('turn/search','Synthetic original')['receipt']
        self.assertEqual(self.generation(),1)
        self.assertTrue(self.ingest('turn/search','Synthetic original')['replay'])
        self.assertEqual(self.generation(),1)
        self.edit()
        self.assertEqual(self.generation(),2)
        self.edit(batch=True,tombstone=True,native='session')
        self.assertEqual(self.generation(),3)
        with self.store.connect() as c:
            members=lookup_current_search_membership(self.store,tenant_id=self.tenant,
                authorized_sources=(self.source,),receipts=(self.receipt,),connection=c,
                deadline_at=time.monotonic()+3)
        self.assertEqual(members,set())

    def test_body_locator_and_noop_updates_are_neutral(self):
        receipt=self.ingest('turn/body','Synthetic body')['receipt']
        with self.store.connect() as c:
            c.execute("UPDATE canonical_chunks SET text_redacted='' WHERE tenant_id=%s",(self.tenant,))
            c.execute("UPDATE canonical_documents SET text_redacted='',body_record_ordinal=0,body_record_count=1 WHERE tenant_id=%s",(self.tenant,))
            c.execute('UPDATE canonical_documents SET is_current=is_current WHERE tenant_id=%s',(self.tenant,))
        self.assertEqual(self.generation(),1)
        with self.store.connect() as c:
            members=lookup_current_search_membership(self.store,tenant_id=self.tenant,
                authorized_sources=(self.source,),receipts=(receipt,),connection=c,
                deadline_at=time.monotonic()+3)
        self.assertEqual(members,{(self.source,receipt)})

    def test_profile_and_chunk_membership_changes_invalidate(self):
        self.ingest('turn/profile','Synthetic profile')
        with self.store.connect() as c:
            c.execute('UPDATE canonical_documents SET chunk_receipt_profile=1 WHERE tenant_id=%s',(self.tenant,))
            c.execute('DELETE FROM canonical_chunks WHERE tenant_id=%s',(self.tenant,))
        self.assertEqual(self.generation(),2)
        with self.store.connect() as c:
            c.execute('UPDATE canonical_documents SET chunk_receipt_profile=2 WHERE tenant_id=%s',(self.tenant,))
        self.assertEqual(self.generation(),3)

    def test_parent_move_invalidates_old_and_new_keys(self):
        self.ingest('turn/move','Synthetic move')
        with self.store.connect() as c:
            c.execute("UPDATE canonical_events SET native_parent_id='moved-parent' WHERE tenant_id=%s",(self.tenant,))
        self.assertEqual(self.generation(),2)
        self.assertEqual(self.generation('moved-parent'),1)

    def test_chunk_receipt_and_document_currentness_changes_invalidate(self):
        self.ingest('turn/receipt','Synthetic receipt')
        with self.store.connect() as c:
            c.execute("UPDATE canonical_chunks SET ordinal=1,receipt=replace(receipt,'#item=0','#item=1') WHERE tenant_id=%s",(self.tenant,))
        self.assertEqual(self.generation(),2)
        with self.store.connect() as c:
            c.execute('UPDATE canonical_documents SET is_current=false,deleted_at=now() WHERE tenant_id=%s',(self.tenant,))
        self.assertEqual(self.generation(),3)

    def test_ordered_hard_deletion_retains_parent_version(self):
        self.ingest('turn/cascade','Synthetic cascade')
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_chunks WHERE tenant_id=%s',(self.tenant,))
            c.execute('DELETE FROM canonical_documents WHERE tenant_id=%s',(self.tenant,))
            c.execute('DELETE FROM canonical_events WHERE tenant_id=%s',(self.tenant,))
        self.assertEqual(self.generation(),2)

    def test_rollback_after_early_flush_reverts_generation_and_data(self):
        self.ingest('turn/rollback','Synthetic rollback')
        with self.store.connect() as c, c.transaction(force_rollback=True):
            c.execute('UPDATE canonical_documents SET is_current=false WHERE tenant_id=%s',(self.tenant,))
            c.execute('SET CONSTRAINTS ALL IMMEDIATE')
            self.assertEqual(self.generation(connection=c),2)
        self.assertEqual(self.generation(),1)
        with self.store.connect() as c:
            self.assertTrue(c.execute('SELECT is_current FROM canonical_documents WHERE tenant_id=%s',(self.tenant,)).fetchone()['is_current'])

    def test_publication_and_catalog_deletion_do_not_reset_generation(self):
        self.prepare_search()
        self.assertEqual(self.generation(),1)
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_evidence_documents WHERE tenant_id=%s',(self.tenant,))
        self.assertEqual(self.generation(),1)
        self.edit()
        self.assertEqual(self.generation(),2)

    def test_forget_revokes_before_archive_cleanup_and_preserves_native_proof(self):
        from recall_server.canonical import CanonicalLifecycleError
        self.prepare_search()
        original=self.generation()
        request=dict(contract='recall.forget-request.v1',schema_version=1,
            tenant_id=self.tenant,principal_id=self.principal,source_id=self.source,
            target_receipt=self.receipt,mode='explicit_forget',reason='owner_requested',
            requested_at='2026-10-14T00:01:00Z',idempotency_key='generation-forget')
        with patch.object(self.archive,'delete_internal_raw_bounded',side_effect=OSError('synthetic archive failure')):
            with self.assertRaisesRegex(CanonicalLifecycleError,'archive_delete_failed'):
                self.plane.forget(request)
        self.assertGreater(self.generation(),original)
        with patch.object(self.store,'verified_singleton_receipts',True):
            self.assertEqual(self.search()['results'],[])


    def test_migration_requires_optional_profile_marker_and_column(self):
        migration=(Path(__file__).resolve().parents[1]/'schema'/'082_parent_authority_generation.sql').read_text()
        for alteration in ("DELETE FROM schema_migrations WHERE version=80",
                           "ALTER TABLE canonical_documents RENAME COLUMN chunk_receipt_profile TO hidden_profile"):
            with self.subTest(alteration=alteration):
                with self.store.connect() as c, c.transaction(force_rollback=True):
                    c.execute(alteration)
                    with self.assertRaisesRegex(psycopg.errors.RaiseException,'parent_authority_profile_schema_required'):
                        c.execute(migration)

    def test_migration_replay_preserves_existing_generation(self):
        self.ingest('turn/migration','Synthetic migration replay')
        migration=(Path(__file__).resolve().parents[1]/'schema'/'082_parent_authority_generation.sql').read_text()
        with self.store.connect() as c:
            c.execute(migration)
        self.assertEqual(self.generation(),1)
        self.edit(native='turn/migration')
        self.assertEqual(self.generation(),2)

    def test_native_concurrent_ingest_commits_without_added_retries(self):
        self.ingest('warmup','Synthetic warmup')
        initial=self.generation()
        barrier=threading.Barrier(4)
        def writer(index):
            barrier.wait(5)
            for ordinal in range(10):
                self.ingest(f'parallel/{index}/{ordinal}','Synthetic concurrent parent')
            return 10
        with ThreadPoolExecutor(4) as pool:
            completed=sum(pool.map(writer,range(4)))
        self.assertEqual(completed,40)
        self.assertEqual(self.generation(),initial+40)

    def test_sorted_multi_parent_flush_creates_missing_versions_concurrently(self):
        for native in ('one','two','three','four'):
            self.ingest(native,'Synthetic parent move')
        initial=self.generation()
        with ExitStack() as stack:
            a=stack.enter_context(psycopg.connect(os.environ['RECALL_DATABASE_URL']))
            b=stack.enter_context(psycopg.connect(os.environ['RECALL_DATABASE_URL']))
            for c in (a,b):
                c.execute("SET statement_timeout='4s'")
            for c,targets in ((a,(('one','z-parent'),('two','a-parent'))),
                              (b,(('three','a-parent'),('four','z-parent')))):
                for native,parent in targets:
                    c.execute('UPDATE canonical_events SET native_parent_id=%s WHERE tenant_id=%s AND native_id=%s',
                              (parent,self.tenant,native))
            with ThreadPoolExecutor(2) as pool:
                jobs=[pool.submit(c.commit) for c in (a,b)]
                for job in jobs:
                    job.result(6)
        self.assertEqual(self.generation(),initial+2)
        self.assertEqual(self.generation('a-parent'),2)
        self.assertEqual(self.generation('z-parent'),2)

    def test_early_constraint_flush_deadlock_rolls_back_the_entire_write(self):
        self.ingest('turn/first','Synthetic first')
        self.ingest('turn/second','Synthetic second')
        initial=self.generation()
        with ExitStack() as stack:
            a=stack.enter_context(psycopg.connect(os.environ['RECALL_DATABASE_URL']))
            b=stack.enter_context(psycopg.connect(os.environ['RECALL_DATABASE_URL']))
            monitor=stack.enter_context(psycopg.connect(os.environ['RECALL_DATABASE_URL'],autocommit=True))
            for c in (a,b):
                c.execute("SET deadlock_timeout='100ms'")
                c.execute("SET statement_timeout='4s'")
                c.commit()
            a.execute("UPDATE canonical_documents SET is_current=false WHERE tenant_id=%s AND native_id='turn/first'",(self.tenant,))
            a.execute('SET CONSTRAINTS ALL IMMEDIATE')
            a.execute('SET CONSTRAINTS ALL DEFERRED')
            b.execute("UPDATE canonical_documents SET is_current=false WHERE tenant_id=%s AND native_id='turn/second'",(self.tenant,))
            def finish(c,lock_document=False):
                try:
                    if lock_document:
                        c.execute("SELECT 1 FROM canonical_documents WHERE tenant_id=%s AND native_id='turn/second' FOR UPDATE",(self.tenant,))
                    c.commit()
                    return 'committed'
                except psycopg.errors.DeadlockDetected:
                    c.rollback()
                    return 'deadlock'
            with ThreadPoolExecutor(2) as pool:
                pending=pool.submit(finish,b)
                until=time.monotonic()+2
                while time.monotonic()<until:
                    row=monitor.execute('SELECT wait_event_type FROM pg_stat_activity WHERE pid=%s',(b.info.backend_pid,)).fetchone()
                    if row and row[0]=='Lock':
                        break
                    time.sleep(.01)
                else:
                    self.fail('expected authority wait did not occur')
                own=pool.submit(finish,a,True)
                outcomes=[own.result(6),pending.result(6)]
        self.assertEqual(sorted(outcomes),['committed','deadlock'])
        self.assertEqual(self.generation(),initial+1)
        with self.store.connect() as c:
            revoked=c.execute('SELECT count(*) AS count FROM canonical_documents WHERE tenant_id=%s AND NOT is_current',(self.tenant,)).fetchone()['count']
        self.assertEqual(revoked,1)
        print(json.dumps(dict(test='early_flush_atomic_rollback',synthetic=True,outcomes=outcomes)),flush=True)

    def test_native_ingest_forget_retire_publish_deferred(self):
        self.prepare_search(mixed=True)
        target=self.ingest('turn/retire','Synthetic retirement body')
        self.projector.project_pending(tenant_id=self.tenant,batch_size=10,max_batches=1,upload_concurrency=1)
        document=self.baseline(target['receipt'])
        args=dict(tenant_id=self.tenant,source_id=self.source,document_ids=(document['document_id'],))
        with patch.dict(os.environ,{'RECALL_CHUNK_BODY_READS':'archive'}):
            preview=retire_current_chunks(self.store,self.archive,**args)
        initial=self.generation()
        self.ingest('turn/project-queued','Synthetic queued publication')
        publication_attempts=[]
        native_commit=self.projector._commit
        def observe_commit(*args,**kwargs):
            first=not publication_attempts
            publication_attempts.append('entered')
            if first:
                barrier.wait(5)
            result=native_commit(*args,**kwargs)
            publication_attempts.append(result)
            return result
        barrier=threading.Barrier(4)
        request=dict(contract='recall.forget-request.v1',schema_version=1,tenant_id=self.tenant,principal_id=self.principal,source_id=self.source,target_receipt=self.physical_receipt,mode='explicit_forget',reason='owner_requested',requested_at='2026-10-14T00:01:00Z',idempotency_key='shadow-concurrent-forget')
        def run(name):
            if name!='publish':
                barrier.wait(5)
            if name=='ingest': return self.ingest('turn/new','Synthetic concurrent new body')['status']
            if name=='forget': return self.plane.forget(request)['status']
            if name=='retire':
                try:
                    return retire_current_chunks(self.store,self.archive,**args,apply=True,reviewed_plan=preview['plan'])['status']
                except ChunkRetirementError as exc:
                    return type(exc).__name__
            report=self.projector.project_pending(tenant_id=self.tenant,batch_size=10,max_batches=1,upload_concurrency=1)
            return {'committed':report.get('committed'), 'failed':report['failed'], 'documents':report['documents']}
        started=time.monotonic()
        with patch.dict(os.environ,{'RECALL_CHUNK_BODY_READS':'archive'}), patch.object(self.projector,'_commit',side_effect=observe_commit), ThreadPoolExecutor(4) as pool:
            outcomes=dict(zip(('ingest','forget','retire','publish'),pool.map(run,('ingest','forget','retire','publish'))))
        self.assertEqual(outcomes['ingest'],'committed')
        self.assertEqual(outcomes['forget'],'deleted')
        # A native retirement proof race may defer, but cannot silently clear
        # changed input. Its independent verifier remains unchanged.
        self.assertIn(outcomes['retire'],('applied','ChunkRetirementError'))
        self.assertGreaterEqual(self.generation(),initial+3)
        self.assertIn('entered',publication_attempts)
        before_publication=self.generation()
        with patch.object(self.projector,'_commit',side_effect=observe_commit):
            completed=self.projector.project_pending(tenant_id=self.tenant,batch_size=10,max_batches=1,upload_concurrency=1)
        self.assertEqual(completed['failed'],0)
        self.assertIn('committed',publication_attempts)
        self.assertEqual(self.generation(),before_publication)
        print(json.dumps(dict(test=self._testMethodName,synthetic=True,native_write_owners=True,outcomes=outcomes,publication_attempts=publication_attempts,elapsed_ms=round((time.monotonic()-started)*1000,3))),flush=True)


if __name__=='__main__':
    names=[name for name in ParentAuthorityGenerationTests.__dict__ if name.startswith('test_')]
    suite=unittest.TestSuite(ParentAuthorityGenerationTests(name) for name in names)
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
