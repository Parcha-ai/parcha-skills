#!/usr/bin/env python3
"""Native outbox progress survives projector replacement, without proof shortcuts."""
import unittest
import threading
import time
import json
import copy
from psycopg.types.json import Jsonb
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from contextlib import contextmanager
from datetime import date
from e2e_logical_parent_progress import insert_record
from recall_server.logical_evidence_projection import mark_logical_evidence_dirty
from unittest.mock import patch

from e2e_turbopuffer_quantum import CooperativePublication
from recall_server.turbopuffer_projection import TurbopufferProjector
from tests.central_brain.fake_turbopuffer import FakeTurbopuffer


class DurablePublication(CooperativePublication):
    def restart(self):
        for _, steps, _ in self.writer._quantum_months:
            steps.close()
        self.writer = TurbopufferProjector(
            self.store, self.settings, client=self.client,
            batch_rows=1, write_concurrency=1, tokens_per_minute=0)

    def test_restart_does_not_republish_acknowledged_page(self):
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        sent = []
        original = namespace.write
        def record(**kwargs):
            sent.extend(row['id'] for row in kwargs.get('upsert_rows', []))
            return original(**kwargs)
        with patch.object(self.store, 'verified_singleton_receipts', True), \
                patch.object(namespace, 'write', side_effect=record):
            self.assertEqual(self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)['rows'], 1)
            first = sent[:]
            self.assertEqual(len(first), 1)
            self.restart()
            self.assertEqual(self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)['rows'], 1)
            self.assertNotEqual(sent[-1], first[0], 'fresh process replayed an ACKed page')
            for _ in range(12):
                result = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
                if not result['pending']:
                    break
            self.assertEqual(result['pending'], 0)
            self.assertEqual(len(sent), len(set(sent)))
            with self.store.connect() as connection:
                rows = connection.execute('''SELECT sum(row_count) AS n FROM search_projection_shards
                    WHERE tenant_id=%s''', (self.tenant,)).fetchone()['n']
            self.assertEqual(rows, len(sent), 'EOF catalog must include pages from both processes')

    def checkpoint(self):
        with self.store.connect() as connection:
            return connection.execute("""SELECT page_checkpoint FROM search_projection_outbox
                WHERE tenant_id=%s AND source_id=%s""",
                (self.tenant, self.sources['large'])).fetchone()['page_checkpoint']

    def test_restart_invalidates_generation_catalog_and_destination(self):
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        original = namespace.write
        sent = []
        def record(**kwargs):
            sent.extend(row['id'] for row in kwargs.get('upsert_rows', []))
            return original(**kwargs)
        with patch.object(namespace, 'write', side_effect=record):
            self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
            first = sent[0]
            for changed in ('generation', 'queued_at', 'reason', 'catalog', 'destination', 'version'):
                with self.subTest(changed=changed):
                    with self.store.connect() as connection:
                        if changed == 'generation':
                            connection.execute("""UPDATE search_projection_outbox SET generation=generation+1
                                WHERE tenant_id=%s""", (self.tenant,))
                        elif changed == 'queued_at':
                            connection.execute("UPDATE search_projection_outbox SET queued_at=queued_at+interval '1 second' WHERE tenant_id=%s AND source_id=%s", (self.tenant,self.sources['large']))
                        elif changed == 'reason':
                            connection.execute("UPDATE search_projection_outbox SET reason='header-change' WHERE tenant_id=%s", (self.tenant,))
                        elif changed == 'catalog':
                            connection.execute("""INSERT INTO search_projection_shards
                                (tenant_id,source_id,month,generation,dataset_uri,row_count,built_at)
                                VALUES (%s,%s,'2026-07-01',1,'turbopuffer://different',0,'2020-01-01')""",
                                (self.tenant,self.sources['large']))
                        elif changed == 'version':
                            connection.execute("""UPDATE search_projection_outbox
                                SET page_checkpoint=jsonb_set(page_checkpoint,'{scope,version}','999')
                                WHERE tenant_id=%s""", (self.tenant,))
                    if changed == 'destination':
                        self.settings = replace(self.settings, embed_dims=1024)
                    self.restart()
                    self.assertEqual(self.writer.drain_quantum(tenant_id=self.tenant,max_months=1)['rows'],1)
                    self.assertEqual(sent[-1],first)

    def test_partial_parallel_sdk_ack_never_advances_checkpoint(self):
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        original = namespace.write
        calls = []
        barrier = threading.Barrier(2)
        self.writer.write_concurrency = 2
        self.writer.page_rows = 2
        def lost_ack(**kwargs):
            ids = [row['id'] for row in kwargs.get('upsert_rows', [])]
            original(**kwargs)
            calls.extend(ids)
            first = len(calls) == 1
            barrier.wait(timeout=5)
            if first:
                raise RuntimeError('synthetic lost vendor ACK after accepted write')
        with patch.object(namespace, 'write', side_effect=lost_ack):
            self.assertEqual(self.writer.drain_quantum(tenant_id=self.tenant,max_months=1)['failed'],1)
        self.assertEqual(len(calls),2, 'all parallel writes must settle before returning')
        self.assertIsNone(self.checkpoint())
        self.restart()
        retried = []
        def record(**kwargs):
            retried.extend(row['id'] for row in kwargs.get('upsert_rows', []))
            return original(**kwargs)
        with patch.object(namespace, 'write', side_effect=record):
            self.writer.drain_quantum(tenant_id=self.tenant,max_months=1)
        self.assertIn(retried[0],calls)

    def test_checkpoint_failure_before_or_after_sql_commit_never_skips_unacked_page(self):
        for committed in (False, True):
            with self.subTest(committed=committed):
                self.restart()
                before = self.checkpoint()
                save = self.writer._save_page_checkpoint
                connect = self.store.connect
                def unknown(*args, **kwargs):
                    class Connection:
                        def __init__(self, native):
                            self.native = native
                        def __getattr__(self, key):
                            return getattr(self.native, key)
                        def commit(self):
                            if committed:
                                self.native.commit()
                            else:
                                self.native.rollback()
                            raise RuntimeError('synthetic SQL commit acknowledgement lost')
                    @contextmanager
                    def wrapped():
                        with connect() as native:
                            yield Connection(native)
                    with patch.object(self.store,'connect',side_effect=wrapped):
                        return save(*args, **kwargs)
                with patch.object(self.writer,'_save_page_checkpoint',side_effect=unknown):
                    result = self.writer.drain_quantum(tenant_id=self.tenant,max_months=1)
                self.assertEqual(result['failed'],1)
                after = self.checkpoint()
                self.assertEqual(after == before, not committed)
                self.restart()
                self.assertEqual(self.writer.drain_quantum(tenant_id=self.tenant,max_months=1)['failed'],0)

    def test_restored_page_still_checks_current_receipt_authority(self):
        with patch.object(self.store,'verified_singleton_receipts',True):
            self.writer.drain_quantum(tenant_id=self.tenant,max_months=1)
            checkpoint = self.checkpoint()
            with self.store.connect() as connection:
                claim = next(row for row in self.writer.claim_months(connection,tenant_id=self.tenant,limit=10)
                             if row['source_id'] == self.sources['large'])
                after = self.writer._restore_page_checkpoint(checkpoint,checkpoint['scope'])[0]
                page = self.writer.passage_page(connection,claim,since=None,after=after)
                receipt = page[0]['receipts'][0]
                missing = page[0]['passage_id']
                connection.execute("""UPDATE canonical_documents SET is_current=false,deleted_at=clock_timestamp()
                    WHERE tenant_id=%s AND source_id=%s AND document_id IN (
                        SELECT document_id FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s AND receipt=%s)""",
                    (self.tenant,self.sources['large'],self.tenant,self.sources['large'],receipt))
            self.restart()
            checked = []
            authorize = self.writer._authorized_publication_rows
            def observe(connection, tenant, rows, **kwargs):
                checked.extend(row['passage_id'] for row in rows)
                return authorize(connection,tenant,rows,**kwargs)
            with patch.object(self.writer,'_authorized_publication_rows',side_effect=observe):
                result = self.writer.drain_quantum(tenant_id=self.tenant,max_months=1)
            self.assertIn(missing,checked)
            self.assertEqual(result['rows'],0)
            self.assertNotIn(missing,self.client.namespace(self.settings.namespace(self.tenant)).rows)

    def test_late_native_arrival_invalidates_checkpoint_and_preserves_full_eof(self):
        self.writer.drain_quantum(tenant_id=self.tenant,max_months=1)
        self.assertIsNotNone(self.checkpoint())
        with self.store.connect() as connection:
            insert_record(connection,tenant=self.tenant,source=self.sources['large'],
                parent='late-behind-cursor',native='late-behind-cursor',text='Late native finding.',
                role='user',byte_start=0)
            mark_logical_evidence_dirty(connection,tenant_id=self.tenant,source_id=self.sources['large'],
                native_ids=['late-behind-cursor'],reason='ingest')
        self.logical.project_pending(batch_size=10,max_batches=1,upload_concurrency=2)
        self.passages.project_pending(batch_size=10,max_batches=1,concurrency=2)
        checkpoint=self.checkpoint()
        self.assertIsNotNone(checkpoint,'enqueue preserves opaque progress until the publisher revalidates it')
        with self.store.connect() as connection:
            generation=connection.execute('SELECT generation FROM search_projection_outbox WHERE tenant_id=%s AND source_id=%s',
                (self.tenant,self.sources['large'])).fetchone()['generation']
        self.assertNotEqual(checkpoint['scope']['claim'][3],generation,'real producer must invalidate checkpoint scope')
        self.restart()
        for _ in range(16):
            result=self.writer.drain_quantum(tenant_id=self.tenant,max_months=1)
            if not result['pending']:
                break
        self.assertEqual(result['pending'],0)
        with self.store.connect() as connection:
            expected={row['passage_id'] for row in connection.execute(
                'SELECT passage_id FROM canonical_passages WHERE tenant_id=%s',(self.tenant,))}
        self.assertEqual(set(self.client.namespace(self.settings.namespace(self.tenant)).rows),expected)

    def test_eof_commit_failure_retains_page_progress_and_original_watermark(self):
        with patch.object(self.writer,'finish_month',side_effect=RuntimeError('EOF failed before commit')):
            for _ in range(10):
                result=self.writer.drain_quantum(tenant_id=self.tenant,max_months=1)
                if result['failed']:
                    break
        self.assertEqual(result['failed'],1)
        checkpoint=self.checkpoint()
        self.assertIsNotNone(checkpoint)
        self.restart()
        namespace=self.client.namespace(self.settings.namespace(self.tenant))
        with patch.object(namespace,'write',side_effect=AssertionError('ACKed pages replayed after EOF failure')):
            result=self.writer.drain_quantum(tenant_id=self.tenant,max_months=1)
        self.assertEqual(result['failed'],0)
        self.assertEqual(result['months'],1)
        with self.store.connect() as connection:
            catalog=connection.execute("""SELECT row_count,built_at FROM search_projection_shards
                WHERE tenant_id=%s AND source_id=%s AND month=%s""",
                (self.tenant,self.sources['large'],date(2026,7,1))).fetchone()
        self.assertEqual(catalog['row_count'],checkpoint['rows'])
        self.assertEqual(catalog['built_at'].isoformat(),checkpoint['watermark'])

    def test_model_region_namespace_changes_restart_from_beginning(self):
        self.writer.drain_quantum(tenant_id=self.tenant,max_months=1)
        first=self.checkpoint()['after'][1]
        for field,value in [('embed_model','voyage/new-model'),('region','aws-other'),('namespace_prefix','other')]:
            with self.subTest(field=field):
                self.settings=replace(self.settings,**{field:value})
                self.restart()
                namespace=self.client.namespace(self.settings.namespace(self.tenant))
                write=namespace.write
                ids=[]
                def record(**kwargs):
                    ids.extend(row['id'] for row in kwargs.get('upsert_rows',[]))
                    return write(**kwargs)
                with patch.object(namespace,'write',side_effect=record):
                    result=self.writer.drain_quantum(tenant_id=self.tenant,max_months=1)
                self.assertEqual(result['rows'],1)
                self.assertEqual(ids,[first])

    def test_checkpoint_is_invisible_until_every_parallel_ack(self):
        self.writer.write_concurrency=2
        self.writer.page_rows=2
        namespace=self.client.namespace(self.settings.namespace(self.tenant))
        write=namespace.write
        entered=threading.Barrier(3)
        first_done,release=threading.Event(),threading.Event()
        lock=threading.Lock()
        calls=[]
        def hold(**kwargs):
            with lock:
                calls.append(kwargs['upsert_rows'][0]['id'])
                ordinal=len(calls)
            entered.wait(timeout=5)
            if ordinal == 2:
                if not release.wait(5):
                    raise AssertionError('second ACK not released')
            result=write(**kwargs)
            if ordinal == 1:
                first_done.set()
            return result
        with patch.object(namespace,'write',side_effect=hold), ThreadPoolExecutor(max_workers=1) as executor:
            future=executor.submit(self.writer.drain_quantum,tenant_id=self.tenant,max_months=1)
            try:
                entered.wait(timeout=5)
                self.assertTrue(first_done.wait(5))
                self.assertIsNone(self.checkpoint())
                self.assertFalse(future.done())
            finally:
                release.set()
            self.assertEqual(future.result(timeout=5)['rows'],2)
        self.assertEqual(self.checkpoint()['rows'],2)
        self.restart()
        result=self.writer.drain_quantum(tenant_id=self.tenant,max_months=1)
        self.assertEqual(result['rows'],1)

    def test_superseded_checkpoint_cannot_move_backwards(self):
        self.writer.drain_quantum(tenant_id=self.tenant,max_months=1)
        first=self.checkpoint()
        with self.store.connect() as connection:
            claim=next(row for row in self.writer.claim_months(connection,tenant_id=self.tenant,limit=10)
                       if row['source_id']==self.sources['large'])
        self.writer.drain_quantum(tenant_id=self.tenant,max_months=1)
        second=self.checkpoint()
        with self.assertRaisesRegex(ValueError,'publication_checkpoint_scope_changed'):
            self.writer._save_page_checkpoint(claim,None,first)
        self.assertEqual(self.checkpoint(),second)

    def test_native_per_page_checkpoint_cost(self):
        # Existing native source/authority fixture; no vendor or performance threshold.
        elapsed=[]
        save=self.writer._save_page_checkpoint
        def measure(*args,**kwargs):
            started=time.perf_counter()
            result=save(*args,**kwargs)
            elapsed.append((time.perf_counter()-started)*1000)
            return result
        with patch.object(self.writer,'_save_page_checkpoint',side_effect=measure):
            for _ in range(12):
                result=self.writer.drain_quantum(tenant_id=self.tenant,max_months=1)
                if not result['pending']:
                    break
        self.assertEqual(result['pending'],0)
        self.assertEqual(len(elapsed),4)
        print('CHECKPOINT_NATIVE_COST '+json.dumps({'pages':len(elapsed),'wall_ms':elapsed,
            'scope':'local disposable PG, four one-row pages; not production latency',
            'sql_per_successful_page':{'statement_deadline':1,'compare_and_set_update':1,'commit':1}}))

    def test_malformed_checkpoint_replays_without_skipping(self):
        self.writer.drain_quantum(tenant_id=self.tenant,max_months=1)
        valid=self.checkpoint()
        first=valid['after'][1]
        variants=[None, [], {'scope':{}},
            {**valid,'rows':True}, {**valid,'rows':-1},
            {**valid,'after':[]}, {**valid,'after':['bad',first]},
            {**valid,'watermark':'bad'},
            {**valid,'scope':{**valid['scope'],'version':True}}]
        for value in variants:
            with self.subTest(value=value):
                with self.store.connect() as connection:
                    connection.execute("""UPDATE search_projection_outbox SET page_checkpoint=%s
                        WHERE tenant_id=%s AND source_id=%s""",
                        (Jsonb(copy.deepcopy(value)),self.tenant,self.sources['large']))
                self.restart()
                result=self.writer.drain_quantum(tenant_id=self.tenant,max_months=1)
                self.assertEqual(result['failed'],0)
                self.assertEqual(self.checkpoint()['after'][1],first)

    def test_changed_vendor_account_with_same_namespace_replays_into_empty_destination(self):
        with patch.object(self.store,'verified_singleton_receipts',True):
            self.writer.drain_quantum(tenant_id=self.tenant,max_months=1)
            first=self.checkpoint()['after'][1]
            self.settings=replace(self.settings,api_key='synthetic-second-vendor-account')
            self.client=FakeTurbopuffer()
            self.restart()
            namespace=self.client.namespace(self.settings.namespace(self.tenant))
            self.assertFalse(namespace.rows)
            result=self.writer.drain_quantum(tenant_id=self.tenant,max_months=1)
            self.assertEqual(result['failed'],0)
            self.assertIn(first,namespace.rows,'a different account must not skip the old account ACKed page')
            self.assertNotIn(self.settings.api_key,json.dumps(self.checkpoint()))

    def test_pre091_schema_native_enqueue_and_seed_remain_compatible(self):
        from recall_server.search_outbox import enqueue_search_outbox, seed_search_outbox
        with self.store.connect() as connection:
            connection.execute('ALTER TABLE search_projection_outbox DROP COLUMN page_checkpoint')
            connection.execute('DELETE FROM schema_migrations WHERE version=91')
        try:
            with self.store.connect() as connection:
                month=connection.execute('SELECT month FROM search_projection_outbox WHERE tenant_id=%s LIMIT 1',
                    (self.tenant,)).fetchone()['month']
                count=enqueue_search_outbox(connection,tenant_id=self.tenant,source_id=self.sources['large'],
                    months=[month],reason='logical-update')
                self.assertEqual(count,1)
                # SQL resolves the full conflict UPDATE even when the fixture has no shards.
                self.assertGreaterEqual(seed_search_outbox(connection,tenant_id=self.tenant),0)
        finally:
            self.store.migrate()
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM schema_migrations WHERE version=91').fetchone()['n'],1)

    def test_migration_records_version_once_and_preserves_checkpoint(self):
        self.writer.drain_quantum(tenant_id=self.tenant,max_months=1)
        before=self.checkpoint()
        self.assertIsNotNone(before)
        for _ in range(2):
            self.store.migrate()
            with self.store.connect() as connection:
                count=connection.execute('SELECT count(*) AS n FROM schema_migrations WHERE version=91').fetchone()['n']
            self.assertEqual(count,1,'migration must durably record version91')
            self.assertEqual(self.checkpoint(),before)


if __name__ == '__main__':
    suite = unittest.TestSuite(DurablePublication(name) for name in DurablePublication.__dict__
                               if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
