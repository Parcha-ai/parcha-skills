#!/usr/bin/env python3
"""Native page timeouts retain proven in-memory publication progress only."""
import gc
import time
import threading
import weakref
from concurrent.futures import ThreadPoolExecutor
import unittest
from unittest.mock import patch
from e2e_turbopuffer_quantum import CooperativePublication
from e2e_logical_parent_progress import insert_record
from recall_server.logical_evidence_projection import mark_logical_evidence_dirty


class SearchPageResume(CooperativePublication):
    def prepare_large_month(self):
        with self.store.connect() as connection:
            for index in range(40):
                native = f'resume-extra-{index}'
                insert_record(connection, tenant=self.tenant, source=self.sources['large'],
                    parent=native, native=native, text=f'Current resume evidence {index}.',
                    role='user', byte_start=0)
                mark_logical_evidence_dirty(connection, tenant_id=self.tenant,
                    source_id=self.sources['large'], native_ids=[native], reason='ingest')
        self.logical.project_pending(batch_size=100, max_batches=1, upload_concurrency=2)
        self.passages.project_pending(batch_size=100, max_batches=1, concurrency=2)
        with self.store.connect() as connection:
            connection.execute("""UPDATE search_projection_outbox SET reason='logical-update',
                queued_at=clock_timestamp()-interval '1 hour'
                WHERE tenant_id=%s AND source_id=%s""", (self.tenant, self.sources['large']))
            connection.execute("""INSERT INTO search_projection_tombstones
                (tenant_id,source_id,month,passage_id,deleted_at)
                SELECT %s,%s,'2026-07-01'::date,'psg_'||substr(encode(sha256(convert_to('deleted-'||i,'UTF8')),'hex'),1,32),clock_timestamp()
                FROM generate_series(1,40000) i""", (self.tenant, self.sources['large']))
            expected = {row['passage_id'] for row in connection.execute(
                'SELECT passage_id FROM canonical_passages WHERE tenant_id=%s', (self.tenant,))}
        return expected

    def test_page_timeout_after_41_pages_does_not_replay_deletes_or_skip_authority(self):
        expected = self.prepare_large_month()
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        original = self.writer._authorized_publication_rows
        attempted = []
        failed_ids = []
        def authorize(connection, tenant, rows, *, deadline_at):
            self.assertGreater(deadline_at-time.monotonic(), 0)
            self.assertLessEqual(deadline_at-time.monotonic(), 5)
            attempted.append(tuple(row['passage_id'] for row in rows))
            if len(attempted) == 42:
                failed_ids.extend(attempted[-1])
                # Actual PostgreSQL cancellation, translated by the original owner.
                self.store._execute_bounded(connection, 'SELECT pg_sleep(1)', (),
                    time.monotonic()+.002).fetchall()
                self.fail('native timeout did not fire')
            return original(connection, tenant, rows, deadline_at=deadline_at)
        with patch.object(self.store, 'verified_singleton_receipts', True), \
                patch.object(namespace, 'write', wraps=namespace.write) as writes, \
                patch.object(self.writer, '_authorized_publication_rows', side_effect=authorize):
            deleted = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
            self.assertEqual(deleted['deleted'], 40000)
            for _ in range(41):
                report = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
                self.assertEqual(report['rows'], 1)
                self.assertEqual(report['failed'], 0)
            failed = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
            self.assertEqual(failed['failed'], 1)
            self.assertFalse(any(pid in namespace.rows for pid in failed_ids))
            resumed = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
            self.assertEqual(resumed['deleted'], 0, 'page timeout replayed 40000 completed deletions')
            self.assertEqual(resumed['rows'], 1)
            self.assertEqual(attempted[-1], tuple(failed_ids), 'unverified failed page was skipped')
            for _ in range(8):
                report = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
                if report['pending'] == 0:
                    break
            self.assertEqual(report['pending'], 0)
        self.assertEqual(set(namespace.rows), expected)
        # Missing-namespace deletes are an idempotent native success; the fake
        # does not retain those in namespace.writes, so count actual SDK calls.
        self.assertEqual(sum(len(call.kwargs.get('deletes', ())) for call in writes.call_args_list), 40000)


    def pause_after_first_page(self):
        first = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
        self.assertEqual(first['rows'], 1)
        failed_ids = []
        def timeout(connection, tenant, rows, *, deadline_at):
            failed_ids.extend(row['passage_id'] for row in rows)
            self.store._execute_bounded(connection, 'SELECT pg_sleep(1)', (),
                time.monotonic()+.002).fetchall()
        with patch.object(self.writer, '_authorized_publication_rows', side_effect=timeout):
            failed = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
        self.assertEqual(failed['failed'], 1)
        return failed_ids

    def test_changed_generation_or_catalog_discards_resume_without_writing(self):
        for change in ('generation', 'catalog'):
            with self.subTest(change=change):
                if change == 'catalog':
                    self.setUp()
                with patch.object(self.store, 'verified_singleton_receipts', True):
                    self.pause_after_first_page()
                    namespace = self.client.namespace(self.settings.namespace(self.tenant))
                    writes = len(namespace.writes)
                    with self.store.connect() as connection:
                        if change == 'generation':
                            connection.execute('UPDATE search_projection_outbox SET generation=generation+1 WHERE tenant_id=%s AND source_id=%s',
                                (self.tenant, self.sources['large']))
                        else:
                            connection.execute("""INSERT INTO search_projection_shards
                                (tenant_id,source_id,month,generation,dataset_uri,row_count,built_at)
                                VALUES (%s,%s,'2026-07-01',1,'turbopuffer://synthetic/test',0,'2020-01-01')""",
                                (self.tenant, self.sources['large']))
                    rejected = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
                    self.assertEqual(rejected['failed'], 1)
                    self.assertEqual(rejected['rows'], 0)
                    self.assertEqual(len(namespace.writes), writes)
                    self.assertEqual(len(self.writer._quantum_months), 0)
                    for _ in range(10):
                        report = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
                        if not report['pending']:
                            break
                    self.assertEqual(report['pending'], 0)

    def test_resume_rereads_current_receipt_authority(self):
        with patch.object(self.store, 'verified_singleton_receipts', True):
            failed_ids = self.pause_after_first_page()
            with self.store.connect() as connection:
                # Independently invalidate the exact failed row's physical source;
                # unchanged queue deliberately tests the mandatory fresh proof.
                connection.execute("""UPDATE canonical_documents SET is_current=false,deleted_at=clock_timestamp()
                    WHERE tenant_id=%s AND source_id=%s AND document_id IN (
                      SELECT chunk.document_id FROM canonical_chunks chunk
                      JOIN canonical_passages passage ON passage.tenant_id=chunk.tenant_id
                       AND passage.source_id=chunk.source_id AND chunk.receipt=ANY(passage.receipts)
                      WHERE passage.tenant_id=%s AND passage.passage_id=ANY(%s::text[]))""",
                    (self.tenant,self.sources['large'],self.tenant,failed_ids))
            resumed = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
            self.assertEqual(resumed['failed'], 0)
            self.assertEqual(resumed['rows'], 0)
            namespace = self.client.namespace(self.settings.namespace(self.tenant))
            self.assertTrue(all(pid not in namespace.rows for pid in failed_ids))
            for _ in range(8):
                report = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
                if not report['pending']:
                    break
            self.assertEqual(report['pending'], 0)


    def test_deferred_page_releases_bodies_and_lease_and_yields_to_other_month(self):
        references = []
        class Row(dict):
            pass
        original_page = self.writer.passage_page
        def tracked(*args, **kwargs):
            rows = [Row(row) for row in original_page(*args, **kwargs)]
            references.extend(weakref.ref(row) for row in rows)
            return rows
        def timeout(connection, tenant, rows, *, deadline_at):
            self.store._execute_bounded(connection, 'SELECT pg_sleep(1)', (),
                time.monotonic()+.002).fetchall()
        with patch.object(self.store, 'verified_singleton_receipts', True):
            with patch.object(self.writer, 'passage_page', side_effect=tracked), \
                    patch.object(self.writer, '_authorized_publication_rows', side_effect=timeout), \
                    self.assertLogs('recall_server.turbopuffer_projection', level='WARNING') as logs:
                failed = self.writer.drain_quantum(tenant_id=self.tenant, max_months=2)
            self.assertEqual(failed['failed'], 1)
            self.assertTrue(any('page deferred' in message for message in logs.output))
            gc.collect()
            self.assertTrue(references)
            self.assertTrue(all(reference() is None for reference in references))
            # Native backend lock inspection avoids reentrant same-session locks
            # making a leaked publication lease look available.
            with self.store.connect() as connection:
                self.assertEqual(connection.execute("SELECT count(*) AS n FROM pg_locks "
                    "WHERE locktype='advisory' AND granted AND database="
                    "(SELECT oid FROM pg_database WHERE datname=current_database())").fetchone()['n'], 0)
            other = self.writer.drain_quantum(tenant_id=self.tenant, max_months=2)
            self.assertEqual(other['rows'], 1)
            namespace = self.client.namespace(self.settings.namespace(self.tenant))
            self.assertTrue(any(self.receipts['small'] in row['receipts'] for row in namespace.rows.values()))
            # Process shutdown abandons only resumable metadata; no queue ACK.
            while self.writer._quantum_months:
                _, steps, _ = self.writer._quantum_months.popleft()
                steps.close()
            with self.store.connect() as connection:
                self.assertTrue(connection.execute('SELECT 1 FROM search_projection_outbox WHERE tenant_id=%s AND source_id=%s',
                    (self.tenant, self.sources['large'])).fetchone())
                self.assertEqual(connection.execute("SELECT count(*) AS n FROM pg_locks "
                    "WHERE locktype='advisory' AND granted AND database="
                    "(SELECT oid FROM pg_database WHERE datname=current_database())").fetchone()['n'], 0)


    def test_parallel_unknown_write_settles_and_replays_original_page(self):
        from recall_server.turbopuffer_projection import TurbopufferProjector
        self.writer = TurbopufferProjector(self.store, self.settings, client=self.client,
            batch_rows=1, write_concurrency=2, tokens_per_minute=0)
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        original = namespace.write
        blocked, release = threading.Event(), threading.Event()
        count_lock = threading.Lock()
        calls = []
        def uncertain(**kwargs):
            with count_lock:
                calls.append(kwargs)
                ordinal = len(calls)
            if ordinal == 2:
                blocked.set()
                if not release.wait(8):
                    raise AssertionError('test failed to release second write')
            if ordinal == 1 and not blocked.wait(5):
                raise AssertionError('second write did not enter before first failure')
            result = original(**kwargs)
            if ordinal == 1:
                raise RuntimeError('synthetic lost first write acknowledgement')
            return result
        with patch.object(self.store, 'verified_singleton_receipts', True):
            with patch.object(namespace, 'write', side_effect=uncertain), ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self.writer.drain_quantum, tenant_id=self.tenant, max_months=1)
                try:
                    self.assertTrue(blocked.wait(5))
                    self.assertFalse(future.done(), 'owner returned before parallel write settled')
                    with self.store.connect() as connection:
                        self.assertEqual(connection.execute('SELECT count(*) AS n FROM search_projection_shards WHERE tenant_id=%s',
                            (self.tenant,)).fetchone()['n'], 0)
                finally:
                    release.set()
                failed = future.result(timeout=5)
            self.assertEqual(failed['failed'], 1)
            self.assertEqual(len(self.writer._quantum_months), 0)
            prior_ids = set(namespace.rows)
            self.assertEqual(len(prior_ids), 2)
            with patch.object(namespace, 'write', wraps=namespace.write) as replay:
                resumed = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
            self.assertEqual(resumed['rows'], 2)
            self.assertEqual(len(replay.call_args_list), 2)
            self.assertEqual(set(namespace.rows), prior_ids)
            for _ in range(8):
                report = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
                if not report['pending']:
                    break
            self.assertEqual(report['pending'], 0)


if __name__ == '__main__':
    names = [name for name in SearchPageResume.__dict__ if name.startswith('test_')]
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(
        unittest.TestSuite(SearchPageResume(name) for name in names)).wasSuccessful())
