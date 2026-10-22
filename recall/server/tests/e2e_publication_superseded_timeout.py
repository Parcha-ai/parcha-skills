#!/usr/bin/env python3
"""An acknowledged hot-month pass should survive a later page timeout."""
import time
from dataclasses import replace
import unittest
from unittest.mock import patch

from e2e_turbopuffer_quantum import CooperativePublication
from e2e_search_page_resume import SearchPageResume
from e2e_publication_guard_codes import PublicationGuardCodes
from recall_server.search_outbox import enqueue_search_outbox


class SupersededPassTimeout(CooperativePublication):
    def paused_successor(self):
        verified = patch.object(self.store, 'verified_singleton_receipts', True)
        verified.start()
        self.addCleanup(verified.stop)
        with self.store.connect() as connection:
            claim = self.writer.claim_months(connection, tenant_id=self.tenant, limit=1)[0]
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        original_save = self.writer._save_page_checkpoint
        saved = []

        def save(*args):
            outcome = original_save(*args)
            saved.append(outcome)
            return outcome

        def outbox():
            with self.store.connect() as connection:
                return connection.execute('''SELECT * FROM search_projection_outbox
                    WHERE tenant_id=%s AND source_id=%s AND month=%s''',
                    (self.tenant, claim['source_id'], claim['month'])).fetchone()

        def timeout(connection, *args, **kwargs):
            return self.store._execute_bounded(connection, 'SELECT pg_sleep(1)', (),
                time.monotonic() + .02).fetchall()

        with patch.object(self.writer, '_save_page_checkpoint', side_effect=save):
            first = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
            self.assertEqual((first['rows'], first['failed']), (1, 0))
            with self.store.connect() as connection:
                enqueue_search_outbox(connection, tenant_id=self.tenant,
                    source_id=claim['source_id'], months=[claim['month']], reason='logical-update')
            successor = outbox()
            self.assertGreater(successor['generation'], claim['generation'])
            second = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
            self.assertEqual((second['rows'], second['failed']), (1, 0))
            self.assertEqual(saved, [True, False], 'native CAS recognizes hot generation')
            self.assertEqual(outbox(), successor, 'old pass must not alter successor')
            sent = [row['id'] for call in namespace.writes for row in call.get('upsert_rows', [])]
            self.assertEqual(len(sent), 2)
            self.assertEqual(len(set(sent)), 2)
            with patch.object(self.writer, 'passage_page', side_effect=timeout):
                deferred = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
            self.assertEqual((deferred['rows'], deferred['failed']), (0, 1))
            self.assertEqual(len(self.writer._quantum_months), 1)
            self.assertEqual(outbox(), successor)
        return claim, namespace, outbox, successor, sent

    def test_recognized_successor_does_not_discard_live_acknowledged_cursor(self):
        claim, namespace, outbox, successor, sent = self.paused_successor()
        resumed = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
        self.assertEqual(outbox(), successor, 'resume may not ACK the newer generation')
        self.assertEqual(resumed['failed'], 0,
            'recognized superseded live pass lost its acknowledged cursor on timeout')
        self.assertEqual(resumed['rows'], 1)
        after = [row['id'] for call in namespace.writes for row in call.get('upsert_rows', [])]
        self.assertEqual(after[:2], sent)
        self.assertEqual(len(after), len(set(after)), 'ACKed prefix must not be replayed')
        completed = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
        self.assertEqual(completed['months'], 1)
        self.assertEqual(completed['requeued'], 1)
        self.assertGreater(outbox()['generation'], claim['generation'])

    def refuse(self, change):
        claim, namespace, outbox, successor, sent = self.paused_successor()
        key = (self.tenant, claim['source_id'], claim['month'])
        with self.store.connect() as connection:
            if change in ('vanished', 'recreated'):
                connection.execute('DELETE FROM search_projection_outbox WHERE tenant_id=%s AND source_id=%s AND month=%s', key)
                if change == 'recreated':
                    connection.execute('''INSERT INTO search_projection_outbox
                        (tenant_id,source_id,month,generation,reason,queued_at,first_queued_at)
                        VALUES (%s,%s,%s,%s,'logical-update',clock_timestamp(),clock_timestamp())''',
                        (*key, successor['generation'] + 10))
            elif change == 'same-generation':
                connection.execute('''UPDATE search_projection_outbox SET generation=%s,
                    queued_at=clock_timestamp() WHERE tenant_id=%s AND source_id=%s AND month=%s''',
                    (claim['generation'], *key))
            elif change == 'catalog':
                connection.execute('''INSERT INTO search_projection_shards
                    (tenant_id,source_id,month,generation,dataset_uri,row_count,built_at)
                    VALUES (%s,%s,%s,1,'turbopuffer://synthetic/other',0,clock_timestamp())
                    ON CONFLICT(tenant_id,source_id,month) DO UPDATE SET built_at=excluded.built_at''', key)
        if change == 'missing-episode':
            self.writer._quantum_months[0][0].pop('first_queued_at')
        elif change == 'destination':
            self.writer.settings = replace(self.writer.settings, embed_dims=1024)
        elif change == 'account':
            self.writer.settings = replace(self.writer.settings, api_key='different-synthetic-account')
        elif change == 'singleton':
            self.store.verified_singleton_receipts = False
        before = outbox()
        with self.assertLogs('recall_server.turbopuffer_projection', level='WARNING') as logged:
            refused = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
        self.assertEqual((refused['rows'], refused['failed']), (0, 1))
        self.assertEqual(outbox(), before)
        self.assertEqual([row['id'] for call in namespace.writes for row in call.get('upsert_rows', [])], sent)
        self.assertEqual(len(self.writer._quantum_months), 0)
        self.assertTrue(any('publication_resume_scope_changed' in line for line in logged.output))

    def test_vanished_queue_refused(self):
        self.refuse('vanished')

    def test_recreated_higher_generation_has_different_episode(self):
        self.refuse('recreated')

    def test_same_generation_changed_identity_refused(self):
        self.refuse('same-generation')

    def test_missing_episode_identity_refused(self):
        self.refuse('missing-episode')

    def test_catalog_advance_refused(self):
        self.refuse('catalog')

    def test_destination_drift_refused(self):
        self.refuse('destination')

    def test_account_drift_refused(self):
        self.refuse('account')

    def test_singleton_contract_drift_refused(self):
        self.refuse('singleton')


if __name__ == '__main__':
    suite = unittest.TestSuite([
        *(SupersededPassTimeout(name) for name in SupersededPassTimeout.__dict__ if name.startswith('test_')),
        SearchPageResume('test_changed_generation_or_catalog_discards_resume_without_writing'),
        PublicationGuardCodes('test_timeout_then_generation_change_logs_guard_and_keeps_work'),
    ])
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
