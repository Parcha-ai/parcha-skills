#!/usr/bin/env python3
"""Signed messages must survive logical→passage handoff under aged backlog."""
import time
import unittest
from unittest.mock import patch

from e2e_notification_priority import (
    NotificationPriority, TENANT, SLACK_SOURCE, SLACK_WORKSPACE, post_slack, slack_event,
)
from recall_server.logical_evidence_projection import mark_logical_evidence_dirty


class NotificationPassageHandoff(NotificationPriority):
    def setUp(self):
        super().setUp()
        self.history(1000, 20)
        result = self.logical.project_pending(batch_size=20, max_batches=1, upload_concurrency=1)
        self.assertEqual(result['documents'], 20)
        with self.store.connect() as connection:
            count = connection.execute("""UPDATE canonical_passage_projection_queue
                SET changed_at=clock_timestamp()-interval '10 minutes'
                WHERE tenant_id=%s AND source_id=%s""", (TENANT, SLACK_SOURCE)).rowcount
        self.assertEqual(count, 20)
        self.timestamp_seconds = int(time.time())
        timestamp = f'{self.timestamp_seconds}.000100'
        self.raw_notification = slack_event('Synthetic live notification ruby harbor.', timestamp)
        status, acknowledgement = post_slack(self.server,
            self.raw_notification)
        self.assertEqual((status, acknowledgement['routes']), (200, 1))
        self.native = f'slack:{SLACK_WORKSPACE}:C0E2E:{timestamp}'
        with self.store.connect() as connection:
            self.receipt = connection.execute('''SELECT chunk.receipt FROM canonical_chunks chunk
                JOIN canonical_documents document USING(tenant_id,source_id,document_id)
                WHERE document.tenant_id=%s AND document.source_id=%s AND document.native_id=%s
                  AND document.is_current AND document.deleted_at IS NULL AND chunk.deleted_at IS NULL''',
                (TENANT, SLACK_SOURCE, self.native)).fetchone()['receipt']

    def hits(self):
        return self.retrieval.search('notification ruby harbor', lexical_query='notification ruby harbor',
            since=None, until=None, limit=10)['results']

    def contains_notification(self):
        return any(self.receipt in item.get('receipts', ()) for hit in self.hits()
                   for item in hit['matching_ranges'])

    def test_bounded_handoff_publishes_notification_before_aged_backlog_drains(self):
        passage_results = []
        def bounded_publish():
            passage_results.append(self.passages.project_pending(batch_size=1, max_batches=2,
                concurrency=1, on_progress=lambda: self.search_writer.drain(tenant_id=TENANT, max_months=4)))
        result = self.logical.project_pending(batch_size=1, max_batches=1,
            upload_concurrency=1, on_progress=bounded_publish)
        self.assertEqual(result['documents'], 1)
        self.assertIsNone(self.queue(self.native), 'notification must reach the logical→passage boundary')
        self.assertEqual(sum(item['documents'] for item in passage_results), 2)
        with self.store.connect() as connection:
            remaining = connection.execute('''SELECT count(*) AS n
                FROM canonical_passage_projection_queue WHERE tenant_id=%s AND source_id=%s
                  AND changed_at<clock_timestamp()-interval '5 minutes' ''',
                (TENANT, SLACK_SOURCE)).fetchone()['n']
            passage_count = connection.execute('''SELECT count(*) AS n FROM canonical_passages
                WHERE tenant_id=%s AND source_id=%s AND %s=ANY(receipts)''',
                (TENANT, SLACK_SOURCE, self.receipt)).fetchone()['n']
        self.assertGreater(remaining, 0, 'acceptance cannot drain the entire passage backlog')
        self.assertLess(remaining, 20, 'aged history must retain bounded progress')
        print(f'notification-handoff-proof aged_remaining={remaining} target_passages={passage_count}', flush=True)
        self.assertTrue(self.contains_notification(),
            'logically committed signed notification was stranded behind aged passage backlog')

    def passage_queue(self):
        with self.store.connect() as connection:
            return connection.execute("""SELECT queue.* FROM canonical_passage_projection_queue queue
                JOIN canonical_evidence_documents evidence USING(tenant_id,source_id,logical_document_id)
                WHERE queue.tenant_id=%s AND queue.source_id=%s AND evidence.native_parent_id=%s""",
                (TENANT, SLACK_SOURCE, self.native.replace('slack:', 'slack-thread:', 1))).fetchone()

    def project_notification(self):
        return self.logical.project_pending(batch_size=1, max_batches=1, upload_concurrency=1)

    def queue_repair(self):
        with self.store.connect() as connection:
            mark_logical_evidence_dirty(connection, tenant_id=TENANT, source_id=SLACK_SOURCE,
                native_ids=[self.native], reason='backfill')

    def test_oldest_logical_round_carries_locked_stamp_and_history_cannot_demote_it(self):
        before = self.queue(self.native)['notification_queued_at']
        candidate = self.logical._pending(tenant_id=TENANT, limit=1, prefer_recent=False)[0]
        self.assertIsNone(candidate.notification_queued_at, 'oldest admission deliberately masks priority')
        self.logical._prefer_recent_admission = False
        self.assertEqual(self.project_notification()['documents'], 1)
        first = self.passage_queue()
        self.assertEqual(first['notification_queued_at'], before)
        self.history(self.timestamp_seconds - 1700000000, 1,
            text='Synthetic corrected historical content for the same notification.')
        self.assertIsNone(self.queue(self.native)['notification_queued_at'])
        self.assertEqual(self.project_notification()['documents'], 1)
        later = self.passage_queue()
        self.assertGreater(later['generation'], first['generation'])
        self.assertGreater(later['revision'], first['revision'])
        self.assertEqual(later['notification_queued_at'], before)

    def test_repaired_commit_uses_late_locked_stamp_without_generation_or_revision_churn(self):
        self.project_notification()
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_passage_projection_queue SET notification_queued_at=NULL')
        before = self.passage_queue()
        self.queue_repair()
        self.assertIsNone(self.queue(self.native)['notification_queued_at'])
        original = self.logical._commit_upload
        def notification_after_prepare(candidate, upload):
            self.assertEqual(post_slack(self.server, self.raw_notification, retry='1')[0], 200)
            return original(candidate, upload)
        self.logical._prefer_recent_admission = False
        with patch.object(self.logical, '_commit_upload', notification_after_prepare):
            result = self.project_notification()
        self.assertEqual((result['documents'], result['repaired']), (0, 1))
        after = self.passage_queue()
        self.assertIsNotNone(after['notification_queued_at'])
        for field in ('revision', 'generation', 'changed_at', 'reason'):
            self.assertEqual(after[field], before[field], field)
        self.assertIsNone(self.queue(self.native))

    def test_repaired_priority_does_not_recreate_completed_passage_work(self):
        self.project_notification()
        self.passages.project_pending(batch_size=30, max_batches=1, concurrency=1)
        self.assertIsNone(self.passage_queue())
        self.queue_repair()
        self.assertEqual(post_slack(self.server, self.raw_notification, retry='1')[0], 200)
        self.assertEqual(self.project_notification()['repaired'], 1)
        self.assertIsNone(self.passage_queue())

    def test_normal_and_notification_rounds_persist_across_single_batch_calls(self):
        self.project_notification()
        first = self.passages.project_pending(batch_size=1, max_batches=1, concurrency=1)
        self.assertEqual(first['documents'], 1)
        self.assertIsNotNone(self.passage_queue(), 'first normal round must serve aged history')
        second = self.passages.project_pending(batch_size=1, max_batches=1, concurrency=1)
        self.assertEqual(second['documents'], 1)
        self.assertIsNone(self.passage_queue(), 'notification round must survive worker callbacks')
        self.search_writer.drain(tenant_id=TENANT, max_months=4)
        self.assertTrue(self.contains_notification())
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_passage_projection_queue').fetchone()['n'], 19)
        third = self.passages.project_pending(batch_size=1, max_batches=1, concurrency=1)
        self.assertEqual(third['documents'], 1, 'normal history resumes after notification')

    def test_notification_fifo_precedes_size_within_notification_round(self):
        self.project_notification()
        later = self.notify(self.timestamp_seconds - 1700000000 + 1, 'Tiny later notification.')
        self.assertIsNotNone(later)
        self.project_notification()
        first = self.passage_queue()
        self.passages._prefer_notification_admission = True
        selected = self.passages._pending(tenant_id=TENANT, limit=1)
        self.assertEqual([item.logical_document_id for item in selected], [first['logical_document_id']])

    def test_stale_logical_commit_does_not_handoff_priority_or_consume_forget(self):
        original = self.logical._commit_upload
        def forget_after_prepare(candidate, upload):
            with self.store.connect() as connection:
                connection.execute('UPDATE canonical_chunks SET deleted_at=clock_timestamp() WHERE receipt=%s', (self.receipt,))
                mark_logical_evidence_dirty(connection, tenant_id=TENANT, source_id=SLACK_SOURCE,
                    native_ids=[self.native], reason='forget')
            return original(candidate, upload)
        with patch.object(self.logical, '_commit_upload', forget_after_prepare):
            result = self.project_notification()
        self.assertEqual((result['documents'], result['source_races']), (0, 1))
        self.assertIsNone(self.passage_queue())
        self.assertEqual(self.queue(self.native)['reason'], 'forget')
        with self.assertRaisesRegex(PermissionError, 'tenant'):
            self.passages.project_pending(tenant_id='synthetic:foreign')

    def test_priority_does_not_bypass_passage_commit_generation_fence(self):
        self.project_notification()
        before = self.passage_queue()
        self.passages._prefer_notification_admission = True
        original = self.passages._commit
        def supersede_before_commit(prepared):
            with self.store.connect() as connection:
                connection.execute("""UPDATE canonical_passage_projection_queue SET generation=generation+1
                    WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s""",
                    (TENANT, SLACK_SOURCE, before['logical_document_id']))
            return original(prepared)
        with patch.object(self.passages, '_commit', supersede_before_commit):
            result = self.passages.project_pending(batch_size=1, max_batches=1, concurrency=1)
        self.assertEqual((result['documents'], result['stale']), (0, 1))
        after = self.passage_queue()
        self.assertEqual(after['generation'], before['generation'] + 1)
        self.assertEqual(after['notification_queued_at'], before['notification_queued_at'])
        self.search_writer.drain(tenant_id=TENANT, max_months=4)
        self.assertFalse(self.contains_notification())

    def test_full_drain_control_proves_notification_body_and_authority_are_valid(self):
        self.logical.project_pending(batch_size=1, max_batches=1, upload_concurrency=1)
        result = self.passages.project_pending(batch_size=30, max_batches=1, concurrency=1)
        self.assertEqual(result['documents'], 21)
        self.search_writer.drain(tenant_id=TENANT, max_months=4)
        self.assertTrue(self.contains_notification())


if __name__ == '__main__':
    suite = unittest.TestSuite(NotificationPassageHandoff(name)
        for name in NotificationPassageHandoff.__dict__ if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
