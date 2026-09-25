#!/usr/bin/env python3
"""Signed messages must survive logical→passage handoff under aged backlog."""
import time
import unittest

from e2e_notification_priority import (
    NotificationPriority, TENANT, SLACK_SOURCE, SLACK_WORKSPACE, post_slack, slack_event,
)


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
        timestamp = f'{int(time.time())}.000200'
        status, acknowledgement = post_slack(self.server,
            slack_event('Synthetic live notification ruby harbor.', timestamp))
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
