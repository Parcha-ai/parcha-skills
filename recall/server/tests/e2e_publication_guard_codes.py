#!/usr/bin/env python3
"""Publication refusals remain durable and log only fixed guard codes."""
import time
import unittest
from unittest.mock import patch

from e2e_search_durable_page import DurablePublication
from recall_server import search_parent_reconciliation
from recall_server.search_outbox import enqueue_search_outbox


class PublicationGuardCodes(DurablePublication):
    def test_timeout_then_generation_change_logs_guard_and_keeps_work(self):
        with self.store.connect() as connection:
            claim = self.writer.claim_months(connection, tenant_id=self.tenant, limit=1)[0]
            expected_ids = {row['passage_id'] for row in connection.execute(
                'SELECT passage_id FROM canonical_passages WHERE tenant_id=%s',
                (self.tenant,)).fetchall()}

        def deadline(connection, *args, **kwargs):
            return self.store._execute_bounded(
                connection, 'SELECT pg_sleep(1)', (), time.monotonic() + 0.02).fetchall()

        with patch.object(self.store, 'verified_singleton_receipts', True):
            with patch.object(self.writer, 'passage_page', side_effect=deadline):
                deferred = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
            self.assertEqual(deferred['failed'], 1)
            with self.store.connect() as connection:
                enqueue_search_outbox(connection, tenant_id=self.tenant,
                    source_id=claim['source_id'], months=[claim['month']], reason='logical-update')
                before = connection.execute('''SELECT * FROM search_projection_outbox
                    WHERE tenant_id=%s AND source_id=%s AND month=%s''',
                    (self.tenant, claim['source_id'], claim['month'])).fetchone()
            self.assertGreater(before['generation'], claim['generation'])
            with self.assertLogs('recall_server.turbopuffer_projection', level='WARNING') as logged:
                refused = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
            self.assertEqual(refused['failed'], 1)
            with self.store.connect() as connection:
                after = connection.execute('''SELECT * FROM search_projection_outbox
                    WHERE tenant_id=%s AND source_id=%s AND month=%s''',
                    (self.tenant, claim['source_id'], claim['month'])).fetchone()
            self.assertEqual(after, before, 'Refusal must preserve the newer durable work.')
            # The failed generator was removed; ordinary turns reclaim the
            # newer generation without replacing the projector or process.
            for _ in range(12):
                outcome = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
                if not outcome['pending']:
                    break
            self.assertEqual(outcome['pending'], 0)
            self.assertEqual(outcome['failed'], 0)
            namespace = self.client.namespace(self.settings.namespace(self.tenant))
            self.assertTrue(expected_ids <= set(namespace.rows))
        self.assertTrue(any('code=publication_resume_scope_changed' in line for line in logged.output),
                        logged.output)

    def test_all_catch_sites_withhold_arbitrary_exception_text(self):
        secret = 'private source text https://private.example/token=secret'
        for mode in ('parent', 'month', 'quantum'):
            for error, code in ((ValueError(secret), 'unknown'),
                                (ValueError('search_publication_busy'), 'search_publication_busy'),
                                (RuntimeError('search_publication_busy'), 'unknown'),
                                (ValueError('search_publication_busy', secret), 'unknown'),
                                (ValueError('search_publication_busy ' + secret), 'unknown'),
                                (ValueError([secret]), 'unknown')):
                with self.subTest(mode=mode, args=error.args):
                    self.restart()
                    if mode == 'parent':
                        target, method = search_parent_reconciliation, 'step'
                        call = lambda: self.writer._reconcile_parent(self.tenant, {'failed': 0, 'rows': 0})
                        failure = error
                    elif mode == 'month':
                        target, method = self.writer, 'project_month'
                        call = lambda: self.writer.drain(tenant_id=self.tenant, max_months=1)
                        failure = error
                    else:
                        target, method = self.writer, '_month_steps'
                        call = lambda: self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
                        def failure(*args, **kwargs):
                            yield from ()
                            raise error
                    with patch.object(target, method, side_effect=failure), \
                            self.assertLogs('recall_server.turbopuffer_projection', level='WARNING') as logged:
                        call()
                    matching = [line for line in logged.output if ' failed ' in line]
                    self.assertTrue(matching, logged.output)
                    self.assertTrue(all('code=' + code in line for line in matching), matching)
                    self.assertNotIn(secret, '\n'.join(logged.output))


if __name__ == '__main__':
    suite = unittest.TestSuite(PublicationGuardCodes(name) for name in PublicationGuardCodes.__dict__
                               if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
