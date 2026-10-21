#!/usr/bin/env python3
"""Live Slack capture commits before bounded historical attribution work."""
import json
import sqlite3
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from connectors.sdk import ConnectorPage
from connectors.slack_source import normalize_slack_message
from e2e_managed_slack_captured_actors import CapturedSlackActors


class CaptureBeforeRepair(CapturedSlackActors):
    def test_live_page_is_attributed_and_acked_before_historical_archive_read(self):
        worker, _ = self.captured()
        fresh = normalize_slack_message(workspace_id='T123', channel_id='C123', value={
            'ts': '1789981201.000001', 'user': 'U123', 'text': 'Synthetic fresh message'})
        source, contacts = self.source, self.contacts

        class FreshPage:
            source_id = source
            connector_id = 'slack.messages'

            def pull(self, cursor):
                return ConnectorPage((*contacts, fresh), 'fresh-page-acked', True)

        worker.connector_factory = lambda *_: (FreshPage(), self.spool)
        observed = []
        original = worker.archive.read_raw

        def historical_read(reference):
            with sqlite3.connect(self.spool) as db:
                cursor = json.loads(db.execute(
                    "SELECT value FROM meta WHERE key='committed_cursor'").fetchone()[0])
                pending = db.execute('SELECT count(*) FROM pages').fetchone()[0]
            with self.store.connect() as connection:
                attributed = connection.execute('''SELECT 1 FROM canonical_events event
                    JOIN canonical_event_actors actor USING(tenant_id,source_id,event_id)
                    WHERE event.tenant_id=%s AND event.source_id=%s AND event.native_id=%s
                    AND actor.actor_id=%s AND actor.relation='author' ''',
                    (self.tenant, self.source, fresh.native_id, self.actor)).fetchone() is not None
            observed.append((cursor, pending, attributed))
            return original(reference)

        with patch.object(worker.archive, 'read_raw', side_effect=historical_read):
            result = self.step(worker)
        self.assertEqual(result['failed'], 0, result)
        self.assertGreater(result['acked'], 0)
        self.assertTrue(observed, 'Native historical repair must actually read an archive')
        self.assertTrue(all(item == ('fresh-page-acked', 0, True) for item in observed), observed)
        self.assertGreater(result['captured_attribution']['examined'], 0)

    def test_slow_capture_preserves_repair_checkpoint_until_next_available_turn(self):
        from recall_server import managed_worker, slack_captured_attribution as captured
        worker, _ = self.captured()
        before = self.captured_scheduling_rows()
        source = self.source
        clock = [1000.0]

        class SlowPage:
            source_id = source
            connector_id = 'slack.messages'

            def pull(self, cursor):
                # Only the worker's scheduling clock advances. Native SQL,
                # lease authority and SDK ACK transactions remain real.
                clock[0] += worker.interval_seconds
                return ConnectorPage((), 'slow-page-acked', True)

        live_factory = worker.connector_factory
        worker.connector_factory = lambda *_: (SlowPage(), self.spool)
        with patch.object(managed_worker, 'time', SimpleNamespace(monotonic=lambda: clock[0])), patch.object(
                captured, 'repair_slice', wraps=captured.repair_slice) as repair:
            result = self.step(worker)
        self.assertEqual(result['failed'], 0, result)
        self.assertEqual(repair.call_count, 0)
        self.assertEqual(self.captured_scheduling_rows(), before)
        with sqlite3.connect(self.spool) as db:
            self.assertEqual(json.loads(db.execute(
                "SELECT value FROM meta WHERE key='committed_cursor'").fetchone()[0]), 'slow-page-acked')
        # The next ordinary turn still owns and advances the retained repair.
        worker.connector_factory = live_factory
        resumed = self.step(worker)
        self.assertEqual(resumed['failed'], 0, resumed)
        self.assertGreater(resumed['captured_attribution']['examined'], 0)
        self.assertNotEqual(self.captured_scheduling_rows(), before)


if __name__ == '__main__':
    suite = unittest.TestSuite(CaptureBeforeRepair(name) for name in CaptureBeforeRepair.__dict__
                               if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
