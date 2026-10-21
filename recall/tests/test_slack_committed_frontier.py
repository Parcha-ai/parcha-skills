"""Coverage reports committed progress without exposing Slack cursor contents."""
import json
import unittest

from connectors.sdk import ConnectorRunError
import test_slack_channel_coverage as coverage_tests
from test_slack_source_plugin import slack_response


class SlackCommittedFrontierTests(unittest.TestCase):
    setUp = coverage_tests.SlackChannelCoverageTests.setUp
    runner = coverage_tests.SlackChannelCoverageTests.runner
    discovery = coverage_tests.SlackChannelCoverageTests.discovery
    cycle = coverage_tests.SlackChannelCoverageTests.cycle

    def test_lost_ack_preserves_report_then_restart_advances_it(self):
        r = self.runner()
        self.rail.add('users.list', {'ok': True, 'members': []})
        self.discovery(['CSECRET'])
        self.rail.add('messages.history', slack_response([
            {'ts': '1784332800.000100', 'user': 'U111', 'text': 'SECRET_BODY'}
        ]))
        r.run_once()
        r.run_once()
        before = r.doctor()['coverage']['committed_live_cursor']
        self.assertEqual(before['status'], 'observed')
        self.assertEqual(before['phase'], 'history')
        self.assertEqual((before['channel_index'], before['channel_count']), (0, 1))
        self.brain.fail_after_commit = True
        with self.assertRaises(ConnectorRunError):
            r.run_once()
        self.assertEqual(r.doctor()['coverage']['committed_live_cursor'], before)
        r = self.runner()
        calls = len(self.rail.calls)
        r.run_once()
        self.assertEqual(len(self.rail.calls), calls)
        after = r.doctor()['coverage']['committed_live_cursor']
        self.assertEqual(after['cycle'], before['cycle'] + 1)
        self.assertNotEqual(after['cursor_sha256'], before['cursor_sha256'])
        self.assertNotIn('CSECRET', json.dumps(after))
        self.assertNotIn('SECRET_BODY', json.dumps(after))

    def test_oldest_frontier_distinguishes_stale_inventory(self):
        from unittest.mock import patch
        r = self.runner()
        with patch('connectors.slack_workspace._now', return_value='2026-09-23T00:00:00Z'):
            self.cycle(r, ['CALWAYS', 'COLD', 'COLDTIE'])
        with patch('connectors.slack_workspace._now', return_value='2026-09-24T00:00:00Z'):
            self.cycle(r, ['CALWAYS'])
            # The preceding ACK fixed the next cycle's upper bound.
            self.cycle(r, ['CALWAYS'])
        coverage = r.doctor()['coverage']
        self.assertEqual(coverage['known_channels'], 3)
        self.assertEqual(coverage['last_complete_discovery_channels'], 1)
        self.assertEqual(coverage['oldest_history_through'], '2026-09-23T00:00:00Z')
        scope = coverage['last_completed_discovery_inventory']
        self.assertEqual(scope['status'], 'observed')
        self.assertEqual(scope['channels_seen_since'], 1)
        self.assertEqual(scope['channels_not_seen_since'], 2)
        self.assertEqual(scope['oldest_history_through_seen_since'], '2026-09-24T00:00:00Z')
        self.assertEqual(scope['oldest_history_through_not_seen_since'], '2026-09-23T00:00:00Z')

    def reconciled_runner(self):
        from connectors.sdk import ConnectorRunner
        from connectors.slack_workspace import SlackWorkspaceConnector
        connector = SlackWorkspaceConnector(
            rail=self.rail, source_id='synthetic:slack:coverage', workspace_id='T123')
        connector.reconciliation_epoch = 'a' * 64
        runner = ConnectorRunner(connector=connector, brain=self.brain, spool_path=self.path)
        self.runners.append(runner)
        return runner

    def test_reconciliation_marker_resolves_only_matching_committed_live_state(self):
        r = self.reconciled_runner()
        self.rail.add('users.list', {'ok': True, 'members': []})
        self.discovery(['CSECRET'])
        self.rail.add('messages.history', slack_response([
            {'ts': '1784332800.000100', 'user': 'U111', 'text': 'SECRET_BODY'}
        ]))
        r.run_once()
        r.run_once()
        self.assertEqual(json.loads(r._cursor())['slack_reconciliation'], 1)
        before = r.doctor()['coverage']['committed_live_cursor']
        self.assertEqual(before['phase'], 'history')
        self.brain.fail_after_commit = True
        with self.assertRaises(ConnectorRunError):
            r.run_once()
        self.assertEqual(r.doctor()['coverage']['committed_live_cursor'], before)
        r = self.reconciled_runner()
        calls = len(self.rail.calls)
        r.run_once()
        self.assertEqual(len(self.rail.calls), calls)
        after = r.doctor()['coverage']['committed_live_cursor']
        self.assertEqual(after['cycle'], before['cycle'] + 1)
        self.assertNotEqual(after['cursor_sha256'], before['cursor_sha256'])
        stored = r._cursor()
        saved = json.loads(r.db.execute('SELECT state_json FROM slack_reconciliation_state').fetchone()[0])
        r._set_meta('committed_cursor', json.dumps(saved['live_cursor']))
        r.db.commit()
        self.assertEqual(r.doctor()['coverage']['committed_live_cursor'], {'status': 'invalid'})
        r._set_meta('committed_cursor', json.dumps(stored))
        r.db.commit()
        marker = json.loads(stored)
        marker['step'] += 1
        r._set_meta('committed_cursor', json.dumps(json.dumps(marker)))
        r.db.commit()
        self.assertEqual(r.doctor()['coverage']['committed_live_cursor'], {'status': 'invalid'})
        r._set_meta('committed_cursor', json.dumps(stored))
        r.db.commit()
        r.connector._reconciliation.epoch = 'b' * 64
        self.assertEqual(r.doctor()['coverage']['committed_live_cursor'],
                         {'status': 'stale_authority_or_selection'})
        self.assertEqual(r._cursor(), stored)

    def test_malformed_and_legacy_cursors_are_qualified_without_payload(self):
        r = self.runner()
        self.assertEqual(r.doctor()['coverage']['committed_live_cursor'], {'status': 'absent'})
        for raw in ('not-json-SECRET', json.dumps({'v': 4, 'phase': 'SECRET'}),
                    json.dumps({'slack_reconciliation': 1, 'step': 1}),
                    json.dumps('SECRET'), json.dumps(None), '"' + 'X' * 5000 + '"'):
            with self.subTest(raw_length=len(raw)):
                r._set_meta('committed_cursor', raw)
                r.db.commit()
                before = r._get_meta('committed_cursor')
                result = r.doctor()['coverage']['committed_live_cursor']
                self.assertIn(result['status'], ('invalid', 'absent', 'unsupported_version'))
                self.assertEqual(set(result), {'status'})
                self.assertEqual(r._get_meta('committed_cursor'), before)
                self.assertNotIn('SECRET', json.dumps(result))

    def test_reply_progress_has_only_counts_presence_and_hash(self):
        r = self.runner()
        self.rail.add('users.list', {'ok': True, 'members': []})
        self.discovery(['CSECRET'])
        self.rail.add('messages.history', slack_response([
            {'ts': '1784332800.000100', 'user': 'U111', 'text': 'SECRET_BODY', 'reply_count': 2}
        ]))
        self.rail.add('messages.replies', slack_response([
            {'ts': '1784332801.000100', 'user': 'U111', 'text': 'SECRET_REPLY'}
        ], cursor='SECRET_REPLY_PAGE'))
        for _ in range(4):
            r.run_once()
        report = r.doctor()['coverage']['committed_live_cursor']
        self.assertEqual(report['status'], 'observed')
        self.assertEqual(report['phase'], 'threads')
        self.assertEqual((report['thread_index'], report['thread_count']), (0, 1))
        self.assertTrue(report['reply_page_present'])
        self.assertNotIn('SECRET', json.dumps(report))
        self.assertEqual(len(report['cursor_sha256']), 64)
