from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from connectors.remote_api import RemoteApiError
from connectors.sdk import (
    ConnectorContractError, ConnectorRateLimited, ConnectorRunner, ConnectorRunError,
    ConnectorUpstreamError,
)
from connectors.slack_workspace import SlackWorkspaceConnector, _cursor, _initial_state
from test_connector_sdk import FakeBrain
from test_slack_source_plugin import Rail, slack_response


class ThrowingRail(Rail):
    def request(self, operation, **parameters):
        result = super().request(operation, **parameters)
        if isinstance(result, Exception):
            raise result
        return result


def messages(count, *, files=0, text='synthetic content', threads=False):
    return [{
        'type': 'message', 'user': 'U123', 'ts': f'1784332800.{i:06d}', 'text': text,
        'reply_count': 1 if threads else 0,
        'files': [{'id': f'F{i:04d}{j:02d}', 'url_private': f'https://files.slack.com/{i}/{j}'}
                  for j in range(files)],
    } for i in range(count)]


class SlackPageThroughputTests(unittest.TestCase):
    def setUp(self):
        self.rail = ThrowingRail()
        self.connector = SlackWorkspaceConnector(
            rail=self.rail, source_id='synthetic:slack:throughput', workspace_id='T123',
        )
        self.state = {**_initial_state(), 'phase': 'history', 'channels': ['C123:1'],
                      'page': 'original-provider-cursor'}

    def queries(self, operation='messages.history'):
        return [kwargs['query'] for op, kwargs in self.rail.calls if op == operation]

    def test_default_history_requests100_and_preserves_all_records(self):
        self.rail.add('messages.history', slack_response(messages(100), cursor='after100'))
        page = self.connector.pull(_cursor(self.state))
        self.assertEqual([q['limit'] for q in self.queries()], [100])
        self.assertEqual(len(page.records), 100)
        self.assertEqual(json.loads(page.next_cursor)['page'], 'after100')

    def test_file_dense_page_retries_same_cursor_before_any_large_page_download(self):
        self.rail.add('messages.history',
                      slack_response(messages(100, files=5), cursor='discarded100'),
                      slack_response(messages(20, files=5), cursor='after20'))
        page = self.connector.pull(_cursor(self.state))
        self.assertEqual([q['limit'] for q in self.queries()], [100, 20])
        self.assertEqual({q['cursor'] for q in self.queries()}, {'original-provider-cursor'})
        self.assertEqual(len(page.records), 120)
        self.assertEqual(json.loads(page.next_cursor)['page'], 'after20')
        self.assertEqual(sum(op == 'binary.download' for op, _ in self.rail.calls), 100)

    def test_thread_dense_cursor_retries_without_losing_selected_thread_roots(self):
        self.state['channels'] = [f'C{i:031d}:1' for i in range(50)]
        self.rail.add('messages.history',
                      slack_response(messages(100, threads=True), cursor='discarded100'),
                      slack_response(messages(20, threads=True), cursor='after20'))
        page = self.connector.pull(_cursor(self.state))
        self.assertEqual([q['limit'] for q in self.queries()], [100, 20])
        self.assertLessEqual(len(page.next_cursor.encode()), 4096)
        state = json.loads(page.next_cursor)
        self.assertEqual(state['threads'], [m['ts'] for m in messages(20)])
        self.assertEqual(state['page'], 'after20')
        self.assertEqual(state['phase'], 'threads')

    def test_message_byte_capacity_retries_without_truncating_content(self):
        large = messages(100, text='x' * 100_000)
        self.rail.add('messages.history', slack_response(large, cursor='discarded100'),
                      slack_response(large[:20], cursor='after20'))
        page = self.connector.pull(_cursor(self.state))
        self.assertEqual([q['limit'] for q in self.queries()], [100, 20])
        self.assertEqual(len(page.records), 20)
        self.assertTrue(all(len(r.content['text']) == 100_000 for r in page.records))

    def test_transport_capacity_retries_once_but_rate_auth_and_contract_do_not(self):
        for failure, expected, calls in (
            (RemoteApiError('response_too_large'), None, 2),
            (ConnectorRateLimited(retry_after_seconds=17), ConnectorRateLimited, 1),
            (RemoteApiError('authority_revoked'), ConnectorUpstreamError, 1),
            ({'ok': False, 'error': 'missing_scope'}, ConnectorUpstreamError, 1),
            (slack_response([{'text': 'invalid native identity'}]), ConnectorContractError, 1),
        ):
            with self.subTest(failure=type(failure).__name__, calls=calls):
                self.rail = ThrowingRail()
                self.connector.rail = self.rail
                self.rail.add('messages.history', failure, slack_response(messages(20)))
                if expected:
                    with self.assertRaises(expected) as raised:
                        self.connector.pull(_cursor(self.state))
                    if expected is ConnectorRateLimited:
                        self.assertEqual(raised.exception.retry_after_seconds, 17)
                else:
                    self.assertEqual(len(self.connector.pull(_cursor(self.state)).records), 20)
                self.assertEqual(len(self.queries()), calls)

    def test_small_page_capacity_failure_stops_after_one_retry(self):
        self.rail.add('messages.history', RemoteApiError('response_too_large'),
                      RemoteApiError('response_too_large'))
        with self.assertRaises((ConnectorContractError, ConnectorUpstreamError)):
            self.connector.pull(_cursor(self.state))
        self.assertEqual([q['limit'] for q in self.queries()], [100, 20])

    def test_explicit_small_request_does_not_retry_or_grow(self):
        self.connector = SlackWorkspaceConnector(
            rail=self.rail, source_id='synthetic:slack:throughput', workspace_id='T123',
            page_size=10,
        )
        self.rail.add('messages.history', RemoteApiError('response_too_large'))
        with self.assertRaises(ConnectorContractError):
            self.connector.pull(_cursor(self.state))
        self.assertEqual([q['limit'] for q in self.queries()], [10])

    def test_retry_keeps_time_bounds_and_joins_only_once(self):
        self.state.update(page=None, channels=['C123:0'],
                          channel_lower='2026-07-01T00:00:00Z')
        self.rail.add('channels.join', {'ok': True})
        self.rail.add('messages.history', RemoteApiError('response_too_large'),
                      slack_response(messages(20), cursor='after20'))
        self.connector.pull(_cursor(self.state))
        large, small = self.queries()
        self.assertEqual({k: v for k, v in large.items() if k != 'limit'},
                         {k: v for k, v in small.items() if k != 'limit'})
        self.assertIn('oldest', large)
        self.assertEqual(sum(op == 'channels.join' for op, _ in self.rail.calls), 1)

    def test_capacity_logs_never_echo_provider_payload_or_cursor(self):
        self.rail.add('messages.history', RemoteApiError('response_too_large'),
                      slack_response(messages(20, text='private-source-prose')))
        with self.assertLogs('connectors.slack_workspace', level='WARNING') as logs:
            self.connector.pull(_cursor(self.state))
        output = '\n'.join(logs.output)
        self.assertIn('response_too_large', output)
        for sensitive in ('private-source-prose', 'original-provider-cursor', 'C123'):
            self.assertNotIn(sensitive, output)

    def test_user_page_capacity_retries_at_same_provider_cursor(self):
        state = {**_initial_state(), 'page': 'original-user-cursor'}
        self.rail.add('users.list', RemoteApiError('response_too_large'),
                      {'ok': True, 'members': [], 'response_metadata': {'next_cursor': 'after20'}})
        page = self.connector.pull(_cursor(state))
        self.assertEqual([q['limit'] for q in self.queries('users.list')], [100, 20])
        self.assertEqual({q['cursor'] for q in self.queries('users.list')}, {'original-user-cursor'})
        self.assertEqual(json.loads(page.next_cursor)['page'], 'after20')

    def test_replies_use_same_uncommitted_thread_cursor_on_capacity_retry(self):
        self.state.update(phase='threads', threads=['1784332799.000001'], thread_index=0,
                          thread_page='original-thread-cursor', page='next-history-page')
        self.rail.add('messages.replies', RemoteApiError('response_too_large'),
                      slack_response(messages(20), cursor='next-thread-page'))
        page = self.connector.pull(_cursor(self.state))
        queries = self.queries('messages.replies')
        self.assertEqual([q['limit'] for q in queries], [100, 20])
        self.assertEqual({q['cursor'] for q in queries}, {'original-thread-cursor'})
        state = json.loads(page.next_cursor)
        self.assertEqual(state['thread_page'], 'next-thread-page')
        self.assertEqual(state['page'], 'next-history-page')

    def test_lost_ack_restarts_without_refetching_large_or_fallback_page(self):
        with tempfile.TemporaryDirectory() as directory:
            brain = FakeBrain()
            spool = Path(directory) / 'spool.db'
            self.rail.add('users.list', {'ok': True, 'members': []})
            self.rail.add('channels.list', {'ok': True, 'channels': [{'id': 'C123', 'is_archived': True}]})
            self.rail.add('messages.history', RemoteApiError('response_too_large'),
                          slack_response(messages(20), cursor='after20'))
            runner = ConnectorRunner(connector=self.connector, brain=brain, spool_path=spool)
            try:
                runner.run_once()
                runner.run_once()
                before = runner._cursor()
                brain.fail_after_commit = True
                with self.assertRaises(ConnectorRunError):
                    runner.run_once()
                self.assertEqual(runner._cursor(), before)
                self.assertEqual(runner.doctor()['coverage']['history_baselined_channels'], 0)
            finally:
                runner.close()
            runner = ConnectorRunner(connector=self.connector, brain=brain, spool_path=spool)
            try:
                runner.run_once()
                self.assertEqual(json.loads(runner._cursor())['page'], 'after20')
                self.assertEqual(len(brain.events), 20)
                self.assertEqual([q['limit'] for q in self.queries()], [100, 20])
                self.assertEqual(runner.doctor()['coverage']['history_baselined_channels'], 0)
            finally:
                runner.close()
