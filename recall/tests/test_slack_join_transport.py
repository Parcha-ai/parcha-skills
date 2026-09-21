from __future__ import annotations

import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock
import urllib.error
from urllib.parse import parse_qs, urlsplit

from connectors.remote_api import RemoteApiError
from connectors.sdk import ConnectorRateLimited, ConnectorRunError, ConnectorRunner
from connectors.slack_workspace import SlackWorkspaceConnector, _cursor, _initial_state
from connectors.work_apis import slack_public_history_rail, slack_rail


class Response(io.BytesIO):
    headers = {'Content-Type': 'application/json'}


class SlackJoinTransportTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.root.chmod(0o700)
        self.bot = self.authority('bot')
        self.user = self.authority('user')
        self.requests = []
        self.join_error = None
        self.rate_limited = False

    def authority(self, kind):
        path = self.root / kind
        path.write_text('synthetic-' + kind)
        path.chmod(0o600)
        return path

    def opener(self, request, **_kwargs):
        self.requests.append(request)
        parsed = urlsplit(request.full_url)
        if parsed.path == '/api/conversations.join':
            if self.rate_limited:
                raise urllib.error.HTTPError(request.full_url, 429, 'rate limited', {'Retry-After': '17'}, None)
            # Mirror Slack's documented JSON contract, not connector kwargs.
            if parsed.query or json.loads(request.data) != {'channel': 'C111'}:
                payload = {'ok': False, 'error': 'invalid_arguments'}
            elif self.join_error:
                payload = {'ok': False, 'error': self.join_error}
            else:
                payload = {'ok': True, 'channel': {'id': 'C111'}}
        elif parsed.path == '/api/conversations.history':
            payload = {'ok': True, 'messages': [], 'response_metadata': {'next_cursor': ''}}
        else:
            raise AssertionError('unexpected operation')
        return Response(json.dumps(payload).encode())

    def connector(self, *, dual=False):
        rail = slack_public_history_rail(bot_authority_path=self.bot, user_authority_path=self.user, opener=self.opener) if dual else slack_rail(authority_path=self.bot, opener=self.opener)
        return SlackWorkspaceConnector(rail=rail, source_id='synthetic:slack:wire', workspace_id='T111')

    def cursor(self, *, public=False):
        return _cursor(dict(_initial_state(public_history=public), phase='history', channels=['C111:0']))

    def assert_join_request(self, request):
        self.assertEqual(request.method, 'POST')
        self.assertEqual(request.full_url, 'https://slack.com/api/conversations.join')
        self.assertEqual(request.get_header('Content-type'), 'application/json')
        self.assertEqual(json.loads(request.data), {'channel': 'C111'})
        self.assertEqual(request.get_header('Authorization'), 'Bearer synthetic-bot')

    def test_factory_sends_required_channel_in_json(self):
        rail = slack_rail(authority_path=self.bot, opener=self.opener)
        self.assertTrue(rail.request('channels.join', json_body={'channel': 'C111'})['ok'])
        self.assert_join_request(self.requests[0])

    def test_workspace_join_then_history_uses_distinct_wire_formats(self):
        self.connector().pull(self.cursor())
        self.assertEqual(len(self.requests), 2)
        self.assert_join_request(self.requests[0])
        history = self.requests[1]
        self.assertEqual(history.method, 'GET')
        self.assertIsNone(history.data)
        self.assertEqual(parse_qs(urlsplit(history.full_url).query)['channel'], ['C111'])

    def test_public_history_keeps_bot_join_and_user_read_authority(self):
        self.connector(dual=True).pull(self.cursor(public=True))
        self.assertEqual(len(self.requests), 2)
        self.assert_join_request(self.requests[0])
        self.assertEqual(self.requests[1].get_header('Authorization'), 'Bearer synthetic-user')

    def test_failed_join_does_not_advance_real_runner_or_start_history(self):
        self.join_error = 'missing_scope'
        cursor = self.cursor()
        brain = Mock()
        runner = ConnectorRunner(connector=self.connector(), brain=brain, spool_path=self.root / 'spool.db')
        try:
            runner._set_meta('committed_cursor', json.dumps(cursor))
            runner.db.commit()
            for _ in range(2):
                with self.assertLogs('connectors.slack_workspace', level='WARNING') as logs:
                    with self.assertRaisesRegex(ConnectorRunError, '^connector_upstream_error$'):
                        runner.run_once()
                self.assertEqual(logs.records[0].slack_error_code, 'missing_scope')
                self.assertNotIn('C111', str(logs.records[0].__dict__))
                self.assertNotIn('synthetic-bot', str(logs.records[0].__dict__))
                self.assertEqual(runner._cursor(), cursor)
                self.assertEqual(runner.db.execute('SELECT count(*) FROM pages').fetchone()[0], 0)
                self.assertEqual(runner.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 0)
            self.assertEqual(len(self.requests), 2)
            for request in self.requests:
                self.assert_join_request(request)
            brain.ingest.assert_not_called()
            self.join_error = None
            self.assertEqual(runner.run_once()['status'], 'committed')
            self.assertNotEqual(runner._cursor(), cursor)
            self.assertEqual(len(self.requests), 4)
            self.assert_join_request(self.requests[2])
            self.assertEqual(urlsplit(self.requests[3].full_url).path, '/api/conversations.history')
            self.assertEqual(runner.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 0)
        finally:
            runner.close()

    def test_rate_limit_still_preserves_retry_after_and_skips_history(self):
        self.rate_limited = True
        with self.assertNoLogs('connectors.slack_workspace', level='WARNING'):
            with self.assertRaises(ConnectorRateLimited) as caught:
                self.connector().pull(self.cursor())
        self.assertEqual(caught.exception.retry_after_seconds, 17)
        self.assertEqual(len(self.requests), 1)
        self.assert_join_request(self.requests[0])

    def test_join_query_arguments_refused_before_transport(self):
        rail = slack_rail(authority_path=self.bot, opener=self.opener)
        with self.assertRaisesRegex(RemoteApiError, '^parameter_not_allowed$'):
            rail.request('channels.join', query={'channel': 'C111'})
        self.assertEqual(self.requests, [])


if __name__ == '__main__':
    unittest.main()
