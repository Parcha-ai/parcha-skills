from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

from connectors.remote_api import RemoteApiError
from connectors.sdk import ConnectorRateLimited, ConnectorRunError, ConnectorRunner, ConnectorUpstreamError
from connectors.slack_workspace import SlackWorkspaceConnector, _cursor, _initial_state


LOGGER = 'connectors.slack_workspace'


class SlackRequestDiagnosticsTests(unittest.TestCase):
    def connector(self, response=None, error=None):
        rail = Mock()
        rail.request = Mock(return_value=response, side_effect=error)
        return SlackWorkspaceConnector(
            rail=rail, source_id='synthetic:slack:diagnostic', workspace_id='private-workspace',
        )

    def test_response_failure_identifies_operation_without_payload(self):
        connector = self.connector({'ok': False, 'error': 'not_in_channel', 'private': 'private-body'})
        with self.assertLogs(LOGGER, level='WARNING') as logs:
            with self.assertRaisesRegex(ConnectorUpstreamError, '^connector_upstream_error$'):
                connector._request('messages.history', {'channel': 'private-channel', 'cursor': 'private-cursor'})
        record = logs.records[0]
        self.assertEqual((record.slack_operation, record.slack_error_kind, record.slack_error_code),
                         ('messages.history', 'response', 'not_in_channel'))
        self.assertNotIn('private', str(record.__dict__))

    def test_transport_failure_preserves_public_error(self):
        connector = self.connector(error=RemoteApiError('authority_forbidden'))
        with self.assertLogs(LOGGER, level='WARNING') as logs:
            with self.assertRaisesRegex(ConnectorUpstreamError, '^connector_upstream_error$'):
                connector._request('messages.replies', {'ts': 'private-thread'})
        self.assertEqual(logs.records[0].slack_error_kind, 'transport')
        self.assertEqual(logs.records[0].slack_error_code, 'authority_forbidden')
        self.assertFalse(logs.records[0].exc_info)

    def test_unknown_operation_and_errors_never_become_log_fields(self):
        for response, error in [({'ok': False, 'error': 'private-response'}, None),
                                ({'ok': False, 'error': {'private': 'response'}}, None),
                                (None, RemoteApiError('private-transport'))]:
            connector = self.connector(response, error)
            with self.assertLogs(LOGGER, level='WARNING') as logs:
                with self.assertRaises(ConnectorUpstreamError):
                    connector._request('private-operation', {'secret': 'private-token'})
            self.assertEqual(logs.records[0].slack_operation, 'unrecognized')
            self.assertEqual(logs.records[0].slack_error_code, 'unrecognized')
            self.assertNotIn('private', str(logs.records[0].__dict__))

    def test_invalid_response_shape_has_closed_diagnostic(self):
        connector = self.connector(['private-body'])
        with self.assertLogs(LOGGER, level='WARNING') as logs:
            with self.assertRaises(ConnectorUpstreamError):
                connector._request('channels.list', {})
        self.assertEqual(logs.records[0].slack_error_code, 'response_invalid')

    def test_success_and_rate_limit_behavior_unchanged(self):
        response = {'ok': True, 'channels': [{'name': 'private-channel'}]}
        connector = self.connector(response)
        with self.assertNoLogs(LOGGER, level='WARNING'):
            self.assertIs(connector._request('channels.list', {}), response)
        error = ConnectorRateLimited(retry_after_seconds=60)
        connector = self.connector(error=error)
        with self.assertNoLogs(LOGGER, level='WARNING'):
            with self.assertRaises(ConnectorRateLimited) as caught:
                connector._request('users.list', {})
        self.assertIs(caught.exception, error)

    def test_runner_keeps_cursor_and_repeats_same_read_after_failure(self):
        connector = self.connector({'ok': False, 'error': 'missing_scope'})
        state = dict(_initial_state(public_history=False), phase='history', channels=['C111:1'])
        cursor = _cursor(state)
        brain = Mock()
        with tempfile.TemporaryDirectory() as directory:
            runner = ConnectorRunner(connector=connector, brain=brain, spool_path=Path(directory) / 'spool.db')
            try:
                runner._set_meta('committed_cursor', json.dumps(cursor))
                runner.db.commit()
                for _ in range(2):
                    with self.assertLogs(LOGGER, level='WARNING'):
                        with self.assertRaisesRegex(ConnectorRunError, '^connector_upstream_error$'):
                            runner.run_once()
                    self.assertEqual(runner._cursor(), cursor)
                    self.assertEqual(runner.db.execute('SELECT count(*) FROM pages').fetchone()[0], 0)
                    self.assertEqual(runner.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 0)
                self.assertEqual(connector.rail.request.call_args_list[0], connector.rail.request.call_args_list[1])
                self.assertEqual(connector.rail.request.call_args_list[0].args[0], 'messages.history')
                brain.ingest.assert_not_called()
            finally:
                runner.close()

    def test_existing_join_failure_is_logged_before_any_history_call(self):
        connector = self.connector({'ok': False, 'error': 'is_archived'})
        state = dict(_initial_state(public_history=False), phase='history', channels=['C111:0'])
        with self.assertLogs(LOGGER, level='WARNING') as logs:
            with self.assertRaises(ConnectorUpstreamError):
                connector.pull(_cursor(state))
        self.assertEqual(connector.rail.request.call_count, 1)
        self.assertEqual(logs.records[0].slack_operation, 'channels.join')
        self.assertEqual(logs.records[0].slack_error_code, 'is_archived')


if __name__ == '__main__':
    unittest.main()
