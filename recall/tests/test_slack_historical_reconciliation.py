"""Missed Slack changes must recover without blocking current messages."""

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from connectors.sdk import ConnectorRunner, ConnectorRunError, ConnectorRateLimited
from connectors.slack_workspace import SlackWorkspaceConnector
from test_connector_sdk import FakeBrain


class HistoricalRail:
    public_history = True

    def __init__(self):
        self.now = Decimal('1784505600')
        self.root = {'ts': '1784332800.000100', 'user': 'U111', 'text': 'Original decision'}
        self.messages = [self.root]
        self.channel_messages = {'C111': self.messages}
        self.limited_channels = set()
        self.calls = []
        self.limited_history = False
        self.page_size = 100
        self.cursors = {}
        self.expire_cursors = False

    def timestamp(self):
        return datetime.fromtimestamp(float(self.now), timezone.utc).isoformat().replace('+00:00', 'Z')

    def request(self, operation, **arguments):
        self.calls.append((operation, arguments))
        if operation == 'users.list':
            return {'ok': True, 'members': [], 'response_metadata': {'next_cursor': ''}}
        if operation == 'channels.list':
            return {'ok': True, 'channels': [{'id': channel} for channel in self.channel_messages], 'response_metadata': {'next_cursor': ''}}
        if operation == 'channels.join':
            return {'ok': True}
        query = arguments['query']
        messages = self.channel_messages[query['channel']]
        lower = Decimal(query.get('oldest', '0'))
        upper = Decimal(query.get('latest', str(self.now)))
        if operation == 'messages.history':
            rows = [message for message in messages
                    if 'thread_ts' not in message and lower <= Decimal(message['ts']) <= upper]
            rows.sort(key=lambda message: Decimal(message['ts']), reverse=True)
        elif operation == 'messages.replies':
            rows = [message for message in messages
                    if (message['ts'] == query['ts'] or message.get('thread_ts') == query['ts'])
                    and lower <= Decimal(message['ts']) <= upper]
            rows.sort(key=lambda message: Decimal(message['ts']))
        else:
            raise AssertionError('Unexpected provider operation')
        if not query.get('inclusive', False):
            rows = [message for message in rows if lower < Decimal(message['ts']) < upper]
        if query.get('cursor'):
            if self.expire_cursors:
                return {'ok': False, 'error': 'invalid_cursor'}
            rows = self.cursors[query['cursor']]
        remainder = rows[self.page_size:]
        next_cursor = ''
        if remainder:
            next_cursor = f'provider-page-{len(self.cursors)}'
            self.cursors[next_cursor] = remainder
        response = {'ok': True, 'messages': rows[:self.page_size], 'has_more': bool(remainder),
                    'response_metadata': {'next_cursor': next_cursor}}
        if (self.limited_history or query['channel'] in self.limited_channels) and operation == 'messages.history' and 'oldest' not in query:
            response['is_limited'] = True
        return response


class SlackHistoricalReconciliation(unittest.TestCase):
    def setUp(self):
        clock = patch('connectors.slack_workspace._now', return_value='2026-07-20T00:00:00Z')
        clock.start()
        self.addCleanup(clock.stop)

    def test_historical_pages_finish_without_reusing_expiring_provider_cursors(self):
        rail = HistoricalRail()
        brain = FakeBrain()
        with tempfile.TemporaryDirectory() as directory:
            connector = SlackWorkspaceConnector(
                rail=rail, source_id='synthetic:slack:cursor-expiry', workspace_id='T111',
            )
            connector.reconciliation_epoch = 'a' * 64
            active = ConnectorRunner(connector=connector, brain=brain,
                                     spool_path=Path(directory) / 'spool.db')
            try:
                with patch('connectors.slack_workspace._now', side_effect=rail.timestamp):
                    for _ in range(3):
                        active.run_once()
                    rail.page_size = 2
                    rail.expire_cursors = True
                    rail.root['reply_count'] = 5
                    for number in range(5):
                        rail.messages.append({'ts': str(1784000000 + number), 'user': 'U111',
                                              'text': f'Older message {number}'})
                        rail.messages.append({'ts': str(1784332900 + number), 'user': 'U111',
                                              'thread_ts': rail.root['ts'], 'text': f'Older reply {number}'})
                    for _ in range(70):
                        active.run_once()
                texts = {event['content'].get('text') for event in brain.events.values()}
                self.assertTrue({f'Older message {number}' for number in range(5)} <= texts)
                self.assertTrue({f'Older reply {number}' for number in range(5)} <= texts)
                self.assertGreaterEqual(active.doctor()['coverage']['historical_reconciliation']['completed_channel_visits'], 1)
            finally:
                active.close()

    def test_channel_selection_change_restarts_discovery_without_fetching_removed_channel(self):
        rail = HistoricalRail()
        rail.channel_messages['C222'] = [{'ts': '1784332801.000100', 'user': 'U111', 'text': 'Selected channel'}]
        brain = FakeBrain()
        with tempfile.TemporaryDirectory() as directory:
            def runner(channels):
                connector = SlackWorkspaceConnector(
                    rail=rail, source_id='synthetic:slack:selection', workspace_id='T111',
                    channel_ids=channels,
                )
                connector.reconciliation_epoch = 'a' * 64
                return ConnectorRunner(connector=connector, brain=brain,
                                       spool_path=Path(directory) / 'spool.db')

            active = runner(('C111', 'C222'))
            try:
                for _ in range(9):
                    active.run_once()
            finally:
                active.close()
            before = len(rail.calls)
            active = runner(('C222',))
            try:
                for _ in range(20):
                    active.run_once()
                status = active.doctor()['coverage']
                self.assertTrue(status['discovery_matches_current_selection'])
                self.assertEqual(status['known_channels'], 1)
                self.assertGreaterEqual(status['historical_reconciliation']['completed_channel_visits'], 1)
            finally:
                active.close()
            fetched = [values.get('query', values.get('json_body', {})).get('channel')
                       for operation, values in rail.calls[before:]
                       if operation in {'messages.history', 'messages.replies', 'channels.join'}]
            self.assertTrue(fetched)
            self.assertEqual(set(fetched), {'C222'})

    def test_old_authority_pending_ack_does_not_certify_new_authority(self):
        rail = HistoricalRail()
        brain = FakeBrain()
        with tempfile.TemporaryDirectory() as directory:
            def runner(epoch):
                connector = SlackWorkspaceConnector(
                    rail=rail, source_id='synthetic:slack:pending-authority', workspace_id='T111',
                )
                connector.reconciliation_epoch = epoch * 64
                return ConnectorRunner(connector=connector, brain=brain,
                                       spool_path=Path(directory) / 'spool.db')

            active = runner('a')
            try:
                for _ in range(8):
                    active.run_once()
                rail.root.update(text='Old-authority edit', edited={'ts': str(rail.now)})
                brain.fail_after_commit = True
                with self.assertRaises(ConnectorRunError):
                    active.run_once()
                self.assertEqual(active.doctor()['coverage']['historical_reconciliation']['completed_channel_visits'], 0)
            finally:
                active.close()
            calls = len(rail.calls)
            active = runner('b')
            try:
                active.run_once()
                self.assertEqual(len(rail.calls), calls)
                self.assertFalse(active.doctor()['coverage']['historical_reconciliation']['authority_current'])
                active.run_once()
                status = active.doctor()['coverage']['historical_reconciliation']
                self.assertTrue(status['authority_current'])
                self.assertEqual(status['completed_channel_visits'], 0)
                self.assertEqual(status['historical_pages'], 0)
                self.assertFalse(active.doctor()['coverage']['historical_mutations_verified'])
            finally:
                active.close()

    def test_disabling_reconciliation_preserves_live_cursor_and_resumes_history(self):
        rail = HistoricalRail()
        brain = FakeBrain()
        with tempfile.TemporaryDirectory() as directory:
            def runner(enabled):
                connector = SlackWorkspaceConnector(
                    rail=rail, source_id='synthetic:slack:rollback', workspace_id='T111',
                )
                connector.reconciliation_epoch = 'a' * 64
                connector.reconciliation_enabled = enabled
                return ConnectorRunner(connector=connector, brain=brain,
                                       spool_path=Path(directory) / 'spool.db')

            active = runner(True)
            try:
                for _ in range(9):
                    active.run_once()
                count = active.doctor()['coverage']['historical_reconciliation']['historical_pages']
                self.assertEqual(count, 1)
            finally:
                active.close()
            active = runner(False)
            try:
                with patch('connectors.slack_workspace._now', side_effect=rail.timestamp):
                    for number in range(20):
                        rail.now += 2
                        rail.messages.append({'ts': str(rail.now), 'user': 'U111',
                                              'text': f'While disabled {number}'})
                        active.run_once()
                self.assertEqual(active.doctor()['coverage']['historical_reconciliation']['historical_pages'], count)
                self.assertTrue(any(event['content'].get('text', '').startswith('While disabled')
                                    for event in brain.events.values()))
            finally:
                active.close()
            active = runner(True)
            try:
                active.run_once()
                self.assertEqual(active.doctor()['coverage']['historical_reconciliation']['historical_pages'], count + 1)
            finally:
                active.close()

    def test_vanished_thread_starts_a_new_window_and_recovers_later_old_edit(self):
        rail = HistoricalRail()
        other = {'ts': '1784000000', 'user': 'U111', 'text': 'Other old decision'}
        rail.messages.append(other)
        brain = FakeBrain()
        with tempfile.TemporaryDirectory() as directory:
            connector = SlackWorkspaceConnector(
                rail=rail, source_id='synthetic:slack:vanished-thread', workspace_id='T111',
            )
            connector.reconciliation_epoch = 'a' * 64
            active = ConnectorRunner(connector=connector, brain=brain,
                                     spool_path=Path(directory) / 'spool.db')
            try:
                for _ in range(3):
                    active.run_once()
                rail.root['reply_count'] = 1
                for _ in range(14):
                    active.run_once()
                rail.messages.remove(rail.root)
                original_request = rail.request

                def vanished(operation, **arguments):
                    if operation == 'messages.replies':
                        return {'ok': False, 'error': 'thread_not_found'}
                    return original_request(operation, **arguments)

                with patch.object(rail, 'request', side_effect=vanished):
                    active.run_once()
                    failed = active.doctor()['coverage']['historical_reconciliation']
                    self.assertEqual(failed['completed_channel_visits'], 0)
                    self.assertEqual(failed['historical_failures'], 1)
                    other.update(text='Recovered after root vanished', edited={'ts': str(rail.now)})
                    for _ in range(9):
                        active.run_once()
                status = active.doctor()['coverage']['historical_reconciliation']
                self.assertEqual(status['completed_channel_visits'], 1)
                self.assertEqual(status['historical_failures'], 1)
                self.assertIn('Recovered after root vanished',
                              {event['content'].get('text') for event in brain.events.values()})
                self.assertFalse(active.doctor()['coverage']['historical_mutations_verified'])
                self.assertFalse(any(event['kind'] == 'tombstone' for event in brain.events.values()))
            finally:
                active.close()

    def test_thread_root_contract_and_empty_truncated_pages(self):
        for reply_shape, expected_complete in [('root_only', 1), ('empty', 0),
                                                ('empty_truncated', 0), ('wrong_root', 0)]:
            with self.subTest(reply_shape=reply_shape), tempfile.TemporaryDirectory() as directory:
                rail = HistoricalRail()
                brain = FakeBrain()
                connector = SlackWorkspaceConnector(
                    rail=rail, source_id='synthetic:slack:thread-contract', workspace_id='T111',
                )
                connector.reconciliation_epoch = 'a' * 64
                active = ConnectorRunner(connector=connector, brain=brain,
                                         spool_path=Path(directory) / 'spool.db')
                try:
                    for _ in range(3):
                        active.run_once()
                    rail.root['reply_count'] = 1
                    for _ in range(14):
                        active.run_once()
                    rows = []
                    if reply_shape == 'root_only':
                        rows = [{**rail.root, 'reply_count': 0}]
                    elif reply_shape == 'wrong_root':
                        rows = [{'ts': '1784332802', 'user': 'U111', 'text': 'Unrelated root'}]
                    response = {'ok': True, 'messages': rows,
                                'has_more': reply_shape == 'empty_truncated',
                                'response_metadata': {'next_cursor': 'opaque' if reply_shape == 'empty_truncated' else ''}}
                    with patch.object(rail, 'request', return_value=response):
                        active.run_once()
                    status = active.doctor()['coverage']['historical_reconciliation']
                    self.assertEqual(status['completed_channel_visits'], expected_complete)
                    self.assertEqual(status['historical_channels_with_errors'], 1 - expected_complete)
                    self.assertFalse(active.doctor()['coverage']['historical_mutations_verified'])
                    self.assertFalse(any(event['kind'] == 'tombstone' for event in brain.events.values()))
                finally:
                    active.close()

    def test_historical_provider_outage_preserves_worker_failure_and_position(self):
        rail = HistoricalRail()
        brain = FakeBrain()
        with tempfile.TemporaryDirectory() as directory:
            connector = SlackWorkspaceConnector(
                rail=rail, source_id='synthetic:slack:provider-outage', workspace_id='T111',
            )
            connector.reconciliation_epoch = 'a' * 64
            active = ConnectorRunner(connector=connector, brain=brain,
                                     spool_path=Path(directory) / 'spool.db')
            try:
                for _ in range(8):
                    active.run_once()
                before = active._cursor()
                for code in ('service_unavailable', 'invalid_auth', 'missing_scope'):
                    with patch.object(rail, 'request', return_value={'ok': False, 'error': code}):
                        with self.assertRaisesRegex(ConnectorRunError, 'connector_upstream_error'):
                            active.run_once()
                    self.assertEqual(active._cursor(), before)
                    self.assertFalse(active.doctor()['coverage']['historical_reconciliation']['pending_checkpoint'])
            finally:
                active.close()

    def test_historical_rate_limit_preserves_the_sdk_backoff_and_checkpoint(self):
        rail = HistoricalRail()
        brain = FakeBrain()
        with tempfile.TemporaryDirectory() as directory:
            connector = SlackWorkspaceConnector(
                rail=rail, source_id='synthetic:slack:rate-limit', workspace_id='T111',
            )
            connector.reconciliation_epoch = 'a' * 64
            active = ConnectorRunner(connector=connector, brain=brain,
                                     spool_path=Path(directory) / 'spool.db')
            try:
                for _ in range(8):
                    active.run_once()
                before = active._cursor()
                with patch.object(rail, 'request', side_effect=ConnectorRateLimited(retry_after_seconds=60)):
                    result = active.run_once()
                self.assertEqual(result['status'], 'backoff')
                self.assertGreaterEqual(result['retry_after_seconds'], 54)
                self.assertEqual(active._cursor(), before)
                self.assertFalse(active.doctor()['coverage']['historical_reconciliation']['pending_checkpoint'])
                active.run_once()
                self.assertEqual(active.doctor()['coverage']['historical_reconciliation']['historical_pages'], 1)
            finally:
                active.close()

    def test_restricted_channel_does_not_starve_other_historical_channels(self):
        rail = HistoricalRail()
        second = {'ts': '1784332801.000100', 'user': 'U111', 'text': 'Second original'}
        rail.channel_messages['C222'] = [second]
        brain = FakeBrain()
        with tempfile.TemporaryDirectory() as directory:
            connector = SlackWorkspaceConnector(
                rail=rail, source_id='synthetic:slack:channel-fairness', workspace_id='T111',
            )
            connector.reconciliation_epoch = 'a' * 64
            active = ConnectorRunner(connector=connector, brain=brain,
                                     spool_path=Path(directory) / 'spool.db')
            try:
                with patch('connectors.slack_workspace._now', side_effect=rail.timestamp):
                    for _ in range(4):
                        active.run_once()
                    self.assertEqual(active.doctor()['coverage']['history_baselined_channels'], 2)
                    rail.limited_channels.add('C111')
                    second.update(text='Recovered second channel edit', edited={'ts': str(rail.now)})
                    for _ in range(32):
                        active.run_once()
                status = active.doctor()['coverage']['historical_reconciliation']
                texts = {event['content'].get('text') for event in brain.events.values()}
                self.assertIn('Recovered second channel edit', texts)
                self.assertGreaterEqual(status['completed_channel_visits'], 1)
                self.assertGreaterEqual(status['historical_failures'], 1)
                self.assertFalse(active.doctor()['coverage']['historical_mutations_verified'])
            finally:
                active.close()

    def test_historical_pagination_survives_restart_without_certifying_partial_channel(self):
        rail = HistoricalRail()
        brain = FakeBrain()
        with tempfile.TemporaryDirectory() as directory:
            def runner():
                connector = SlackWorkspaceConnector(
                    rail=rail, source_id='synthetic:slack:paged', workspace_id='T111',
                )
                connector.reconciliation_epoch = 'a' * 64
                return ConnectorRunner(connector=connector, brain=brain,
                                       spool_path=Path(directory) / 'spool.db')

            active = runner()
            try:
                for _ in range(3):
                    active.run_once()
            finally:
                active.close()
            rail.page_size = 2
            for number in range(5):
                rail.messages.append({'ts': str(1784000000 + number), 'user': 'U111',
                                      'text': f'Missed historical message {number}'})
            observed_partial = False
            for _ in range(40):
                active = runner()
                try:
                    active.run_once()
                    status = active.doctor()['coverage']['historical_reconciliation']
                    if status['historical_pages'] == 1:
                        observed_partial = True
                        self.assertEqual(status['completed_channel_visits'], 0)
                        self.assertTrue(status['historical_page_in_progress'])
                    if status['completed_channel_visits']:
                        break
                finally:
                    active.close()
        self.assertTrue(observed_partial)
        self.assertEqual(status['completed_channel_visits'], 1)
        text = {event['content'].get('text') for event in brain.events.values()}
        self.assertTrue({f'Missed historical message {number}' for number in range(5)} <= text)
        continued = [values['query'] for operation, values in rail.calls
                     if operation == 'messages.history' and values['query'].get('inclusive') is False]
        self.assertGreaterEqual(len({query['latest'] for query in continued}), 3)
        self.assertFalse(any('cursor' in query for query in continued))

    def test_provider_authority_change_discards_old_historical_page_cursor(self):
        rail = HistoricalRail()
        brain = FakeBrain()
        with tempfile.TemporaryDirectory() as directory:
            def runner(epoch):
                connector = SlackWorkspaceConnector(
                    rail=rail, source_id='synthetic:slack:authority', workspace_id='T111',
                )
                connector.reconciliation_epoch = epoch * 64
                return ConnectorRunner(connector=connector, brain=brain,
                                       spool_path=Path(directory) / 'spool.db')

            active = runner('a')
            try:
                for _ in range(3):
                    active.run_once()
                rail.page_size = 1
                rail.messages.append({'ts': '1784000000', 'user': 'U111', 'text': 'Old evidence'})
                for _ in range(6):
                    active.run_once()
                self.assertTrue(active.doctor()['coverage']['historical_reconciliation']['historical_page_in_progress'])
            finally:
                active.close()
            before = len(rail.calls)
            active = runner('b')
            try:
                self.assertFalse(active.doctor()['coverage']['historical_reconciliation']['authority_current'])
                for _ in range(9):
                    active.run_once()
                self.assertTrue(active.doctor()['coverage']['historical_reconciliation']['authority_current'])
                self.assertEqual(rail.calls[before][0], 'users.list')
                self.assertFalse(active.doctor()['coverage']['historical_mutations_verified'])
            finally:
                active.close()
            historical = [values['query'] for operation, values in rail.calls[before:]
                          if operation == 'messages.history' and 'oldest' not in values['query']]
            self.assertEqual(len(historical), 1)
            self.assertNotIn('cursor', historical[0])

    def test_limited_history_remains_unverified_and_does_not_starve_live_pages(self):
        rail = HistoricalRail()
        brain = FakeBrain()
        with tempfile.TemporaryDirectory() as directory:
            connector = SlackWorkspaceConnector(
                rail=rail, source_id='synthetic:slack:limited', workspace_id='T111',
            )
            connector.reconciliation_epoch = 'a' * 64
            active = ConnectorRunner(connector=connector, brain=brain,
                                     spool_path=Path(directory) / 'spool.db')
            try:
                with patch('connectors.slack_workspace._now', side_effect=rail.timestamp):
                    for _ in range(3):
                        active.run_once()
                    rail.limited_history = True
                    for number in range(25):
                        rail.now += 2
                        rail.messages.append({'ts': str(rail.now), 'user': 'U111',
                                              'text': f'Current message {number}'})
                        active.run_once()
                coverage = active.doctor()['coverage']
                historical = coverage['historical_reconciliation']
                self.assertEqual(historical['completed_channel_visits'], 0)
                self.assertEqual(historical['last_error_code'], 'slack_history_limited')
                self.assertFalse(coverage['historical_mutations_verified'])
                self.assertGreaterEqual(sum(event['content'].get('text', '').startswith('Current message')
                                            for event in brain.events.values()), 5)
                self.assertFalse(any(event['kind'] == 'tombstone' for event in brain.events.values()))
            finally:
                active.close()

    def test_inventory_waits_for_ack_and_survives_lost_ack_restart(self):
        rail = HistoricalRail()
        brain = FakeBrain()
        with tempfile.TemporaryDirectory() as directory:
            def runner():
                connector = SlackWorkspaceConnector(
                    rail=rail, source_id='synthetic:slack:ack', workspace_id='T111',
                )
                connector.reconciliation_epoch = 'a' * 64
                return ConnectorRunner(connector=connector, brain=brain,
                                       spool_path=Path(directory) / 'spool.db')

            active = runner()
            try:
                active.run_once()
                active.run_once()
                brain.fail_after_commit = True
                with self.assertRaises(ConnectorRunError):
                    active.run_once()
                status = active.doctor()['coverage']['historical_reconciliation']
                self.assertEqual(status['acknowledged_messages'], 0)
                self.assertTrue(status['pending_checkpoint'])
            finally:
                active.close()
            provider_calls = len(rail.calls)
            active = runner()
            try:
                active.run_once()
                status = active.doctor()['coverage']['historical_reconciliation']
                self.assertEqual(status['acknowledged_messages'], 1)
                self.assertFalse(status['pending_checkpoint'])
                self.assertFalse(active.doctor()['coverage']['historical_mutations_verified'])
                self.assertEqual(len(rail.calls), provider_calls)
            finally:
                active.close()

    def test_missed_old_edit_and_reply_recover_while_live_messages_keep_arriving(self):
        rail = HistoricalRail()
        brain = FakeBrain()
        with tempfile.TemporaryDirectory() as directory:
            spool = Path(directory) / 'spool.db'

            def runner():
                connector = SlackWorkspaceConnector(
                    rail=rail, source_id='synthetic:slack:reconcile', workspace_id='T111',
                )
                connector.reconciliation_epoch = 'a' * 64
                return ConnectorRunner(connector=connector, brain=brain, spool_path=spool)

            with patch('connectors.slack_workspace._now', side_effect=rail.timestamp):
                active = runner()
                try:
                    for _ in range(3):
                        active.run_once()
                    self.assertEqual(active.doctor()['coverage']['history_baselined_channels'], 1)
                finally:
                    active.close()
                rail.root.update(text='Changed old decision', edited={'ts': str(rail.now)}, reply_count=1)
                rail.messages.append({'ts': str(rail.now + 1), 'thread_ts': rail.root['ts'],
                                      'user': 'U111', 'text': 'Reply on old root'})
                for number in range(40):
                    rail.now += 2
                    rail.messages.append({'ts': str(rail.now), 'user': 'U111',
                                          'text': f'Current message {number}'})
                    active = runner()
                    try:
                        active.run_once()
                    finally:
                        active.close()

        events = list(brain.events.values())
        text = [event['content'].get('text') for event in events]
        self.assertIn('Changed old decision', text)
        self.assertIn('Reply on old root', text)
        self.assertGreaterEqual(sum(value.startswith('Current message') for value in text if value), 5)
        edits = [event for event in events if event['content'].get('text') == 'Changed old decision']
        self.assertEqual({event['native_id'] for event in edits}, {'slack:T111:C111:1784332800.000100'})


if __name__ == '__main__':
    unittest.main()
