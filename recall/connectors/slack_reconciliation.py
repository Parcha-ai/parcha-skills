"""ACK-bound historical Slack traversal alongside the existing live cursor."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import sqlite3
from decimal import Decimal
from typing import Callable

from connectors.sdk import ConnectorContractError, ConnectorPage, ConnectorUpstreamError

LIVE_PAGES_PER_HISTORY_PAGE = 8
LOG = logging.getLogger(__name__)
_CONTRACT_REASONS = {
    'slack time pagination is invalid': 'pagination_invalid',
    'slack time pagination timestamp is invalid': 'pagination_timestamp_invalid',
    'slack time pagination did not advance': 'pagination_not_advanced',
    'slack time pagination has no boundary': 'pagination_boundary_missing',
    'slack historical thread root is missing': 'thread_root_missing',
}


def time_page(response, messages, query, *, descending):
    """Slack timestamp pagination survives pauses longer than API cursor leases."""
    from connectors.slack_workspace import _next
    cursor = _next({**response, 'has_more': False})
    if response.get('has_more') not in (None, True, False):
        raise ConnectorContractError('slack time pagination is invalid')
    timestamps = []
    for message in messages:
        value = message.get('ts') if isinstance(message, dict) else None
        if not isinstance(value, str) or not re.fullmatch(r'[0-9]{1,16}(?:\.[0-9]{1,6})?', value):
            raise ConnectorContractError('slack time pagination timestamp is invalid')
        lower, current, upper = (Decimal(query.get('oldest', '0')),
                                 Decimal(value), Decimal(query['latest']))
        # Replies may echo only the requested parent after the last reply.
        # Accept that explicit terminal page, never a truncated continuation.
        if (not descending and 'oldest' in query and len(messages) == 1
                and value == query.get('ts')
                and current <= lower and current < upper
                and response.get('has_more') is False and cursor is None):
            continue
        if not lower < current < upper:
            relation = ('before_oldest' if current < lower else
                        'equal_oldest' if current == lower else
                        'equal_latest' if current == upper else 'after_latest')
            LOG.warning(
                'slack historical boundary refused direction=%s relation=%s '
                'root=%s more=%s cursor=%s count=%s',
                'descending' if descending else 'ascending', relation,
                value == query.get('ts'), response.get('has_more') is True,
                cursor is not None, len(messages),
            )
            raise ConnectorContractError('slack time pagination did not advance')
        timestamps.append(value)
    if response.get('has_more') is not True and cursor is None:
        return None
    if not timestamps:
        raise ConnectorContractError('slack time pagination has no boundary')
    return (min if descending else max)(timestamps, key=Decimal)


class SlackReconciliation:
    def __init__(self, connector, db: sqlite3.Connection, provider_epoch: str):
        if not isinstance(provider_epoch, str) or not re.fullmatch('[0-9a-f]{64}', provider_epoch):
            raise ConnectorContractError('slack reconciliation authority is invalid')
        self.connector = connector
        self.db = db
        self.scope = connector._scope
        self.epoch = hashlib.sha256(json.dumps([
            connector.source_id, self.scope, connector._selection, provider_epoch,
        ], separators=(',', ':')).encode()).hexdigest()
        self.proposed = None
        db.execute('''CREATE TABLE IF NOT EXISTS slack_reconciliation_state(
            workspace_id TEXT NOT NULL, authority TEXT NOT NULL, state_json TEXT NOT NULL,
            PRIMARY KEY(workspace_id,authority))''')
        db.execute('''CREATE TABLE IF NOT EXISTS slack_reconciliation_pending(
            workspace_id TEXT NOT NULL, authority TEXT NOT NULL,
            state_json TEXT NOT NULL, observations_json TEXT NOT NULL,
            PRIMARY KEY(workspace_id,authority))''')
        db.execute('''CREATE TABLE IF NOT EXISTS slack_reconciliation_channels(
            workspace_id TEXT NOT NULL, authority TEXT NOT NULL, channel_id TEXT NOT NULL,
            epoch TEXT NOT NULL, cursor_json TEXT, generation INTEGER NOT NULL,
            last_error_code TEXT, PRIMARY KEY(workspace_id,authority,channel_id))''')
        db.execute('''CREATE TABLE IF NOT EXISTS slack_message_inventory(
            workspace_id TEXT NOT NULL, authority TEXT NOT NULL, native_id TEXT NOT NULL,
            channel_id TEXT NOT NULL, message_ts TEXT NOT NULL, parent_id TEXT,
            edited_at TEXT, observed_epoch TEXT NOT NULL, historical_generation INTEGER NOT NULL,
            PRIMARY KEY(workspace_id,authority,native_id))''')

    @staticmethod
    def marker(cursor):
        if cursor is None:
            return None
        try:
            value = json.loads(cursor)
        except (TypeError, ValueError):
            return None
        if not isinstance(value, dict) or 'slack_reconciliation' not in value:
            return None
        if (set(value) != {'slack_reconciliation', 'step'}
                or value['slack_reconciliation'] != 1
                or type(value['step']) is not int or value['step'] < 1):
            raise ConnectorContractError('slack reconciliation cursor is invalid')
        return value['step']

    def state(self, cursor):
        marker = self.marker(cursor)
        row = self.db.execute('''SELECT state_json FROM slack_reconciliation_state
            WHERE workspace_id=? AND authority=?''', self.scope).fetchone()
        if marker is None:
            if row:
                raise ConnectorContractError('slack reconciliation cursor lost its checkpoint')
            state = {'step': 0, 'epoch': self.epoch, 'selection': self.connector._selection,
                     'live_cursor': cursor, 'last_channel': '',
                     'live_pages': 0, 'historical_pages': 0,
                     'completed_channels': 0, 'acknowledged_messages': 0,
                     'last_error_code': None, 'historical_failures': 0}
        else:
            if not row:
                raise ConnectorContractError('slack reconciliation checkpoint is missing')
            state = json.loads(row[0])
            if state['step'] != marker:
                raise ConnectorContractError('slack reconciliation checkpoint is inconsistent')
        if state['epoch'] != self.epoch:
            from connectors.slack_workspace import _cursor, _initial_state
            prior = json.loads(state['live_cursor']) if state['live_cursor'] else {}
            fresh = _initial_state(cycle=prior.get('cycle', 0) + 1,
                                   public_history=self.connector.public_history)
            state.update(epoch=self.epoch, last_channel='', live_pages=0, last_error_code=None,
                         selection=self.connector._selection, live_cursor=_cursor(fresh),
                         historical_pages=0, completed_channels=0, historical_failures=0)
        return state

    def next_channel(self, state):
        selected = self.connector.channel_ids
        clause = (' AND channel_id IN (' + ','.join('?' for _ in selected) + ')') if selected else ''
        query = '''SELECT channel_id FROM slack_channel_coverage
            WHERE workspace_id=? AND authority=? AND history_through IS NOT NULL
              AND capture_version=? AND last_seen_cycle >= COALESCE(
                  (SELECT completed_cycle FROM slack_discovery_coverage
                   WHERE workspace_id=? AND authority=?),0)''' + clause + '''
              AND channel_id>? ORDER BY channel_id LIMIT 1'''
        from connectors.slack_source import SLACK_MESSAGE_CAPTURE_VERSION
        params = (*self.scope, SLACK_MESSAGE_CAPTURE_VERSION, *self.scope, *selected)
        row = self.db.execute(query, (*params, state['last_channel'])).fetchone()
        if row is None:
            row = self.db.execute(query, (*params, '')).fetchone()
        return row[0] if row else None

    def pull(self, cursor, pull: Callable[..., ConnectorPage]):
        from connectors.slack_workspace import (
            EPOCH, SlackChannelUnavailable, SlackThreadUnavailable, _cursor, _initial_state,
        )
        state = self.state(cursor)
        historical = (getattr(self.connector, 'reconciliation_enabled', True)
                      and state['live_pages'] >= LIVE_PAGES_PER_HISTORY_PAGE)
        channel_state = None
        channel = self.next_channel(state) if historical else None
        historical = channel is not None
        if historical:
            row = self.db.execute('''SELECT epoch,cursor_json,generation FROM slack_reconciliation_channels
                WHERE workspace_id=? AND authority=? AND channel_id=?''', (*self.scope, channel)).fetchone()
            same_epoch = row and row[0] == self.epoch
            before = row[1] if same_epoch else None
            generation = row[2] if same_epoch else 0
            if before is None:
                generation += 1
                initial = _initial_state(public_history=self.connector.public_history)
                initial.update(phase='history', channels=[channel + ':1'], channel_lower=EPOCH,
                               configured_index=len(self.connector.channel_ids))
                before = _cursor(initial)
            channel_state = {'channel': channel, 'cursor': before,
                             'generation': generation, 'error': None}
        else:
            before = state['live_cursor']
        failure = None
        try:
            page = pull(before, historical=historical)
        except (SlackChannelUnavailable, ConnectorContractError) as error:
            if not historical:
                raise
            if isinstance(error, ConnectorContractError):
                # Emit only fixed labels, never exception text or provider data.
                reason = error.args[0] if len(error.args) == 1 and type(error.args[0]) is str else None
                LOG.warning('slack historical page refused reason=%s',
                            _CONTRACT_REASONS.get(reason, 'unclassified_contract_error'))
            # Preserve the historical position but yield to live traffic. This
            # empty checkpoint records a failed attempt, never completed history.
            failure = (error.error_code if isinstance(error, ConnectorUpstreamError)
                       else 'connector_invalid_page')
            if isinstance(error, SlackThreadUnavailable):
                # Abandon only this incomplete window. The next turn starts a
                # new generation; a missing thread never proves deletion.
                channel_state['cursor'] = None
                failure = 'slack_thread_not_found'
            page = ConnectorPage(records=(), next_cursor=before, has_more=True)
        after = dict(state)
        if historical:
            after['last_channel'] = channel
        if failure:
            channel_state['error'] = failure
            after.update(live_pages=0, last_error_code=failure,
                         historical_failures=state['historical_failures'] + 1)
        elif historical:
            history = json.loads(page.next_cursor)
            after.update(live_pages=0, historical_pages=state['historical_pages'] + 1,
                         last_error_code=None)
            if history['phase'] == 'users':
                channel_state['cursor'] = None
                after['completed_channels'] += 1
            else:
                channel_state['cursor'] = page.next_cursor
        else:
            after.update(live_cursor=page.next_cursor, live_pages=state['live_pages'] + 1)
        after['step'] += 1
        after['channel_checkpoint'] = channel_state
        after['live_before'] = state['live_cursor']
        self.proposed = after
        next_cursor = json.dumps({'slack_reconciliation': 1, 'step': after['step']}, separators=(',', ':'))
        return ConnectorPage(records=page.records, next_cursor=next_cursor,
                             has_more=page.has_more if not historical else True)

    def stage(self, current, records):
        step = self.marker(current)
        if step is None:
            return
        if self.proposed is None or self.proposed['step'] != step:
            raise ConnectorContractError('slack reconciliation page was not prepared')
        observations = []
        prefix = 'slack:' + self.connector.workspace_id + ':'
        for record in records:
            if record.content.get('kind') != 'communication_message.v1' or record.deleted:
                continue
            if not record.native_id.startswith(prefix):
                raise ConnectorContractError('slack reconciliation native identity is invalid')
            channel, timestamp = record.native_id[len(prefix):].split(':', 1)
            observations.append([record.native_id, channel, timestamp,
                                 record.native_parent_id, record.content.get('edited_at')])
        self.db.execute('''INSERT INTO slack_reconciliation_pending
            (workspace_id,authority,state_json,observations_json) VALUES (?,?,?,?)
            ON CONFLICT(workspace_id,authority) DO UPDATE SET
                state_json=excluded.state_json, observations_json=excluded.observations_json''',
            (*self.scope, json.dumps(self.proposed), json.dumps(observations)))

    def commit(self, previous, current, commit_live):
        step = self.marker(current)
        if step is None:
            commit_live(previous, current)
            return
        row = self.db.execute('''SELECT state_json,observations_json FROM slack_reconciliation_pending
            WHERE workspace_id=? AND authority=?''', self.scope).fetchone()
        if not row:
            raise ConnectorContractError('slack reconciliation pending checkpoint is missing')
        state = json.loads(row[0])
        if state['step'] != step:
            raise ConnectorContractError('slack reconciliation pending checkpoint is inconsistent')
        channel_state = state.pop('channel_checkpoint')
        historical = channel_state is not None
        live_before = state.pop('live_before')
        if not historical and state['epoch'] == self.epoch:
            commit_live(live_before, state['live_cursor'])
        if historical:
            self.db.execute('''INSERT INTO slack_reconciliation_channels
                (workspace_id,authority,channel_id,epoch,cursor_json,generation,last_error_code)
                VALUES (?,?,?,?,?,?,?) ON CONFLICT(workspace_id,authority,channel_id)
                DO UPDATE SET epoch=excluded.epoch,cursor_json=excluded.cursor_json,
                    generation=excluded.generation,last_error_code=excluded.last_error_code''',
                (*self.scope, channel_state['channel'], state['epoch'],
                 channel_state['cursor'], channel_state['generation'], channel_state['error']))
        observations = json.loads(row[1])
        identities = {observation[0] for observation in observations}
        if identities:
            existing = self.db.execute('''SELECT count(*) FROM slack_message_inventory
                WHERE workspace_id=? AND authority=? AND native_id IN ('''
                + ','.join('?' for _ in identities) + ')', (*self.scope, *identities)).fetchone()[0]
            state['acknowledged_messages'] += len(identities) - existing
        self.db.executemany('''INSERT INTO slack_message_inventory
            (workspace_id,authority,native_id,channel_id,message_ts,parent_id,edited_at,
             observed_epoch,historical_generation) VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(workspace_id,authority,native_id) DO UPDATE SET
                parent_id=excluded.parent_id,edited_at=excluded.edited_at,
                observed_epoch=excluded.observed_epoch,
                historical_generation=CASE WHEN ? THEN excluded.historical_generation
                    WHEN observed_epoch<>excluded.observed_epoch THEN -1 ELSE historical_generation END''',
            [(*self.scope, *observation, state['epoch'], channel_state['generation'] if historical else -1, historical)
             for observation in observations])
        self.db.execute('''INSERT INTO slack_reconciliation_state(workspace_id,authority,state_json)
            VALUES (?,?,?) ON CONFLICT(workspace_id,authority) DO UPDATE SET state_json=excluded.state_json''',
            (*self.scope, json.dumps(state)))
        self.db.execute('''DELETE FROM slack_reconciliation_pending
            WHERE workspace_id=? AND authority=?''', self.scope)

    def status(self):
        row = self.db.execute('''SELECT state_json FROM slack_reconciliation_state
            WHERE workspace_id=? AND authority=?''', self.scope).fetchone()
        state = json.loads(row[0]) if row else {}
        pending = self.db.execute('''SELECT 1 FROM slack_reconciliation_pending
            WHERE workspace_id=? AND authority=?''', self.scope).fetchone()
        channels = self.db.execute('''SELECT
            sum(CASE WHEN cursor_json IS NOT NULL THEN 1 ELSE 0 END),
            sum(CASE WHEN last_error_code IS NOT NULL THEN 1 ELSE 0 END)
            FROM slack_reconciliation_channels WHERE workspace_id=? AND authority=? AND epoch=?''',
            (*self.scope, self.epoch)).fetchone()
        return {
            'enabled': getattr(self.connector, 'reconciliation_enabled', True),
            'authority_current': state.get('epoch') == self.epoch,
            'acknowledged_messages': state.get('acknowledged_messages', 0),
            'historical_pages': state.get('historical_pages', 0),
            'completed_channel_visits': state.get('completed_channels', 0),
            'historical_page_in_progress': bool(channels[0]),
            'historical_channels_with_errors': channels[1] or 0,
            'pending_checkpoint': pending is not None,
            'last_error_code': state.get('last_error_code'),
            'historical_failures': state.get('historical_failures', 0),
        }
