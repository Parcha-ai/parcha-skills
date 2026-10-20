"""Native cancellation of the optional probe must leave time for archive proof."""
from dataclasses import replace
import hashlib
import json
import os
import time
import unittest
from unittest.mock import patch

import psycopg

import e2e_parent_retirement_window as original
from recall_server.db import SearchDeadlineExceeded
from recall_server.parent_chunk_proof import parent_retirement_plan, read_parent_catalog
from recall_server.parent_chunk_window import retire_window
from recall_server import parent_chunk_window as window_owner


class NoWorkBudgetTests(unittest.TestCase):
    state = original.WindowTests.state
    progress = original.WindowTests.progress
    actual_writer_fixture = original.WindowTests.actual_writer_fixture

    def setUp(self):
        original.WindowTests.setUp(self)
        self.actual_writer_fixture()
        self.store.verified_singleton_receipts = True
        self.store.search_plane = 'turbopuffer'
        with self.store.connect() as connection:
            current = read_parent_catalog(self.store, connection, self.scope, time.monotonic()+5)
        self.plan = parent_retirement_plan(self.scope, current['manifest'])

    def test_cancelled_optional_selection_falls_back_to_same_parent_archive_proof(self):
        execute = self.store._execute_bounded
        diagnostic = {}
        observations = []
        before = self.state()
        started = time.monotonic()
        parent_deadline = started + 15
        with patch.object(self.archive, 'read_raw', wraps=self.archive.read_raw) as archive_read:
            def blocked_selection(connection, sql, values, deadline):
                if 'AS physical_row' not in sql:
                    return execute(connection, sql, values, deadline)
                self.assertFalse(observations)
                self.assertEqual(diagnostic['phase'], 'no_work_sql')
                self.assertEqual(archive_read.call_count, 0)
                # The exact native SELECT first references this relation here.
                # No authority SQL, query text, or statement deadline is replaced.
                with psycopg.connect(os.environ['RECALL_DATABASE_URL']) as blocker:
                    blocker.execute("SET LOCAL lock_timeout='1s'")
                    blocker.execute('LOCK canonical_chunks IN ACCESS EXCLUSIVE MODE')
                    try:
                        return execute(connection, sql, values, deadline)
                    except SearchDeadlineExceeded as exc:
                        self.assertIsInstance(exc.__cause__, psycopg.errors.QueryCanceled)
                        observations.append(dict(sql_sha256=hashlib.sha256(sql.encode()).hexdigest(),
                            phase=diagnostic['phase'], archive_gets=archive_read.call_count,
                            elapsed_ms=(time.monotonic()-started)*1000,
                            parent_remaining_ms=(parent_deadline-time.monotonic())*1000))
                        raise

            try:
                with patch.object(self.store, '_execute_bounded', blocked_selection):
                    result = retire_window(self.store, self.archive, scope=self.scope,
                        principal_id=self.principal, limits=replace(self.limits, batch_documents=1),
                        deadline_at=parent_deadline, reviewed_plan=self.plan,
                        metadata_mode='singleton', diagnostic=diagnostic)
            except SearchDeadlineExceeded:
                self.assertEqual(len(observations), 1)
                self.assertEqual(self.state(), before)
                self.assertEqual(self.progress()['last_record_ordinal'], -1)
                self.assertEqual(archive_read.call_count, 0)
                print(json.dumps(dict(kind='optional_probe_blocked_native_window', observations=observations)))
                raise
        self.assertEqual(len(observations), 1)
        self.assertGreater(observations[0]['parent_remaining_ms'], 5000)
        self.assertGreater(archive_read.call_count, 0)
        self.assertEqual(result['cleared_documents'], 1)
        self.assertEqual(result['retired_metadata_chunks'], 1)
        self.assertEqual(self.progress()['last_record_ordinal'], 0)
        self.assertFalse(result['complete'])
        print(json.dumps(dict(kind='optional_probe_native_fallback', observations=observations,
            archive_gets=archive_read.call_count, cleared_documents=result['cleared_documents'])))

    def test_real_no_work_commit_then_lost_ack_never_reenters_archive_proof(self):
        arguments = dict(scope=self.scope, principal_id=self.principal,
            limits=replace(self.limits, batch_documents=1), reviewed_plan=self.plan,
            metadata_mode='singleton', deadline_at=time.monotonic()+15, diagnostic={})
        first = retire_window(self.store, self.archive, **arguments)
        self.assertEqual(first['retired_metadata_chunks'], 1)
        before = self.state()
        with self.store.connect() as connection:
            connection.execute('''UPDATE canonical_chunk_retirement_progress
                SET last_record_ordinal=-1 WHERE tenant_id=%s AND source_id=%s
                AND native_parent_id=%s''', self.scope)
        advance = window_owner._advance_without_physical_work
        def lost_after_commit(*args, **kwargs):
            result = advance(*args, **kwargs)
            self.assertEqual(result['skipped_records'], 1)
            self.assertEqual(kwargs['diagnostic']['phase'], 'no_work_commit_ack')
            raise ConnectionError('synthetic lost acknowledgement after real probe commit')
        with patch.object(window_owner, '_advance_without_physical_work', lost_after_commit), \
             patch.object(self.archive, 'read_raw', wraps=self.archive.read_raw) as read:
            with self.assertRaises(ConnectionError):
                retire_window(self.store, self.archive, **arguments)
        read.assert_not_called()
        self.assertEqual(self.state(), before)
        self.assertEqual(self.progress()['last_record_ordinal'], 0)

    def test_timed_out_probe_is_not_repeated_within_one_parent_attempt(self):
        from recall_server.chunk_bodies import read_archived_chunks
        documents = tuple(row['document_id'] for row in self.docs)
        expected = read_archived_chunks(self.store, self.archive, tenant_id=self.tenant,
            source_ids=(self.source,), document_ids=documents, deadline_at=time.monotonic()+10)
        execute = self.store._execute_bounded
        cancellations = []
        diagnostic = {}

        def blocked_selection(connection, sql, values, deadline):
            if 'AS physical_row' not in sql:
                return execute(connection, sql, values, deadline)
            with psycopg.connect(os.environ['RECALL_DATABASE_URL']) as blocker:
                blocker.execute("SET LOCAL lock_timeout='1s'")
                blocker.execute('LOCK canonical_chunks IN ACCESS EXCLUSIVE MODE')
                try:
                    return execute(connection, sql, values, deadline)
                except SearchDeadlineExceeded as error:
                    self.assertIsInstance(error.__cause__, psycopg.errors.QueryCanceled)
                    self.assertEqual(diagnostic['phase'], 'no_work_sql')
                    cancellations.append(self.progress()['last_record_ordinal'])
                    raise

        arguments = dict(tenant_id=self.tenant, source_id=self.source,
            native_parent_id=self.scope[2], apply=True, reviewed_plan=self.plan,
            required_scope_epoch=self.progress()['scope_epoch'],
            owner_principal_id=self.principal, metadata_mode='singleton',
            limits=replace(self.limits, batch_documents=1, max_batches=2),
            diagnostic=diagnostic)
        with patch.object(self.store, '_execute_bounded', blocked_selection):
            first = window_owner.retire_parent_windows(self.store, self.archive,
                deadline_at=time.monotonic()+20, **arguments)
            self.assertEqual(first['cleared_documents'], 2)
            self.assertEqual(first['retired_metadata_chunks'], 2)
            self.assertEqual(first['batches'], 2)
            self.assertEqual(self.progress()['last_record_ordinal'], 1)
            self.assertEqual(cancellations, [-1])
            self.assertFalse(first['complete'])
            # A new bounded attempt retries the optimization; it retains no
            # scheduling state from the earlier attempt or another parent.
            second = window_owner.retire_parent_windows(self.store, self.archive,
                deadline_at=time.monotonic()+20, **arguments)
            self.assertEqual(cancellations, [-1, 1])
            self.assertTrue(second['complete'])
            self.assertEqual(second['cleared_documents'], 0)
        self.assertEqual(self.progress()['status'], 'complete')
        self.assertEqual(read_archived_chunks(self.store, self.archive,
            tenant_id=self.tenant, source_ids=(self.source,), document_ids=documents,
            deadline_at=time.monotonic()+10), expected)


if __name__ == '__main__':
    unittest.main()
