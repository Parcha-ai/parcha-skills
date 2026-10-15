"""A precommit lock refusal is known only after a real rollback ACK."""
import unittest
import time
import threading
from contextlib import contextmanager
from unittest.mock import patch

import psycopg

from e2e_collector_history_recovery import CollectorHistoryRecovery
from recall_server.collector_history_recovery import (
    CollectorHistoryRecoveryNotApplied, recover_collector_history,
    inspect_collector_history_recovery,
)


class RecoveryLockOutcome(unittest.TestCase):
    setUpClass = classmethod(CollectorHistoryRecovery.setUpClass.__func__)
    tearDownClass = classmethod(CollectorHistoryRecovery.tearDownClass.__func__)
    setUp = CollectorHistoryRecovery.setUp
    ingest = CollectorHistoryRecovery.ingest
    delete = CollectorHistoryRecovery.delete
    recover = CollectorHistoryRecovery.recover
    assert_no_correction = CollectorHistoryRecovery.assert_no_correction

    def test_native_lock_refusal_has_positive_rollback_ack(self):
        with self.store.connect() as holder, holder.transaction():
            holder.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',
                           (f'v2\x1f{self.tenant}\x1f{self.source}\x1f{self.expected["native_id"]}',))
            with self.assertRaises(RuntimeError) as caught:
                self.recover()
        self.assertEqual(getattr(caught.exception, 'status', None), 'known_not_applied')
        self.assertTrue(caught.exception.rollback_acknowledged)
        self.assertEqual(caught.exception.operation_id, self.operation_id)
        self.assert_no_correction()

    def native_holder(self, connection):
        connection.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',
                           (f'v2\x1f{self.tenant}\x1f{self.source}\x1f{self.expected["native_id"]}',))

    def test_sql_row_lock_refusal_is_known_not_applied(self):
        with self.store.connect() as holder, holder.transaction():
            holder.execute('SELECT 1 FROM canonical_documents WHERE tenant_id=%s AND source_id=%s FOR UPDATE',
                           (self.tenant, self.source))
            with self.assertRaises(CollectorHistoryRecoveryNotApplied):
                self.recover()
        self.assert_no_correction()

    def test_real_partial_write_is_rolled_back_after_audit_lock_refusal(self):
        original = self.plane._write_authorized_document
        written = []
        def write(connection, **kwargs):
            result = original(connection, **kwargs)
            written.append(result['revision'])
            return result
        with self.store.connect() as holder, holder.transaction(force_rollback=True):
            holder.execute("INSERT INTO control_audit_events(id,principal_id,operation,status,target_sha256) VALUES(%s,%s,'collector.history.recover','success',%s)",
                           (self.operation_id, self.admin, '0' * 64))
            with patch.object(self.plane, '_write_authorized_document', write):
                with self.assertRaises(CollectorHistoryRecoveryNotApplied):
                    self.recover()
        self.assertEqual(written, [3])
        self.assert_no_correction()

    def test_explicit_rollback_transport_failure_keeps_original_unknown(self):
        execute = psycopg.Connection.execute
        calls = []
        def fail(connection, query, *args, **kwargs):
            if query == 'ROLLBACK':
                calls.append(query)
                raise psycopg.OperationalError('synthetic rollback response lost')
            return execute(connection, query, *args, **kwargs)
        with self.store.connect() as holder, holder.transaction():
            self.native_holder(holder)
            with patch.object(psycopg.Connection, 'execute', fail):
                with self.assertRaisesRegex(RuntimeError, 'native_busy') as caught:
                    self.recover()
        self.assertNotIsInstance(caught.exception, CollectorHistoryRecoveryNotApplied)
        self.assertEqual(calls, ['ROLLBACK'])
        self.assert_no_correction()

    def test_transaction_rollback_failure_stays_unknown(self):
        command = psycopg.Connection._exec_command
        calls = []
        def fail(connection, query, *args, **kwargs):
            if query in ('ROLLBACK', b'ROLLBACK'):
                calls.append('ROLLBACK')
                raise psycopg.OperationalError('synthetic transaction rollback failure')
            return (yield from command(connection, query, *args, **kwargs))
        with self.store.connect() as holder, holder.transaction():
            self.native_holder(holder)
            with patch.object(psycopg.Connection, '_exec_command', fail):
                with self.assertRaises(Exception) as caught:
                    self.recover()
        self.assertNotIsInstance(caught.exception, CollectorHistoryRecoveryNotApplied)
        self.assertTrue(calls)
        self.assert_no_correction()

    def test_context_replacement_lock_error_stays_unknown(self):
        transaction = psycopg.Connection.transaction
        @contextmanager
        def replace_error(connection, *args, **kwargs):
            try:
                with transaction(connection, *args, **kwargs):
                    yield
            except RuntimeError:
                raise psycopg.errors.LockNotAvailable('synthetic replacement during exit')
        with self.store.connect() as holder, holder.transaction():
            self.native_holder(holder)
            with patch.object(psycopg.Connection, 'transaction', replace_error):
                with self.assertRaises(psycopg.errors.LockNotAvailable):
                    self.recover()
        self.assert_no_correction()

    def test_explicit_rollback_late_ack_keeps_original_unknown(self):
        execute = psycopg.Connection.execute
        deadline = time.monotonic() + 2
        late_clock = None
        def late(connection, query, *args, **kwargs):
            nonlocal late_clock
            result = execute(connection, query, *args, **kwargs)
            if query == 'ROLLBACK':
                late_clock = patch('recall_server.collector_history_recovery.time.monotonic', return_value=deadline + 1)
                late_clock.start()
            return result
        try:
            with self.store.connect() as holder, holder.transaction():
                self.native_holder(holder)
                with patch.object(psycopg.Connection, 'execute', late):
                    with self.assertRaisesRegex(RuntimeError, 'native_busy') as caught:
                        recover_collector_history(self.plane, tenant_id=self.tenant, principal_id=self.admin,
                            expected=self.expected, operation_id=self.operation_id, deadline_at=deadline)
            self.assertNotIsInstance(caught.exception, CollectorHistoryRecoveryNotApplied)
        finally:
            if late_clock is not None:
                late_clock.stop()
        self.assert_no_correction()

    def test_sql_lock_error_after_real_commit_stays_unknown(self):
        transaction = psycopg.Connection.transaction
        write = self.plane._write_authorized_document
        mutation_connections = set()
        def record(connection, **kwargs):
            mutation_connections.add(id(connection))
            return write(connection, **kwargs)
        @contextmanager
        def late_error(connection, *args, **kwargs):
            with transaction(connection, *args, **kwargs):
                yield
            if id(connection) in mutation_connections:
                raise psycopg.errors.LockNotAvailable('synthetic error after actual commit')
        with patch.object(self.plane, '_write_authorized_document', record), patch.object(psycopg.Connection, 'transaction', late_error):
            with self.assertRaises(psycopg.errors.LockNotAvailable):
                self.recover()
        result = inspect_collector_history_recovery(self.store, tenant_id=self.tenant,
            principal_id=self.admin, expected=self.expected, operation_id=self.operation_id)
        self.assertEqual(result['status'], 'recorded_applied')
        self.assertEqual(result['original_commit_outcome'], 'unknown')

    def test_same_operation_contenders_never_double_apply(self):
        write = self.plane._write_authorized_document
        entered, release = threading.Event(), threading.Event()
        outcomes = []
        def held(connection, **kwargs):
            entered.set()
            if not release.wait(5):
                raise AssertionError('contender did not finish')
            return write(connection, **kwargs)
        def first():
            try:
                outcomes.append(self.recover())
            except Exception as error:
                outcomes.append(error)
        with patch.object(self.plane, '_write_authorized_document', held):
            thread = threading.Thread(target=first)
            thread.start()
            try:
                self.assertTrue(entered.wait(5))
                with self.assertRaises(CollectorHistoryRecoveryNotApplied):
                    self.recover()
            finally:
                release.set()
                thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(outcomes), 1)
        self.assertIsInstance(outcomes[0], dict)
        self.assertEqual(outcomes[0]['revision'], 3)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_events WHERE tenant_id=%s AND source_id=%s AND revision=3',
                (self.tenant, self.source)).fetchone()['n'], 1)


if __name__ == '__main__':
    unittest.main(defaultTest='RecoveryLockOutcome', verbosity=2)
