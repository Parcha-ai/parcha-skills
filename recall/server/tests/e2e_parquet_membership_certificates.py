#!/usr/bin/env python3
"""Parquet reuses exact membership certificates inside its native stream snapshot."""
import os
import time
import unittest
from contextlib import contextmanager
from unittest.mock import patch

from psycopg import ServerCursor, sql

from e2e_parent_membership_enrollment import ParentEnrollmentTests
from recall_server import parent_membership_enrollment as enrollment
from recall_server import parent_membership_read as reader
from recall_server.db import SearchDeadlineExceeded
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.parquet_scan import CanonicalParquetScanProjector
from recall_server.receipt_membership import lookup_current_search_membership


class ParquetCertificateTests(ParentEnrollmentTests):
    def setUp(self):
        super().setUp()
        self.store.verified_singleton_receipts = True
        self.store.search_deadline_ms = 6000
        self.scan = CanonicalParquetScanProjector(self.store, LogicalEvidenceProjectionStore(self.archive))
        self.candidate = self.scan._pending(tenant_id=self.tenant, limit=1)[0]
        with self.store.connect() as connection:
            self.documents = [row['logical_document_id'] for row in connection.execute(
                'SELECT DISTINCT logical_document_id FROM canonical_passages WHERE tenant_id=%s',
                (self.tenant,)).fetchall()]

    def stream(self, enabled=True, epoch='synthetic-epoch-a'):
        with patch.dict(os.environ, RECALL_PARENT_MEMBERSHIP_READ='1' if enabled else '0',
                        RECALL_PARENT_MEMBERSHIP_EPOCH=epoch):
            return list(self.scan._passages(self.candidate, self.documents))

    def test_cancelled_native_passage_proof_does_not_start_sibling_preservation(self):
        execute = self.store._execute_bounded
        attempts = []

        def cancelled(connection, statement, parameters, deadline):
            if statement.startswith('WITH requested AS MATERIALIZED'):
                attempts.append(deadline)
                # Actual PostgreSQL statement cancellation inside the native
                # receipt proof, without changing its error/cleanup owner.
                return execute(connection, 'SELECT pg_sleep(0.1)', (), time.monotonic() + 0.02)
            return execute(connection, statement, parameters, deadline)

        with patch.dict(os.environ, RECALL_PARENT_MEMBERSHIP_READ='0'), \
             patch.object(self.store, '_execute_bounded', side_effect=cancelled), \
             patch.object(self.scan, '_preserve_rows', wraps=self.scan._preserve_rows) as preserve:
            with self.assertRaises(SearchDeadlineExceeded):
                self.scan._build(self.candidate)
        self.assertEqual(len(attempts), 1)
        preserve.assert_not_called()
        with self.store.connect() as connection:
            for table in ('canonical_parquet_scan_shards', 'canonical_evidence_cleanup_queue'):
                self.assertEqual(connection.execute(
                    'SELECT count(*) AS n FROM ' + table + ' WHERE tenant_id=%s',
                    (self.tenant,)).fetchone()['n'], 0)
            queue = connection.execute(
                'SELECT generation,changed_at FROM canonical_parquet_scan_queue WHERE tenant_id=%s',
                (self.tenant,)).fetchone()
        self.assertEqual(queue, dict(generation=self.candidate.generation,
                                     changed_at=self.candidate.changed_at))

    def test_new_queue_generation_during_preservation_keeps_original_publication_fence(self):
        preserve = self.scan._preserve_rows
        proved = []
        native = lookup_current_search_membership

        def proof(*args, **kwargs):
            result = native(*args, **kwargs)
            proved.append(True)
            return result

        def changed(*args):
            self.assertTrue(proved)
            self.assertEqual(self.scan.seed_backfill(tenant_id=self.tenant), 1)
            return preserve(*args)

        totals = {'stale': 0}
        with patch.dict(os.environ, RECALL_PARENT_MEMBERSHIP_READ='0'), \
             patch('recall_server.receipt_membership.lookup_current_search_membership', side_effect=proof), \
             patch.object(self.scan, '_preserve_rows', side_effect=changed):
            self.assertTrue(self.scan._process(self.candidate, totals))
        self.assertEqual(totals, {'stale': 1})
        with self.store.connect() as connection:
            queue = connection.execute(
                'SELECT generation FROM canonical_parquet_scan_queue WHERE tenant_id=%s',
                (self.tenant,)).fetchone()
            self.assertGreater(queue['generation'], self.candidate.generation)
            self.assertEqual(connection.execute(
                'SELECT count(*) AS n FROM canonical_parquet_scan_shards WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['n'], 0)
            self.assertGreater(connection.execute(
                'SELECT count(*) AS n FROM canonical_evidence_cleanup_queue WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['n'], 0)

    def test_fetched_metadata_and_bytes_match_original_query_in_same_snapshot(self):
        self.enroll()
        connect = self.store.connect
        compared = []
        case = self
        class ComparedCursor(ServerCursor):
            def fetchmany(self, size=0):
                rows = super().fetchmany(size)
                if rows:
                    metadata = self.connection.execute(reader._METADATA_SQL,
                        (case.tenant, [row['source_id'] for row in rows],
                         [row['passage_id'] for row in rows])).fetchall()
                    expected = {(row['source_id'], row['passage_id']): row for row in metadata}
                    case.assertEqual(sum(row['membership_metadata_bytes'] for row in rows),
                                     metadata[0]['metadata_bytes'])
                    for row in rows:
                        case.assertTrue(row['membership_metadata_complete'])
                        extra = dict(expected[row['source_id'], row['passage_id']])
                        extra.pop('metadata_bytes'); extra.pop('selected_count')
                        case.assertEqual(row['membership_metadata'], extra)
                    compared.append(len(rows))
                return rows
        class Connection:
            def __init__(self, connection): self.connection = connection
            def __getattr__(self, name): return getattr(self.connection, name)
            def cursor(self, name):
                return ComparedCursor(self.connection, name, row_factory=self.connection.row_factory)
        @contextmanager
        def checking():
            with connect() as connection:
                yield Connection(connection)
        baseline = self.stream(False)
        with patch.object(self.store, 'connect', side_effect=checking):
            self.assertEqual(self.stream(), baseline)
        self.assertEqual(compared, [len(self.ids)])

    def test_certified_stream_matches_disabled_without_native_receipt_query(self):
        self.enroll()
        before = self.stream(False)
        self.assertTrue(before)
        with patch('recall_server.receipt_membership.lookup_current_search_membership',
                   wraps=lookup_current_search_membership) as native:
            after = self.stream()
        self.assertEqual(after, before)
        native.assert_not_called()

    def test_disabled_does_not_query_certificates(self):
        self.enroll()
        with patch.object(reader, 'partition', side_effect=AssertionError('disabled')):
            with patch('recall_server.receipt_membership.lookup_current_search_membership',
                       wraps=lookup_current_search_membership) as native:
                self.assertTrue(self.stream(False))
        self.assertEqual(native.call_count, 1)
        self.assertEqual(set(native.call_args.kwargs['receipts']), {self.receipt, self.physical_receipt})

    def test_full_scan_build_and_publication_preserve_native_objects(self):
        self.enroll()
        with patch.dict(os.environ, RECALL_PARENT_MEMBERSHIP_READ='0'):
            baseline = self.scan._build(self.candidate)
        with patch.dict(os.environ, RECALL_PARENT_MEMBERSHIP_READ='1',
                        RECALL_PARENT_MEMBERSHIP_EPOCH='synthetic-epoch-a'):
            with patch('recall_server.receipt_membership.lookup_current_search_membership',
                       wraps=lookup_current_search_membership) as native:
                certified = self.scan._build(self.candidate)
        native.assert_not_called()
        self.assertEqual(certified.references, baseline.references)
        self.assertEqual(certified.row_counts, baseline.row_counts)
        self.assertEqual(certified.members, baseline.members)
        self.assertEqual(certified.generation_sha256, baseline.generation_sha256)
        self.assertEqual(self.scan._commit(self.candidate, certified), 'committed')
        with self.store.connect() as c:
            self.assertEqual(c.execute('SELECT count(*) AS n FROM canonical_parquet_scan_queue '
                                       'WHERE tenant_id=%s', (self.tenant,)).fetchone()['n'], 0)
            self.assertEqual(c.execute('SELECT count(DISTINCT dataset) AS n FROM canonical_parquet_scan_shards '
                                       'WHERE tenant_id=%s', (self.tenant,)).fetchone()['n'], 4)

    def test_missing_and_wrong_epoch_keep_exact_native_fallback(self):
        baseline = self.stream(False)
        for epoch in ('synthetic-epoch-a', 'wrong-epoch'):
            with self.subTest(epoch=epoch):
                with patch('recall_server.receipt_membership.lookup_current_search_membership',
                           wraps=lookup_current_search_membership) as native:
                    self.assertEqual(self.stream(epoch=epoch), baseline)
                self.assertEqual(native.call_count, 1)
                self.assertEqual(set(native.call_args.kwargs['receipts']), {self.receipt, self.physical_receipt})
            self.enroll()

    def clone_passages(self, count, receipt):
        with self.store.connect() as c:
            columns = [row['attname'] for row in c.execute(
                "SELECT attname FROM pg_attribute WHERE attrelid='canonical_passages'::regclass "
                "AND attnum>0 AND NOT attisdropped AND attgenerated='' ORDER BY attnum").fetchall()]
            # New rows deliberately carry no certificates, even when the source is enrolled.
            overrides = {'passage_id', 'ordinal', 'receipts', 'membership_generation', 'membership_binding_sha256'}
            selected = [sql.Placeholder() if key in overrides else sql.Identifier(key) for key in columns]
            statement = sql.SQL('INSERT INTO canonical_passages ({}) SELECT {} FROM canonical_passages '
                                'WHERE tenant_id=%s AND passage_id=%s').format(
                sql.SQL(',').join(map(sql.Identifier, columns)), sql.SQL(',').join(selected))
            values = []
            for ordinal in range(count):
                data = dict(passage_id=f'psg_{ordinal:032x}', ordinal=ordinal + 100,
                            receipts=[receipt], membership_generation=None, membership_binding_sha256=None)
                values.append(tuple(data[key] for key in columns if key in overrides) + (self.tenant, self.ids[0]))
            with c.cursor() as cursor:
                cursor.executemany(statement, values)

    def test_mixed_certificates_verify_only_original_fallback_receipts(self):
        self.enroll()
        self.clone_passages(2, self.physical_receipt)
        baseline = self.stream(False)
        with patch('recall_server.receipt_membership.lookup_current_search_membership',
                   wraps=lookup_current_search_membership) as native:
            self.assertEqual(self.stream(), baseline)
        self.assertEqual(native.call_count, 1)
        self.assertEqual(native.call_args.kwargs['receipts'], (self.physical_receipt,))

    def test_changed_fields_follow_exact_binding_contract(self):
        changes = (
            ('canonical_passages', 'text_sha256', 'a' * 64),
            ('canonical_passages', 'receipts', ['recall://' + self.source + '/missing?rev=1#item=0']),
            ('canonical_evidence_documents', 'manifest_content_sha256', 'b' * 64),
            ('canonical_passage_documents', 'source_document_sha256', 'c' * 64),
        )
        for table, column, changed in changes:
            with self.subTest(column=column):
                self.enroll()
                with self.store.connect() as c:
                    old = c.execute(sql.SQL('SELECT {} FROM {} WHERE tenant_id=%s LIMIT 1').format(
                        sql.Identifier(column), sql.Identifier(table)), (self.tenant,)).fetchone()[column]
                    update = sql.SQL('UPDATE {} SET {}=%s WHERE tenant_id=%s').format(
                        sql.Identifier(table), sql.Identifier(column))
                    c.execute(update, (changed, self.tenant))
                baseline = self.stream(False)
                with patch('recall_server.receipt_membership.lookup_current_search_membership',
                           wraps=lookup_current_search_membership) as native:
                    self.assertEqual(self.stream(), baseline)
                stable = column not in enrollment._BINDING_FIELDS
                self.assertEqual(native.call_count, 0 if stable else 1)
                if stable:
                    self.assertTrue(baseline)
                    compared = self.compare()
                    self.assertEqual(compared['native_allowed_passages'], len(self.ids))
                    self.assertEqual(compared['certified_passages'], len(self.ids))
                    self.assertEqual(compared['disagreements'], 0)
                with self.store.connect() as c:
                    c.execute(update, (old, self.tenant))

    def test_deleted_publication_cannot_leave_certified_scan_rows(self):
        self.enroll()
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_passage_documents WHERE tenant_id=%s', (self.tenant,))
        baseline = self.stream(False)
        self.assertEqual(baseline, [])
        with patch('recall_server.receipt_membership.lookup_current_search_membership',
                   wraps=lookup_current_search_membership) as native:
            self.assertEqual(self.stream(), baseline)
        native.assert_not_called()

    def test_incomplete_optional_metadata_keeps_native_passage(self):
        self.enroll()
        baseline = self.stream(False)
        original = reader.partition_fetched
        def missing(*, selected, **kwargs):
            selected[0]['membership_metadata_complete'] = False
            return original(selected=selected, **kwargs)
        with patch.object(reader, 'partition_fetched', side_effect=missing):
            with patch('recall_server.receipt_membership.lookup_current_search_membership',
                       wraps=lookup_current_search_membership) as native:
                self.assertEqual(self.stream(), baseline)
        self.assertEqual(native.call_count, 1)

    def test_metadata_timeout_preserves_named_cursor_and_later_batch(self):
        self.enroll()
        self.clone_passages(2000, 'recall://' + self.source + '/missing?rev=1#item=0')
        baseline = self.stream(False)
        self.assertEqual(len(baseline), len(self.ids))
        original = reader.partition_fetched
        partition_calls = []
        native_deadlines = []
        clock = time.monotonic
        execute = self.store._execute_bounded
        def delayed(*, selected, settings, deadline_at):
            partition_calls.append(deadline_at)
            if len(partition_calls) == 1:
                start = clock()
                # Expire only the100ms in-memory validation window. No database
                # timeout is changed; background proof receives its own bounded operation.
                with patch.object(reader.time, 'monotonic', side_effect=[start, start + .101, start + .102]):
                    result = original(selected=selected, settings=settings, deadline_at=deadline_at)
                self.assertEqual(result[1]['status'], 'deadline')
                return result
            return original(selected=selected, settings=settings, deadline_at=deadline_at)
        def counted(connection, statement, parameters, deadline):
            if statement.startswith('WITH requested AS MATERIALIZED'):
                native_deadlines.append(deadline)
            return execute(connection, statement, parameters, deadline)
        with patch.object(reader, 'partition_fetched', side_effect=delayed), \
             patch.object(self.store, '_execute_bounded', side_effect=counted):
            self.assertEqual(self.stream(), baseline)
        self.assertEqual(len(partition_calls), 2)
        self.assertEqual(len(native_deadlines), 1)
        self.assertGreater(native_deadlines[0], partition_calls[0])

    def test_successful_certificate_restores_timeout_before_next_native_fetch(self):
        self.clone_passages(1, self.physical_receipt)
        with self.store.connect() as c:
            self.scope['passage_ids'] = tuple(row['passage_id'] for row in c.execute(
                'SELECT passage_id FROM canonical_passages WHERE tenant_id=%s', (self.tenant,)).fetchall())
        self.enroll()
        connect = self.store.connect
        timeouts = []
        class SlowSecondFetch(ServerCursor):
            def execute(self, statement, parameters=None, **kwargs):
                statement = ('WITH native_rows AS MATERIALIZED (' + statement + ') '
                             'SELECT native_rows.*,pg_sleep(CASE WHEN row_number() OVER ()=2 '
                             'THEN 0.15 ELSE 0 END) AS test_delay FROM native_rows')
                return super().execute(statement, parameters, **kwargs)

            def fetchmany(self, size=0):
                timeouts.append(self.connection.execute('SHOW statement_timeout').fetchone()['statement_timeout'])
                print({'fetch': len(timeouts), 'statement_timeout': timeouts[-1]}, flush=True)
                # Real PostgreSQL FETCH boundaries, using two small native batches.
                return super().fetchmany(1)

        class Connection:
            def __init__(self, connection):
                self.connection = connection

            def __getattr__(self, name):
                return getattr(self.connection, name)

            def cursor(self, name):
                return SlowSecondFetch(self.connection, name, row_factory=self.connection.row_factory)

        @contextmanager
        def delayed():
            with connect() as connection:
                yield Connection(connection)

        with patch.object(self.store, 'connect', side_effect=delayed):
            with patch('recall_server.receipt_membership.lookup_current_search_membership',
                       wraps=lookup_current_search_membership) as native:
                rows = self.stream()
        self.assertEqual(len(rows), 2)
        self.assertEqual(len(timeouts), 3)
        self.assertTrue(timeouts[1].endswith('ms'))
        self.assertGreater(int(timeouts[1][:-2]), 1000)
        self.assertLessEqual(int(timeouts[1][:-2]), 6000)
        native.assert_not_called()

    def test_concurrent_edit_keeps_snapshot_then_next_stream_rejects(self):
        self.enroll()
        baseline = self.stream(False)
        original = reader.partition_fetched
        def changed(*args, **kwargs):
            self.edit()
            return original(*args, **kwargs)
        with patch.object(reader, 'partition_fetched', side_effect=changed):
            self.assertEqual(self.stream(), baseline)
        self.assertEqual(self.stream(), [])

    def test_partial_forget_falls_back_and_matches_native_before_cleanup(self):
        self.enroll()
        # The existing native Forget test proves invalidation before archive cleanup,
        # including actual canonical mutation rather than a fake generation update.
        super().test_forget_revocation_precedes_archive_cleanup_failure()
        with patch('recall_server.receipt_membership.lookup_current_search_membership',
                   wraps=lookup_current_search_membership) as native:
            self.assertEqual(self.stream(), self.stream(False))
        self.assertEqual(native.call_count, 2)
        self.assertEqual(self.stream(), [])

    def test_source_deletion_recreation_does_not_reuse_certificates(self):
        super().test_actual_source_deletion_recreation_retains_generation_and_loses_certificate()
        self.assertEqual(self.stream(), self.stream(False))
        self.assertEqual(self.stream(), [])

    def test_retirement_invalidates_then_reenrollment_uses_same_receipts(self):
        self.enroll()
        baseline = self.stream(False)
        for receipt in (self.receipt, self.physical_receipt):
            self.admit_fixture(self.baseline(receipt)['document_id'])
        with patch('recall_server.receipt_membership.lookup_current_search_membership',
                   wraps=lookup_current_search_membership) as native:
            self.assertEqual(self.stream(), baseline)
        self.assertEqual(native.call_count, 0 if self.additive_membership_generation else 1)
        self.assertEqual(self.stream(False), baseline)
        if self.additive_membership_generation:
            compared = self.compare()
            self.assertEqual(compared['native_allowed_passages'], len(self.ids))
            self.assertEqual(compared['certified_passages'], len(self.ids))
            self.assertEqual(compared['disagreements'], 0)
        self.enroll()
        with patch('recall_server.receipt_membership.lookup_current_search_membership',
                   wraps=lookup_current_search_membership) as native:
            self.assertEqual(self.stream(), baseline)
        native.assert_not_called()

    def test_publication_revision_change_preserves_native_rows_under_binding_contract(self):
        self.enroll()
        with self.store.connect() as c:
            c.execute('UPDATE canonical_passage_documents SET revision=revision+1 WHERE tenant_id=%s',
                      (self.tenant,))
        baseline = self.stream(False)
        self.assertTrue(baseline)
        with patch('recall_server.receipt_membership.lookup_current_search_membership',
                   wraps=lookup_current_search_membership) as native:
            self.assertEqual(self.stream(), baseline)
        stable = 'projected_revision' not in enrollment._BINDING_FIELDS
        self.assertEqual(native.call_count, 0 if stable else 1)
        if stable:
            compared = self.compare()
            self.assertEqual(compared['native_allowed_passages'], len(self.ids))
            self.assertEqual(compared['certified_passages'], len(self.ids))
            self.assertEqual(compared['disagreements'], 0)
        self.edit()
        with patch('recall_server.receipt_membership.lookup_current_search_membership',
                   wraps=lookup_current_search_membership) as native:
            self.assertEqual(self.stream(), [])
        self.assertEqual(native.call_count, 1)
        self.assertEqual(self.stream(False), [])

    def test_stream_orders_before_receipt_filter_and_enrichment(self):
        self.store.search_deadline_ms = 5000
        self.enroll()
        self.clone_passages(2001, self.physical_receipt)
        connect = self.store.connect
        plans, batches = [], []
        class Cursor(ServerCursor):
            def execute(cursor, statement, parameters=None, **kwargs):
                declaration = cursor._make_declare_statement(statement).as_string()
                plans.append(cursor.connection.execute(
                    'EXPLAIN (FORMAT JSON) ' + declaration, parameters
                ).fetchone()['QUERY PLAN'][0]['Plan'])
                return super().execute(statement, parameters, **kwargs)
            def fetchmany(cursor, size=0):
                rows = super().fetchmany(size)
                batches.append(len(rows))
                return rows
        class Connection:
            def __init__(self, connection): self.connection = connection
            def __getattr__(self, name): return getattr(self.connection, name)
            def cursor(self, name):
                return Cursor(self.connection, name, row_factory=self.connection.row_factory)
        @contextmanager
        def connected():
            with connect() as connection:
                yield Connection(connection)
        with patch.object(self.store, 'connect', side_effect=connected):
            actual = self.stream()
        self.assertEqual(batches, [2000, 2, 0])
        self.assertEqual(actual, self.stream(False))
        self.assertEqual(len(actual), 2002)
        self.assertEqual(actual, sorted(actual, key=lambda row: (
            row['logical_document_id'], row['first_occurred_at'],
            row['last_occurred_at'], row['passage_id'])))
        def nodes(node):
            yield node
            for child in node.get('Plans', ()):
                yield from nodes(child)
        sorts = [node for node in nodes(plans[0]) if 'Sort' in node['Node Type']]
        self.assertTrue(sorts, plans)
        for sort in sorts:
            for node in nodes(sort):
                self.assertNotIn('cardinality', str(node.get('Filter', '')))
                self.assertNotIn(node.get('Relation Name'), (
                    'canonical_passage_actors', 'brain_actors',
                    'canonical_evidence_documents', 'canonical_passage_documents',
                    'canonical_parent_authority'))

    def test_defensive_empty_receipt_filter_keeps_eof_and_metadata(self):
        self.clone_passages(3, self.physical_receipt)
        # Historical/corrupt state only: current schema rejects empty receipts.
        # Restore the exact constraint after deleting the invalid fixture rows.
        with self.store.connect() as connection:
            constraint = connection.execute("""SELECT conname,pg_get_constraintdef(oid) AS definition
                FROM pg_constraint WHERE conrelid='canonical_passages'::regclass
                  AND contype='c' AND pg_get_constraintdef(oid) LIKE '%%cardinality(receipts)%%'""").fetchone()
            self.assertIsNotNone(constraint)
            connection.execute(sql.SQL('ALTER TABLE canonical_passages DROP CONSTRAINT {}').format(
                sql.Identifier(constraint['conname'])))
            connection.execute("UPDATE canonical_passages SET receipts=ARRAY[]::text[] "
                "WHERE tenant_id=%s AND passage_id=ANY(%s)",
                (self.tenant, ['psg_' + f'{i:032x}' for i in (0, 2)]))
        try:
            disabled = self.stream(False)
            enabled = self.stream()
            self.assertEqual(enabled, disabled)
            self.assertEqual(len(enabled), 2)
            self.assertTrue(all(row['receipts'] for row in enabled))
        finally:
            with self.store.connect() as connection:
                connection.execute('DELETE FROM canonical_passages WHERE tenant_id=%s '
                    'AND cardinality(receipts)=0', (self.tenant,))
                connection.execute(sql.SQL('ALTER TABLE canonical_passages ADD CONSTRAINT {} ').format(
                    sql.Identifier(constraint['conname'])) + sql.SQL(constraint['definition']))
                restored = connection.execute("SELECT pg_get_constraintdef(oid) AS definition,convalidated "
                    "FROM pg_constraint WHERE conrelid='canonical_passages'::regclass AND conname=%s",
                    (constraint['conname'],)).fetchone()
                self.assertEqual(restored, dict(definition=constraint['definition'], convalidated=True))

    def test_no_certificate_overhead_needs_no_second_metadata_query(self):
        original = self.store._execute_bounded
        statements = []
        def counted(connection, statement, parameters, deadline):
            statements.append(statement)
            return original(connection, statement, parameters, deadline)
        start = time.monotonic()
        with patch.object(self.store, '_execute_bounded', side_effect=counted):
            enabled = self.stream()
        enabled_ms = (time.monotonic() - start) * 1000
        self.assertEqual(sum(statement == reader._METADATA_SQL for statement in statements), 0)
        self.assertEqual(sum(statement.startswith('WITH requested AS MATERIALIZED') for statement in statements), 1)
        start = time.monotonic()
        disabled = self.stream(False)
        disabled_ms = (time.monotonic() - start) * 1000
        self.assertEqual(enabled, disabled)
        print({'local_no_certificate_enabled_ms': round(enabled_ms, 3),
               'local_disabled_ms': round(disabled_ms, 3)}, flush=True)


if __name__ == '__main__':
    suite = unittest.TestSuite(ParquetCertificateTests(name) for name in ParquetCertificateTests.__dict__
                              if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
