"""Native counterexamples for unnecessary presence-comparison body work."""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
import hashlib
import json
import unittest
from unittest.mock import patch

from e2e_retained_passage_receipts import RetainedPassageReceipts
from recall_server import search_parent_reconciliation
from recall_server.turbopuffer_plane import passage_row
from recall_server.turbopuffer_projection import (
    _PASSAGE_FROM_SQL, _PASSAGE_METADATA_SELECT_SQL, _PASSAGE_SELECT_SQL,
)


class ReconciliationMetadataFirst(RetainedPassageReceipts):
    def prepared(self):
        text = ' '.join(f'Native source statement {i} retains exact receipt evidence.'
                        for i in range(600))
        self.ingest(1, text=text)
        self.project()
        self.writer.drain(tenant_id=self.tenant, max_months=2)
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        expected = deepcopy(namespace.rows)
        self.assertGreater(len(expected), 2)
        self.ingest(2, text=text)
        self.project()
        self.store.verified_singleton_receipts = True
        return self.restart_writer(batch_rows=128), namespace, expected

    @contextmanager
    def body_work(self, writer):
        observed = dict(full_row_serializations=0, serialized_rows=0, body_reads=0, body_bytes=0)
        execute, hydrate = self.store._execute_bounded, writer.passages_by_id

        def timed_execute(connection, sql, args, deadline):
            if 'row_to_json(hydrated)' in sql and 'passage.text_redacted' in sql:
                observed['full_row_serializations'] += 1
                observed['serialized_rows'] += len(args[1])
            return execute(connection, sql, args, deadline)

        def read(*args, **kwargs):
            rows = hydrate(*args, **kwargs)
            bodies = [row['text_redacted'] for row in rows if 'text_redacted' in row]
            observed['body_reads'] += bool(bodies)
            observed['body_bytes'] += sum(len(body.encode()) for body in bodies)
            return rows

        with patch.object(self.store, '_execute_bounded', side_effect=timed_execute), \
                patch.object(writer, 'passages_by_id', side_effect=read):
            yield observed

    def test_matching_page_does_not_materialize_bodies(self):
        writer, namespace, expected = self.prepared()
        writes = len(namespace.writes)
        with self.body_work(writer) as work:
            outcome = search_parent_reconciliation.step(writer, self.tenant)
        self.assertEqual(outcome, dict(attempted=1, examined=len(expected), written=0))
        self.assertEqual(namespace.rows, expected)
        self.assertEqual(len(namespace.writes), writes)
        self.assertIsNone(self.reconciliation_row())
        print('matching_page_body_work=' + json.dumps(work, sort_keys=True), flush=True)
        self.assertEqual(work, dict(full_row_serializations=0, serialized_rows=0,
                                   body_reads=0, body_bytes=0))

    def test_one_missing_row_keeps_new_generation_and_exact_output(self):
        writer, namespace, expected = self.prepared()
        missing = next(iter(expected))
        del namespace.rows[missing]
        with self.store.connect() as connection:
            connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            current = writer.passages_by_id(connection, self.tenant, [missing])[0]
        expected[missing] = passage_row({**current, 'actors': [tuple(a) for a in current['actors']]})
        before = self.reconciliation_row()
        query = namespace.query

        def enqueue_during_comparison(**kwargs):
            response = query(**kwargs)
            with self.store.connect() as connection:
                search_parent_reconciliation.enqueue(connection, self.tenant, self.source,
                                                     before['logical_document_id'])
            return response

        writes = len(namespace.writes)
        with self.body_work(writer) as work, \
                patch.object(namespace, 'query', side_effect=enqueue_during_comparison):
            outcome = search_parent_reconciliation.step(writer, self.tenant)
        self.assertEqual(outcome, dict(attempted=1, examined=len(expected), written=1))
        self.assertEqual(namespace.rows, expected)
        self.assertEqual(len(namespace.writes), writes + 1)
        pending = self.reconciliation_row()
        self.assertEqual(pending['generation'], before['generation'] + 1)
        self.assertIsNone(pending['page_checkpoint'])
        self.assertEqual(search_parent_reconciliation.step(writer, self.tenant)['written'], 0)
        self.assertIsNone(self.reconciliation_row())
        print('one_missing_body_work=' + json.dumps(work, sort_keys=True), flush=True)
        self.assertLessEqual(work['serialized_rows'], 2,
                             'Only the changed row needs prefix and final leased body sizing.')
        self.assertEqual(work['body_reads'], 1,
                         'Only the final leased write needs canonical body hydration.')

    def test_mixed_body_prefix_resumes_without_skipping_changed_rows(self):
        writer, namespace, expected = self.prepared()
        with self.store.connect() as connection:
            keys = connection.execute('SELECT passage_id,ordinal FROM canonical_passages '
                                      'WHERE tenant_id=%s ORDER BY ordinal', (self.tenant,)).fetchall()
            missing = [row['passage_id'] for row in keys[1::2]]
            for current in writer.passages_by_id(connection, self.tenant, missing):
                expected[current['passage_id']] = passage_row(
                    {**current, 'actors': [tuple(a) for a in current['actors']]})
            sizes = []
            for select in (_PASSAGE_METADATA_SELECT_SQL, _PASSAGE_SELECT_SQL):
                rows = connection.execute(
                    'SELECT hydrated.passage_id,octet_length(row_to_json(hydrated)::text)+128 AS bytes '
                    'FROM (' + select + _PASSAGE_FROM_SQL +
                    ' WHERE passage.tenant_id=%s) hydrated', (self.tenant,)).fetchall()
                sizes.append({row['passage_id']: row['bytes'] for row in rows})
        metadata_sizes, body_sizes = sizes
        limit = max(sum(metadata_sizes.values()), max(body_sizes[i] for i in missing)) + 1
        self.assertLess(limit, sum(body_sizes[i] for i in missing))
        for identifier in missing:
            del namespace.rows[identifier]
        before_writes = len(namespace.writes)
        total = 0
        for key in keys:
            size = body_sizes[key['passage_id']] if key['passage_id'] in missing else 0
            if total + size > limit:
                blocked_ordinal = key['ordinal']
                break
            total += size
        with patch.object(search_parent_reconciliation, 'MAX_METADATA_BYTES', limit):
            first = search_parent_reconciliation.step(writer, self.tenant)
            self.assertGreater(first['written'], 0)
            self.assertLess(first['written'], len(missing))
            self.assertEqual(self.reconciliation_row()['page_checkpoint']['after'], blocked_ordinal - 1)
            for _ in range(len(keys) + 1):
                search_parent_reconciliation.step(self.restart_writer(batch_rows=128), self.tenant)
                if self.reconciliation_row() is None:
                    break
        self.assertIsNone(self.reconciliation_row())
        self.assertEqual(namespace.rows, expected)
        written = [row['id'] for write in namespace.writes[before_writes:]
                   for row in write.get('upsert_rows', [])]
        self.assertCountEqual(written, missing)

    def test_matching_body_larger_than_budget_needs_no_body_read(self):
        writer, namespace, _ = self.prepared()
        identifier = next(iter(namespace.rows))
        body = 'x' * (search_parent_reconciliation.MAX_METADATA_BYTES + 1)
        digest = hashlib.sha256(body.encode()).hexdigest()
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_passages SET text_redacted=%s,text_sha256=%s '
                               'WHERE tenant_id=%s AND passage_id=%s',
                               (body, digest, self.tenant, identifier))
        namespace.rows[identifier]['text_sha256'] = digest
        expected = deepcopy(namespace.rows)
        with self.body_work(writer) as work:
            outcome = search_parent_reconciliation.step(writer, self.tenant)
        self.assertEqual(outcome['written'], 0)
        self.assertEqual(namespace.rows, expected)
        self.assertIsNone(self.reconciliation_row())
        self.assertEqual(work, dict(full_row_serializations=0, serialized_rows=0,
                                   body_reads=0, body_bytes=0))

    def test_large_header_refuses_before_metadata_transfer(self):
        writer, namespace, expected = self.prepared()
        with self.store.connect() as connection:
            identifier = connection.execute('SELECT passage_id FROM canonical_passages '
                                            'WHERE tenant_id=%s ORDER BY ordinal LIMIT 1',
                                            (self.tenant,)).fetchone()['passage_id']
            connection.execute('UPDATE canonical_passages SET header_redacted=%s '
                               'WHERE tenant_id=%s AND passage_id=%s',
                               ('h' * (search_parent_reconciliation.MAX_METADATA_BYTES + 1),
                                self.tenant, identifier))
        queries, writes = len(namespace.queries), len(namespace.writes)
        with patch.object(writer, 'passages_by_id', wraps=writer.passages_by_id) as read:
            with self.assertRaises(search_parent_reconciliation.ReconciliationMetadataBudgetExceeded):
                search_parent_reconciliation.step(writer, self.tenant)
        read.assert_not_called()
        self.assertEqual((len(namespace.queries), len(namespace.writes)), (queries, writes))
        self.assertEqual(namespace.rows, expected)
        self.assertEqual(self.reconciliation_row()['page_checkpoint']['after'], -1)

    def test_oversized_changed_body_cannot_skip_to_later_keys(self):
        writer, namespace, _ = self.prepared()
        body = 'x' * (search_parent_reconciliation.MAX_METADATA_BYTES + 1)
        with self.store.connect() as connection:
            keys = connection.execute('SELECT passage_id,ordinal FROM canonical_passages '
                                      'WHERE tenant_id=%s ORDER BY ordinal', (self.tenant,)).fetchall()
            identifier = keys[1]['passage_id']
            connection.execute('UPDATE canonical_passages SET text_redacted=%s,text_sha256=%s '
                               'WHERE tenant_id=%s AND passage_id=%s',
                               (body, hashlib.sha256(body.encode()).hexdigest(), self.tenant, identifier))
        del namespace.rows[identifier]
        before = deepcopy(namespace.rows)
        writes = len(namespace.writes)
        with self.body_work(writer) as work:
            first = search_parent_reconciliation.step(writer, self.tenant)
        self.assertEqual(first['written'], 0)
        self.assertEqual(work['body_bytes'], 0)
        self.assertEqual(self.reconciliation_row()['page_checkpoint']['after'], keys[0]['ordinal'])
        with self.assertRaises(search_parent_reconciliation.ReconciliationMetadataBudgetExceeded):
            search_parent_reconciliation.step(self.restart_writer(batch_rows=128), self.tenant)
        self.assertEqual(self.reconciliation_row()['page_checkpoint']['after'], keys[0]['ordinal'])
        self.assertEqual(namespace.rows, before)
        self.assertEqual(len(namespace.writes), writes)


def load_tests(loader, tests, pattern):
    return unittest.TestSuite(ReconciliationMetadataFirst(name)
                              for name in ReconciliationMetadataFirst.__dict__
                              if name.startswith('test_'))


if __name__ == '__main__':
    unittest.main()
