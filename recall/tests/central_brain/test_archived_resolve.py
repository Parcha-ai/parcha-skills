"""Receipt resolution keeps its historical contract and complete authority."""
import copy
import hashlib
import os
import sys
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'server'))
from recall_server.app import Handler
from recall_server.chunk_bodies import ChunkBodyError
from recall_server.db import BrainStore, SearchDeadlineExceeded

TENANT = 'tenant:test'
SOURCE = 'source:test'
RECEIPT = 'recall://source:test/native?rev=2#item=0'


class Result:
    def __init__(self, rows):
        self.rows = rows
    def fetchone(self):
        return copy.deepcopy(self.rows[0]) if self.rows else None
    def fetchall(self):
        return copy.deepcopy(self.rows)


class ArchivedResolveTests(unittest.TestCase):
    def setUp(self):
        self.store = BrainStore('postgresql://synthetic.invalid/recall')
        self.connected = False
        self.calls = []
        self.event = dict(tenant_id=TENANT, event_id='event:test', source_id=SOURCE,
                          native_id='native', revision=2, occurred_at=None, provenance={})
        self.items = [dict(ordinal=0, occurred_at=None, role=None, surface=None,
                           text_redacted='body', receipt=RECEIPT, source_id=SOURCE,
                           document_id='document:test', is_current=True,
                           text_sha256=hashlib.sha256(b'body').hexdigest())]
        self.connection = mock.Mock()
        self.connection.execute.side_effect = self.execute
        @contextmanager
        def connect():
            self.connected = True
            try:
                yield self.connection
            finally:
                self.connected = False
        self.store.connect = connect
        self.store._execute_bounded = lambda conn, sql, values, deadline: conn.execute(sql, values)

    def execute(self, sql, values):
        self.calls.append((sql, values))
        if 'FROM canonical_events event' in sql:
            return Result([self.event] if self.event else [])
        if 'FROM canonical_chunks chunk' in sql:
            return Result(self.items)
        raise AssertionError('scoped canonical request reached legacy SQL')

    def read(self, **kwargs):
        return self.store.resolve(RECEIPT, tenant_id=TENANT, authorized_sources=(SOURCE,), **kwargs)

    def test_scoped_lookup_contains_tenant_and_no_legacy_fallback(self):
        self.event = None
        self.assertIsNone(self.read())
        self.assertEqual(len(self.calls), 1)
        self.assertIn('event.tenant_id=%s', self.calls[0][0])
        self.assertIn(TENANT, self.calls[0][1])

    def test_explicit_legacy_resolution_never_queries_canonical_tables(self):
        self.connection.execute.side_effect = lambda sql, args: (
            Result([dict(id=1, source_id=SOURCE)]) if 'FROM source_events event' in sql
            else Result([dict(text_redacted='legacy only')])
        )
        with mock.patch.object(self.store, '_resolve_canonical') as canonical:
            result = self.store.resolve(RECEIPT, authorized_source=SOURCE, legacy_only=True)
        canonical.assert_not_called()
        self.assertEqual(result['items'][0]['text_redacted'], 'legacy only')
        with self.assertRaises(ValueError):
            self.read(legacy_only=True)

    def test_denied_sources_do_no_database_or_archive_io(self):
        for grants in ((), ('source:other',)):
            with self.subTest(grants=grants):
                self.assertIsNone(self.store.resolve(RECEIPT, tenant_id=TENANT,
                                                    authorized_sources=grants, chunk_body_archive=mock.Mock()))
        self.assertFalse(self.calls)

    def test_current_archive_hydration_occurs_after_connection_closes(self):
        expected = self.read()
        self.items[0]['text_redacted'] = None
        def archived(store, archive, **kwargs):
            self.assertFalse(self.connected)
            self.assertEqual(kwargs['tenant_id'], TENANT)
            self.assertEqual(kwargs['source_ids'], (SOURCE,))
            self.assertIsNotNone(kwargs['deadline_at'])
            return {(SOURCE, 'document:test'): [dict(ordinal=0, receipt=RECEIPT, text_redacted='body')]}
        with mock.patch('recall_server.chunk_bodies.read_archived_chunks', side_effect=archived):
            self.assertEqual(self.read(chunk_body_archive=object()), expected)
        self.assertEqual(set(expected['items'][0]),
                         {'ordinal', 'occurred_at', 'role', 'surface', 'text_redacted', 'receipt'})
        self.assertNotIn('tenant_id', expected['event'])
        self.assertNotIn('event_id', expected['event'])

    def test_historical_revision_keeps_inline_text_without_opening_archive(self):
        self.items[0]['is_current'] = False
        expected = self.read()
        with mock.patch('recall_server.chunk_bodies.read_archived_chunks') as archived:
            self.assertEqual(self.read(chunk_body_archive=object()), expected)
        archived.assert_not_called()
        self.items[0]['text_redacted'] = ''
        with self.assertRaises(ChunkBodyError):
            self.read(chunk_body_archive=object())

    def test_archive_failure_and_deadline_never_fall_back(self):
        for error in (ChunkBodyError('archived_chunk_body_unavailable'), SearchDeadlineExceeded()):
            with self.subTest(error=type(error).__name__):
                with mock.patch('recall_server.chunk_bodies.read_archived_chunks', side_effect=error):
                    with self.assertRaises(type(error)):
                        self.read(chunk_body_archive=object())
        self.assertTrue(all('source_events' not in sql for sql, _ in self.calls))

    def test_historical_hash_validation_also_checks_operation_deadline(self):
        self.items[0]['is_current'] = False
        with mock.patch('recall_server.db.time.monotonic', side_effect=[10, 100]):
            with self.assertRaises(SearchDeadlineExceeded):
                self.read(chunk_body_archive=object())


class ResolveHandlerAuthorityTests(unittest.TestCase):
    def handler(self, principal):
        handler = object.__new__(Handler)
        handler.path = '/v1/receipts/resolve?receipt=' + RECEIPT.replace('#', '%23')
        handler.require = mock.Mock(return_value=principal)
        handler.store = mock.Mock()
        handler.store.resolve.return_value = None
        handler.evidence_archive_store = object()
        handler.send_json = mock.Mock()
        return handler

    def test_http_passes_tenant_and_empty_grants_without_broadening(self):
        handler = self.handler({'kind': 'mcp', 'tenant_id': TENANT, 'authorized_sources': ()})
        with mock.patch.dict(os.environ, {'RECALL_CHUNK_BODY_READS': 'archive'}, clear=True):
            handler.do_GET()
        handler.store.resolve.assert_called_once_with(
            RECEIPT, authorized_source=None, tenant_id=TENANT, authorized_sources=(),
            chunk_body_archive=handler.evidence_archive_store, legacy_only=False)

    def test_legacy_development_and_collector_use_ingest_tenant_default(self):
        for principal in ({'kind': 'development'}, {'kind': 'collector', 'source_id': SOURCE},
                          {'kind': 'tailscale-user'}, {'kind': 'webhook', 'tenant_id': TENANT, 'source_id': SOURCE}):
            handler = self.handler(principal)
            with mock.patch.dict(os.environ, {'RECALL_LEGACY_INGEST_TENANT_ID': 'tenant:legacy'}, clear=True):
                handler.do_GET()
            actual = handler.store.resolve.call_args.kwargs
            self.assertEqual(actual['tenant_id'], principal.get('tenant_id', 'tenant:legacy'))
            self.assertEqual(actual['authorized_source'], principal.get('source_id'))
            self.assertIsNone(actual['chunk_body_archive'])

    def test_explicit_development_rollback_keeps_unscoped_legacy_receipts(self):
        handler = self.handler({'kind': 'development'})
        with mock.patch.dict(os.environ, {'RECALL_LEGACY_READS': '1'}, clear=True):
            handler.do_GET()
        self.assertIsNone(handler.store.resolve.call_args.kwargs['tenant_id'])
        self.assertIsNone(handler.store.resolve.call_args.kwargs['authorized_sources'])
        self.assertTrue(handler.store.resolve.call_args.kwargs['legacy_only'])

    def test_legacy_collector_rollback_retains_source_grants(self):
        handler = self.handler({'kind': 'collector', 'source_id': SOURCE,
                                'principal_id': 'principal:legacy', 'authorized_sources': ()})
        with mock.patch.dict(os.environ, {'RECALL_LEGACY_READS': '1'}, clear=True):
            handler.do_GET()
        actual = handler.store.resolve.call_args.kwargs
        self.assertTrue(actual['legacy_only'])
        self.assertEqual(actual['authorized_source'], SOURCE)
        self.assertEqual(actual['authorized_sources'], ())
        handler.store.authorized_canonical_source_ids.assert_not_called()

    def test_legacy_rollback_flag_does_not_broaden_mcp_authority(self):
        handler = self.handler({'kind': 'mcp', 'tenant_id': TENANT, 'authorized_sources': ()})
        with mock.patch.dict(os.environ, {'RECALL_LEGACY_READS': '1'}, clear=True):
            handler.do_GET()
        self.assertEqual(handler.store.resolve.call_args.kwargs['tenant_id'], TENANT)
        self.assertEqual(handler.store.resolve.call_args.kwargs['authorized_sources'], ())

    def test_legacy_principal_uses_current_canonical_grants(self):
        handler = self.handler({'kind': 'collector', 'tenant_id': TENANT,
                                'principal_id': 'principal:test', 'authorized_sources': ()})
        handler.store.authorized_canonical_source_ids.return_value = (SOURCE,)
        handler.do_GET()
        handler.store.authorized_canonical_source_ids.assert_called_once_with(TENANT, 'principal:test')
        self.assertEqual(handler.store.resolve.call_args.kwargs['authorized_sources'], (SOURCE,))

    def test_mcp_missing_tenant_is_denied_before_store(self):
        handler = self.handler({'kind': 'mcp', 'authorized_sources': (SOURCE,)})
        with mock.patch.dict(os.environ, {}, clear=True):
            handler.do_GET()
        handler.store.resolve.assert_not_called()
        self.assertEqual(handler.send_json.call_args.args[0], 403)

    def test_deadline_and_corruption_return_sanitized_unavailability(self):
        for error in (SearchDeadlineExceeded(), ChunkBodyError('archived_chunk_body_unavailable')):
            handler = self.handler({'kind': 'development'})
            handler.store.resolve.side_effect = error
            with mock.patch.dict(os.environ, {}, clear=True):
                handler.do_GET()
            handler.send_json.assert_called_once_with(503, {'error': 'receipt unavailable'})


if __name__ == '__main__':
    unittest.main()
