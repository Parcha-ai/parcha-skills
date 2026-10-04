"""Source events behind a search hit: real hydration, fake current catalog."""
from __future__ import annotations

import copy
import hashlib
import sys
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

sys.path[:0] = [str(Path(__file__).resolve().parents[2]), str(Path(__file__).resolve().parents[2] / 'server')]
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.chunk_bodies import ChunkBodyError
from recall_server.mcp import _encoded_result_size, _call_tool, CANONICAL_SHOW_TOOL, McpProtocolError

TENANT = 'tenant:test'
SOURCE = 'codex:test'
DOC = 'ldoc_' + 'a' * 32
PASSAGE = 'psg_' + 'b' * 32
LOCATOR = dict(source_id=SOURCE, logical_document_id=DOC, revision=17,
               manifest_content_sha256='c' * 64, passage_id=PASSAGE)


def receipt(event, ordinal=0):
    return f'recall://{SOURCE}/{event}?rev=1#item={ordinal}'


class Rows:
    def __init__(self, rows): self.rows = copy.deepcopy(rows)
    def fetchall(self): return self.rows


class Store:
    search_deadline_ms = 5000

    def __init__(self, texts=None):
        texts = texts or [('earlier', ['earlier context']), ('wrapper', ['agent message header']),
                          ('review', ['Review: enforce scoped inner-invocation ceiling and trustworthy receipts.'])]
        self.chunks = []
        self.texts = {}
        for e, bodies in texts:
            for i, text in enumerate(bodies):
                r = dict(source_id=SOURCE, document_id='doc:' + e, native_id=e,
                         revision=1, kind='transcript_record', occurred_at='2026-07-18T23:10:00+00:00',
                         observed_at='2026-07-18T23:10:01+00:00', ordinal=i,
                         receipt=receipt(e, i), text_sha256=hashlib.sha256(text.encode()).hexdigest())
                self.chunks.append(r); self.texts[r['receipt']] = text
        self.metadata = dict(passage_id=PASSAGE, ordinal=8, policy_fingerprint='d' * 64,
                             text_sha256='e' * 64, spans=[{'record_ordinal': 249}, {'record_ordinal': 256}],
                             receipts=[r['receipt'] for r in self.chunks])
        self.live = True
        self.granted = True
        self.redirect = False
        self.calls = []
        self.hydrations = []
        self.after_hydrate = None

    @contextmanager
    def connect(self): yield self

    def _execute_bounded(self, connection, sql, values, deadline_at):
        self.calls.append((sql, values, deadline_at))
        if 'FROM canonical_passages passage' in sql:
            wanted = (TENANT, SOURCE, DOC, 17, 'c' * 64, [PASSAGE])
            return Rows([self.metadata] if self.live and tuple(values) == wanted else [])
        if 'WITH selected_events AS' in sql:
            assert values[0] == TENANT and values[1] == SOURCE
            assert 'brain_access_grants' in sql and 'canonical_source_grants' in sql
            assert 'is_tombstone' in sql and 'document.is_current' in sql
            return Rows(self.chunks if self.live and self.granted and not self.redirect else [])
        raise AssertionError('unexpected query')

    def archived(self, *args, **kwargs):
        self.hydrations.append(copy.deepcopy(kwargs['chunk_ordinals']))
        result = {}
        for r in self.chunks:
            key = (r['source_id'], r['document_id'])
            if r['ordinal'] in kwargs['chunk_ordinals'].get(key, ()):
                result.setdefault(key, []).append(dict(ordinal=r['ordinal'], receipt=r['receipt'],
                                                       text_redacted=self.texts[r['receipt']]))
        if self.after_hydrate: self.after_hydrate()
        return result


def retrieval(store, **kwargs):
    return BoundCanonicalRetrieval(store, tenant_id=kwargs.get('tenant', TENANT),
        principal_id='principal:test', authorized_sources=kwargs.get('sources', (SOURCE,)),
        chunk_body_archive=object())


class PassageSourceOpenTests(unittest.TestCase):
    def opened(self, store, target=LOCATOR, **kwargs):
        with patch('recall_server.chunk_bodies.read_archived_chunks', side_effect=store.archived):
            return retrieval(store).show(copy.deepcopy(target), **kwargs)

    def test_hit_opens_review_event_not_only_short_wrapper(self):
        store = Store()
        result = self.opened(store)
        self.assertTrue(result['complete'])
        self.assertEqual([c['native_id'] for c in result['chunks']], ['earlier', 'wrapper', 'review'])
        self.assertIn('scoped inner-invocation', result['chunks'][-1]['text'])
        self.assertEqual(result['opened_receipts'], [r['receipt'] for r in store.chunks])
        self.assertEqual(result['scope'], 'source_records_behind_passage')
        self.assertEqual(len(store.hydrations), 1)
        self.assertEqual(len(set(deadline for _, _, deadline in store.calls)), 1)
        self.assertNotIn('canonical_redacted', result['chunks'][0])

    def test_multiple_receipts_same_event_and_unselected_event_chunks(self):
        store = Store([('review', ['head', 'tail', 'extra full-source chunk'])])
        store.metadata['receipts'] = [receipt('review', 0), receipt('review', 1)]
        result = self.opened(store)
        self.assertEqual([c['text'] for c in result['chunks']], ['head', 'tail', 'extra full-source chunk'])
        self.assertEqual(len(set(c['receipt'] for c in result['chunks'])), 3)
        self.assertEqual(len(store.hydrations), 1)

    def test_long_unicode_chunk_lossless_pages_and_only_emitted_receipts(self):
        store = Store([('long', ['α🧠\n' * 3000, 'tail']), ('last', ['final'])])
        cursor = None; restored = {}; pages = []
        for _ in range(100):
            result = self.opened(store, cursor=cursor, page_bytes=4096); pages.append(result)
            self.assertLessEqual(_encoded_result_size(result), 4096)
            self.assertEqual(result['opened_receipts'], list(dict.fromkeys(c['receipt'] for c in result['chunks'])))
            for c in result['chunks']:
                text = restored.setdefault(c['receipt'], '')
                self.assertEqual(c['content_start'], len(text))
                restored[c['receipt']] += c['text']
                self.assertEqual(c['content_end'], len(restored[c['receipt']]))
                self.assertEqual(c['byte_start'], len(text.encode()))
                self.assertEqual(c['byte_end'], len(restored[c['receipt']].encode()))
                self.assertEqual(c['content_complete'], c['content_start'] == 0 and c['content_end'] == c['content_length'])
            if result['complete']: break
            self.assertIsNotNone(result['next_cursor']); self.assertNotEqual(result['next_cursor'], cursor)
            cursor = result['next_cursor']
        else: self.fail('pagination did not complete')
        self.assertEqual(restored, store.texts)
        self.assertNotIn(receipt('last'), pages[0]['opened_receipts'])
        self.assertGreater(len(pages), 1)

    def test_exact_locator_rejects_bad_scope_revision_hash_and_extra_fields(self):
        for field, value in [('source_id', 'foreign'), ('revision', 16), ('revision', True),
                             ('manifest_content_sha256', '0' * 64), ('passage_id', 'psg_bad'),
                             ('logical_document_id', 'ldoc_' + 'f' * 32), ('unexpected', 'x')]:
            store = Store(); target = {**LOCATOR, field: value}
            with self.subTest(field=field, value=value), self.assertRaises(ValueError): self.opened(store, target)
            self.assertFalse(store.hydrations)
        store = Store()
        with self.assertRaises(ValueError): retrieval(store, tenant='tenant:wrong').show(LOCATOR)
        self.assertFalse(store.hydrations)

    def test_deleted_ungranted_or_redirected_source_fails_without_body(self):
        for key, value in [('live', False), ('granted', False), ('redirect', True)]:
            store = Store(); setattr(store, key, value)
            with self.subTest(key=key), self.assertRaises(ValueError): self.opened(store)
            self.assertFalse(store.hydrations)

    def test_midread_revision_receipt_or_grant_change_releases_no_result(self):
        mutations = [lambda s: setattr(s, 'live', False), lambda s: setattr(s, 'granted', False),
                     lambda s: s.metadata.update(text_sha256='f' * 64),
                     lambda s: s.chunks[0].update(text_sha256='f' * 64)]
        for mutate in mutations:
            store = Store(); store.after_hydrate = lambda: mutate(store)
            with self.subTest(mutate=mutate), self.assertRaises(ValueError): self.opened(store)

    def test_continuation_rejects_changed_chunk_membership_before_hydration(self):
        store = Store([('long', ['abc' * 10000])]); first = self.opened(store, page_bytes=4096)
        self.assertFalse(first['complete']); before = len(store.hydrations)
        store.chunks[0]['text_sha256'] = 'f' * 64
        with self.assertRaises(ValueError): self.opened(store, cursor=first['next_cursor'], page_bytes=4096)
        self.assertEqual(len(store.hydrations), before)

    def test_archive_error_never_falls_back_to_hint_text(self):
        store = Store()
        with patch('recall_server.chunk_bodies.read_archived_chunks', side_effect=ChunkBodyError('archive_corrupt')):
            with self.assertRaises(ChunkBodyError): retrieval(store).show(LOCATOR)

    def test_archive_document_batch_limit_paginates_instead_of_rejecting(self):
        store = Store([(str(i), ['record']) for i in range(105)])
        first = self.opened(store, page_bytes=1024*1024)
        self.assertEqual(len(first['chunks']), 100)
        self.assertFalse(first['complete'])
        second = self.opened(store, cursor=first['next_cursor'], page_bytes=1024*1024)
        self.assertEqual(len(second['chunks']), 5)
        self.assertTrue(second['complete'])
        self.assertEqual([len(batch) for batch in store.hydrations], [100, 5])

    def test_mcp_object_target_dispatch_and_receipt_paging_rejection(self):
        store = Store()
        with patch('recall_server.chunk_bodies.read_archived_chunks', side_effect=store.archived):
            result = _call_tool(retrieval(store), {}, 'recall_show', {'target': LOCATOR})
        self.assertTrue(result['complete'])
        arguments = {'target': {**LOCATOR, 'revision': 17.0}, 'page_bytes': '32768'}
        before = copy.deepcopy(arguments)
        with patch('recall_server.chunk_bodies.read_archived_chunks', side_effect=store.archived):
            self.assertTrue(_call_tool(retrieval(store), {}, 'recall_show', arguments)['complete'])
        self.assertEqual(arguments, before)
        self.assertIn('oneOf', CANONICAL_SHOW_TOOL['inputSchema']['properties']['target'])
        for args in ({'target': LOCATOR, 'tail': 1},
                     {'target': receipt('review'), 'cursor': 'invalid'},
                     {'target': {**LOCATOR, 'revision': True}}):
            with self.subTest(args=args), self.assertRaises(McpProtocolError):
                _call_tool(retrieval(store), {}, 'recall_show', args)

    def test_tampered_cursor_and_invalid_page_budget(self):
        for options in ({'cursor':'bad'}, {'page_bytes':True}, {'page_bytes':0}, {'page_bytes':2**30}):
            with self.subTest(options=options), self.assertRaises(ValueError): self.opened(Store(), **options)


if __name__ == '__main__': unittest.main()
