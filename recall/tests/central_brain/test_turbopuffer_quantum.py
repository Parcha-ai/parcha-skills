"""Publication quanta yield without acknowledging unfinished months."""
import gc
import unittest
import weakref
from .test_turbopuffer_projection import (
    _Catalog, _projector, _passage, _at, _upserts, _deletes,
    TENANT, SOURCE, SETTINGS, JULY, AUGUST, RateLimitError,
)


def quantum(projector, *, max_months=2):
    # The fallback witnesses today's behavioral failure before the new entry exists.
    method = getattr(projector, 'drain_quantum', projector.drain)
    return method(tenant_id=TENANT, max_months=max_months)


class QuantumTests(unittest.TestCase):
    def fixture(self, **kwargs):
        catalog = _Catalog()
        catalog.passages = [_passage(index, 7, index+1) for index in range(1, 6)]
        catalog.enqueue(JULY, queued_at=_at(9, 1, 1))
        projector, client = _projector(catalog, tokens_per_minute=0, **kwargs)
        return catalog, projector, client

    def test_page_returns_before_month_finish_and_small_month_gets_a_turn(self):
        catalog, projector, client = self.fixture()
        catalog.passages.append(_passage(8, 8, 1))
        catalog.enqueue(AUGUST, queued_at=_at(9, 1, 2))
        first = quantum(projector)
        self.assertEqual((first['rows'], first['months']), (2, 0))
        self.assertEqual(catalog.shards, {})
        with self.assertLogs('recall_server.turbopuffer_projection', level='INFO') as logs:
            second = quantum(projector)
        self.assertTrue(any('cooperative=1' in message for message in logs.output))
        self.assertEqual((second['rows'], second['months']), (1, 1))
        self.assertNotIn((SOURCE, JULY), catalog.shards)
        self.assertIn((SOURCE, JULY), catalog.outbox)
        total = first['rows'] + second['rows']
        for _ in range(3):
            total += quantum(projector)['rows']
        self.assertEqual(total, 6)
        self.assertEqual(catalog.outbox, {})
        self.assertEqual(len(client.namespace(SETTINGS.namespace(TENANT)).rows), 6)

    def test_paused_month_retains_no_page_rows_or_body_text(self):
        catalog, projector, client = self.fixture()
        references = []
        class Body(str):
            pass
        class Row(dict):
            pass
        page = projector.passage_page
        def tracked(*args, **kwargs):
            result = []
            for original in page(*args, **kwargs):
                row = Row(original)
                row['text_redacted'] = Body(original['text_redacted'])
                references.extend([weakref.ref(row), weakref.ref(row['text_redacted'])])
                result.append(row)
            return result
        projector.passage_page = tracked
        # Real provider calls do not retain Python input rows; use a sink instead
        # of the fake vendor index, which intentionally stores those same rows.
        client.namespace(SETTINGS.namespace(TENANT)).write = lambda **kwargs: None
        self.assertEqual(quantum(projector)['rows'], 2)
        gc.collect()
        self.assertTrue(references)
        self.assertTrue(all(reference() is None for reference in references))
        self.assertEqual(catalog.shards, {})

    def test_restart_replays_unfinished_month_without_early_watermark(self):
        catalog, projector, client = self.fixture()
        self.assertEqual(quantum(projector)['months'], 0)
        self.assertEqual(catalog.shards, {})
        restarted, _ = _projector(catalog, client=client, tokens_per_minute=0)
        first = quantum(restarted)
        self.assertEqual(first['rows'], 2)
        for _ in range(3):
            quantum(restarted)
        ns = client.namespace(SETTINGS.namespace(TENANT))
        first_id = catalog.passages[0]['passage_id']
        self.assertEqual(sum(first_id in _upserts(write) for write in ns.writes), 2)
        self.assertEqual(len(ns.rows), 5)
        self.assertEqual(catalog.shards[(SOURCE, JULY)]['built_at'], catalog.watermark)

    def test_generation_advanced_during_quantum_stays_queued_after_full_finish(self):
        catalog, projector, client = self.fixture()
        self.assertEqual(quantum(projector)['months'], 0)
        late = _passage(9, 7, 2, created_at=_at(9, 2))
        catalog.passages.append(late)  # Earlier event time, behind the paused cursor.
        catalog.enqueue(JULY, generation=2, reason='backfill')
        quantum(projector)
        final = quantum(projector)
        self.assertEqual((final['months'], final['requeued']), (1, 1))
        self.assertEqual(catalog.outbox[(SOURCE, JULY)]['generation'], 2)
        self.assertEqual(catalog.shards[(SOURCE, JULY)]['generation'], 1)
        following = quantum(projector)
        self.assertEqual((following['rows'], following['months']), (1, 1))
        self.assertIn(late['passage_id'], client.namespace(SETTINGS.namespace(TENANT)).rows)
        self.assertEqual(catalog.outbox, {})

    def test_failed_partial_page_replays_and_never_advances_watermark(self):
        catalog, projector, client = self.fixture(max_batch_bytes=1024)
        ns = client.namespace(SETTINGS.namespace(TENANT))
        write = ns.write
        calls = []
        def fail_second(**kwargs):
            calls.append(kwargs)
            if len(calls) == 2:
                raise ValueError('synthetic partial page failure')
            return write(**kwargs)
        ns.write = fail_second
        with self.assertLogs('recall_server.turbopuffer_projection', level='WARNING'):
            self.assertEqual(quantum(projector)['failed'], 1)
        self.assertEqual(catalog.shards, {})
        ns.write = write
        self.assertEqual(quantum(projector)['rows'], 2)
        for _ in range(3):
            quantum(projector)
        self.assertEqual(len(ns.rows), 5)
        self.assertEqual(catalog.outbox, {})

    def test_tombstone_batches_yield_and_only_exact_captured_ids_finish(self):
        catalog, projector, client = self.fixture(max_batch_bytes=1024)
        ids = [f"psg_{index:032x}" for index in range(30)]
        catalog.tombstones = [dict(source_id=SOURCE, passage_id=value, month=JULY) for value in ids]
        client.namespace(SETTINGS.namespace(TENANT)).write(
            upsert_rows=[dict(id=ids[0], text='synthetic tombstone target')])
        first = quantum(projector)
        self.assertEqual((first['deleted'], first['rows'], first['months']), (26, 0, 0))
        self.assertEqual(len(catalog.tombstones), 30)
        late = 'psg_' + 'd' * 32
        catalog.tombstones.append(dict(source_id=SOURCE, passage_id=late, month=JULY))
        catalog.enqueue(JULY, generation=2, reason='forget')
        second = quantum(projector)
        self.assertEqual((second['deleted'], second['rows']), (4, 0))
        for _ in range(3):
            quantum(projector)
        self.assertEqual([row['passage_id'] for row in catalog.tombstones], [late])
        ns = client.namespace(SETTINGS.namespace(TENANT))
        self.assertEqual([value for write in ns.writes for value in _deletes(write)], ids)

    def test_retry_budget_is_shared_across_page_callbacks(self):
        catalog, projector, client = self.fixture(rate_limit_budget_seconds=3)
        ns = client.namespace(SETTINGS.namespace(TENANT))
        write = ns.write
        calls = []
        def transient(**kwargs):
            calls.append(kwargs)
            if len(calls) in {1, 3, 4}:
                raise RateLimitError('synthetic retry')
            return write(**kwargs)
        ns.write = transient
        first = quantum(projector)
        self.assertEqual((first['rows'], first['rate_limited']), (2, 1))
        with self.assertLogs('recall_server.turbopuffer_projection', level='WARNING'):
            self.assertEqual(quantum(projector)['failed'], 1)
        self.assertEqual(len(calls), 4)
        self.assertEqual(catalog.shards, {})
        self.assertIn((SOURCE, JULY), catalog.outbox)


if __name__ == '__main__':
    unittest.main()
