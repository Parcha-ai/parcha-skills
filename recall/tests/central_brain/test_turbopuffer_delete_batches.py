"""Explicit delete quanta use bytes, independently of embedding row limits."""
import json
import unittest
from unittest import mock

from recall_server.turbopuffer_projection import delete_id_batches

from tests.central_brain.test_turbopuffer_projection import (
    _Catalog, _projector, _deletes, TENANT, SOURCE, SETTINGS, JULY,
    RateLimitError, NotFoundError,
)


class DeleteBatchTests(unittest.TestCase):
    def fixture(self, count=96, **kwargs):
        ids = [f"psg_{index:032x}" for index in range(count)]
        catalog = _Catalog()
        catalog.tombstones = [dict(source_id=SOURCE, passage_id=value, month=JULY)
                              for value in ids]
        catalog.enqueue(JULY, reason="forget")
        projector, client = _projector(catalog, batch_rows=32, tokens_per_minute=0,
                                       **kwargs)
        namespace = client.namespace(SETTINGS.namespace(TENANT))
        namespace.write(upsert_rows=[dict(id=ids[0], text="synthetic")])
        namespace.writes.clear()
        return ids, catalog, projector, namespace

    def test_many_small_ids_fit_one_quantum_without_early_ack(self):
        ids, catalog, projector, namespace = self.fixture()
        first = projector.drain_quantum(tenant_id=TENANT, max_months=1)
        self.assertEqual(first["deleted"], len(ids),
                         "embedding row limit still fragments explicit deletes")
        self.assertEqual(first["months"], 0)
        self.assertEqual([_deletes(row) for row in namespace.writes], [ids])
        self.assertEqual(len(catalog.tombstones), len(ids))
        self.assertEqual(catalog.shards, {})
        final = projector.drain_quantum(tenant_id=TENANT, max_months=1)
        self.assertEqual(final["months"], 1)
        self.assertEqual(catalog.tombstones, [])

    def test_existing_byte_budget_bounds_arrays_and_preserves_all_ids(self):
        ids, catalog, projector, namespace = self.fixture(max_batch_bytes=1024)
        while catalog.outbox:
            result = projector.drain_quantum(tenant_id=TENANT, max_months=1)
            self.assertEqual(result["failed"], 0)
        arrays = [_deletes(row) for row in namespace.writes]
        self.assertEqual([item for batch in arrays for item in batch], ids)
        self.assertTrue(all(len(json.dumps(batch, separators=(",", ":")).encode())
                            <= 1024 for batch in arrays), "delete array exceeded byte budget")
        self.assertEqual(len(arrays), 4)

    def test_failed_delete_retains_queue_and_watermark(self):
        ids, catalog, projector, namespace = self.fixture()
        def fail(**_):
            raise ValueError("synthetic provider failure")
        namespace.write = fail
        with self.assertLogs("recall_server.turbopuffer_projection", level="WARNING"):
            result = projector.drain_quantum(tenant_id=TENANT, max_months=1)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(len(catalog.tombstones), len(ids))
        self.assertEqual(catalog.shards, {})
        self.assertTrue(catalog.outbox)

    def test_json_array_bound_accounts_for_brackets_commas_and_escaping(self):
        from turbopuffer.lib import json as sdk_json
        values = ['a', 'b', 'quote"', '\\', '\n', 'é', '😀']
        for bound in (16, 17, 20, 30):
            with self.subTest(bound=bound):
                batches = list(delete_id_batches(values, max_bytes=bound))
                self.assertEqual([value for batch in batches for value in batch], values)
                for batch in batches:
                    encoded = json.dumps(batch, ensure_ascii=True, separators=(",", ":")).encode()
                    self.assertLessEqual(len(encoded), bound)
                    self.assertLessEqual(len(sdk_json.dumps(batch)), bound)
                    self.assertEqual(json.loads(encoded), batch)
        self.assertEqual(list(delete_id_batches(['a', 'b'], max_bytes=9)), [['a', 'b']])
        self.assertEqual(list(delete_id_batches(['a', 'b'], max_bytes=8)), [['a'], ['b']])
        self.assertEqual(list(delete_id_batches([], max_bytes=1024)), [])
        with self.assertRaisesRegex(ValueError, "delete ID exceeds"):
            list(delete_id_batches(['too long'], max_bytes=4))

    def test_transient_retries_same_array_without_embedding_pacer(self):
        ids, catalog, projector, namespace = self.fixture()
        writes, sleeps = [], []
        original = namespace.write
        def flaky(**kwargs):
            writes.append(kwargs["deletes"].copy())
            if len(writes) == 1:
                raise RateLimitError("synthetic")
            return original(**kwargs)
        namespace.write = flaky
        projector.sleep = sleeps.append
        with mock.patch.object(projector.pacer, "wait_for", side_effect=AssertionError):
            first = projector.drain_quantum(tenant_id=TENANT, max_months=1)
        self.assertEqual(first["deleted"], len(ids))
        self.assertEqual(first["rate_limited"], 1)
        self.assertEqual(writes, [ids, ids])
        self.assertEqual(sleeps, [1])
        self.assertEqual(len(catalog.tombstones), len(ids))
        self.assertEqual(catalog.shards, {})

    def test_not_found_large_array_finishes_only_after_yield(self):
        ids, catalog, projector, namespace = self.fixture()
        def absent(**kwargs):
            self.assertEqual(kwargs["deletes"], ids)
            raise NotFoundError("synthetic")
        namespace.write = absent
        first = projector.drain_quantum(tenant_id=TENANT, max_months=1)
        self.assertEqual((first["deleted"], first["failed"], first["months"]), (96, 0, 0))
        self.assertEqual(catalog.shards, {})
        self.assertEqual(len(catalog.tombstones), 96)
        final = projector.drain_quantum(tenant_id=TENANT, max_months=1)
        self.assertEqual(final["months"], 1)
        self.assertEqual(catalog.tombstones, [])

    def test_partial_failure_replays_captured_ids_and_preserves_late_tombstone(self):
        ids, catalog, projector, namespace = self.fixture(max_batch_bytes=1024)
        original = namespace.write
        first = projector.drain_quantum(tenant_id=TENANT, max_months=1)
        self.assertEqual(first["deleted"], 26)
        def fail(**kwargs):
            raise ValueError("synthetic permanent failure")
        namespace.write = fail
        with self.assertLogs("recall_server.turbopuffer_projection", level="WARNING"):
            self.assertEqual(projector.drain_quantum(tenant_id=TENANT, max_months=1)["failed"], 1)
        self.assertEqual(len(catalog.tombstones), len(ids))
        self.assertEqual(catalog.shards, {})
        namespace.write = original
        self.assertEqual(projector.drain_quantum(tenant_id=TENANT, max_months=1)["deleted"], 26)
        late = 'psg_' + 'f' * 32
        catalog.tombstones.append(dict(source_id=SOURCE, passage_id=late, month=JULY))
        catalog.enqueue(JULY, generation=2, reason="forget")
        for _ in range(3):
            self.assertEqual(projector.drain_quantum(tenant_id=TENANT, max_months=1)["months"], 0)
        final = projector.drain_quantum(tenant_id=TENANT, max_months=1)
        self.assertEqual((final["months"], final["requeued"]), (1, 1))
        self.assertEqual([row["passage_id"] for row in catalog.tombstones], [late])
        deleted = [value for batch in namespace.writes for value in _deletes(batch)]
        self.assertEqual(deleted, ids[:26] + ids)

    def test_invalid_oversized_id_fails_before_write_and_retains_ack(self):
        _, catalog, projector, namespace = self.fixture(count=1, max_batch_bytes=1024)
        catalog.tombstones[0]["passage_id"] = "x" * 1024
        with mock.patch.object(namespace, "write") as write, \
             self.assertLogs("recall_server.turbopuffer_projection", level="WARNING"):
            result = projector.drain_quantum(tenant_id=TENANT, max_months=1)
        self.assertEqual((result["failed"], result["months"]), (1, 0))
        write.assert_not_called()
        self.assertEqual(catalog.shards, {})
        self.assertEqual(len(catalog.tombstones), 1)
        self.assertTrue(catalog.outbox)
