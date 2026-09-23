"""Scoped repair plans remain read-only unless queueing is explicitly selected."""

import unittest
from contextlib import nullcontext
from unittest.mock import Mock

from recall_server.passage_index import CanonicalPassageProjector
from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY


class EmptyPassageRepairTest(unittest.TestCase):
    def projector(self):
        con = Mock()
        con.transaction.return_value = nullcontext()
        con.execute.return_value.rowcount = 1
        store = Mock()
        store.connect.return_value = nullcontext(con)
        projector = CanonicalPassageProjector(
            store, Mock(), policy=DEFAULT_PASSAGE_POLICY
        )
        projector.shadow_diff = Mock(
            return_value={
                "documents": [
                    {
                        "logical_document_id": "ldoc_" + "1" * 32,
                        "revision": 1,
                        "source_document_sha256": "a" * 64,
                        "policy_fingerprint": DEFAULT_PASSAGE_POLICY.fingerprint,
                        "status": "compared",
                        "passages_existing": 0,
                        "passages_recomputed": 1,
                        "archive_bytes": 500,
                        "embedding_bytes_estimate": 600,
                        "embedding_tokens_estimate": 150,
                    },
                    {
                        "logical_document_id": "ldoc_" + "2" * 32,
                        "revision": 1,
                        "status": "compared",
                        "passages_existing": 0,
                        "passages_recomputed": 0,
                        "archive_bytes": 200,
                        "embedding_bytes_estimate": 0,
                        "embedding_tokens_estimate": 0,
                    },
                ]
            }
        )
        return projector, con

    def test_default_is_read_only_and_cost_unknown_without_price(self):
        p, con = self.projector()
        result = p.repair_empty(tenant_id="tenant:test", source_id="source:test")
        self.assertTrue(result["read_only"])
        self.assertEqual(result["eligible_documents"], 1)
        self.assertEqual(result["archive_bytes"], 700)
        self.assertEqual(result["embedding_tokens_estimate"], 150)
        self.assertIsNone(result["embedding_cost_usd_estimate"])
        self.assertEqual(result["next_after"], "ldoc_" + "2" * 32)
        con.execute.assert_not_called()

    def test_apply_enqueues_only_recomputed_documents_and_supplied_price_is_advisory(
        self,
    ):
        p, con = self.projector()
        result = p.repair_empty(
            tenant_id="tenant:test",
            source_id="source:test",
            apply=True,
            price_per_mtoken=1.0,
        )
        self.assertEqual(result["queued"], 1)
        self.assertEqual(result["embedding_cost_usd_estimate"], 0.00015)
        sql, args = con.execute.call_args.args
        self.assertIn("ON CONFLICT", sql)
        self.assertIn("DO NOTHING", sql)
        self.assertNotIn("DO UPDATE", sql)
        self.assertIn("projected.passage_count=0", sql)
        self.assertIn(
            "evidence.document_content_sha256=selected.source_document_sha256", sql
        )
        self.assertIn("projected.policy_fingerprint=selected.policy_fingerprint", sql)
        self.assertEqual(args[1:], ("tenant:test", "source:test"))
        self.assertNotIn("ldoc_" + "2" * 32, args[0])

    def test_invalid_scope_batch_cursor_price_or_apply_refused_before_reads(self):
        for options in (
            {"source_id": ""},
            {"limit": 0},
            {"limit": True},
            {"after": "arbitrary"},
            {"price_per_mtoken": float("nan")},
            {"price_per_mtoken": -1},
            {"price_per_mtoken": True},
            {"apply": "yes"},
        ):
            p, con = self.projector()
            with self.subTest(options=options), self.assertRaises(ValueError):
                p.repair_empty(
                    **{
                        "tenant_id": "tenant:test",
                        "source_id": "source:test",
                        **options,
                    }
                )
            p.shadow_diff.assert_not_called()
            con.execute.assert_not_called()

    def test_archive_failure_never_queues_a_partial_plan(self):
        p, con = self.projector()
        p.shadow_diff.side_effect = ValueError("synthetic immutable archive mismatch")
        with self.assertRaises(ValueError):
            p.repair_empty(tenant_id="tenant:test", source_id="source:test", apply=True)
        con.execute.assert_not_called()

    def test_no_new_passages_means_no_queue_write(self):
        p, con = self.projector()
        p.shadow_diff.return_value = {"documents": []}
        self.assertEqual(
            p.repair_empty(
                tenant_id="tenant:test", source_id="source:test", apply=True
            )["queued"],
            0,
        )
        con.execute.assert_not_called()
