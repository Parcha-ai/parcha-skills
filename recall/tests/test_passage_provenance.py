"""Native provenance is opt-in and reaches actual search consumers."""
from __future__ import annotations

import dataclasses
import hashlib
import json
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from recall_server import passage_projection as projection
from recall_server.logical_evidence import LogicalEvidenceRecord
from recall_server.passage_index import CanonicalPassageProjector
from recall_server.passage_retrieval import rerank_context
from recall_server.turbopuffer_plane import passage_row
from recall_server.turbopuffer_retrieval import arm_row, TurbopufferHintRetrieval

SOURCE = "codex:test"
TIME = "2026-07-12T12:28:55.939Z"
DOC = "ldoc_" + "1" * 32


def record(ordinal, content, roles=("assistant",)):
    return LogicalEvidenceRecord(
        ordinal=ordinal, event_native_id=f"event-{ordinal}",
        event_kind=content.get("type", "message"), occurred_at=TIME,
        roles=roles, receipts=(f"recall://{SOURCE}/event-{ordinal}?rev=1#item=0",),
        segment_ordinal=0, segment_count=1, text=json.dumps(content),
    )


def records(session="child-a", parent="parent", message_id="msg-old"):
    return (
        record(0, {"type": "session_meta", "payload": {
            "id": session, "forked_from_id": parent,
            "source": {"subagent": {"thread_spawn": {"parent_thread_id": parent}}},
        }}, ()),
        record(1, {"type": "event_msg", "payload": {
            "type": "agent_message", "message": "Earlier status report."}}),
        record(2, {"type": "response_item", "payload": {
            "type": "message", "role": "assistant", "id": message_id,
            "content": [{"type": "output_text", "text": "Earlier status report."}]}}),
        record(3, {"type": "response_item", "payload": {
            "type": "message", "role": "assistant", "id": "msg-new",
            "content": [{"type": "output_text", "text": "Later distinct status."}]}}),
    )


def build(values, policy):
    messages = projection.visible_messages(values, policy=policy)
    return messages, projection.build_passages(
        tenant_id="tenant:test", source_id=SOURCE, logical_document_id=DOC,
        revision=1, messages=messages, policy=policy,
    )


class PassageProvenanceTests(unittest.TestCase):
    def test_opt_in_retains_both_mirrors_and_distinct_child_work(self):
        messages, passages = build(records(), projection.PROVENANCE_PASSAGE_POLICY)
        self.assertEqual(len(messages), 3)
        self.assertEqual([m.provenance.native_message_id for m in messages],
                         [None, "msg-old", "msg-new"])
        self.assertEqual([m.provenance.record_type for m in messages],
                         ["event_msg", "response_item", "response_item"])
        self.assertEqual(messages[0].provenance.visible_text_sha256,
                         messages[1].provenance.visible_text_sha256)
        self.assertEqual(len(passages[0].receipts), 3)
        for message in messages:
            self.assertEqual(message.provenance.native_session_id, "child-a")
            self.assertEqual(message.provenance.fork_parent_session_id, "parent")
            self.assertEqual(message.provenance.serialized_at, TIME)
            self.assertIsNone(message.provenance.original_occurred_at)
        self.assertEqual(projection.reconstruct_passage(passages[0], messages),
                         passages[0].text)

    def test_sibling_identity_is_not_whole_document_deduplication(self):
        a, _ = build(records(), projection.PROVENANCE_PASSAGE_POLICY)
        b, _ = build(records(session="child-b"), projection.PROVENANCE_PASSAGE_POLICY)
        self.assertEqual(a[1].provenance.native_message_id, b[1].provenance.native_message_id)
        self.assertNotEqual(a[1].provenance.native_session_id, b[1].provenance.native_session_id)
        self.assertEqual(len(a), len(b))
        changed = list(records())
        changed[2] = record(2, {"type": "response_item", "payload": {
            "type": "message", "id": "msg-old", "role": "assistant", "content": "Edited"}})
        c, _ = build(changed, projection.PROVENANCE_PASSAGE_POLICY)
        self.assertNotEqual(a[1].provenance.visible_text_sha256, c[1].provenance.visible_text_sha256)

    def test_conflicting_or_missing_ancestry_never_invents_parent(self):
        for conflict in (True, False):
            values = list(records())
            payload = json.loads(values[0].text)["payload"]
            if conflict:
                payload["source"]["subagent"]["thread_spawn"]["parent_thread_id"] = "different"
            else:
                payload.pop("forked_from_id")
                payload.pop("source")
            values[0] = record(0, {"type": "session_meta", "payload": payload}, ())
            messages, _ = build(values, projection.PROVENANCE_PASSAGE_POLICY)
            self.assertTrue(all(m.provenance.fork_parent_session_id is None for m in messages))

    def test_later_conflicting_header_invalidates_all_lineage_not_native_ids(self):
        values = (*records(), record(4, {"type": "session_meta", "payload": {
            "id": "different-child", "forked_from_id": "different-parent"}}, ()))
        messages, _ = build(values, projection.PROVENANCE_PASSAGE_POLICY)
        self.assertTrue(all(m.provenance.native_session_id is None for m in messages))
        self.assertTrue(all(m.provenance.fork_parent_session_id is None for m in messages))
        self.assertEqual(messages[1].provenance.native_message_id, "msg-old")

    def test_hidden_reasoning_payloads_do_not_become_visible_provenance(self):
        for change in ({"channel": "analysis"}, {"type": "reasoning"}, {"type": "agent_reasoning"}):
            values = list(records())
            content = json.loads(values[2].text)
            content["payload"].update(change)
            values[2] = record(2, content)
            messages, _ = build(values, projection.PROVENANCE_PASSAGE_POLICY)
            self.assertEqual([m.record_ordinal for m in messages], [1, 3])
            self.assertNotIn("msg-old", [m.provenance.native_message_id for m in messages])

    def test_nested_hidden_blocks_and_malformed_types(self):
        variants = (
            {"type": "response_item", "payload": {"type": "message", "content": [
                {"type": "output_text", "text": "Visible"}, {"type": "reasoning", "text": "Hidden"}]}},
            {"type": "message", "channel": "analysis", "text": "Hidden"},
            {"type": "message", "message": {"channel": "analysis", "content": "Hidden"}},
            {"type": "response_item", "payload": {"type": [], "content": "Visible"}},
        )
        for content in variants:
            values = (records()[0], record(1, content))
            messages = projection.visible_messages(values, policy=projection.PROVENANCE_PASSAGE_POLICY)
            self.assertNotIn("Hidden", " ".join(m.text for m in messages))

    def test_deep_visible_content_is_preserved_without_depth_truncation(self):
        content = [{"type": "output_text", "text": "Visible at depth 100"},
                   {"type": "reasoning", "text": "Hidden at depth 100"}]
        for _ in range(100):
            content = {"content": content}
        values = (records()[0], record(1, {"type": "response_item", "payload": {
            "type": "message", "role": "assistant", "id": "msg-deep", "content": content}}))
        messages = projection.visible_messages(values, policy=projection.PROVENANCE_PASSAGE_POLICY)
        self.assertEqual([message.text for message in messages], ["Visible at depth 100"])

    def test_malformed_hydrated_provenance_does_not_qualify_time(self):
        messages, _ = build(records(), projection.PROVENANCE_PASSAGE_POLICY)
        value = dataclasses.asdict(messages[1].provenance)
        for bad in ({}, {**value, "visible_text_sha256": "bad"},
                    {**value, "serialized_at": "invalid"}, {**value, "native_session_id": "x" * 513},
                    {**value, "original_occurred_at": TIME}):
            row = {"header_redacted": "legacy", "spans": [{"provenance": bad}]}
            self.assertEqual(rerank_context(row), "legacy")

    def test_unbound_or_malformed_provenance_is_rejected(self):
        messages, _ = build(records(), projection.PROVENANCE_PASSAGE_POLICY)
        message = messages[1]
        for change in (
            {"visible_text_sha256": "0" * 64}, {"serialized_at": "2020-01-01T00:00:00Z"},
            {"contract": "invented"}, {"native_message_id": "x" * 513},
            {"native_session_id": None}, {"fork_parent_session_id": "child-a"},
            {"original_occurred_at": "2020-01-01T00:00:00Z"}, {"record_type": "bad\nvalue"},
        ):
            with self.subTest(change=change), self.assertRaises(ValueError):
                dataclasses.replace(message, provenance=dataclasses.replace(message.provenance, **change)).validate()
        with self.assertRaises(ValueError):
            dataclasses.replace(message, provenance={}).validate()

    def test_consistent_header_append_preserves_completed_windows_and_provenance(self):
        policy = projection.PassagePolicy(target_tokens=8, overlap_tokens=2,
                                         contract=projection.PROVENANCE_PASSAGE_CONTRACT)
        values = (records()[0], record(1, {"type": "response_item", "payload": {
            "type": "message", "role": "assistant", "id": "msg-long",
            "content": " ".join(f"word-{index}" for index in range(26))}}))
        messages, before = build(values, policy)
        appended = (*values, record(2, {"type": "response_item", "payload": {
            "type": "message", "role": "assistant", "id": "msg-appended",
            "content": "Additional distinct child work remains searchable."}}))
        new_messages, after = build(appended, policy)
        self.assertGreater(len(before), 2)
        self.assertGreater(len(after), len(before))
        for old, retained in zip(before[:-1], after):
            self.assertEqual(old.passage_id, retained.passage_id)
            self.assertEqual(old.spans, retained.spans)
            self.assertEqual(old.receipts, retained.receipts)
            self.assertEqual(projection.reconstruct_passage(old, messages),
                             projection.reconstruct_passage(retained, new_messages))
        self.assertEqual(new_messages[-1].provenance.native_message_id, "msg-appended")
        self.assertEqual(new_messages[-1].provenance.fork_parent_session_id, "parent")

    def test_legacy_serialization_and_context_are_unchanged(self):
        messages = projection.visible_messages(records())
        _, passages = build(records(), projection.DEFAULT_PASSAGE_POLICY)
        _, candidate = build(records(), projection.PROVENANCE_PASSAGE_POLICY)
        self.assertTrue(all(type(m) is projection.PassageMessage for m in messages))
        self.assertTrue(all(type(s) is projection.PassageSpan for s in passages[0].spans))
        self.assertNotIn("provenance", projection.canonical_spans_json(passages[0].spans))
        self.assertEqual(passages[0].text, candidate[0].text)
        self.assertEqual(passages[0].receipts, candidate[0].receipts)
        self.assertNotEqual(passages[0].passage_id, candidate[0].passage_id)
        self.assertEqual(rerank_context({"source_id": SOURCE, "header_redacted": "Exact old header"}),
                         "Exact old header")
        with self.assertRaises(ValueError):
            projection.build_passages(tenant_id="tenant:test", source_id=SOURCE,
                logical_document_id=DOC, revision=1,
                messages=projection.visible_messages(records(), policy=projection.PROVENANCE_PASSAGE_POLICY),
                policy=projection.DEFAULT_PASSAGE_POLICY)

    def test_full_search_returns_qualified_hints_and_sends_qualified_provider_text(self):
        from tests.central_brain.fake_turbopuffer import FakeTurbopuffer
        from tests.central_brain import test_turbopuffer_retrieval as fixtures

        _, passages = build(records(), projection.PROVENANCE_PASSAGE_POLICY)
        native = passage_row({**dataclasses.asdict(passages[0]),
            "text_redacted": passages[0].text, "doc_first_occurred_at": TIME,
            "doc_last_occurred_at": TIME, "native_parent_id": "file-child",
            "manifest_object_key": "k", "manifest_content_sha256": "x"})
        client = FakeTurbopuffer()
        retrieval, store = fixtures.ArmTests()._retrieval(client)
        retrieval.sources = [SOURCE]
        retrieval.policy_fingerprint = projection.PROVENANCE_PASSAGE_POLICY.fingerprint
        namespace = client.namespace(fixtures.SETTINGS.namespace(fixtures.TENANT))
        namespace.write(upsert_rows=[native])
        store.rerank_runtime = fixtures.RerankWiringTests._FakeRerank({})
        store.query_clauses = False
        response = retrieval.search("Earlier status report", lexical_query="status report",
                                    since=None, until=None, limit=10)
        self.assertEqual(response["diagnostics"]["authority_status"], "ok")
        self.assertEqual(response["diagnostics"]["hydrate_status"], "ok")
        hints = response["results"][0]["matching_ranges"]
        self.assertEqual(hints[0]["time_provenance"], {
            "basis": "fork_serialization", "original_occurred_at": None})
        self.assertEqual(hints[0]["receipts"], list(passages[0].receipts))
        documents = store.rerank_runtime.calls[0]["documents"]
        self.assertEqual(len(store.rerank_runtime.calls), 1)
        self.assertIn("may include replayed history", documents[0])
        self.assertIn("Later distinct status.", documents[0])

    def test_projector_and_native_hydration_reach_rerank_consumer(self):
        raw = b"".join(r.encode(source_id=SOURCE) for r in records())
        reference = {"artifact_id": "a", "object_key": "k", "content_sha256": hashlib.sha256(raw).hexdigest(),
                     "size_bytes": len(raw), "media_type": "application/jsonl", "version_id": "v"}
        manifest = {"logical_document_id": DOC, "revision": 1, "document_content_sha256": "x",
                    "parts": [{"ordinal": 0, **reference}]}
        logical = SimpleNamespace(read_manifest=Mock(return_value=manifest), read_part=Mock(return_value=raw))
        projector = CanonicalPassageProjector(SimpleNamespace(), logical,
                                              policy=projection.PROVENANCE_PASSAGE_POLICY)
        candidate = SimpleNamespace(tenant_id="tenant:test", source_id=SOURCE, logical_document_id=DOC,
                                    revision=1, source_document_sha256="x", manifest_reference={},
                                    part_references=(reference,))
        passage = projector._prepare(candidate).passages[0]
        catalog = {**dataclasses.asdict(passage), "text_redacted": passage.text,
                   "doc_first_occurred_at": TIME, "doc_last_occurred_at": TIME,
                   "native_parent_id": "file-child", "manifest_object_key": "k",
                   "manifest_content_sha256": "x", "header_redacted": "passage start: " + TIME}
        native = passage_row(catalog)
        leg = arm_row({**native, "text": None}, .8)
        leg["text_redacted"] = ""
        hint = {"passage_id": passage.passage_id, "text": ""}
        results = [{"matching_ranges": [hint]}]
        retrieval = object.__new__(TurbopufferHintRetrieval)
        retrieval._query = Mock(return_value=([native], "ok"))
        with patch.object(TurbopufferHintRetrieval, "_authorize_ranges", return_value={"authority_status": "ok"}):
            retrieval._hydrate_ranges(results, (("dense", 1., [leg]),), deadline_at=100.)
        self.assertEqual(hint["spans"][1]["provenance"]["native_message_id"], "msg-old")
        self.assertIn("original event time unknown", rerank_context(leg))
        self.assertIn("fork serialization", rerank_context(leg))


if __name__ == "__main__":
    unittest.main()
