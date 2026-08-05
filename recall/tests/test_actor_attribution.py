from __future__ import annotations

import unittest

from recall_server.actor_attribution import (
    ActorIdentityIndex,
    ActorLink,
    actor_id_for_principal,
    actor_links,
    canonical_actor_links,
    is_local_user_authored,
    native_actor_references,
)
from recall_server.logical_evidence import LogicalEvidenceRecord
from recall_server.passage_projection import (
    PassageMessage,
    PassagePolicy,
    build_passages,
    decode_logical_record,
    visible_messages,
)


class ActorAttributionTests(unittest.TestCase):
    def test_principal_actor_id_is_stable_and_tenant_scoped(self) -> None:
        first = actor_id_for_principal("tenant:company:a", "principal:alice")
        self.assertEqual(
            first,
            actor_id_for_principal("tenant:company:a", "principal:alice"),
        )
        self.assertNotEqual(
            first,
            actor_id_for_principal("tenant:company:b", "principal:alice"),
        )

    def test_identity_index_normalizes_email_but_keeps_provider_ids_exact(self) -> None:
        calls = []

        def blind(value, *, purpose):
            calls.append((value, purpose))
            return "a" * 64

        index = ActorIdentityIndex(blind)
        self.assertEqual(
            index.lookup(
                "tenant:company:one",
                "google.gmail",
                "author_id",
                " Alice@Example.com ",
            ),
            ("identity", "email", "a" * 64),
        )
        self.assertEqual(
            index.lookup(
                "tenant:company:one", "slack", "author_id", "U012ABC"
            ),
            ("slack", "author_id", "a" * 64),
        )
        self.assertEqual(calls[0][0], "alice@example.com")
        self.assertEqual(calls[1][0], "U012ABC")
        index.lookup(
            "tenant:company:two",
            "slack",
            "author_id",
            "U012ABC",
        )
        self.assertNotEqual(calls[1][1], calls[2][1])

    def test_connector_actor_references_are_typed_not_inferred_from_text(self) -> None:
        references = native_actor_references({
            "content": {
                "author_id": "alice@example.com",
                "owner_ids": ["bob@example.com"],
                "participant_ids": ["U012ABC"],
                "text": "Carol said she wrote this",
            }
        })
        self.assertEqual(
            {(item.namespace, item.subject, item.relation) for item in references},
            {
                ("author_id", "alice@example.com", "author"),
                ("owner_ids", "bob@example.com", "owner"),
                ("participant_ids", "U012ABC", "participant"),
            },
        )

    def test_local_author_rules_exclude_tool_results_sidechains_and_duplicates(self) -> None:
        claude = {
            "provenance": {"harness": "claude"},
            "content": {
                "type": "user",
                "message": {"content": "ship the actor resolver"},
            },
        }
        self.assertTrue(is_local_user_authored(claude, "claude.jsonl"))
        self.assertFalse(is_local_user_authored({
            **claude,
            "content": {**claude["content"], "isSidechain": True},
        }, "claude.jsonl"))
        self.assertFalse(is_local_user_authored({
            **claude,
            "content": {
                "type": "user",
                "message": {"content": [{"type": "tool_result", "content": "x"}]},
            },
        }, "claude.jsonl"))
        codex = {
            "provenance": {"harness": "codex"},
            "content": {
                "type": "event_msg",
                "payload": {"type": "user_message", "message": "ship it"},
            },
        }
        self.assertTrue(is_local_user_authored(codex, "codex.jsonl"))
        self.assertFalse(is_local_user_authored({
            **codex,
            "content": {
                "type": "response_item",
                "payload": {"type": "message", "role": "user"},
            },
        }, "codex.jsonl"))

    def test_exact_provider_author_is_a_dense_message_without_harness_role(self) -> None:
        record = LogicalEvidenceRecord(
            ordinal=0,
            event_native_id="slack-message-1",
            event_kind="connector_record",
            occurred_at="2026-08-05T12:00:00Z",
            roles=(),
            receipts=("recall://slack/source?rev=1#item=0",),
            segment_ordinal=0,
            segment_count=1,
            text='{"author_id":"U123","text":"shipped the worker"}',
            actor_links=(ActorLink("actor_" + "a" * 32, "author"),),
        )
        messages = visible_messages((record,))
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].roles, ("user",))
        self.assertEqual(messages[0].text, "shipped the worker")

    def test_contributor_only_tool_record_stays_sparse(self) -> None:
        record = LogicalEvidenceRecord(
            ordinal=0,
            event_native_id="tool-record-1",
            event_kind="connector_record",
            occurred_at="2026-08-05T12:00:00Z",
            roles=(),
            receipts=("recall://codex/source?rev=1#item=0",),
            segment_ordinal=0,
            segment_count=1,
            text='{"output":"large tool response"}',
            actor_links=(ActorLink("actor_" + "a" * 32, "contributor"),),
        )
        self.assertEqual(visible_messages((record,)), ())

    def test_links_are_deduplicated_and_canonically_ordered(self) -> None:
        alice = "actor_0123456789abcdef0123456789abcdef"
        bob = "actor_fedcba9876543210fedcba9876543210"

        links = actor_links((
            {"actor_id": bob, "relation": "contributor"},
            {"actor_id": alice, "relation": "author"},
            {"actor_id": bob, "relation": "contributor"},
        ))

        self.assertEqual(
            canonical_actor_links(links),
            [
                {"actor_id": alice, "relation": "author"},
                {"actor_id": bob, "relation": "contributor"},
            ],
        )

    def test_principal_ids_are_not_valid_actor_ids(self) -> None:
        with self.assertRaisesRegex(ValueError, "invalid actor attribution"):
            ActorLink("principal:alice", "author")

    def test_relations_are_explicit_not_free_form(self) -> None:
        with self.assertRaisesRegex(ValueError, "invalid actor attribution"):
            ActorLink(
                "actor_0123456789abcdef0123456789abcdef",
                "probably-wrote",
            )

    def test_actor_links_round_trip_through_logical_s3_records(self) -> None:
        link = ActorLink(
            "actor_0123456789abcdef0123456789abcdef",
            "author",
        )
        record = LogicalEvidenceRecord(
            ordinal=0,
            event_native_id="message:1",
            event_kind="communication_message.v1",
            occurred_at="2026-08-05T00:00:00Z",
            roles=("user",),
            receipts=("recall://source:test/item?rev=1#item=0",),
            segment_ordinal=0,
            segment_count=1,
            text="Alice wrote the deployment note.",
            actor_links=(link,),
        )

        decoded = decode_logical_record(
            record.encode(source_id="source:test"),
            source_id="source:test",
        )

        self.assertEqual(decoded.actor_links, (link,))

    def test_passages_union_only_their_message_actor_links(self) -> None:
        alice = ActorLink(
            "actor_0123456789abcdef0123456789abcdef",
            "author",
        )
        bob = ActorLink(
            "actor_fedcba9876543210fedcba9876543210",
            "contributor",
        )
        passages = build_passages(
            tenant_id="tenant:company:test",
            source_id="source:test",
            logical_document_id="ldoc_0123456789abcdef0123456789abcdef",
            revision=1,
            messages=(
                PassageMessage(
                    record_ordinal=0,
                    occurred_at="2026-08-05T00:00:00Z",
                    roles=("user",),
                    receipts=("recall://source:test/1?rev=1#item=0",),
                    text="alpha beta gamma",
                    actor_links=(alice,),
                ),
                PassageMessage(
                    record_ordinal=1,
                    occurred_at="2026-08-05T00:01:00Z",
                    roles=("assistant",),
                    receipts=("recall://source:test/2?rev=1#item=0",),
                    text="delta epsilon zeta",
                    actor_links=(bob,),
                ),
            ),
            policy=PassagePolicy(target_tokens=8, overlap_tokens=0),
        )

        self.assertEqual(passages[0].actor_links, (alice, bob))


if __name__ == "__main__":
    unittest.main()
