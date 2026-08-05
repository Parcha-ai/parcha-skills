from __future__ import annotations

import unittest

from recall_server.actor_attribution import (
    ActorLink,
    actor_links,
    canonical_actor_links,
)
from recall_server.logical_evidence import LogicalEvidenceRecord
from recall_server.passage_projection import (
    PassageMessage,
    PassagePolicy,
    build_passages,
    decode_logical_record,
)


class ActorAttributionTests(unittest.TestCase):
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
