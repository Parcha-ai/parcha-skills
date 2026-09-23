"""Provider text visibility must not depend on an employee identity mapping."""

import hashlib
import json
import unittest
from dataclasses import replace
from unittest.mock import patch

from connectors.slack_source import normalize_slack_message, normalize_slack_user
from recall_server.actor_attribution import ActorLink
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector
from recall_server.passage_projection import (
    DEFAULT_PASSAGE_POLICY,
    build_passages,
    decode_logical_record,
    visible_messages,
)
from recall_server.webhooks import build_webhook_event

SOURCE = "synthetic:slack:visibility"
TEXT = "The release was delayed because the archive was unavailable."
AUTHOR = ActorLink("actor_" + "a" * 32, "author")


def slack_record(text=TEXT):
    return normalize_slack_message(
        workspace_id="T123",
        channel_id="C123",
        value={
            "ts": "1784332800.000100",
            "user": "U111",
            "text": text,
        },
    )


def logical_records(record, links=()):
    event = build_webhook_event(
        {
            "schema_version": 1,
            "event_id": record.native_id,
            "parent_id": record.native_parent_id,
            "occurred_at": record.occurred_at,
            "record": record.content,
            "deleted": record.deleted,
        },
        {
            "source_id": SOURCE,
            "principal_id": "principal:synthetic",
            "webhook_privacy_mode": "scrub",
            "connector_id": "slack.messages",
        },
    ).event
    text = json.dumps(event["content"], sort_keys=True, separators=(",", ":"))
    payload = text.encode()
    digest = hashlib.sha256(payload).hexdigest()
    row = {
        "event_text": text,
        "source_chunks": [
            {"ordinal": 0, "size_bytes": len(payload), "text_sha256": digest}
        ],
        "chunk_receipts": [f"recall://{SOURCE}/{record.native_id}?rev=1#item=0"],
        "chunk_count": 1,
        "document_text_sha256": digest,
        "document_revision": 1,
        "raw_media_type": "application/json",
        "native_id": record.native_id,
        "kind": event["kind"],
        "occurred_at": event["occurred_at"],
        "actor_links": links,
    }
    projector = CanonicalLogicalEvidenceProjector(object(), object())
    records = tuple(projector._record_stream([row]))
    return tuple(
        decode_logical_record(r.encode(source_id=SOURCE), source_id=SOURCE)
        for r in records
    )


class SlackVisibilityTest(unittest.TestCase):
    def passages(self, records):
        messages = visible_messages(records)
        return (
            build_passages(
                tenant_id="tenant:synthetic",
                source_id=SOURCE,
                logical_document_id="ldoc_" + "b" * 32,
                revision=1,
                messages=messages,
                policy=DEFAULT_PASSAGE_POLICY,
            )
            if messages
            else ()
        )

    def test_unmapped_slack_author_produces_exact_passage_without_invented_actor(self):
        records = logical_records(slack_record())
        self.assertEqual(records[0].roles, ())
        self.assertEqual(records[0].actor_links, ())
        passages = self.passages(records)
        self.assertEqual([p.text for p in passages], [TEXT])
        self.assertEqual(passages[0].actor_links, ())
        self.assertEqual(passages[0].receipts, records[0].receipts)

    def test_mapped_author_retains_exact_text_and_attribution(self):
        passages = self.passages(logical_records(slack_record(), (AUTHOR,)))
        self.assertEqual([p.text for p in passages], [TEXT])
        self.assertEqual(passages[0].actor_links, (AUTHOR,))

    def test_identity_metadata_and_empty_messages_stay_out(self):
        identity = normalize_slack_user(
            workspace_id="T123",
            value={
                "id": "U111",
                "name": "Synthetic User",
                "profile": {"title": "Engineer"},
            },
        )[0]
        for record in (identity, slack_record(""), slack_record("  ")):
            self.assertEqual(self.passages(logical_records(record)), ())

    def test_explicit_tool_and_hidden_roles_stay_out(self):
        records = logical_records(slack_record())
        for role in ("tool", "system", "developer"):
            self.assertEqual(self.passages((replace(records[0], roles=(role,)),)), ())

    def test_untyped_record_text_is_not_promoted(self):
        records = logical_records(slack_record())
        for content in (
            {"text": TEXT},
            {"kind": "contact_identity.v1", "text": TEXT},
            {"kind": "unknown.v1", "text": TEXT},
        ):
            self.assertEqual(
                self.passages((replace(records[0], text=json.dumps(content)),)), ()
            )
        self.assertEqual(
            self.passages((replace(records[0], event_kind="tool_result"),)), ()
        )

    def test_segmented_message_reassembles_before_eligibility(self):
        with patch("recall_server.logical_evidence_projection.TEXT_SEGMENT_BYTES", 64):
            records = logical_records(slack_record())
        self.assertGreater(len(records), 1)
        self.assertEqual([p.text for p in self.passages(records)], [TEXT])
        with self.assertRaisesRegex(Exception, "segment_incomplete"):
            self.passages(records[:-1])
