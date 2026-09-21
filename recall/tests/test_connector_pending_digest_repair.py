"""Durable pending-page recovery uses the original SDK archive witness, never a pull."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

from connectors.sdk import ConnectorPage, ConnectorRecordV2, ConnectorRunner, ConnectorRunError
from privacy.policy import PrivacyPolicy
from server.recall_server.archive import FilesystemArchiveStore
from server.recall_server.projectors import canonical_json, validate_envelope
from tests.test_connector_sdk import FakeBrain, SyntheticConnector

PAYLOAD = b"%PDF-synthetic-digest-recovery-199"
DIGEST = hashlib.sha256(PAYLOAD).hexdigest()
STAMP = "2026-09-21T09:00:00Z"


class ValidatingBrain(FakeBrain):
    def ingest(self, events):
        for event in events:
            validate_envelope(event)
        return super().ingest(events)


class PendingDigestRepairTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.archive_store = FilesystemArchiveStore(self.root / "archive", namespace_key=b"n" * 32)
        self.archive = Mock(wraps=self.archive_store)
        self.connector = SyntheticConnector({})
        self.connector.connector_id = "slack.messages"
        self.connector.source_id = "slack:synthetic"
        self.brain = ValidatingBrain()
        self.runner = self.open_runner()
        self.addCleanup(lambda: self.runner.close())

    def open_runner(self):
        return ConnectorRunner(connector=self.connector, brain=self.brain,
                               spool_path=self.root / "spool.sqlite", archive=self.archive,
                               tenant_id="tenant:synthetic", principal_id="owner:synthetic",
                               privacy=PrivacyPolicy(mode="scrub"))

    def record(self, suffix="1"):
        native = "slack-file:T123:F" + suffix
        return ConnectorRecordV2(
            schema_version=2, native_id=native, native_parent_id="slack:T123:C123:123.456",
            occurred_at=STAMP,
            content={"kind": "document.v1", "content_fidelity": "complete", "document_id": native,
                     "name": "Synthetic attachment", "mime_type": "application/pdf", "surface": "slack",
                     "parent_id": "slack:T123:C123:123.456", "artifact_content_sha256": DIGEST,
                     "text": "Contact synthetic@example.invalid"},
            provenance={"uri": "connector://slack/files/" + native},
            archive_payload=PAYLOAD, archive_media_type="application/pdf",
        )

    def legacy_event(self, record):
        decision = PrivacyPolicy(mode="scrub").apply({"content": record.content, "provenance": record.provenance})
        self.assertIn("[REDACTED:financial_id]", decision.value["content"]["artifact_content_sha256"])
        reference = self.runner._archive_raw(record)
        # Real local archive proves this fixture's reference witnesses the exact binary bytes.
        self.assertEqual(self.archive_store.read_raw(reference), PAYLOAD)
        return self.runner._event(record, decision.value["content"], decision.value["provenance"], reference)

    def save_page(self, events):
        with self.runner.db:
            self.runner._set_meta("committed_cursor", json.dumps("before"))
            page = self.runner.db.execute(
                "INSERT INTO pages(cursor_before,cursor_after,has_more,created_at) VALUES (?,?,?,?)",
                ('"before"', '"after"', 1, 1.0),
            ).lastrowid
            self.runner.db.executemany("INSERT INTO outbox(page_id,envelope_json,state) VALUES (?,?,'pending')",
                                      [(page, json.dumps(event, sort_keys=True, separators=(",", ":"))) for event in events])

    def snapshot(self):
        return {
            "pages": [tuple(row) for row in self.runner.db.execute("SELECT * FROM pages ORDER BY id")],
            "outbox": [tuple(row) for row in self.runner.db.execute("SELECT * FROM outbox ORDER BY id")],
            "ack": [tuple(row) for row in self.runner.db.execute("SELECT * FROM acknowledged_records")],
            "cursor": self.runner._cursor(),
        }

    def test_reopen_repairs_only_digest_and_outer_hash_before_full_ack(self):
        record = self.record()
        original = self.legacy_event(record)
        self.save_page([original])
        self.runner.close()
        self.runner = self.open_runner()
        result = self.runner.run_once()
        self.assertEqual(result["acked"], 1)
        self.assertEqual(self.connector.pulls, [])
        self.assertEqual(self.archive.put_raw.call_count, 1)
        self.archive.read_raw.assert_not_called()
        actual = next(iter(self.brain.events.values()))
        expected = copy.deepcopy(original)
        expected["content"]["artifact_content_sha256"] = DIGEST
        expected["content_sha256"] = hashlib.sha256(canonical_json(expected["content"])).hexdigest()
        self.assertEqual(actual, expected)
        self.assertNotIn("synthetic@example.invalid", json.dumps(actual))
        self.assertEqual(self.runner._cursor(), "after")
        self.assertEqual(self.runner.doctor()["pending_pages"], 0)
        self.assertEqual(self.runner.db.execute("SELECT content_sha256 FROM acknowledged_records").fetchone()[0], expected["content_sha256"])

    def test_lost_ack_reopens_same_corrected_identity_without_repair_or_rearchive(self):
        original = self.legacy_event(self.record())
        self.save_page([original])
        self.brain.fail_after_commit = True
        with self.assertRaises(ConnectorRunError):
            self.runner.run_once()
        pending = self.snapshot()
        self.assertEqual(pending["cursor"], "before")
        self.assertFalse(pending["ack"])
        repaired = json.loads(pending["outbox"][0][2])
        self.assertEqual(repaired["content"]["artifact_content_sha256"], DIGEST)
        self.assertEqual(len(self.brain.events), 1)
        self.runner.close()
        self.runner = self.open_runner()
        self.assertEqual(self.runner.run_once()["acked"], 1)
        self.assertEqual(self.brain.duplicate_events, 1)
        self.assertEqual(len(self.brain.events), 1)
        self.assertEqual(next(iter(self.brain.events.values())), repaired)
        self.assertEqual(self.archive.put_raw.call_count, 1)
        self.assertEqual(self.connector.pulls, [])

    def test_malformed_ack_retains_repaired_page_without_cursor_or_ledger_advance(self):
        self.save_page([self.legacy_event(self.record())])
        self.brain.ingest = Mock(return_value={"status": "committed", "inserted": 1})
        with self.assertRaisesRegex(ConnectorRunError, "brain_invalid_acknowledgement"):
            self.runner.run_once()
        state = self.snapshot()
        self.assertEqual(state["cursor"], "before")
        self.assertFalse(state["ack"])
        self.assertEqual(len(state["pages"]), 1)
        self.assertEqual(json.loads(state["outbox"][0][2])["content"]["artifact_content_sha256"], DIGEST)

    def test_ambiguous_or_foreign_witness_cannot_repair(self):
        base = self.legacy_event(self.record())
        variants = [
            ("schema-bool", lambda e: e.update(schema_version=True)),
            ("schema-float", lambda e: e.update(schema_version=1.0)),
            ("connector-schema-float", lambda e: e["provenance"].update(connector_schema_version=2.0)),
            ("unknown-envelope", lambda e: e.update(private_extra="synthetic")),
            ("observed-time", lambda e: e.update(observed_at="2026-09-20T09:00:00Z")),
            ("tenant", lambda e: e["provenance"]["artifact_ref"].update(tenant_id="foreign")),
            ("ref-source", lambda e: e["provenance"]["artifact_ref"].update(source_id="foreign")),
            ("source", lambda e: e.update(source_id="foreign")),
            ("principal", lambda e: e.update(principal_id="foreign")),
            ("connector", lambda e: e["provenance"].update(connector_id="google.gmail")),
            ("native", lambda e: e.update(native_id="slack-file:T123:OTHER")),
            ("time", lambda e: e["provenance"]["artifact_ref"].update(created_at="2026-09-20T09:00:00Z")),
            ("mime", lambda e: e["provenance"]["artifact_ref"].update(media_type="text/plain")),
            ("missing-ref", lambda e: e["provenance"].pop("artifact_ref")),
            ("fake-marker", lambda e: e["content"].update(artifact_content_sha256="[REDACTED:financial_id]fake")),
            ("digest-changed", lambda e: e["provenance"]["artifact_ref"].update(content_sha256="a" * 64)),
        ]
        for label, mutate in variants:
            with self.subTest(label=label):
                event = copy.deepcopy(base)
                mutate(event)
                event["content_sha256"] = hashlib.sha256(canonical_json(event["content"])).hexdigest()
                self.save_page([event])
                before = self.snapshot()
                expected_code = ("archive_invalid_reference" if label in
                                 {"tenant", "ref-source", "time", "mime", "missing-ref"}
                                 else "connector_invalid_page")
                with self.assertRaisesRegex(ConnectorRunError, expected_code):
                    self.runner.run_once()
                self.assertEqual(self.snapshot(), before)
                self.assertFalse(self.brain.events)
                self.assertEqual(self.connector.pulls, [])
                with self.runner.db:
                    self.runner.db.execute("DELETE FROM outbox")
                    self.runner.db.execute("DELETE FROM pages")
        self.assertEqual(self.archive.put_raw.call_count, 1)

    def test_nonmatching_legacy_or_valid_digest_is_not_rewritten(self):
        for variant in ("schema1", "valid_digest"):
            with self.subTest(variant=variant):
                event = self.legacy_event(self.record())
                if variant == "schema1":
                    event["provenance"]["connector_schema_version"] = 1
                else:
                    event["content"]["artifact_content_sha256"] = "a" * 64
                    event["content_sha256"] = hashlib.sha256(canonical_json(event["content"])).hexdigest()
                self.save_page([event])
                before = self.snapshot()
                self.brain.ingest = Mock(side_effect=OSError("synthetic unavailable"))
                with self.assertRaises(ConnectorRunError):
                    self.runner.run_once()
                self.assertEqual(self.snapshot(), before)
                self.brain.ingest.assert_called_once_with([event])
                with self.runner.db:
                    self.runner.db.execute("DELETE FROM outbox")
                    self.runner.db.execute("DELETE FROM pages")


    def test_original_outer_hash_and_whole_page_validation_precede_mutation(self):
        for mismatch in ("outer_hash", "later_invalid_content"):
            with self.subTest(mismatch=mismatch):
                first = self.legacy_event(self.record("1"))
                second = self.legacy_event(self.record("2"))
                if mismatch == "outer_hash":
                    second["content_sha256"] = "0" * 64
                else:
                    second["content"]["content_fidelity"] = "invalid"
                    second["content_sha256"] = hashlib.sha256(canonical_json(second["content"])).hexdigest()
                self.save_page([first, second])
                before = self.snapshot()
                with self.assertRaisesRegex(ConnectorRunError, "connector_invalid_page"):
                    self.runner.run_once()
                self.assertEqual(self.snapshot(), before)
                self.assertFalse(self.brain.events)
                with self.runner.db:
                    self.runner.db.execute("DELETE FROM outbox")
                    self.runner.db.execute("DELETE FROM pages")

    def test_second_sqlite_update_failure_rolls_back_complete_page_repair(self):
        self.save_page([self.legacy_event(self.record("1")), self.legacy_event(self.record("2"))])
        before = self.snapshot()
        second = before["outbox"][1][0]
        self.runner.db.execute(f"CREATE TRIGGER fail_repair BEFORE UPDATE ON outbox WHEN OLD.id={second} BEGIN SELECT RAISE(ABORT, 'synthetic crash'); END")
        self.runner.db.commit()
        with self.assertRaisesRegex(ConnectorRunError, "connector_spool_error"):
            self.runner.run_once()
        self.assertEqual(self.snapshot(), before)
        self.assertFalse(self.brain.events)
        self.assertEqual(self.connector.pulls, [])

    def test_changed_second_row_refuses_and_rolls_back_prior_update(self):
        self.save_page([self.legacy_event(self.record("1")), self.legacy_event(self.record("2"))])
        before = self.snapshot()
        first, second = (row[0] for row in before["outbox"])
        # A database-side intervening modification defeats the old-envelope CAS.
        self.runner.db.execute(
            f"CREATE TRIGGER race_repair AFTER UPDATE ON outbox WHEN OLD.id={first} "
            f"BEGIN UPDATE outbox SET envelope_json=envelope_json || ' ' WHERE id={second}; END"
        )
        self.runner.db.commit()
        with self.assertRaisesRegex(ConnectorRunError, "connector_spool_error"):
            self.runner.run_once()
        self.assertEqual(self.snapshot(), before)
        self.assertFalse(self.brain.events)
        self.assertEqual(self.connector.pulls, [])


    def test_future_stage_and_repaired_pending_content_identity_are_equal(self):
        record = self.record()
        self.save_page([self.legacy_event(record)])
        self.runner.run_once()
        repaired = copy.deepcopy(next(iter(self.brain.events.values())))
        self.runner.close()
        self.runner = ConnectorRunner(connector=self.connector, brain=self.brain,
                                      spool_path=self.root / "fresh.sqlite", archive=self.archive,
                                      tenant_id="tenant:synthetic", principal_id="owner:synthetic",
                                      privacy=PrivacyPolicy(mode="scrub"))
        self.runner._stage(ConnectorPage(records=(record,), next_cursor="after", has_more=True), None)
        staged = json.loads(self.runner.db.execute("SELECT envelope_json FROM outbox").fetchone()[0])
        self.assertEqual(staged, repaired)


if __name__ == "__main__":
    unittest.main()
