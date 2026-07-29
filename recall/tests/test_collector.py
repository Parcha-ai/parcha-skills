from __future__ import annotations

import gzip
import json
import hashlib
import os
import tempfile
import threading
import time
import unittest
from unittest import mock
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from collector.collector import (
    MAX_CANONICAL_BATCH_EVENTS,
    Collector,
    CollectorRuntimeError,
)
from privacy.policy import PrivacyPolicy


def claude_line(text: str, timestamp: str = "2026-07-12T22:00:00Z") -> str:
    return json.dumps({"type": "user", "timestamp": timestamp, "message": {"content": text}}) + "\n"


class AckServer(BaseHTTPRequestHandler):
    batches: dict[str, dict] = {}
    drop_after_first_commit = False
    requests = 0
    received: list[dict] = []

    def log_message(self, *_args) -> None:
        pass

    def do_POST(self) -> None:
        length = int(self.headers["Content-Length"])
        body = json.loads(self.rfile.read(length))
        type(self).received.append(body)
        key = self.headers["Idempotency-Key"]
        type(self).requests += 1
        ack = type(self).batches.get(key)
        replay = ack is not None
        if ack is None:
            ack = {
                "batch_id": "fake-" + str(len(type(self).batches) + 1),
                "status": "committed",
                "inserted": len(body["events"]),
                "duplicate_events": 0,
                "receipts": [f"recall://{e['source_id']}/{e['native_id']}?rev=1" for e in body["events"]],
            }
            type(self).batches[key] = ack
            if type(self).drop_after_first_commit:
                type(self).drop_after_first_commit = False
                self.connection.shutdown(2)
                self.connection.close()
                return
        rendered = json.dumps({**ack, "replay": replay}).encode()
        self.send_response(200 if replay else 201)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(rendered)))
        self.end_headers()
        self.wfile.write(rendered)


class CollectorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "claude"
        self.root.mkdir()
        self.spool = Path(self.tmp.name) / "spool.db"
        AckServer.batches = {}
        AckServer.requests = 0
        AckServer.drop_after_first_commit = False
        AckServer.received = []
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), AckServer)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.endpoint = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.tmp.cleanup()

    def collector(self, endpoint: str | None = None) -> Collector:
        return Collector(
            root=self.root,
            harness="claude",
            source_id="claude:linux:test",
            spool_path=self.spool,
            endpoint=endpoint or self.endpoint,
            token="test-token-not-a-secret",
        )

    def test_default_archive_concurrency_leaves_capacity_for_ingest(self) -> None:
        collector = self.collector()
        self.assertEqual(collector.archive_workers, 2)
        collector.close()

    def test_canonical_batch_size_can_reach_safe_server_cap(self) -> None:
        collector = Collector(
            root=self.root,
            harness="claude",
            source_id="claude:linux:test",
            spool_path=self.spool,
            endpoint=self.endpoint,
            token="test-token-not-a-secret",
            brain_writer=object(),
            archive=object(),
            tenant_id="tenant:test",
            batch_size=MAX_CANONICAL_BATCH_EVENTS + 1,
        )
        self.assertEqual(collector.batch_size, MAX_CANONICAL_BATCH_EVENTS)
        self.assertEqual(MAX_CANONICAL_BATCH_EVENTS, 1_000)
        collector.close()

    def test_discovery_prioritizes_newest_sessions_for_backfill_freshness(self) -> None:
        older = self.root / "older.jsonl"
        newer = self.root / "newer.jsonl"
        older.write_text(claude_line("older"))
        newer.write_text(claude_line("newer"))
        os.utime(older, ns=(1_000_000_000, 1_000_000_000))
        os.utime(newer, ns=(2_000_000_000, 2_000_000_000))
        collector = self.collector()
        self.assertEqual(collector.discover(), [newer, older])
        collector.close()

    def test_committed_cursor_waits_for_ack_and_survives_restart(self) -> None:
        transcript = self.root / "session.jsonl"
        transcript.write_text(claude_line("first") + claude_line("second"))
        offline = self.collector("http://127.0.0.1:1")
        scan = offline.scan()
        self.assertEqual(scan["records_queued"], 2)
        self.assertEqual(offline.doctor()["committed_files"], 0)
        self.assertEqual(offline.flush()["acked"], 0)
        self.assertEqual(
            offline.doctor(include_dead_letters=False)["last_error_code"],
            "brain_unavailable",
        )
        offline.close()

        resumed = self.collector()
        self.assertEqual(resumed.flush()["acked"], 2)
        doctor = resumed.doctor()
        self.assertEqual(doctor["pending"], 0)
        self.assertEqual(doctor["committed_files"], 1)
        self.assertIsNone(doctor["last_error_code"])
        self.assertGreater(doctor["last_success_epoch"], 0)
        resumed.close()

    def test_disconnect_after_commit_replays_without_duplicate(self) -> None:
        (self.root / "session.jsonl").write_text(claude_line("durable once"))
        collector = self.collector()
        collector.scan()
        AckServer.drop_after_first_commit = True
        result = collector.flush()
        self.assertEqual(result["acked"], 1)
        self.assertEqual(result["replayed_batches"], 1)
        self.assertEqual(collector.doctor()["pending"], 0)
        self.assertEqual(AckServer.requests, 2)
        self.assertEqual(len(AckServer.batches), 1)
        receipt = next(iter(AckServer.batches.values()))["receipts"][0]
        located = collector.locate_receipt(receipt)
        self.assertEqual(Path(located["path"]).name, "session.jsonl")
        self.assertGreater(located["end_offset"], located["start_offset"])
        collector.close()

    def test_successful_ack_clears_resolved_recovery_dead_letter(self) -> None:
        (self.root / "session.jsonl").write_text(claude_line("recovered"))
        collector = self.collector()
        collector.scan()
        row = collector.db.execute(
            "SELECT path,start_offset FROM outbox WHERE state='pending'"
        ).fetchone()
        collector.db.execute(
            "INSERT INTO dead_letters(path,byte_offset,error_code,error_summary,created_at) "
            "VALUES (?,?,?,?,?)",
            (
                row["path"],
                row["start_offset"],
                "RecoveryError",
                "record recovery rejected",
                time.time(),
            ),
        )
        collector.db.commit()

        self.assertEqual(collector.doctor()["dead_letter_count"], 1)
        self.assertEqual(collector.flush()["acked"], 1)
        self.assertEqual(collector.doctor()["dead_letter_count"], 0)
        collector.close()

    def test_startup_clears_only_legacy_recovery_markers_for_acked_rows(self) -> None:
        (self.root / "session.jsonl").write_text(claude_line("already acknowledged"))
        collector = self.collector()
        collector.scan()
        collector.flush()
        row = collector.db.execute(
            "SELECT path,start_offset FROM outbox WHERE state='acked'"
        ).fetchone()
        collector.db.executemany(
            "INSERT INTO dead_letters(path,byte_offset,error_code,error_summary,created_at) "
            "VALUES (?,?,?,?,?)",
            [
                (
                    row["path"],
                    row["start_offset"],
                    "RecoveryError",
                    "record recovery rejected",
                    time.time(),
                ),
                (
                    row["path"],
                    row["start_offset"] + 1,
                    "JSONDecodeError",
                    "record rejected",
                    time.time(),
                ),
            ],
        )
        collector.db.commit()
        collector.close()

        migrated = self.collector()
        codes = [item["error_code"] for item in migrated.doctor()["dead_letters"]]
        self.assertEqual(codes, ["JSONDecodeError"])
        migrated.close()

    def test_startup_backfills_legacy_last_success_from_acked_outbox(self) -> None:
        (self.root / "session.jsonl").write_text(claude_line("legacy acknowledgement"))
        collector = self.collector()
        collector.scan()
        collector.flush()
        acked_at = collector.db.execute(
            "SELECT acked_at FROM outbox WHERE state='acked'"
        ).fetchone()[0]
        collector.db.execute("DELETE FROM meta WHERE key='last_success_epoch'")
        collector.db.commit()
        collector.close()

        migrated = self.collector()
        self.assertEqual(
            migrated.doctor(include_dead_letters=False)["last_success_epoch"],
            int(acked_at),
        )
        migrated.close()

    def test_process_death_after_remote_commit_before_local_ack_replays_exactly_once(self) -> None:
        (self.root / "session.jsonl").write_text(claude_line("committed before local ack"))
        collector = self.collector()
        collector.scan()

        def die_after_remote_commit(_acknowledgement) -> None:
            raise RuntimeError("simulated death after remote commit before local ack")

        collector._after_remote_commit = die_after_remote_commit
        with self.assertRaisesRegex(RuntimeError, "after remote commit before local ack"):
            collector.flush()
        self.assertEqual(collector.doctor()["pending"], 1)
        self.assertEqual(len(AckServer.batches), 1)
        collector.close()

        resumed = self.collector()
        result = resumed.flush()
        self.assertEqual(result["acked"], 1)
        self.assertEqual(result["replayed_batches"], 1)
        self.assertEqual(resumed.doctor()["pending"], 0)
        self.assertEqual(resumed.doctor()["acked"], 1)
        self.assertEqual(AckServer.requests, 2)
        self.assertEqual(len(AckServer.batches), 1)
        resumed.close()

    def test_append_queues_only_new_complete_records(self) -> None:
        transcript = self.root / "session.jsonl"
        transcript.write_text(claude_line("first"))
        collector = self.collector()
        collector.scan(); collector.flush()
        with transcript.open("a") as output:
            output.write(claude_line("second"))
            output.write('{"type":"user"')
        scan = collector.scan()
        self.assertEqual(scan["records_queued"], 1)
        self.assertEqual(scan["partial_files"], 1)
        self.assertEqual(collector.flush()["acked"], 1)
        collector.close()

    def test_truncation_queues_tombstone_for_removed_record(self) -> None:
        transcript = self.root / "session.jsonl"
        transcript.write_text(claude_line("first") + claude_line("removed"))
        collector = self.collector(); collector.scan(); collector.flush()
        transcript.write_text(claude_line("first"))
        scan = collector.scan()
        self.assertEqual(scan["tombstones_queued"], 1)
        collector.flush()
        self.assertEqual(collector.doctor()["pending"], 0)
        transcript.write_text(claude_line("first") + claude_line("removed"))
        resurrect = collector.scan()
        self.assertEqual(resurrect["records_queued"], 1)
        collector.flush()
        transcript.write_text(claude_line("first"))
        self.assertEqual(collector.scan()["tombstones_queued"], 1)
        collector.close()

    def test_scope_and_master_key_are_sanitized_and_unrelated_files_ignored(self) -> None:
        (self.root / "session.jsonl").write_text(
            json.dumps({"type": "user", "timestamp": "2026-07-12T22:00:00Z", "message": {"content": "ok\x00after"}, "LITELLM_MASTER_KEY": "forbidden-value"}) + "\n"
        )
        (self.root / "notes.txt").write_text("must not ingest")
        collector = self.collector(); collector.scan()
        payloads = collector.pending_envelopes()
        self.assertEqual(len(payloads), 1)
        rendered = json.dumps(payloads)
        self.assertNotIn("forbidden-value", rendered)
        self.assertIn("[REDACTED]", rendered)
        self.assertNotIn("\x00", payloads[0]["content"]["message"]["content"])
        self.assertIn("[NUL]", payloads[0]["content"]["message"]["content"])
        self.assertNotIn("notes.txt", rendered)
        self.assertEqual(payloads[0]["source_id"], "claude:linux:test")
        collector.close()

    def test_private_key_block_and_generic_key_never_enter_spool(self) -> None:
        private_value = "Z" * 50
        private_block = (
            "-----BEGIN " + "PRIVATE KEY-----\n" + ("Q" * 256)
            + "\n-----END " + "PRIVATE KEY-----"
        )
        (self.root / "session.jsonl").write_text(json.dumps({
            "type": "user", "timestamp": "2026-07-12T22:00:00Z",
            "message": {"content": "key=" + private_value + "\n" + private_block + "\nsafe"},
        }) + "\n")
        collector = self.collector()
        collector.scan()
        rendered = json.dumps(collector.pending_envelopes())
        self.assertNotIn(private_value, rendered)
        self.assertNotIn("Q" * 64, rendered)
        self.assertIn("safe", rendered)
        collector.close()

    def test_benign_token_count_key_is_not_treated_as_a_credential(self) -> None:
        (self.root / "session.jsonl").write_text(json.dumps({
            "type": "event_msg", "timestamp": "2026-07-12T22:00:00Z",
            "payload": {"type": "token_count", "info": {"total": 42}},
        }) + "\n")
        collector = Collector(
            root=self.root, harness="claude", source_id="claude:linux:test",
            spool_path=self.spool, endpoint=self.endpoint, token="test-token-not-a-secret",
            privacy=PrivacyPolicy(mode="scrub"),
        )
        collector.scan()
        rendered = json.dumps(collector.pending_envelopes())
        self.assertIn("token_count", rendered)
        self.assertIn('"total": 42', rendered)
        collector.close()

    def test_drop_never_writes_sensitive_record_to_spool_or_network(self) -> None:
        canary = "synthetic-secret-drop-canary-91"
        (self.root / "session.jsonl").write_text(
            claude_line(f"api_key={canary}") + claude_line("safe neighbor survives")
        )
        collector = Collector(
            root=self.root, harness="claude", source_id="claude:linux:test",
            spool_path=self.spool, endpoint=self.endpoint, token="test-token-not-a-secret",
            privacy=PrivacyPolicy(mode="drop"),
        )
        scan = collector.scan()
        self.assertEqual(scan["records_queued"], 1)
        self.assertEqual(scan["privacy"]["actions"], {"drop": 1, "keep": 1})
        self.assertEqual(collector.db.execute("SELECT count(*) FROM outbox").fetchone()[0], 1)
        self.assertNotIn(canary.encode(), self.spool.read_bytes())
        self.assertEqual(collector.flush()["acked"], 1)
        self.assertEqual(AckServer.requests, 1)
        self.assertNotIn(canary, json.dumps(AckServer.received))
        collector.close()

    def test_scrubbed_canary_is_absent_from_spool_and_request(self) -> None:
        canary = "synthetic-secret-scrub-canary-92"
        (self.root / "session.jsonl").write_text(claude_line(f"keep context api_key={canary} after"))
        collector = Collector(
            root=self.root, harness="claude", source_id="claude:linux:test",
            spool_path=self.spool, endpoint=self.endpoint, token="test-token-not-a-secret",
            privacy=PrivacyPolicy(mode="scrub"),
        )
        scan = collector.scan()
        self.assertEqual(scan["privacy"]["actions"], {"scrub": 1})
        self.assertNotIn(canary.encode(), self.spool.read_bytes())
        self.assertIn("keep context", json.dumps(collector.pending_envelopes()))
        collector.flush()
        self.assertNotIn(canary, json.dumps(AckServer.received))
        collector.close()

    def test_canonical_runtime_scrubs_before_archive_spool_and_ack(self) -> None:
        canary = "synthetic-canonical-collector-canary-96"
        (self.root / "session.jsonl").write_text(
            claude_line(f"keep context api_key={canary} after")
        )
        archived = []
        ingested = []

        class Archive:
            def put_raw(self, **kwargs):
                archived.append(kwargs)
                return {
                    "contract": "recall.artifact-ref.v1",
                    "schema_version": 1,
                    "tenant_id": kwargs["tenant_id"],
                    "source_id": kwargs["source_id"],
                    "artifact_id": "art_" + "a" * 32,
                    "storage_backend": "s3",
                    "object_key": "objects/aa/" + "a" * 64,
                    "content_sha256": hashlib.sha256(kwargs["payload"]).hexdigest(),
                    "size_bytes": len(kwargs["payload"]),
                    "media_type": kwargs["media_type"],
                    "encryption": "sse-s3",
                    "version_id": "synthetic-version",
                    "created_at": kwargs["created_at"],
                }

        class Writer:
            def ingest(self, events):
                ingested.extend(events)
                return {
                    "status": "committed",
                    "inserted": len(events),
                    "duplicate_events": 0,
                    "receipts": [
                        f"recall://{event['source_id']}/{event['native_id']}?rev=1"
                        for event in events
                    ],
                    "replay": False,
                }

        collector = Collector(
            root=self.root,
            harness="claude",
            source_id="claude:linux:test",
            spool_path=self.spool,
            endpoint=self.endpoint,
            token="test-token-not-a-secret",
            principal_id="owner",
            privacy=PrivacyPolicy(mode="scrub"),
            brain_writer=Writer(),
            archive=Archive(),
            tenant_id="tenant:personal",
        )
        self.assertEqual(collector.scan()["records_queued"], 1)
        self.assertEqual(collector.flush()["acked"], 1)
        self.assertEqual(len(archived), 1)
        self.assertNotIn(canary.encode(), archived[0]["payload"])
        self.assertIn(b"keep context", archived[0]["payload"])
        self.assertEqual(len(ingested), 1)
        self.assertIn("artifact_ref", ingested[0]["provenance"])
        self.assertNotIn(canary, json.dumps(ingested))
        self.assertNotIn(canary.encode(), self.spool.read_bytes())
        collector.close()

    def test_bulk_manifest_archive_is_content_free_and_amortized(self) -> None:
        (self.root / "session.jsonl").write_text(
            "".join(claude_line(f"private-record-{index}") for index in range(120))
        )
        archived = []
        ingested = []

        class Archive:
            def put_raw(self, **kwargs):
                archived.append(kwargs)
                digest = hashlib.sha256(kwargs["payload"]).hexdigest()
                return {
                    "contract": "recall.artifact-ref.v1",
                    "schema_version": 1,
                    "tenant_id": kwargs["tenant_id"],
                    "source_id": kwargs["source_id"],
                    "artifact_id": "art_" + digest[:32],
                    "storage_backend": "s3",
                    "object_key": "objects/aa/" + digest,
                    "content_sha256": digest,
                    "size_bytes": len(kwargs["payload"]),
                    "media_type": kwargs["media_type"],
                    "encryption": "sse-s3",
                    "version_id": "synthetic-version",
                    "created_at": kwargs["created_at"],
                }

        class Writer:
            def ingest(self, events):
                ingested.extend(events)
                return {
                    "status": "committed",
                    "inserted": len(events),
                    "duplicate_events": 0,
                    "receipts": [
                        f"recall://{event['source_id']}/{event['native_id']}?rev=1"
                        for event in events
                    ],
                    "replay": False,
                }

        collector = Collector(
            root=self.root,
            harness="claude",
            source_id="claude:linux:test",
            spool_path=self.spool,
            endpoint=self.endpoint,
            token="test-token-not-a-secret",
            principal_id="owner",
            privacy=PrivacyPolicy(mode="scrub"),
            brain_writer=Writer(),
            archive=Archive(),
            tenant_id="tenant:personal",
            bulk_manifest_archive=True,
            bulk_bundle_records=50,
        )
        self.assertEqual(collector.scan()["records_queued"], 120)
        self.assertEqual(collector.flush()["acked"], 120)
        self.assertEqual(len(archived), 3)
        self.assertEqual(len(ingested), 120)
        for item in archived:
            self.assertEqual(
                item["media_type"],
                "application/vnd.recall.bulk-manifest+json",
            )
            self.assertNotIn(b"private-record", item["payload"])
        artifact_ids = {
            event["provenance"]["artifact_ref"]["artifact_id"]
            for event in ingested
        }
        self.assertEqual(len(artifact_ids), 3)
        self.assertTrue(all(
            event["provenance"]["artifact_member"]["content_sha256"]
            for event in ingested
        ))
        collector.close()

    def test_canonical_writer_byte_budget_splits_base64_wrapped_batches(self) -> None:
        (self.root / "session.jsonl").write_text(
            "".join(claude_line("x" * 1_000) for _ in range(6))
        )
        ingested_batches = []

        class Archive:
            def put_raw(self, **kwargs):
                digest = hashlib.sha256(kwargs["payload"]).hexdigest()
                return {
                    "contract": "recall.artifact-ref.v1",
                    "schema_version": 1,
                    "tenant_id": kwargs["tenant_id"],
                    "source_id": kwargs["source_id"],
                    "artifact_id": "art_" + digest[:32],
                    "storage_backend": "s3",
                    "object_key": "objects/aa/" + digest,
                    "content_sha256": digest,
                    "size_bytes": len(kwargs["payload"]),
                    "media_type": kwargs["media_type"],
                    "encryption": "sse-s3",
                    "version_id": "synthetic-version",
                    "created_at": kwargs["created_at"],
                }

        class Writer:
            def max_events_payload_bytes(self):
                return 4_000

            def ingest(self, events):
                rendered = json.dumps(
                    events,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
                if len(rendered) > self.max_events_payload_bytes():
                    raise ValueError("canonical ingest batch exceeds client size limit")
                ingested_batches.append(events)
                return {
                    "status": "committed",
                    "inserted": len(events),
                    "duplicate_events": 0,
                    "receipts": [
                        f"recall://{event['source_id']}/{event['native_id']}?rev=1"
                        for event in events
                    ],
                    "replay": False,
                }

        collector = Collector(
            root=self.root,
            harness="claude",
            source_id="claude:linux:test",
            spool_path=self.spool,
            endpoint=self.endpoint,
            token="test-token-not-a-secret",
            principal_id="owner",
            privacy=PrivacyPolicy(mode="scrub"),
            brain_writer=Writer(),
            archive=Archive(),
            tenant_id="tenant:personal",
            bulk_manifest_archive=True,
        )
        self.assertEqual(collector.scan()["records_queued"], 6)
        result = collector.flush()
        self.assertEqual(result["acked"], 6)
        self.assertGreater(len(ingested_batches), 1)
        self.assertTrue(all(
            len(json.dumps(batch, sort_keys=True, separators=(",", ":")).encode())
            <= 4_000
            for batch in ingested_batches
        ))
        self.assertIsNone(
            collector.doctor(include_dead_letters=False)["last_error_code"]
        )
        collector.close()

    def test_local_canonical_rejection_is_not_reported_as_unavailable(self) -> None:
        (self.root / "session.jsonl").write_text(claude_line("safe"))

        class Archive:
            def put_raw(self, **kwargs):
                digest = hashlib.sha256(kwargs["payload"]).hexdigest()
                return {
                    "contract": "recall.artifact-ref.v1",
                    "schema_version": 1,
                    "tenant_id": kwargs["tenant_id"],
                    "source_id": kwargs["source_id"],
                    "artifact_id": "art_" + digest[:32],
                    "storage_backend": "s3",
                    "object_key": "objects/aa/" + digest,
                    "content_sha256": digest,
                    "size_bytes": len(kwargs["payload"]),
                    "media_type": kwargs["media_type"],
                    "encryption": "sse-s3",
                    "version_id": "synthetic-version",
                    "created_at": kwargs["created_at"],
                }

        class Writer:
            def ingest(self, _events):
                raise ValueError("synthetic local rejection")

        collector = Collector(
            root=self.root,
            harness="claude",
            source_id="claude:linux:test",
            spool_path=self.spool,
            endpoint=self.endpoint,
            token="test-token-not-a-secret",
            brain_writer=Writer(),
            archive=Archive(),
            tenant_id="tenant:personal",
            privacy=PrivacyPolicy(mode="scrub"),
        )
        collector.scan()
        self.assertEqual(collector.flush()["errors"], 1)
        self.assertEqual(
            collector.doctor(include_dead_letters=False)["last_error_code"],
            "brain_rejected",
        )
        collector.close()

    def test_bulk_manifest_mode_recovers_oversized_dead_payload(self) -> None:
        transcript = self.root / "session.jsonl"
        canary = "synthetic-secret-value-1234567890"
        transcript.write_text(
            claude_line(
                "keep-start api_key=" + canary + " "
                + ("x" * 10_000)
                + " keep-end"
            )
        )
        archived = []
        ingested = []

        class Archive:
            def put_raw(self, **kwargs):
                archived.append(kwargs)
                digest = hashlib.sha256(kwargs["payload"]).hexdigest()
                return {
                    "contract": "recall.artifact-ref.v1",
                    "schema_version": 1,
                    "tenant_id": kwargs["tenant_id"],
                    "source_id": kwargs["source_id"],
                    "artifact_id": "art_" + digest[:32],
                    "storage_backend": "s3",
                    "object_key": "objects/aa/" + digest,
                    "content_sha256": digest,
                    "size_bytes": len(kwargs["payload"]),
                    "media_type": kwargs["media_type"],
                    "encryption": "sse-s3",
                    "version_id": "synthetic-version",
                    "created_at": kwargs["created_at"],
                }

        class Writer:
            def ingest(self, events):
                ingested.extend(events)
                return {
                    "status": "committed",
                    "inserted": len(events),
                    "duplicate_events": 0,
                    "receipts": [
                        f"recall://{event['source_id']}/{event['native_id']}?rev=1"
                        for event in events
                    ],
                    "replay": False,
                }

        collector = Collector(
            root=self.root,
            harness="claude",
            source_id="claude:linux:test",
            spool_path=self.spool,
            endpoint=self.endpoint,
            token="test-token-not-a-secret",
            archive=Archive(),
            brain_writer=Writer(),
            tenant_id="tenant:personal",
            bulk_manifest_archive=True,
            privacy=PrivacyPolicy(mode="scrub"),
        )
        collector.scan()
        archived_before_recovery = len(archived)
        collector.db.execute(
            "UPDATE outbox SET state='dead',envelope_json='{}'"
        )
        collector.db.commit()

        with (
            mock.patch("collector.collector.MAX_BATCH_BYTES", 4_000),
            mock.patch(
                "collector.collector.OVERSIZED_PROJECTION_TEXT_CHARS",
                100,
            ),
        ):
            result = collector.flush()

        self.assertEqual(result["recovered"], 1)
        self.assertEqual(result["unrecoverable"], 0)
        self.assertEqual(result["errors"], 0)
        self.assertEqual(len(archived), archived_before_recovery + 1)
        full = archived[-1]
        self.assertEqual(
            full["media_type"],
            "application/vnd.recall.oversized-record+gzip",
        )
        uncompressed = gzip.decompress(full["payload"])
        self.assertNotIn(canary.encode(), uncompressed)
        self.assertIn(b"keep-start", uncompressed)
        self.assertIn(b"keep-end", uncompressed)
        self.assertEqual(len(ingested), 1)
        event = ingested[0]
        self.assertEqual(
            event["content"]["contract"],
            "recall.oversized-projection.v1",
        )
        self.assertIn("keep-start", event["content"]["head"])
        self.assertIn("keep-end", event["content"]["tail"])
        self.assertEqual(event["content"]["archive_encoding"], "gzip")
        self.assertEqual(
            event["content"]["full_content_sha256"],
            hashlib.sha256(uncompressed).hexdigest(),
        )
        self.assertEqual(
            event["content"]["archive_size_bytes"],
            len(full["payload"]),
        )
        self.assertEqual(
            event["provenance"]["artifact_ref"]["artifact_id"],
            "art_" + hashlib.sha256(full["payload"]).hexdigest()[:32],
        )
        self.assertEqual(
            collector.doctor(include_dead_letters=False)["dead"],
            0,
        )
        active = collector.db.execute(
            "SELECT content_sha256,receipt FROM active_records"
        ).fetchone()
        self.assertEqual(active["content_sha256"], event["content_sha256"])
        self.assertIsNotNone(active["receipt"])
        collector.close()

    def test_canonical_flush_repairs_and_bounds_legacy_pending_rows(self) -> None:
        (self.root / "session.jsonl").write_text(
            "".join(claude_line(f"legacy pending row {index}") for index in range(51))
        )
        legacy = self.collector()
        self.assertEqual(legacy.scan()["records_queued"], 51)
        self.assertTrue(all(
            "artifact_ref" not in envelope["provenance"]
            for envelope in legacy.pending_envelopes()
        ))
        legacy.close()
        archived = []
        ingested = []
        batch_sizes = []

        class Archive:
            def put_raw(self, **kwargs):
                archived.append(kwargs)
                digest = hashlib.sha256(kwargs["payload"]).hexdigest()
                return {
                    "contract": "recall.artifact-ref.v1",
                    "schema_version": 1,
                    "tenant_id": kwargs["tenant_id"],
                    "source_id": kwargs["source_id"],
                    "artifact_id": "art_" + digest[:32],
                    "storage_backend": "s3",
                    "object_key": "objects/aa/" + digest,
                    "content_sha256": digest,
                    "size_bytes": len(kwargs["payload"]),
                    "media_type": kwargs["media_type"],
                    "encryption": "sse-s3",
                    "version_id": "synthetic-version",
                    "created_at": kwargs["created_at"],
                }

        class Writer:
            def ingest(self, events):
                batch_sizes.append(len(events))
                ingested.extend(events)
                return {
                    "status": "committed",
                    "inserted": len(events),
                    "duplicate_events": 0,
                    "receipts": [
                        f"recall://{event['source_id']}/{event['native_id']}?rev=1"
                        for event in events
                    ],
                    "replay": False,
                }

        canonical = Collector(
            root=self.root,
            harness="claude",
            source_id="claude:linux:test",
            spool_path=self.spool,
            endpoint=self.endpoint,
            token="test-token-not-a-secret",
            principal_id="owner",
            privacy=PrivacyPolicy(mode="scrub"),
            brain_writer=Writer(),
            archive=Archive(),
            tenant_id="tenant:personal",
        )

        self.assertEqual(
            archived,
            [],
            "privacy-state migration must not archive the entire pending spool",
        )
        self.assertEqual(canonical.flush()["acked"], 51)
        self.assertEqual(len(archived), 51)
        self.assertEqual(len(ingested), 51)
        self.assertEqual(batch_sizes, [51])
        self.assertTrue(all(
            "artifact_ref" in event["provenance"]
            for event in ingested
        ))
        canonical.close()

    def test_canonical_scan_archives_concurrently_but_spools_in_source_order(self) -> None:
        (self.root / "session.jsonl").write_text(
            "".join(claude_line(f"record-{index}") for index in range(12))
        )
        lock = threading.Lock()
        active = 0
        peak = 0

        class Archive:
            def put_raw(inner, **kwargs):
                nonlocal active, peak
                with lock:
                    active += 1
                    peak = max(peak, active)
                time.sleep(0.02)
                with lock:
                    active -= 1
                digest = hashlib.sha256(kwargs["payload"]).hexdigest()
                return {
                    "contract": "recall.artifact-ref.v1",
                    "schema_version": 1,
                    "tenant_id": kwargs["tenant_id"],
                    "source_id": kwargs["source_id"],
                    "artifact_id": "art_" + digest[:32],
                    "storage_backend": "s3",
                    "object_key": "objects/aa/" + digest,
                    "content_sha256": digest,
                    "size_bytes": len(kwargs["payload"]),
                    "media_type": kwargs["media_type"],
                    "encryption": "sse-s3",
                    "version_id": "synthetic-version",
                    "created_at": kwargs["created_at"],
                }

        class Writer:
            def ingest(self, events):
                return {
                    "status": "committed",
                    "inserted": len(events),
                    "duplicate_events": 0,
                    "receipts": [
                        f"recall://{event['source_id']}/{event['native_id']}?rev=1"
                        for event in events
                    ],
                    "replay": False,
                }

        collector = Collector(
            root=self.root,
            harness="claude",
            source_id="claude:linux:test",
            spool_path=self.spool,
            endpoint=self.endpoint,
            token="test-token-not-a-secret",
            principal_id="owner",
            privacy=PrivacyPolicy(mode="scrub"),
            brain_writer=Writer(),
            archive=Archive(),
            tenant_id="tenant:personal",
            archive_workers=4,
        )
        self.assertEqual(collector.scan()["records_queued"], 12)
        self.assertGreaterEqual(peak, 2)
        starts = [
            event["provenance"]["byte_start"]
            for event in collector.pending_envelopes()
        ]
        self.assertEqual(starts, sorted(starts))
        collector.close()

    def test_canonical_scan_stops_at_observed_size_when_source_is_appended(self) -> None:
        source = self.root / "session.jsonl"
        source.write_text(claude_line("first"))
        archived = []

        class Archive:
            def put_raw(inner, **kwargs):
                archived.append(kwargs["payload"])
                if len(archived) == 1:
                    with source.open("a") as output:
                        output.write(claude_line("appended-during-scan"))
                digest = hashlib.sha256(kwargs["payload"]).hexdigest()
                return {
                    "contract": "recall.artifact-ref.v1",
                    "schema_version": 1,
                    "tenant_id": kwargs["tenant_id"],
                    "source_id": kwargs["source_id"],
                    "artifact_id": "art_" + digest[:32],
                    "storage_backend": "s3",
                    "object_key": "objects/aa/" + digest,
                    "content_sha256": digest,
                    "size_bytes": len(kwargs["payload"]),
                    "media_type": kwargs["media_type"],
                    "encryption": "sse-s3",
                    "version_id": "synthetic-version",
                    "created_at": kwargs["created_at"],
                }

        class Writer:
            def ingest(self, events):
                return {
                    "status": "committed",
                    "inserted": len(events),
                    "duplicate_events": 0,
                    "receipts": [
                        f"recall://{event['source_id']}/{event['native_id']}?rev=1"
                        for event in events
                    ],
                    "replay": False,
                }

        collector = Collector(
            root=self.root,
            harness="claude",
            source_id="claude:linux:test",
            spool_path=self.spool,
            endpoint=self.endpoint,
            token="test-token-not-a-secret",
            principal_id="owner",
            privacy=PrivacyPolicy(mode="scrub"),
            brain_writer=Writer(),
            archive=Archive(),
            tenant_id="tenant:personal",
            archive_workers=1,
        )
        first = collector.scan()
        self.assertEqual(first["records_queued"], 1)
        self.assertEqual(len(archived), 1)
        second = collector.scan()
        self.assertEqual(second["records_queued"], 1)
        self.assertEqual(len(archived), 2)
        collector.close()

    def test_canonical_scan_flushes_each_durable_checkpoint(self) -> None:
        source = self.root / "session.jsonl"
        source.write_text(
            "".join(claude_line(f"record-{index}") for index in range(1001))
        )
        archived = 0
        ingested: list[list[dict]] = []

        class Archive:
            def put_raw(inner, **kwargs):
                nonlocal archived
                archived += 1
                if archived == 1001:
                    self.assertEqual(
                        sum(len(batch) for batch in ingested),
                        1000,
                        "the durable checkpoint must be indexed before scanning resumes",
                    )
                digest = hashlib.sha256(kwargs["payload"]).hexdigest()
                return {
                    "contract": "recall.artifact-ref.v1",
                    "schema_version": 1,
                    "tenant_id": kwargs["tenant_id"],
                    "source_id": kwargs["source_id"],
                    "artifact_id": "art_" + digest[:32],
                    "storage_backend": "s3",
                    "object_key": "objects/aa/" + digest,
                    "content_sha256": digest,
                    "size_bytes": len(kwargs["payload"]),
                    "media_type": kwargs["media_type"],
                    "encryption": "sse-s3",
                    "version_id": "synthetic-version",
                    "created_at": kwargs["created_at"],
                }

        class Writer:
            def ingest(self, events):
                ingested.append(events)
                return {
                    "status": "committed",
                    "inserted": len(events),
                    "duplicate_events": 0,
                    "receipts": [
                        f"recall://{event['source_id']}/{event['native_id']}?rev=1"
                        for event in events
                    ],
                    "replay": False,
                }

        collector = Collector(
            root=self.root,
            harness="claude",
            source_id="claude:linux:test",
            spool_path=self.spool,
            endpoint=self.endpoint,
            token="test-token-not-a-secret",
            principal_id="owner",
            privacy=PrivacyPolicy(mode="scrub"),
            brain_writer=Writer(),
            archive=Archive(),
            tenant_id="tenant:personal",
            archive_workers=1,
        )
        first = collector.scan()
        self.assertEqual(first["records_queued"], 1000)
        self.assertFalse(first["scan_complete"])
        self.assertEqual(collector.doctor()["acked"], 1000)
        self.assertEqual(collector.doctor()["pending"], 0)
        second = collector.scan()
        self.assertEqual(second["records_queued"], 1)
        self.assertTrue(second["scan_complete"])
        self.assertEqual(collector.doctor()["pending"], 1)
        self.assertEqual(collector.flush()["acked"], 1)
        collector.close()

    def test_canonical_scan_flushes_between_small_source_files(self) -> None:
        (self.root / "first.jsonl").write_text(claude_line("first record"))
        (self.root / "second.jsonl").write_text(claude_line("second record"))
        archived = 0
        ingested: list[list[dict]] = []

        class Archive:
            def put_raw(inner, **kwargs):
                nonlocal archived
                archived += 1
                if archived == 2:
                    self.assertEqual(
                        sum(len(batch) for batch in ingested),
                        1,
                        "the first file must be indexed before the second is archived",
                    )
                digest = hashlib.sha256(kwargs["payload"]).hexdigest()
                return {
                    "contract": "recall.artifact-ref.v1",
                    "schema_version": 1,
                    "tenant_id": kwargs["tenant_id"],
                    "source_id": kwargs["source_id"],
                    "artifact_id": "art_" + digest[:32],
                    "storage_backend": "s3",
                    "object_key": "objects/aa/" + digest,
                    "content_sha256": digest,
                    "size_bytes": len(kwargs["payload"]),
                    "media_type": kwargs["media_type"],
                    "encryption": "sse-s3",
                    "version_id": "synthetic-version",
                    "created_at": kwargs["created_at"],
                }

        class Writer:
            def ingest(self, events):
                ingested.append(events)
                return {
                    "status": "committed",
                    "inserted": len(events),
                    "duplicate_events": 0,
                    "receipts": [
                        f"recall://{event['source_id']}/{event['native_id']}?rev=1"
                        for event in events
                    ],
                    "replay": False,
                }

        collector = Collector(
            root=self.root,
            harness="claude",
            source_id="claude:linux:test",
            spool_path=self.spool,
            endpoint=self.endpoint,
            token="test-token-not-a-secret",
            principal_id="owner",
            privacy=PrivacyPolicy(mode="scrub"),
            brain_writer=Writer(),
            archive=Archive(),
            tenant_id="tenant:personal",
            archive_workers=1,
        )
        self.assertEqual(collector.scan()["records_queued"], 2)
        self.assertEqual(collector.doctor()["acked"], 1)
        self.assertEqual(collector.doctor()["pending"], 1)
        self.assertEqual(collector.flush()["acked"], 1)
        collector.close()

    def test_canonical_scan_flushes_durable_backlog_before_new_source_work(self) -> None:
        transcript = self.root / "session.jsonl"
        transcript.write_text(claude_line("durable backlog"))
        legacy = self.collector()
        self.assertEqual(legacy.scan()["records_queued"], 1)
        legacy.close()
        transcript.write_text(
            claude_line("durable backlog") + claude_line("new source work")
        )
        archived = 0
        ingested: list[dict] = []

        class Archive:
            def put_raw(inner, **kwargs):
                nonlocal archived
                archived += 1
                if archived == 2:
                    self.assertEqual(
                        len(ingested),
                        1,
                        "durable backlog must be indexed before new source work",
                    )
                digest = hashlib.sha256(kwargs["payload"]).hexdigest()
                return {
                    "contract": "recall.artifact-ref.v1",
                    "schema_version": 1,
                    "tenant_id": kwargs["tenant_id"],
                    "source_id": kwargs["source_id"],
                    "artifact_id": "art_" + digest[:32],
                    "storage_backend": "s3",
                    "object_key": "objects/aa/" + digest,
                    "content_sha256": digest,
                    "size_bytes": len(kwargs["payload"]),
                    "media_type": kwargs["media_type"],
                    "encryption": "sse-s3",
                    "version_id": "synthetic-version",
                    "created_at": kwargs["created_at"],
                }

        class Writer:
            def ingest(self, events):
                ingested.extend(events)
                return {
                    "status": "committed",
                    "inserted": len(events),
                    "duplicate_events": 0,
                    "receipts": [
                        f"recall://{event['source_id']}/{event['native_id']}?rev=1"
                        for event in events
                    ],
                    "replay": False,
                }

        canonical = Collector(
            root=self.root,
            harness="claude",
            source_id="claude:linux:test",
            spool_path=self.spool,
            endpoint=self.endpoint,
            token="test-token-not-a-secret",
            principal_id="owner",
            privacy=PrivacyPolicy(mode="scrub"),
            brain_writer=Writer(),
            archive=Archive(),
            tenant_id="tenant:personal",
            archive_workers=1,
        )

        self.assertEqual(canonical.scan()["records_queued"], 1)
        self.assertEqual(canonical.doctor()["acked"], 1)
        self.assertEqual(canonical.doctor()["pending"], 1)
        canonical.close()

    def test_enabling_drop_compacts_sensitive_pending_bytes_from_legacy_spool(self) -> None:
        canary = "synthetic-legacy-spool-canary-95"
        (self.root / "session.jsonl").write_text(claude_line("legacy pending row"))
        legacy = self.collector("http://127.0.0.1:1")
        legacy.scan()
        row = legacy.db.execute("SELECT id,envelope_json FROM outbox").fetchone()
        envelope = json.loads(row["envelope_json"])
        envelope["content"] = {"message": {"content": f"api_key={canary}"}}
        legacy.db.execute("UPDATE outbox SET envelope_json=? WHERE id=?", (json.dumps(envelope), row["id"]))
        legacy.db.commit()
        legacy.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        legacy.close()
        self.assertTrue(any(canary.encode() in artifact.read_bytes() for artifact in self.spool.parent.glob(self.spool.name + "*")))

        protected = Collector(
            root=self.root, harness="claude", source_id="claude:linux:test",
            spool_path=self.spool, endpoint=self.endpoint, token="test-token-not-a-secret",
            privacy=PrivacyPolicy(mode="drop"),
        )
        self.assertEqual(protected.db.execute("SELECT count(*) FROM outbox").fetchone()[0], 0)
        self.assertEqual(protected.doctor()["privacy_mode"], "drop")
        protected.close()
        for artifact in self.spool.parent.glob(self.spool.name + "*"):
            self.assertNotIn(canary.encode(), artifact.read_bytes())

    def test_all_dropped_file_advances_local_committed_cursor_without_network(self) -> None:
        (self.root / "session.jsonl").write_text(claude_line("api_key=synthetic-all-drop-canary-96"))
        collector = Collector(
            root=self.root, harness="claude", source_id="claude:linux:test",
            spool_path=self.spool, endpoint=self.endpoint, token="test-token-not-a-secret",
            privacy=PrivacyPolicy(mode="drop"),
        )
        collector.scan()
        self.assertEqual(collector.doctor()["committed_files"], 1)
        self.assertEqual(AckServer.requests, 0)
        collector.close()

    def test_selected_visibility_reaches_every_envelope(self) -> None:
        (self.root / "session.jsonl").write_text(claude_line("shared evidence"))
        collector = Collector(
            root=self.root,
            harness="claude",
            source_id="claude:mac:shared-test",
            spool_path=self.spool,
            endpoint=self.endpoint,
            token="test-token-not-a-secret",
            visibility="shared",
        )
        collector.scan()
        self.assertEqual({event["visibility"] for event in collector.pending_envelopes()}, {"shared"})
        collector.close()

    def test_large_tree_is_bounded_resumable_and_unchanged_rerun_is_incremental(self) -> None:
        transcript = self.root / "large.jsonl"
        transcript.write_text(
            "".join(claude_line(f"record-{index}") for index in range(2501))
        )
        collector = Collector(
            root=self.root,
            harness="claude",
            source_id="claude:linux:test",
            spool_path=self.spool,
            endpoint=self.endpoint,
            token="test-token-not-a-secret",
            max_scan_records=250,
            max_scan_seconds=5,
        )

        started = time.monotonic()
        slices = []
        while True:
            result = collector.scan()
            slices.append(result)
            self.assertLessEqual(result["records_queued"], 250)
            if result["scan_complete"]:
                break
        elapsed = time.monotonic() - started

        self.assertEqual(sum(item["records_queued"] for item in slices), 2501)
        self.assertEqual(len(slices), 11)
        self.assertLess(elapsed, 5.0)
        rerun = collector.scan()
        self.assertTrue(rerun["scan_complete"])
        self.assertEqual(rerun["records_queued"], 0)
        self.assertEqual(collector.doctor(include_dead_letters=False)["pending"], 2501)
        collector.close()

    def test_archive_failure_is_content_free_recoverable_and_never_advances_scan(self) -> None:
        (self.root / "session.jsonl").write_text(claude_line("synthetic"))

        class Archive:
            fail = True

            def put_raw(inner, **kwargs):
                if inner.fail:
                    raise OSError("private archive detail")
                digest = hashlib.sha256(kwargs["payload"]).hexdigest()
                return {
                    "contract": "recall.artifact-ref.v1",
                    "schema_version": 1,
                    "tenant_id": kwargs["tenant_id"],
                    "source_id": kwargs["source_id"],
                    "artifact_id": "art_" + digest[:32],
                    "storage_backend": "s3",
                    "object_key": "objects/aa/" + digest,
                    "content_sha256": digest,
                    "size_bytes": len(kwargs["payload"]),
                    "media_type": kwargs["media_type"],
                    "encryption": "sse-s3",
                    "version_id": "synthetic-version",
                    "created_at": kwargs["created_at"],
                }

        class Writer:
            def ingest(self, _events):
                raise AssertionError("scan must archive before ingest")

        archive = Archive()
        collector = Collector(
            root=self.root,
            harness="claude",
            source_id="claude:linux:test",
            spool_path=self.spool,
            endpoint=self.endpoint,
            token="test-token-not-a-secret",
            brain_writer=Writer(),
            archive=archive,
            tenant_id="tenant:personal",
            archive_workers=1,
        )

        with self.assertRaisesRegex(CollectorRuntimeError, "archive_unavailable"):
            collector.scan()
        failed = collector.doctor(include_dead_letters=False)
        self.assertEqual(failed["last_error_code"], "archive_unavailable")
        self.assertEqual(failed["pending"], 0)
        self.assertFalse(failed["running"])

        archive.fail = False
        recovered = collector.scan()
        self.assertEqual(recovered["records_queued"], 1)
        self.assertIsNone(collector.doctor(include_dead_letters=False)["last_error_code"])
        collector.close()

    def test_giant_file_resumes_from_durable_scan_checkpoint(self) -> None:
        transcript = self.root / "giant.jsonl"
        transcript.write_text("".join(claude_line(f"record-{index}") for index in range(1001)))
        collector = self.collector()
        save = collector._save_file_progress

        def crash_after_checkpoint(*args, **kwargs):
            save(*args, **kwargs)
            collector.db.commit()
            raise RuntimeError("simulated process death")

        collector._save_file_progress = crash_after_checkpoint
        with self.assertRaisesRegex(RuntimeError, "simulated process death"):
            collector.scan()
        collector.close()

        resumed = self.collector()
        scan = resumed.scan()
        self.assertEqual(scan["records_queued"], 1)
        self.assertEqual(len(resumed.pending_envelopes()), 1001)
        resumed.close()

    def test_flush_repairs_nul_in_an_already_durable_spool_row(self) -> None:
        (self.root / "session.jsonl").write_text(claude_line("before"))
        collector = self.collector(); collector.scan()
        row = collector.db.execute("SELECT id,envelope_json FROM outbox").fetchone()
        envelope = json.loads(row["envelope_json"])
        envelope["content"]["message"]["content"] = "before\x00after"
        envelope["content_sha256"] = hashlib.sha256(
            json.dumps(envelope["content"], sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        collector.db.execute(
            "UPDATE outbox SET envelope_json=?,content_sha256=? WHERE id=?",
            (json.dumps(envelope, sort_keys=True, separators=(",", ":")), envelope["content_sha256"], row["id"]),
        )
        collector.db.commit()
        self.assertEqual(collector.flush()["acked"], 1)
        received = AckServer.received[-1]["events"][0]["content"]["message"]["content"]
        self.assertEqual(received, "before[NUL]after")
        collector.close()

    def test_flush_collapses_repaired_duplicate_of_acknowledged_identity(self) -> None:
        transcript = self.root / "session.jsonl"
        transcript.write_text(claude_line("before[NUL]after"))
        collector = self.collector(); collector.scan(); collector.flush()
        acked = collector.db.execute(
            "SELECT id,path,native_id,content_sha256,start_offset,end_offset,receipt FROM outbox"
        ).fetchone()
        clean = AckServer.received[-1]["events"][0]
        legacy = json.loads(json.dumps(clean))
        legacy["content"]["message"]["content"] = "before\x00after"
        legacy["content_sha256"] = hashlib.sha256(
            json.dumps(legacy["content"], sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        collector.db.execute(
            "INSERT INTO outbox(path,native_id,content_sha256,start_offset,end_offset,shard_key,envelope_json,queued_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (
                acked["path"], acked["native_id"], legacy["content_sha256"], acked["start_offset"],
                acked["end_offset"], 0, json.dumps(legacy, sort_keys=True, separators=(",", ":")), 0,
            ),
        )
        collector.db.execute(
            "UPDATE active_records SET content_sha256=?,receipt=NULL WHERE path=? AND native_id=?",
            (legacy["content_sha256"], acked["path"], acked["native_id"]),
        )
        collector.db.execute("UPDATE files SET committed_offset=0 WHERE path=?", (acked["path"],))
        collector.db.commit()
        requests_before = AckServer.requests

        result = collector.flush()

        self.assertEqual(result["acked"], 0)
        self.assertEqual(AckServer.requests, requests_before)
        self.assertEqual(collector.doctor()["pending"], 0)
        self.assertEqual(collector.doctor()["acked"], 1)
        active = collector.db.execute(
            "SELECT content_sha256,receipt FROM active_records WHERE path=? AND native_id=?",
            (acked["path"], acked["native_id"]),
        ).fetchone()
        self.assertEqual(active["content_sha256"], acked["content_sha256"])
        self.assertEqual(active["receipt"], acked["receipt"])
        self.assertEqual(collector.doctor()["committed_files"], 1)
        collector.close()

    def test_oversized_dead_row_recovers_from_exact_source_window(self) -> None:
        (self.root / "session.jsonl").write_text(claude_line("x" * 1000))
        collector = self.collector(); collector.scan()
        with mock.patch("collector.collector.MAX_BATCH_BYTES", 300):
            first = collector.flush()
        self.assertEqual(first["acked"], 0)
        self.assertEqual(collector.doctor()["dead"], 1)
        with mock.patch("collector.collector.MAX_BATCH_BYTES", 10_000):
            second = collector.flush()
        self.assertEqual(second["recovered"], 1)
        self.assertEqual(second["acked"], 1)
        self.assertEqual(collector.doctor()["dead"], 0)
        self.assertFalse(any(x["error_code"] == "PayloadTooLarge" for x in collector.doctor()["dead_letters"]))
        collector.close()


if __name__ == "__main__":
    unittest.main()
