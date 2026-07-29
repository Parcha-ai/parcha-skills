from __future__ import annotations

import base64
import hashlib
import contextlib
import io
import json
import tempfile
import unittest
import urllib.error
import zipfile
from pathlib import Path
from unittest import mock
from client import cli as client_cli

from client.mac import (
    BrainClient,
    CanonicalArchiveClient,
    CanonicalBrainWriter,
    CanonicalClientError,
    ExportImporter,
    MAX_CANONICAL_INGEST_EVENTS,
    MAX_INGEST_BYTES,
    MemoryClient,
    PrivacyError,
    canonical_json,
    dry_run_manifest,
    load_file_token,
)
from privacy.policy import PrivacyPolicy


class DryRunPrivacyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.home = Path(self.tmp.name)
        self.claude = self.home / ".claude" / "projects"
        self.codex = self.home / ".codex" / "sessions"
        self.claude.mkdir(parents=True)
        self.codex.mkdir(parents=True)

    def test_manifest_is_content_free_exact_and_selection_scoped(self) -> None:
        secret = "C5_TOKEN_CANARY_DO_NOT_RENDER"
        claude_file = self.claude / "project" / "session.jsonl"
        claude_file.parent.mkdir()
        claude_file.write_text(json.dumps({"message": secret}) + "\n")
        (self.claude / "project" / "notes.txt").write_text("not eligible")
        codex_file = self.codex / "2026" / "rollout-one.jsonl"
        codex_file.parent.mkdir()
        codex_file.write_text("{}\n")

        manifest = dry_run_manifest(
            selections=[{"harness": "claude", "root": str(self.claude)}],
            visibility="private",
            home=self.home,
        )

        self.assertEqual(manifest["mode"], "dry-run")
        self.assertEqual(manifest["network_requests"], 0)
        self.assertEqual(manifest["visibility"], "private")
        self.assertEqual(len(manifest["files"]), 1)
        item = manifest["files"][0]
        self.assertEqual(item["relative_path"], "project/session.jsonl")
        self.assertEqual(item["bytes"], claude_file.stat().st_size)
        self.assertEqual(item["sha256"], hashlib.sha256(claude_file.read_bytes()).hexdigest())
        rendered = json.dumps(manifest, sort_keys=True)
        self.assertNotIn(secret, rendered)
        self.assertNotIn(str(codex_file), rendered)
        self.assertNotIn("notes.txt", rendered)

    def test_symlink_escape_and_undocumented_app_roots_fail_closed(self) -> None:
        outside = self.home / "outside.jsonl"
        outside.write_text("{}\n")
        (self.claude / "escape.jsonl").symlink_to(outside)
        with self.assertRaisesRegex(PrivacyError, "escape"):
            dry_run_manifest(
                selections=[{"harness": "claude", "root": str(self.claude)}],
                visibility="private",
                home=self.home,
            )

        chatgpt = self.home / "Library" / "Application Support" / "ChatGPT"
        chatgpt.mkdir(parents=True)
        with self.assertRaisesRegex(PrivacyError, "unsupported private application path"):
            dry_run_manifest(
                selections=[{"harness": "claude", "root": str(chatgpt)}],
                visibility="private",
                home=self.home,
            )

    def test_visibility_is_explicit_and_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "visibility"):
            dry_run_manifest(selections=[], visibility="public", home=self.home)


class CredentialFileTest(unittest.TestCase):
    def test_token_file_is_private_bounded_and_not_a_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "token.json"
            path.write_text(json.dumps({"token": "synthetic-token"}))
            path.chmod(0o600)
            self.assertEqual(load_file_token(path), "synthetic-token")

            alias = Path(temporary) / "token-link.json"
            alias.symlink_to(path)
            with self.assertRaisesRegex(PermissionError, "non-symlink"):
                load_file_token(alias)

            path.chmod(0o644)
            with self.assertRaisesRegex(PermissionError, "group or other"):
                load_file_token(path)


class SupportedExportTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def test_jsonl_and_zip_members_retain_provenance_and_are_idempotent(self) -> None:
        source = self.root / "supported-export.jsonl"
        source.write_text('{"text":"first"}\n{"text":"second"}\n')
        archive = self.root / "supported-export.zip"
        with zipfile.ZipFile(archive, "w") as output:
            info = zipfile.ZipInfo("conversations/chat.json")
            info.date_time = (2020, 1, 1, 0, 0, 0)
            output.writestr(info, json.dumps([{"text": "third"}]))

        importer = ExportImporter(source_id="export:mac:test", principal_id="owner", visibility="private")
        first = importer.inventory([source, archive])
        second = importer.inventory([source, archive])

        self.assertEqual(first, second)
        self.assertEqual(len(first["records"]), 3)
        members = {record["provenance"]["member"] for record in first["records"]}
        self.assertIn("supported-export.jsonl#record=1", members)
        self.assertIn("conversations/chat.json#record=0", members)
        for record in first["records"]:
            provenance = record["provenance"]
            self.assertEqual(
                provenance["original_path"],
                f"{provenance['uri']}/{provenance['member']}",
            )
            self.assertNotIn(str(self.root), provenance["original_path"])
        self.assertTrue(all(record["kind"] == "chat_export" for record in first["records"]))

    def test_archive_traversal_and_symlink_members_are_rejected(self) -> None:
        traversal = self.root / "traversal.zip"
        with zipfile.ZipFile(traversal, "w") as output:
            output.writestr("../escape.json", "{}")
        importer = ExportImporter(source_id="export:mac:test", principal_id="owner", visibility="private")
        with self.assertRaisesRegex(PrivacyError, "unsafe archive member"):
            importer.inventory([traversal])

        symlink = self.root / "symlink.zip"
        with zipfile.ZipFile(symlink, "w") as output:
            info = zipfile.ZipInfo("linked.json")
            info.external_attr = (0o120777 << 16)
            output.writestr(info, "target")
        with self.assertRaisesRegex(PrivacyError, "symlink"):
            importer.inventory([symlink])

        source = self.root / "source.json"
        source.write_text("{}")
        alias = self.root / "alias.json"
        alias.symlink_to(source)
        with self.assertRaisesRegex(PrivacyError, "must not be a symlink"):
            importer.inventory([alias])

    def test_export_file_member_count_and_expansion_limits_fail_closed(self) -> None:
        importer = ExportImporter(
            source_id="export:mac:test", principal_id="owner", visibility="private",
        )
        oversized = self.root / "oversized.jsonl"
        oversized.write_bytes(b"{}\n" * 20)
        with mock.patch("client.mac.MAX_EXPORT_BYTES", 32):
            with self.assertRaisesRegex(PrivacyError, "size limit"):
                importer.inventory([oversized])

        too_many = self.root / "too-many.zip"
        with zipfile.ZipFile(too_many, "w") as output:
            output.writestr("one.json", "{}")
            output.writestr("two.json", "{}")
        with mock.patch("client.mac.MAX_ARCHIVE_MEMBERS", 1):
            with self.assertRaisesRegex(PrivacyError, "too many members"):
                importer.inventory([too_many])

        expansion = self.root / "expansion.zip"
        with zipfile.ZipFile(expansion, "w", compression=zipfile.ZIP_DEFLATED) as output:
            output.writestr("large.json", json.dumps({"text": "x" * 4096}))
        self.assertLess(expansion.stat().st_size, 512)
        with mock.patch("client.mac.MAX_EXPORT_BYTES", 512):
            with self.assertRaisesRegex(PrivacyError, "size limit|expansion limit"):
                importer.inventory([expansion])


class FakeResponse:
    def __init__(self, status: int, body: dict):
        self.status = status
        self.body = json.dumps(body).encode()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self) -> bytes:
        return self.body


class BrainEndpointValidationTest(unittest.TestCase):
    def test_loopback_exception_is_parsed_instead_of_prefix_matched(self) -> None:
        for endpoint in (
            "http://127.0.0.1:8788",
            "http://localhost:8788/base",
            "https://brain.example.invalid",
        ):
            with self.subTest(endpoint=endpoint):
                client = BrainClient(endpoint=endpoint, token="synthetic", source_id="test-source")
                self.assertEqual(client.endpoint, endpoint)

        for endpoint in (
            "http://127.0.0.1:80@attacker.invalid",
            "http://localhost:80@attacker.invalid",
            "http://127.0.0.1",
            "http://127.0.0.1:0",
            "https://owner@brain.example.invalid",
            "https://brain.example.invalid?redirect=https://attacker.invalid",
        ):
            with self.subTest(endpoint=endpoint), self.assertRaises(ValueError):
                BrainClient(endpoint=endpoint, token="synthetic", source_id="test-source")


class CanonicalV2ClientTest(unittest.TestCase):
    def test_writer_sends_exactly_one_thousand_events(self) -> None:
        writer = CanonicalBrainWriter(
            endpoint="https://brain.example.invalid",
            token="synthetic",
            source_id="source:personal",
            tenant_id="tenant:personal",
            principal_id="principal:owner",
        )
        events = [
            {
                "source_id": "source:personal",
                "native_id": f"native:{index}",
                "principal_id": "principal:owner",
                "provenance": {"artifact_ref": {"artifact_id": "synthetic"}},
            }
            for index in range(MAX_CANONICAL_INGEST_EVENTS)
        ]
        requests = []

        def open_request(request, **_kwargs):
            requests.append(request)
            return FakeResponse(201, {
                "status": "committed",
                "inserted": len(events),
                "duplicate_events": 0,
                "receipts": [
                    f"recall://source:personal/native:{index}?rev=1"
                    for index in range(len(events))
                ],
                "replay": False,
            })

        with mock.patch(
            "client.mac.open_no_redirect",
            side_effect=open_request,
        ):
            acknowledgement = writer.ingest(events)

        self.assertEqual(MAX_CANONICAL_INGEST_EVENTS, 1_000)
        self.assertEqual(acknowledgement["inserted"], 1_000)
        body = json.loads(requests[0].data)
        self.assertEqual(
            len(json.loads(base64.b64decode(body["events_base64"]))),
            1_000,
        )
        with self.assertRaisesRegex(ValueError, "batch is invalid"):
            writer.ingest([*events, events[0]])

    def test_archive_then_canonical_writer_use_closed_tenant_scoped_routes(self) -> None:
        requests = []
        raw_payload = b'{"raw":"RAW_ARCHIVE_CANARY"}'
        artifact = {
            "contract": "recall.artifact-ref.v1",
            "schema_version": 1,
            "tenant_id": "tenant:personal",
            "source_id": "source:personal",
            "artifact_id": "art_" + "a" * 32,
            "storage_backend": "s3",
            "object_key": "objects/aa/" + "a" * 64,
            "content_sha256": hashlib.sha256(raw_payload).hexdigest(),
            "size_bytes": len(raw_payload),
            "media_type": "application/json",
            "encryption": "sse-s3",
            "version_id": "r2-sha256-" + hashlib.sha256(raw_payload).hexdigest(),
            "created_at": "2026-07-20T00:00:00Z",
        }

        def open_request(request, **_kwargs):
            requests.append(request)
            if request.full_url.endswith("/v2/archive/objects"):
                return FakeResponse(201, artifact)
            return FakeResponse(201, {
                "status": "committed",
                "inserted": 1,
                "duplicate_events": 0,
                "receipts": ["recall://source:personal/native:one?rev=1"],
                "replay": False,
            })

        common = {
            "endpoint": "https://brain.example.invalid",
            "token": "CANONICAL_TOKEN_CANARY",
            "source_id": "source:personal",
            "tenant_id": "tenant:personal",
            "principal_id": "principal:owner",
        }
        archive = CanonicalArchiveClient(**common)
        writer = CanonicalBrainWriter(**common)
        maximum_events_payload = writer.max_events_payload_bytes()
        empty_body = canonical_json({
            "tenant_id": common["tenant_id"],
            "principal_id": common["principal_id"],
            "source_id": common["source_id"],
            "events_base64": "",
        })
        self.assertLessEqual(
            len(empty_body)
            + len(base64.b64encode(b"x" * maximum_events_payload)),
            MAX_INGEST_BYTES,
        )
        self.assertGreater(
            len(empty_body)
            + len(base64.b64encode(b"x" * (maximum_events_payload + 1))),
            MAX_INGEST_BYTES,
        )
        with mock.patch("client.mac.open_no_redirect", side_effect=open_request):
            reference = archive.put_raw(
                tenant_id="tenant:personal",
                source_id="source:personal",
                native_id="native:one",
                payload=raw_payload,
                media_type="application/json",
                created_at="2026-07-20T00:00:00Z",
            )
            event = {
                "schema_version": 1,
                "source_id": "source:personal",
                "native_id": "native:one",
                "native_parent_id": "native:one",
                "kind": "connector_record",
                "occurred_at": "2026-07-20T00:00:00Z",
                "observed_at": "2026-07-20T00:00:00Z",
                "principal_id": "principal:owner",
                "visibility": "private",
                "content_type": "application/json",
                "content": {"text": "safe"},
                "provenance": {
                    "connector_id": "synthetic.connector",
                    "connector_schema_version": 1,
                    "artifact_ref": reference,
                },
                "content_sha256": hashlib.sha256(b'{"text":"safe"}').hexdigest(),
            }
            acknowledgement = writer.ingest([event])

        self.assertEqual(
            [request.full_url.rsplit("/", 3)[-3:] for request in requests],
            [["v2", "archive", "objects"], ["v2", "ingest", "canonical"]],
        )
        archive_body = json.loads(requests[0].data)
        canonical_body = json.loads(requests[1].data)
        self.assertEqual(archive_body["tenant_id"], "tenant:personal")
        self.assertEqual(canonical_body["principal_id"], "principal:owner")
        self.assertNotIn("events", canonical_body)
        self.assertEqual(
            json.loads(
                base64.b64decode(
                    canonical_body["events_base64"],
                    validate=True,
                )
            ),
            [event],
        )
        self.assertNotIn("RAW_ARCHIVE_CANARY", json.dumps(canonical_body))
        self.assertNotIn(b"CANONICAL_TOKEN_CANARY", requests[0].data)
        self.assertNotIn(b"CANONICAL_TOKEN_CANARY", requests[1].data)
        self.assertEqual(acknowledgement["inserted"], 1)

    def test_canonical_clients_reject_cross_scope_calls_before_network(self) -> None:
        common = {
            "endpoint": "https://brain.example.invalid",
            "token": "synthetic",
            "source_id": "source:personal",
            "tenant_id": "tenant:personal",
            "principal_id": "principal:owner",
        }
        archive = CanonicalArchiveClient(**common)
        with mock.patch("client.mac.open_no_redirect") as opened, self.assertRaises(
            PermissionError
        ):
            archive.put_raw(
                tenant_id="tenant:other",
                source_id="source:personal",
                native_id="native:one",
                payload=b"private",
                media_type="application/json",
                created_at="2026-07-20T00:00:00Z",
            )
        opened.assert_not_called()

    def test_archive_retries_transient_transport_failure_idempotently(self) -> None:
        payload = b'{"raw":"synthetic-retry"}'
        artifact = {
            "contract": "recall.artifact-ref.v1",
            "schema_version": 1,
            "tenant_id": "tenant:personal",
            "source_id": "source:personal",
            "artifact_id": "art_" + "b" * 32,
            "storage_backend": "s3",
            "object_key": "objects/bb/" + "b" * 64,
            "content_sha256": hashlib.sha256(payload).hexdigest(),
            "size_bytes": len(payload),
            "media_type": "application/json",
            "encryption": "sse-s3",
            "version_id": "synthetic-version",
            "created_at": "2026-07-20T00:00:00Z",
        }
        archive = CanonicalArchiveClient(
            endpoint="https://brain.example.invalid",
            token="synthetic",
            source_id="source:personal",
            tenant_id="tenant:personal",
            principal_id="principal:owner",
        )
        attempts = [
            urllib.error.URLError("synthetic-timeout"),
            OSError("synthetic-reset"),
            FakeResponse(201, artifact),
        ]
        with (
            mock.patch(
                "client.mac.open_no_redirect",
                side_effect=attempts,
            ) as opened,
            mock.patch("client.mac.time.sleep") as slept,
        ):
            result = archive.put_raw(
                tenant_id="tenant:personal",
                source_id="source:personal",
                native_id="native:retry",
                payload=payload,
                media_type="application/json",
                created_at="2026-07-20T00:00:00Z",
            )
        self.assertEqual(result, artifact)
        self.assertEqual(opened.call_count, 3)
        self.assertEqual(
            [call.args[0] for call in slept.call_args_list],
            [1, 2],
        )
        bodies = [
            json.loads(call.args[0].data)
            for call in opened.call_args_list
        ]
        self.assertEqual(bodies[0], bodies[1])
        self.assertEqual(bodies[1], bodies[2])

    def test_archive_retry_budget_is_bounded_and_content_free(self) -> None:
        archive = CanonicalArchiveClient(
            endpoint="https://brain.example.invalid",
            token="synthetic",
            source_id="source:personal",
            tenant_id="tenant:personal",
            principal_id="principal:owner",
        )
        with (
            mock.patch(
                "client.mac.open_no_redirect",
                side_effect=OSError("synthetic-private-upstream-detail"),
            ) as opened,
            mock.patch("client.mac.time.sleep") as slept,
            self.assertRaises(CanonicalClientError) as raised,
        ):
            archive.put_raw(
                tenant_id="tenant:personal",
                source_id="source:personal",
                native_id="native:retry-budget",
                payload=b"synthetic",
                media_type="application/json",
                created_at="2026-07-20T00:00:00Z",
            )
        self.assertEqual(raised.exception.error_code, "archive_unavailable")
        self.assertNotIn("upstream", str(raised.exception))
        self.assertEqual(opened.call_count, 5)
        self.assertEqual(
            [call.args[0] for call in slept.call_args_list],
            [1, 2, 4, 8],
        )


class ExplicitMemoryTest(unittest.TestCase):
    def test_put_and_delete_use_canonical_envelopes_without_token_in_argv_or_body(self) -> None:
        requests = []

        def open_request(request, **_kwargs):
            requests.append(request)
            event = json.loads(request.data)["events"][0]
            revision = 2 if event["kind"] == "tombstone" else 1
            return FakeResponse(201, {
                "status": "committed",
                "receipts": [f"recall://{event['source_id']}/{event['native_id']}?rev={revision}"],
            })

        client = MemoryClient(
            endpoint="https://brain.example.ts.net",
            token="C5_TOKEN_CANARY_DO_NOT_RENDER",
            source_id="memory:mac:test",
            principal_id="owner",
            visibility="private",
        )
        with mock.patch("client.mac.open_no_redirect", side_effect=open_request):
            put = client.put("remember this exact marker", provenance={"uri": "manual://test"})
            deleted = client.delete(put["receipt"])

        self.assertEqual(put["kind"], "memory")
        self.assertEqual(deleted["kind"], "tombstone")
        put_event = json.loads(requests[0].data)["events"][0]
        delete_event = json.loads(requests[1].data)["events"][0]
        self.assertEqual(put_event["content"], {"text": "remember this exact marker"})
        self.assertEqual(delete_event["content"]["target_native_id"], put_event["native_id"])
        self.assertEqual(delete_event["native_id"], put_event["native_id"])
        for request in requests:
            self.assertNotIn(b"C5_TOKEN_CANARY_DO_NOT_RENDER", request.data)
            self.assertEqual(request.get_header("Authorization"), "Bearer C5_TOKEN_CANARY_DO_NOT_RENDER")

    def test_memory_drop_makes_no_request_and_scrub_removes_canary(self) -> None:
        canary = "synthetic-memory-secret-canary-93"
        dropped = MemoryClient(
            endpoint="https://brain.example.ts.net", token="synthetic-token",
            source_id="memory:mac:test", privacy=PrivacyPolicy(mode="drop"),
        )
        with mock.patch("client.mac.open_no_redirect") as opened:
            result = dropped.put(f"api_key={canary}")
        opened.assert_not_called()
        self.assertEqual(result["privacy"]["action"], "drop")
        self.assertNotIn(canary, json.dumps(result))

        scrubbed = MemoryClient(
            endpoint="https://brain.example.ts.net", token="synthetic-token",
            source_id="memory:mac:test", privacy=PrivacyPolicy(mode="scrub"),
        )
        requests = []

        def open_request(request, **_kwargs):
            requests.append(request)
            event = json.loads(request.data)["events"][0]
            return FakeResponse(201, {"status": "committed", "receipts": [f"recall://{event['source_id']}/{event['native_id']}?rev=1"]})

        with mock.patch("client.mac.open_no_redirect", side_effect=open_request):
            result = scrubbed.put(f"keep context api_key={canary} after")
        self.assertEqual(result["privacy"]["action"], "scrub")
        self.assertNotIn(canary, requests[0].data.decode())
        self.assertIn("keep context", requests[0].data.decode())

    def test_delete_bypasses_context_judge_failure(self) -> None:
        def unavailable(_text: str) -> list[dict]:
            raise OSError("synthetic judge outage")

        client = MemoryClient(
            endpoint="https://brain.example.ts.net", token="synthetic-token",
            source_id="memory:mac:test",
            privacy=PrivacyPolicy(mode="scrub", judge=unavailable, judge_failure="drop"),
        )
        requests = []

        def open_request(request, **_kwargs):
            requests.append(request)
            event = json.loads(request.data)["events"][0]
            return FakeResponse(201, {"status": "committed", "receipts": [f"recall://{event['source_id']}/{event['native_id']}?rev=2"]})

        with mock.patch("client.mac.open_no_redirect", side_effect=open_request):
            result = client.delete("recall://memory:mac:test/memory-synthetic?rev=1")
        self.assertEqual(result["kind"], "tombstone")
        self.assertEqual(len(requests), 1)

    def test_delete_many_batches_canonical_tombstones(self) -> None:
        client = MemoryClient(
            endpoint="https://brain.example.ts.net", token="synthetic-token",
            source_id="memory:mac:test", privacy=PrivacyPolicy(mode="scrub"),
        )
        requests = []

        def open_request(request, **_kwargs):
            requests.append(request)
            events = json.loads(request.data)["events"]
            return FakeResponse(201, {
                "status": "committed",
                "receipts": [f"recall://{event['source_id']}/{event['native_id']}?rev=2" for event in events],
            })

        receipts = [
            "recall://memory:mac:test/memory-one?rev=1",
            "recall://memory:mac:test/memory-two?rev=1",
        ]
        with mock.patch("client.mac.open_no_redirect", side_effect=open_request):
            result = client.delete_many(receipts)
        self.assertEqual(result["kind"], "tombstones")
        self.assertEqual(len(requests), 1)
        events = json.loads(requests[0].data)["events"]
        self.assertEqual([event["kind"] for event in events], ["tombstone", "tombstone"])
        self.assertEqual([event["content"]["deleted_receipt"] for event in events], receipts)


class ExportPrivacyTest(unittest.TestCase):
    def test_export_drop_and_scrub_share_policy_before_network(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            canary = "synthetic-export-secret-canary-94"
            source = root / "export.jsonl"
            source.write_text(json.dumps({"text": f"api_key={canary}"}) + "\n" + json.dumps({"text": "safe export neighbor"}) + "\n")
            dropped = ExportImporter(
                source_id="export:mac:test", principal_id="owner", visibility="private",
                privacy=PrivacyPolicy(mode="drop"),
            ).inventory([source])
            self.assertEqual(len(dropped["records"]), 1)
            self.assertEqual(dropped["privacy"]["actions"], {"drop": 1, "keep": 1})
            self.assertNotIn(canary, json.dumps(dropped))

            scrubbed = ExportImporter(
                source_id="export:mac:test", principal_id="owner", visibility="private",
                privacy=PrivacyPolicy(mode="scrub"),
            ).inventory([source])
            self.assertEqual(len(scrubbed["records"]), 2)
            self.assertEqual(scrubbed["privacy"]["actions"], {"keep": 1, "scrub": 1})
            self.assertNotIn(canary, json.dumps(scrubbed))

    def test_all_dropped_export_makes_no_request(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            canary = "synthetic-export-all-drop-canary-97"
            source = Path(temporary) / "export.json"
            source.write_text(json.dumps({"text": f"api_key={canary}"}))
            policy = PrivacyPolicy(mode="drop")
            importer = ExportImporter(
                source_id="export:mac:test", principal_id="owner",
                visibility="private", privacy=policy,
            )
            client = MemoryClient(
                endpoint="https://brain.example.ts.net", token="synthetic-token",
                source_id="export:mac:test", privacy=policy,
            )
            with mock.patch("client.mac.open_no_redirect") as opened:
                result = importer.import_with(client, [source])
            opened.assert_not_called()
            self.assertEqual(result["acknowledgement"]["status"], "privacy_filtered")
            self.assertNotIn(canary, json.dumps(result))


class PrivacyPreviewTest(unittest.TestCase):
    def test_preview_prints_only_content_free_receipt(self) -> None:
        canary = "synthetic-preview-canary-98"
        output = io.StringIO()
        with mock.patch("sys.argv", ["recall-brain", "privacy-preview", "--privacy-mode", "scrub"]), \
             mock.patch("sys.stdin", io.StringIO(f"api_key={canary}")), \
             contextlib.redirect_stdout(output), \
             mock.patch("client.mac.open_no_redirect") as opened:
            client_cli.main()
        opened.assert_not_called()
        receipt = json.loads(output.getvalue())
        self.assertEqual(receipt["action"], "scrub")
        self.assertNotIn(canary, output.getvalue())


if __name__ == "__main__":
    unittest.main()
