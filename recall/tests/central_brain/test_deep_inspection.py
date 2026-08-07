from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

from recall_server.archive import FilesystemArchiveStore
from recall_server.agent_scan import AGENT_SCAN_SCRIPT
from recall_server.deep_inspection import (
    AgentExecObject,
    ArchilDeepInspector,
    DeepInspectionBudget,
    DeepInspectionError,
    EvidenceTarget,
    LocalDeepInspector,
    UrllibTransport,
    agent_evidence_receipts,
)
from recall_server.evidence_projection import (
    CanonicalEvidenceProjector,
    EvidenceBundle,
    EvidenceChunk,
    EvidenceProjectionError,
    EvidenceProjectionStore,
)

TENANT = "tenant:company:synthetic"
SOURCE = "source:company:synthetic"
RECEIPT = "recall://source:company:synthetic/native:alpha?rev=1#item=0"
SAFE_TEXT = "Synthetic full evidence: the Atlas runner changed after preview."


class RecordingTransport:
    def __init__(self, response: dict | None = None):
        self.calls: list[dict] = []
        self.response = response or {
            "success": True,
            "data": {
                "stdout": json.dumps(
                    {
                        "findings": [
                            {
                                "receipt": RECEIPT,
                                "text": SAFE_TEXT,
                                "line": 1,
                                "object_key": ("objects/aa/" + "a" * 64),
                            }
                        ],
                        "complete": True,
                        "files_scanned": 1,
                    }
                ),
                "stderr": "",
                "exitCode": 0,
                "timing": {"totalMs": 12, "queueMs": 2, "executeMs": 10},
            },
        }

    def post(self, *, url: str, headers: dict[str, str], body: dict, timeout: float):
        self.calls.append(
            {"url": url, "headers": headers, "body": body, "timeout": timeout}
        )
        return self.response


class DeepInspectionContractTests(unittest.TestCase):
    def test_archil_http_failures_keep_only_safe_status_classes(self):
        expected = {
            401: "deep_inspector_authentication_failed",
            403: "deep_inspector_authentication_failed",
            409: "deep_inspector_busy",
            413: "deep_inspector_request_too_large",
            429: "deep_inspector_rate_limited",
            503: "deep_inspector_upstream_failed",
            400: "deep_inspector_bad_request",
            404: "deep_inspector_endpoint_not_found",
            422: "deep_inspector_validation_failed",
        }
        for status, code in expected.items():
            with (
                self.subTest(status=status),
                mock.patch(
                    "urllib.request.urlopen",
                    side_effect=urllib.error.HTTPError(
                        "https://control.invalid/api/exec",
                        status,
                        "private provider detail",
                        {},
                        None,
                    ),
                ),
                self.assertRaises(DeepInspectionError) as caught,
            ):
                UrllibTransport().post(
                    url="https://control.invalid/api/exec",
                    headers={"Authorization": "synthetic"},
                    body={"command": "true"},
                    timeout=1,
                )
            self.assertEqual(caught.exception.code, code)
            self.assertNotIn("private provider detail", str(caught.exception))

    @staticmethod
    def _write_scan_fixture(
        root: Path,
        document_id: str,
        records: list[dict],
    ) -> Path:
        part_key = "objects/aa/" + "a" * 64
        manifest_key = "objects/bb/" + "b" * 64
        part = root / part_key
        manifest = root / manifest_key
        part.parent.mkdir(parents=True)
        manifest.parent.mkdir(parents=True)
        part.write_text(
            "".join(json.dumps(record) + "\n" for record in records)
        )
        manifest.write_text(json.dumps({
            "logical_document_id": document_id,
            "parts": [{
                "object_key": part_key,
                "first_record_ordinal": records[0]["ordinal"],
                "last_record_ordinal": records[-1]["ordinal"],
            }],
        }))
        helper = root / "recall-scan"
        helper.write_text(AGENT_SCAN_SCRIPT)
        helper.chmod(0o500)
        return helper

    def test_agent_scan_targets_one_manifest_document_and_emits_evidence(self):
        document_id = "ldoc_0123456789abcdef0123456789abcdef"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            part_key = "objects/aa/" + "a" * 64
            manifest_key = "objects/bb/" + "b" * 64
            part = root / part_key
            manifest = root / manifest_key
            part.parent.mkdir(parents=True)
            manifest.parent.mkdir(parents=True)
            part.write_text(json.dumps({
                "content": {"message": "Atlas changed after preview"},
                "event_native_id": "native:alpha",
                "occurred_at": "2026-07-23T00:00:00Z",
                "ordinal": 4,
                "receipts": [RECEIPT],
            }) + "\n" + json.dumps({
                "content": {"message": "Resolution confirmed in the next record"},
                "event_native_id": "native:beta",
                "occurred_at": "2026-07-23T00:01:00Z",
                "ordinal": 5,
                "receipts": [RECEIPT],
            }) + "\n")
            manifest.write_text(json.dumps({
                "logical_document_id": document_id,
                "parts": [{
                    "object_key": part_key,
                    "first_record_ordinal": 4,
                    "last_record_ordinal": 5,
                }],
            }))
            helper = root / "recall-scan"
            helper.write_text(AGENT_SCAN_SCRIPT)
            helper.chmod(0o500)
            result = subprocess.run(
                [
                    sys.executable,
                    str(helper),
                    "--document",
                    document_id,
                    "--pattern",
                    "Atlas",
                    "--records",
                    "4:2",
                    "--context",
                    "1",
                    "--limit",
                    "2",
                ],
                check=False,
                capture_output=True,
                text=True,
                env={
                    **os.environ,
                    "PATH": "",
                    "RECALL_EVIDENCE_ROOT": str(root),
                },
                timeout=5,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Atlas changed after preview", result.stdout)
        self.assertIn("Resolution confirmed in the next record", result.stdout)
        self.assertIn(f'"logical_document_id":"{document_id}"', result.stdout)
        self.assertIn(f"RECALL_EVIDENCE {RECEIPT}", result.stdout)

    def test_agent_scan_applies_host_pointers_until_agent_broadens(self):
        document_id = "ldoc_0123456789abcdef0123456789abcdef"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            selected_key = "objects/aa/" + "a" * 64
            excluded_key = "objects/cc/" + "c" * 64
            manifest_key = "objects/bb/" + "b" * 64
            selected = root / selected_key
            excluded = root / excluded_key
            manifest = root / manifest_key
            selected.parent.mkdir(parents=True)
            excluded.parent.mkdir(parents=True)
            manifest.parent.mkdir(parents=True)
            selected.write_text(json.dumps({
                "content": {"message": "pointer-selected evidence"},
                "event_native_id": "native:selected",
                "occurred_at": "2026-07-23T00:00:00Z",
                "ordinal": 4,
                "receipts": [RECEIPT],
            }) + "\n")
            excluded.write_text(json.dumps({
                "content": {"message": "needle only outside pointer"},
                "event_native_id": "native:excluded",
                "occurred_at": "2026-07-23T00:01:00Z",
                "ordinal": 100,
                "receipts": [RECEIPT],
            }) + "\n")
            manifest.write_text(json.dumps({
                "logical_document_id": document_id,
                "parts": [
                    {
                        "object_key": selected_key,
                        "first_record_ordinal": 4,
                        "last_record_ordinal": 4,
                    },
                    {
                        "object_key": excluded_key,
                        "first_record_ordinal": 100,
                        "last_record_ordinal": 100,
                    },
                ],
            }))
            pointers = root / "pointers.json"
            pointers.write_text(json.dumps({
                document_id: {
                    "spans": [{
                        "record_ordinal": 4,
                        "record_count": 2,
                    }],
                    "routing_receipts": [RECEIPT],
                },
            }))
            helper = root / "recall-scan"
            helper.write_text(AGENT_SCAN_SCRIPT)
            helper.chmod(0o500)
            environment = {
                **os.environ,
                "RECALL_EVIDENCE_ROOT": str(root),
                "RECALL_POINTERS_PATH": str(pointers),
            }
            scoped = subprocess.run(
                [
                    sys.executable,
                    str(helper),
                    "--document",
                    document_id,
                    "--pattern",
                    "needle",
                    "--limit",
                    "2",
                ],
                check=False,
                capture_output=True,
                text=True,
                env=environment,
                timeout=5,
            )
            pointer_window = subprocess.run(
                [
                    str(helper),
                    "--document",
                    document_id,
                    "--all",
                    "--limit",
                    "2",
                ],
                check=False,
                capture_output=True,
                text=True,
                env=environment,
                timeout=5,
            )
            broad = subprocess.run(
                [
                    str(helper),
                    "--document",
                    document_id,
                    "--pattern",
                    "needle",
                    "--limit",
                    "2",
                    "--broad",
                ],
                check=False,
                capture_output=True,
                text=True,
                env=environment,
                timeout=5,
            )
        self.assertEqual(scoped.returncode, 0, scoped.stderr)
        self.assertEqual(scoped.stdout.strip(), '{"matches":0}')
        self.assertEqual(pointer_window.returncode, 0, pointer_window.stderr)
        self.assertIn("pointer-selected evidence", pointer_window.stdout)
        self.assertNotIn("needle only outside pointer", pointer_window.stdout)
        self.assertEqual(broad.returncode, 0, broad.stderr)
        self.assertIn("needle only outside pointer", broad.stdout)

    def test_agent_scan_literal_excerpt_is_centered_on_a_late_match(self):
        document_id = "ldoc_0123456789abcdef0123456789abcdef"
        content = {"message": "x" * 2_500 + "NEEDLE" + "y" * 2_500}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            helper = self._write_scan_fixture(
                root,
                document_id,
                [{
                    "content": content,
                    "event_native_id": "native:centered",
                    "occurred_at": "2026-07-23T00:00:00Z",
                    "ordinal": 4,
                    "receipts": [RECEIPT],
                }],
            )
            result = subprocess.run(
                [
                    sys.executable,
                    str(helper),
                    "--document",
                    document_id,
                    "--pattern",
                    "NEEDLE",
                    "--fixed",
                    "--broad",
                    "--excerpt-chars",
                    "400",
                    "--limit",
                    "1",
                ],
                check=False,
                capture_output=True,
                text=True,
                env={
                    **os.environ,
                    "PATH": "/no-external-tools",
                    "RECALL_EVIDENCE_ROOT": str(root),
                },
                timeout=5,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        record = json.loads(result.stdout.splitlines()[0])
        self.assertIn("NEEDLE", record["content"])
        self.assertGreater(record["content_start"], 0)
        self.assertEqual(
            record["content_end"] - record["content_start"],
            400,
        )
        self.assertFalse(record["content_complete"])

    def test_agent_scan_open_cursor_reconstructs_every_record_exactly(self):
        document_id = "ldoc_0123456789abcdef0123456789abcdef"
        records = [
            {
                "content": {
                    "message": "α" * 4_000 + " middle " + "β" * 4_000,
                    "decision": "keep complete evidence",
                },
                "event_native_id": "native:large",
                "occurred_at": "2026-07-23T00:00:00Z",
                "ordinal": 4,
                "receipts": [RECEIPT],
            },
            {
                "content": {"message": "terminal small record"},
                "event_native_id": "native:small",
                "occurred_at": "2026-07-23T00:01:00Z",
                "ordinal": 5,
                "receipts": [RECEIPT],
            },
        ]
        expected = {
            record["ordinal"]: json.dumps(
                record["content"],
                ensure_ascii=False,
                separators=(",", ":"),
            )
            for record in records
        }
        reconstructed = {ordinal: "" for ordinal in expected}
        cursors = []
        cursor = "0:0:0"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            helper = self._write_scan_fixture(root, document_id, records)
            for _ in range(32):
                result = subprocess.run(
                    [
                        str(helper),
                        "--document",
                        document_id,
                        "--all",
                        "--broad",
                        "--cursor",
                        cursor,
                        "--page-bytes",
                        "1024",
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                    env={**os.environ, "RECALL_EVIDENCE_ROOT": str(root)},
                    timeout=5,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                page = None
                for line in result.stdout.splitlines():
                    if line.startswith("RECALL_PAGE "):
                        page = json.loads(line.removeprefix("RECALL_PAGE "))
                        continue
                    record = json.loads(line)
                    reconstructed[record["ordinal"]] += record["content"]
                self.assertIsNotNone(page)
                if page["complete"]:
                    self.assertIsNone(page["next_cursor"])
                    break
                cursor = page["next_cursor"]
                self.assertNotIn(cursor, cursors)
                cursors.append(cursor)
            else:
                self.fail("open cursor did not terminate")
        self.assertEqual(reconstructed, expected)

    def test_agent_scan_open_starts_at_a_hinted_record(self):
        document_id = "ldoc_0123456789abcdef0123456789abcdef"
        records = [
            {
                "content": {"message": "earlier record"},
                "event_native_id": "native:early",
                "occurred_at": "2026-07-23T00:00:00Z",
                "ordinal": 4,
                "receipts": [RECEIPT],
            },
            {
                "content": {"message": "hinted record"},
                "event_native_id": "native:hinted",
                "occurred_at": "2026-07-23T00:01:00Z",
                "ordinal": 5,
                "receipts": [RECEIPT],
            },
            {
                "content": {"message": "later record"},
                "event_native_id": "native:later",
                "occurred_at": "2026-07-23T00:02:00Z",
                "ordinal": 6,
                "receipts": [RECEIPT],
            },
        ]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            helper = self._write_scan_fixture(root, document_id, records)
            result = subprocess.run(
                [
                    str(helper),
                    "--document",
                    document_id,
                    "--all",
                    "--broad",
                    "--cursor",
                    "0:0:0",
                    "--start-record",
                    "5",
                    "--one-record",
                    "--page-bytes",
                    "6000",
                ],
                check=False,
                capture_output=True,
                text=True,
                env={**os.environ, "RECALL_EVIDENCE_ROOT": str(root)},
                timeout=5,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        projected = [
            json.loads(line)
            for line in result.stdout.splitlines()
            if not line.startswith("RECALL_PAGE ")
        ]
        self.assertEqual(
            [record["ordinal"] for record in projected],
            [5],
        )
        self.assertIn("hinted record", projected[0]["content"])

    def bundle(self, *, revision: int = 1, text: str = SAFE_TEXT) -> EvidenceBundle:
        return EvidenceBundle(
            evidence_id="evd_" + "a" * 32,
            revision=revision,
            occurred_at="2026-07-24T09:00:00Z",
            session_sha256="b" * 64,
            text_sha256=__import__("hashlib").sha256(text.encode()).hexdigest(),
            chunks=(EvidenceChunk(ordinal=0, receipt=RECEIPT, text=text),),
        )

    def test_full_privacy_processed_content_round_trips_with_opaque_key(self):
        with tempfile.TemporaryDirectory() as temporary:
            archive = FilesystemArchiveStore(
                Path(temporary) / "evidence",
                namespace_key=b"e" * 32,
            )
            projection = EvidenceProjectionStore(archive)
            reference = projection.put(
                tenant_id=TENANT,
                source_id=SOURCE,
                document_id="doc_" + "c" * 32,
                bundle=self.bundle(),
            )
            payload = projection.read(
                reference,
                tenant_id=TENANT,
                source_id=SOURCE,
            )
            self.assertEqual(payload.chunks[0].text, SAFE_TEXT)
            self.assertEqual(payload.chunks[0].receipt, RECEIPT)
            rendered_reference = json.dumps(reference, sort_keys=True)
            self.assertNotIn(SAFE_TEXT, rendered_reference)
            self.assertNotIn(TENANT, reference["object_key"])
            self.assertNotIn(SOURCE, reference["object_key"])

            replay = projection.put(
                tenant_id=TENANT,
                source_id=SOURCE,
                document_id="doc_" + "c" * 32,
                bundle=self.bundle(),
            )
            self.assertEqual(replay, reference)
            changed = projection.put(
                tenant_id=TENANT,
                source_id=SOURCE,
                document_id="doc_" + "c" * 32,
                bundle=self.bundle(
                    revision=2,
                    text=SAFE_TEXT + " The verification then passed.",
                ),
            )
            self.assertNotEqual(changed["artifact_id"], reference["artifact_id"])

            with self.assertRaisesRegex(Exception, "not found"):
                projection.read(
                    reference,
                    tenant_id="tenant:personal:synthetic",
                    source_id=SOURCE,
                )
            self.assertTrue(projection.delete(reference))
            with self.assertRaisesRegex(Exception, "not found"):
                projection.read(
                    reference,
                    tenant_id=TENANT,
                    source_id=SOURCE,
                )

    def test_local_inspector_has_same_bounded_receipt_contract(self):
        with tempfile.TemporaryDirectory() as temporary:
            archive = FilesystemArchiveStore(
                Path(temporary) / "evidence",
                namespace_key=b"e" * 32,
            )
            projection = EvidenceProjectionStore(archive)
            reference = projection.put(
                tenant_id=TENANT,
                source_id=SOURCE,
                document_id="doc_" + "c" * 32,
                bundle=self.bundle(),
            )
            target = EvidenceTarget.from_reference(reference)
            result = LocalDeepInspector(projection).inspect(
                tenant_id=TENANT,
                question="What changed in Atlas preview?",
                targets=(target,),
                budget=DeepInspectionBudget(max_files=4, max_matches=5),
            )
            self.assertTrue(result["complete"])
            self.assertEqual(result["findings"][0]["receipt"], RECEIPT)
            self.assertLessEqual(len(result["findings"]), 5)

    def test_runtime_projector_is_hard_bound_to_one_brain(self):
        projector = CanonicalEvidenceProjector(
            object(),
            object(),
            bound_tenant_id=TENANT,
        )
        with self.assertRaisesRegex(
            EvidenceProjectionError,
            "evidence_tenant_not_configured",
        ):
            projector.references_for_receipts(
                tenant_id="tenant:personal:synthetic",
                source_ids=(SOURCE,),
                receipts=(RECEIPT,),
                limit=1,
            )

    def test_projector_batches_chunk_reads_uploads_and_database_writes(self):
        documents = []
        chunks = []
        for index, text in enumerate(("first synthetic body", "second synthetic body")):
            source = f"source:company:batch-{index}"
            document = "doc_" + str(index + 1) * 32
            documents.append(
                {
                    "tenant_id": TENANT,
                    "source_id": source,
                    "document_id": document,
                    "native_id": f"native:{index}",
                    "revision": 1,
                    "text_sha256": __import__("hashlib")
                    .sha256(text.encode())
                    .hexdigest(),
                    "native_parent_id": None,
                    "occurred_at": "2026-07-25T00:00:00Z",
                }
            )
            chunks.append(
                {
                    "tenant_id": TENANT,
                    "source_id": source,
                    "document_id": document,
                    "ordinal": 0,
                    "receipt": f"recall://{source}/native:{index}?rev=1#item=0",
                    "text_redacted": text,
                }
            )

        class Rows:
            def __init__(self, values):
                self.values = values

            def fetchall(self):
                return self.values

        class Cursor:
            def __init__(self, store):
                self.store = store

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def executemany(self, _sql, values):
                self.store.bulk_writes += 1
                for value in values:
                    self.store.stored[(value[0], value[1], value[2])] = {
                        "tenant_id": value[0],
                        "source_id": value[1],
                        "document_id": value[2],
                        "artifact_id": (
                            "art_conflict"
                            if getattr(self.store, "conflict", False)
                            else value[4]
                        ),
                        "text_sha256": value[12],
                    }

        class Connection:
            def __init__(self, store):
                self.store = store

            def execute(self, sql, _params):
                if "FROM canonical_documents document" in sql:
                    self.store.pending_sql = sql
                    self.store.pending_reads += 1
                    return Rows(documents if self.store.pending_reads == 1 else [])
                if "FROM canonical_chunks chunk" in sql:
                    self.store.chunk_reads += 1
                    return Rows(chunks)
                if "FROM canonical_evidence_objects evidence" in sql:
                    return Rows(list(self.store.stored.values()))
                raise AssertionError("unexpected projector query")

            def cursor(self):
                return Cursor(self.store)

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        class Store:
            pending_reads = 0
            chunk_reads = 0
            bulk_writes = 0
            pending_sql = ""
            stored = {}

            def connect(self):
                return Connection(self)

        class Projection:
            def __init__(self):
                self.lock = threading.Lock()
                self.active = 0
                self.max_active = 0
                self.deleted = []

            def put(self, *, tenant_id, source_id, document_id, bundle):
                with self.lock:
                    self.active += 1
                    self.max_active = max(self.max_active, self.active)
                time.sleep(0.02)
                with self.lock:
                    self.active -= 1
                return {
                    "artifact_id": "art_" + document_id.removeprefix("doc_"),
                    "storage_backend": "s3",
                    "object_key": "objects/aa/" + document_id.removeprefix("doc_"),
                    "content_sha256": bundle.text_sha256,
                    "size_bytes": len(bundle.encode()),
                    "media_type": "application/vnd.recall.evidence+json",
                    "encryption": "sse-s3",
                    "version_id": "s3-sha256-" + bundle.text_sha256,
                    "created_at": bundle.occurred_at,
                }

            def delete(self, reference):
                self.deleted.append(reference["artifact_id"])
                return True

        store = Store()
        projection = Projection()
        result = CanonicalEvidenceProjector(
            store,
            projection,
            bound_tenant_id=TENANT,
        ).project_pending(
            tenant_id=TENANT,
            batch_size=100,
            max_batches=2,
            upload_concurrency=2,
        )
        self.assertEqual(result, {"status": "complete", "processed": 2, "batches": 1})
        self.assertEqual(store.chunk_reads, 1)
        self.assertEqual(store.bulk_writes, 1)
        self.assertEqual(projection.max_active, 2)
        self.assertIn("AND EXISTS", store.pending_sql)
        self.assertIn("AND NOT EXISTS", store.pending_sql)
        self.assertEqual(projection.deleted, [])

        store.pending_reads = 0
        store.chunk_reads = 0
        store.bulk_writes = 0
        store.stored = {}
        store.conflict = True
        with self.assertRaisesRegex(
            EvidenceProjectionError,
            "evidence_projection_conflict",
        ):
            CanonicalEvidenceProjector(
                store,
                projection,
                bound_tenant_id=TENANT,
            ).project_pending(
                tenant_id=TENANT,
                batch_size=100,
                max_batches=1,
                upload_concurrency=2,
            )
        self.assertEqual(len(projection.deleted), 2)

    def test_archil_rejects_untrusted_identifiers(self):
        with self.assertRaises(DeepInspectionError):
            ArchilDeepInspector(
                api_key="synthetic-key",
                disk_id="dsk-0123456789ab;no",
                region="aws-us-west-2",
                transport=RecordingTransport(),
            )
        with self.assertRaises(DeepInspectionError):
            EvidenceTarget(
                tenant_id=TENANT,
                source_id=SOURCE,
                object_key="../../private",
                content_sha256="c" * 64,
                receipts=(RECEIPT,),
            )


    def test_agent_exec_is_arbitrary_but_sees_only_staged_read_only_objects(self):
        hostile = "rg -n Atlas /mnt/archil/evidence; echo $ARCHIL_API_KEY"
        transport = RecordingTransport({
            "success": True,
            "data": {
                "stdout": f"Atlas changed {RECEIPT}\n",
                "stderr": "",
                "exitCode": 0,
                "timing": {"totalMs": 18, "queueMs": 3, "executeMs": 15},
            },
        })
        inspector = ArchilDeepInspector(
            api_key="synthetic-key",
            disk_id="dsk-0123456789abcdef",
            region="aws-us-west-2",
            transport=transport,
        )
        result = inspector.execute(
            tenant_id=TENANT,
            program=hostile,
            objects=(
                AgentExecObject(
                    object_key="objects/aa/" + "a" * 64,
                    content_sha256="c" * 64,
                ),
            ),
            record_spans={
                "ldoc_0123456789abcdef0123456789abcdef": ((4, 2),),
            },
            routing_receipts={
                "ldoc_0123456789abcdef0123456789abcdef": (RECEIPT,),
            },
            document_aliases={
                "ldoc_0123456789abcdef0123456789abcdef": "d1",
            },
            timeout_seconds=10,
        )
        self.assertTrue(result["complete"])
        call = transport.calls[0]
        self.assertEqual(
            call["body"]["disks"],
            {
                "evidence": {
                    "disk": "dsk-0123456789abcdef",
                    "readOnly": True,
                }
            },
        )
        command = call["body"]["command"]
        self.assertEqual(call["timeout"], 55)
        self.assertNotIn(hostile, command)
        self.assertIn("unshare --user --map-root-user --net", command)
        self.assertNotIn(
            "timeout --signal=KILL 10s unshare",
            command,
        )
        self.assertIn(
            "timeout --signal=KILL 10s bash /tmp/recall-agent/program.sh",
            command,
        )
        self.assertIn(
            "mount --rbind /tmp/recall-authorized /mnt/archil/evidence",
            command,
        )
        self.assertIn('subprocess.run(["mount","--bind"', command)
        self.assertNotIn("shutil", command)
        self.assertNotIn("hashlib", command)
        self.assertIn("env -i HOME=/tmp", command)
        self.assertIn("bash /tmp/recall-agent/program.sh", command)
        self.assertIn("head -c 40000", command)
        self.assertIn(
            "RECALL_POINTERS_PATH=/tmp/recall-agent/pointers.json",
            command,
        )
        self.assertIn("mount --bind /tmp/recall-docs /docs", command)
        self.assertIn(
            "rm -rf /tmp/recall-authorized /tmp/recall-agent /tmp/recall-docs",
            command,
        )
        self.assertIn("mount -o remount,bind,ro /docs", command)
        self.assertNotIn("synthetic-key", json.dumps(call["body"]))

    def test_agent_evidence_accepts_records_but_not_receipts_inside_content(self):
        quoted = (
            "recall://source:company:synthetic/quoted?rev=1#item=0"
        )
        record = {
            "content": {"message": f"Prose quoted {quoted}"},
            "event_native_id": "native:alpha",
            "occurred_at": "2026-07-23T00:00:00Z",
            "ordinal": 1,
            "receipts": [RECEIPT],
        }
        stdout = (
            f"/mnt/archil/evidence/object:42:{json.dumps(record)}\n"
            f"ordinary prose {quoted}"
        )
        self.assertEqual(agent_evidence_receipts(stdout), [RECEIPT])

    def test_agent_evidence_rejects_a_marker_without_an_opened_record(self):
        self.assertEqual(
            agent_evidence_receipts(f"RECALL_EVIDENCE {RECEIPT}"),
            [],
        )

    def test_agent_exec_timeout_is_bounded_twenty_out_of_twenty(self):
        for _ in range(20):
            transport = RecordingTransport({
                "success": True,
                "data": {
                    "stdout": "",
                    "stderr": "Killed",
                    "exitCode": 137,
                    "timing": {"totalMs": 1001, "queueMs": 1, "executeMs": 1000},
                },
            })
            result = ArchilDeepInspector(
                api_key="synthetic-key",
                disk_id="dsk-0123456789abcdef",
                region="aws-us-west-2",
                transport=transport,
            ).execute(
                tenant_id=TENANT,
                program="sleep 10",
                objects=(
                    AgentExecObject(
                        object_key="objects/aa/" + "a" * 64,
                        content_sha256="c" * 64,
                    ),
                ),
                record_spans={},
                routing_receipts={},
                timeout_seconds=1,
            )
            self.assertEqual(result["stopped_reason"], "timeout")
            self.assertFalse(result["complete"])


if __name__ == "__main__":
    unittest.main()
