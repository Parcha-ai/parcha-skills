from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
from pathlib import Path

from recall_server.archive import FilesystemArchiveStore
from recall_server.deep_inspection import (
    AgentExecObject,
    ArchilDeepInspector,
    DeepInspectionBudget,
    DeepInspectionError,
    EvidenceTarget,
    LocalDeepInspector,
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
        self.assertNotIn(hostile, command)
        self.assertIn("unshare --user --map-root-user --net", command)
        self.assertIn(
            "mount --bind /tmp/recall-authorized /mnt/archil/evidence",
            command,
        )
        self.assertIn("env -i HOME=/tmp", command)
        self.assertNotIn("synthetic-key", json.dumps(call["body"]))

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
                timeout_seconds=1,
            )
            self.assertEqual(result["stopped_reason"], "timeout")
            self.assertFalse(result["complete"])


if __name__ == "__main__":
    unittest.main()
