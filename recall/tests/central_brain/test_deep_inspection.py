from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from recall_server.archive import FilesystemArchiveStore
from recall_server.deep_inspection import (
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
                                "object_key": (
                                    "objects/aa/"
                                    + "a" * 64
                                ),
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

    def test_archil_adapter_uses_fixed_read_only_mount_and_encoded_payload(self):
        transport = RecordingTransport()
        inspector = ArchilDeepInspector(
            api_key="synthetic-key",
            disk_id="dsk-0123456789abcdef",
            region="aws-us-west-2",
            transport=transport,
        )
        target = EvidenceTarget(
            tenant_id=TENANT,
            source_id=SOURCE,
            object_key="objects/aa/" + "a" * 64,
            content_sha256="c" * 64,
            receipts=(RECEIPT,),
        )
        result = inspector.inspect(
            tenant_id=TENANT,
            question='Atlas"; rm -rf / #',
            targets=(target,),
            budget=DeepInspectionBudget(
                max_files=4,
                max_matches=5,
                max_output_bytes=16_000,
                timeout_seconds=10,
            ),
        )
        self.assertTrue(result["complete"])
        call = transport.calls[0]
        self.assertEqual(
            call["url"],
            "https://control.green.us-west-2.aws.prod.archil.com/api/exec",
        )
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
        self.assertNotIn("rm -rf", command)
        self.assertNotIn(SAFE_TEXT, command)
        self.assertNotIn("synthetic-key", json.dumps(call["body"]))
        self.assertEqual(call["headers"]["Authorization"], "synthetic-key")

    def test_archil_rejects_untrusted_identifiers_and_oversized_or_foreign_results(self):
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
        foreign = RecordingTransport(
            {
                "success": True,
                "data": {
                    "stdout": json.dumps(
                        {
                            "findings": [
                                {
                                    "receipt": (
                                        "recall://source:personal:synthetic/"
                                        "private?rev=1#item=0"
                                    ),
                                    "text": "foreign",
                                    "line": 1,
                                    "object_key": "objects/aa/" + "a" * 64,
                                }
                            ],
                            "complete": True,
                            "files_scanned": 1,
                        }
                    ),
                    "stderr": "",
                    "exitCode": 0,
                    "timing": {"totalMs": 1, "queueMs": 0, "executeMs": 1},
                },
            }
        )
        inspector = ArchilDeepInspector(
            api_key="synthetic-key",
            disk_id="dsk-0123456789abcdef",
            region="aws-us-west-2",
            transport=foreign,
        )
        target = EvidenceTarget(
            tenant_id=TENANT,
            source_id=SOURCE,
            object_key="objects/aa/" + "a" * 64,
            content_sha256="c" * 64,
            receipts=(RECEIPT,),
        )
        with self.assertRaisesRegex(DeepInspectionError, "result_invalid"):
            inspector.inspect(
                tenant_id=TENANT,
                question="Atlas",
                targets=(target,),
                budget=DeepInspectionBudget(max_files=1, max_matches=1),
            )


if __name__ == "__main__":
    unittest.main()
