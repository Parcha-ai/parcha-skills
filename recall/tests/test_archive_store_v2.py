from __future__ import annotations

import hashlib
import io
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from server.recall_server.archive import (
    ArchiveCorruption,
    ArchiveNotFound,
    ArchiveRequest,
    FilesystemArchiveStore,
    S3ArchiveStore,
)

PAYLOAD = b'{"kind":"synthetic","text":"private source bytes"}'
TENANT = "tenant:synthetic"
SOURCE = "source:synthetic"


def request(**overrides) -> ArchiveRequest:
    values = {
        "tenant_id": TENANT,
        "source_id": SOURCE,
        "native_id": "native:synthetic-1",
        "media_type": "application/json",
        "payload": PAYLOAD,
    }
    values.update(overrides)
    return ArchiveRequest(**values)


class FakeBody:
    def __init__(self, payload: bytes):
        self.payload = payload
        self.offset = 0

    def read(self, size: int) -> bytes:
        chunk = self.payload[self.offset:self.offset + size]
        self.offset += len(chunk)
        return chunk


class FakeS3:
    def __init__(self):
        self.objects: dict[tuple[str, str, str], dict] = {}
        self.put_calls: list[dict] = []
        self.get_calls: list[dict] = []
        self.head_calls: list[dict] = []
        self.delete_calls: list[dict] = []
        self.version = 0

    def put_object(self, **kwargs):
        self.put_calls.append(kwargs)
        self.version += 1
        version = f"version-{self.version}"
        self.objects[(kwargs["Bucket"], kwargs["Key"], version)] = dict(kwargs)
        return {"VersionId": version}

    def get_object(self, **kwargs):
        self.get_calls.append(kwargs)
        try:
            value = self.objects[(kwargs["Bucket"], kwargs["Key"], kwargs["VersionId"])]
        except KeyError as error:
            raise ArchiveNotFound("archive object not found") from error
        payload = value["Body"]
        response = {
            "Body": FakeBody(payload),
            "ContentLength": len(payload),
            "Metadata": value["Metadata"],
            "VersionId": kwargs["VersionId"],
        }
        if "Range" in kwargs:
            byte_range = kwargs["Range"].removeprefix("bytes=")
            start_text, end_text = byte_range.split("-", 1)
            start, end = int(start_text), int(end_text)
            payload = payload[start:end + 1]
            response.update({
                "Body": FakeBody(payload),
                "ContentLength": len(payload),
                "ContentRange": (
                    f"bytes {start}-{end}/{value['ContentLength']}"
                ),
            })
        return response

    def head_object(self, **kwargs):
        self.head_calls.append(kwargs)
        matches = [
            (version, value)
            for (bucket, key, version), value in self.objects.items()
            if (
                bucket == kwargs["Bucket"]
                and key == kwargs["Key"]
                and (
                    "VersionId" not in kwargs
                    or version == kwargs["VersionId"]
                )
            )
        ]
        if not matches:
            raise ArchiveNotFound("archive object not found")
        version, value = matches[-1]
        return {
            "ContentLength": value["ContentLength"],
            "Metadata": value["Metadata"],
            "VersionId": version,
        }

    def delete_object(self, **kwargs):
        self.delete_calls.append(kwargs)
        self.objects.pop(
            (kwargs["Bucket"], kwargs["Key"], kwargs["VersionId"]),
            None,
        )
        return {}


class FakeR2:
    def __init__(self):
        self.objects: dict[tuple[str, str], dict] = {}
        self.put_calls: list[dict] = []
        self.get_calls: list[dict] = []
        self.head_calls: list[dict] = []
        self.delete_calls: list[dict] = []

    def put_object(self, **kwargs):
        self.put_calls.append(kwargs)
        identity = (kwargs["Bucket"], kwargs["Key"])
        if kwargs.get("IfNoneMatch") == "*" and identity in self.objects:
            raise RuntimeError("precondition failed")
        self.objects[identity] = dict(kwargs)
        return {"ETag": '"synthetic-etag"'}

    def get_object(self, **kwargs):
        self.get_calls.append(kwargs)
        try:
            value = self.objects[(kwargs["Bucket"], kwargs["Key"])]
        except KeyError as error:
            raise ArchiveNotFound("archive object not found") from error
        return {
            "Body": FakeBody(value["Body"]),
            "ContentLength": value["ContentLength"],
            "Metadata": value["Metadata"],
            "ETag": '"synthetic-etag"',
        }

    def head_object(self, **kwargs):
        self.head_calls.append(kwargs)
        try:
            value = self.objects[(kwargs["Bucket"], kwargs["Key"])]
        except KeyError as error:
            raise ArchiveNotFound("archive object not found") from error
        return {
            "ContentLength": value["ContentLength"],
            "Metadata": value["Metadata"],
            "ETag": '"synthetic-etag"',
        }

    def delete_object(self, **kwargs):
        self.delete_calls.append(kwargs)
        self.objects.pop((kwargs["Bucket"], kwargs["Key"]), None)
        return {}


class ArchiveStoreParityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.key = b"k" * 32
        self.temporary = tempfile.TemporaryDirectory()
        self.filesystem = FilesystemArchiveStore(
            Path(self.temporary.name) / "archive",
            namespace_key=self.key,
        )
        self.s3_client = FakeS3()
        self.s3 = S3ArchiveStore(
            bucket="recall-synthetic",
            endpoint_url="https://objects.example.test",
            namespace_key=self.key,
            client=self.s3_client,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_filesystem_and_s3_create_read_version_delete_with_receipt_parity(self):
        for store in (self.filesystem, self.s3):
            first = store.put(request())
            replay = store.put(request())
            changed = store.put(request(payload=PAYLOAD + b"\n"))

            self.assertEqual(first.artifact_id, replay.artifact_id)
            self.assertEqual(first.object_key, replay.object_key)
            self.assertEqual(first.version_id, replay.version_id)
            self.assertNotEqual(first.object_key, changed.object_key)
            self.assertEqual(
                store.read(first, tenant_id=TENANT, source_id=SOURCE),
                PAYLOAD,
            )
            self.assertTrue(
                store.delete(first, tenant_id=TENANT, source_id=SOURCE),
            )
            self.assertFalse(
                store.delete(first, tenant_id=TENANT, source_id=SOURCE),
            )
            with self.assertRaises(ArchiveNotFound):
                store.read(first, tenant_id=TENANT, source_id=SOURCE)

        fs_ref = self.filesystem.put(request(native_id="native:parity"))
        s3_ref = self.s3.put(request(native_id="native:parity"))
        self.assertEqual(fs_ref.artifact_id, s3_ref.artifact_id)
        self.assertEqual(fs_ref.object_key, s3_ref.object_key)
        self.assertEqual(fs_ref.content_sha256, s3_ref.content_sha256)

    def test_scope_and_integrity_fail_closed(self):
        for store in (self.filesystem, self.s3):
            reference = store.put(request())
            with self.assertRaises(ArchiveNotFound):
                store.read(
                    reference,
                    tenant_id="tenant:other",
                    source_id=SOURCE,
                )
            with self.assertRaises(ArchiveNotFound):
                store.read(
                    reference,
                    tenant_id=TENANT,
                    source_id="source:other",
                )
            with self.assertRaises(ArchiveNotFound):
                store.delete(
                    reference,
                    tenant_id="tenant:other",
                    source_id=SOURCE,
                )
            with self.assertRaises(ArchiveNotFound):
                store.read(
                    replace(reference, object_key="../../private"),
                    tenant_id=TENANT,
                    source_id=SOURCE,
                )

        reference = self.filesystem.put(request(native_id="native:corrupt"))
        data_path = self.filesystem.root / reference.object_key / "data"
        data_path.write_bytes(b"tampered")
        with self.assertRaises(ArchiveCorruption):
            self.filesystem.read(reference, tenant_id=TENANT, source_id=SOURCE)

    def test_contract_gateway_delete_is_idempotent_and_tenant_safe(self):
        for store in (self.filesystem, self.s3):
            reference = store.put_raw(
                tenant_id=TENANT,
                source_id=SOURCE,
                native_id="native:gateway-delete",
                payload=PAYLOAD,
                media_type="application/json",
                created_at="2026-07-19T00:00:00Z",
            )
            forged = {**reference, "tenant_id": "tenant:other"}
            self.assertFalse(store.delete_raw(forged))
            internal = store.put(request(native_id="native:gateway-delete"))
            self.assertEqual(
                store.read(internal, tenant_id=TENANT, source_id=SOURCE),
                PAYLOAD,
            )
            self.assertTrue(store.delete_raw(reference))
            self.assertFalse(store.delete_raw(reference))

    def test_internal_delete_verifies_metadata_without_downloading_payload(self):
        reference = self.s3.put_raw(
            tenant_id=TENANT,
            source_id=SOURCE,
            native_id="native:internal-delete",
            payload=PAYLOAD,
            media_type="application/json",
            created_at="2026-07-19T00:00:00Z",
        )
        get_count = len(self.s3_client.get_calls)

        self.assertTrue(self.s3.delete_internal_raw(reference))
        self.assertEqual(len(self.s3_client.get_calls), get_count)
        self.assertTrue(self.s3_client.head_calls)
        self.assertTrue(self.s3_client.delete_calls)

        protected = self.s3.put_raw(
            tenant_id=TENANT,
            source_id=SOURCE,
            native_id="native:internal-delete-protected",
            payload=PAYLOAD,
            media_type="application/json",
            created_at="2026-07-19T00:00:00Z",
        )
        forged = {**protected, "tenant_id": "tenant:other"}
        self.assertFalse(self.s3.delete_internal_raw(forged))
        self.assertEqual(self.s3.read_raw(protected), PAYLOAD)

    def test_filesystem_rejects_a_symlinked_parent(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            real = base / "real"
            real.mkdir()
            alias = base / "alias"
            alias.symlink_to(real, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, "symlink"):
                FilesystemArchiveStore(
                    alias / "archive",
                    namespace_key=self.key,
                )

    def test_private_files_and_content_free_metadata(self):
        reference = self.filesystem.put(request())
        self.assertEqual(self.filesystem.root.stat().st_mode & 0o777, 0o700)
        object_dir = self.filesystem.root / reference.object_key
        self.assertEqual(object_dir.stat().st_mode & 0o777, 0o700)
        self.assertEqual((object_dir / "data").stat().st_mode & 0o777, 0o600)
        metadata_path = object_dir / "metadata.json"
        self.assertEqual(metadata_path.stat().st_mode & 0o777, 0o600)
        metadata = metadata_path.read_text()
        self.assertNotIn("tenant:synthetic", metadata)
        self.assertNotIn("source:synthetic", metadata)
        self.assertNotIn("native:synthetic", metadata)
        self.assertNotIn("private source bytes", metadata)
        self.assertEqual(json.loads(metadata)["content_sha256"], hashlib.sha256(PAYLOAD).hexdigest())

    def test_s3_requires_tls_versioning_encryption_and_no_public_acl(self):
        reference = self.s3.put(request())
        call = self.s3_client.put_calls[-1]
        self.assertEqual(call["ServerSideEncryption"], "AES256")
        self.assertEqual(
            call["ChecksumSHA256"],
            "RW5gu2b9qyDQaDdLdNAHMy5DQJky1WS9GXbmIGr1p3E=",
        )
        self.assertNotIn("ACL", call)
        self.assertNotIn("tenant:synthetic", json.dumps(call["Metadata"]))
        self.assertTrue(reference.version_id)

        with self.assertRaisesRegex(ValueError, "HTTPS"):
            S3ArchiveStore(
                bucket="recall-synthetic",
                endpoint_url="http://objects.example.test",
                namespace_key=self.key,
                client=self.s3_client,
            )
        self.assertEqual(len(self.s3_client.put_calls), 1)

    def test_large_s3_read_uses_verified_parallel_ranges(self):
        payload = b"0123456789abcdef" * 16
        reference = self.s3.put(request(payload=payload))

        with (
            patch(
                "server.recall_server.archive."
                "S3_PARALLEL_READ_THRESHOLD_BYTES",
                1,
            ),
            patch(
                "server.recall_server.archive."
                "S3_PARALLEL_READ_CHUNK_BYTES",
                64,
            ),
        ):
            result = self.s3.read(
                reference,
                tenant_id=TENANT,
                source_id=SOURCE,
            )

        range_calls = [
            call for call in self.s3_client.get_calls if "Range" in call
        ]
        self.assertEqual(result, payload)
        self.assertEqual(len(range_calls), 4)
        self.assertTrue(all(
            call["VersionId"] == reference.version_id
            for call in range_calls
        ))

    def test_r2_uses_immutable_keys_without_unsupported_s3_features(self):
        client = FakeR2()
        store = S3ArchiveStore(
            bucket="recall-synthetic",
            endpoint_url=(
                "https://0123456789abcdef0123456789abcdef."
                "r2.cloudflarestorage.com"
            ),
            namespace_key=self.key,
            client=client,
            compatibility_profile="r2",
        )

        first = store.put(request())
        self.assertEqual(client.head_calls, [])
        replay = store.put(request())
        self.assertEqual(first, replay)
        self.assertEqual(len(client.head_calls), 1)
        self.assertEqual(
            first.version_id,
            "r2-sha256-" + hashlib.sha256(PAYLOAD).hexdigest(),
        )
        call = client.put_calls[-1]
        self.assertEqual(call["IfNoneMatch"], "*")
        self.assertNotIn("ServerSideEncryption", call)
        self.assertNotIn("ChecksumSHA256", call)
        self.assertNotIn("ACL", call)
        self.assertEqual(
            store.read(first, tenant_id=TENANT, source_id=SOURCE),
            PAYLOAD,
        )
        self.assertNotIn("VersionId", client.get_calls[-1])
        self.assertTrue(store.delete(first, tenant_id=TENANT, source_id=SOURCE))
        self.assertNotIn("VersionId", client.delete_calls[-1])

        with self.assertRaisesRegex(ValueError, "R2 endpoint"):
            S3ArchiveStore(
                bucket="recall-synthetic",
                endpoint_url="https://objects.example.test",
                namespace_key=self.key,
                client=client,
                compatibility_profile="r2",
            )

    def test_unversioned_aws_uses_conditional_encrypted_immutable_objects(self):
        client = FakeR2()
        store = S3ArchiveStore(
            bucket="recall-synthetic",
            endpoint_url="https://s3.us-west-2.amazonaws.com",
            namespace_key=self.key,
            client=client,
            compatibility_profile="aws-unversioned",
        )

        first = store.put(request())
        self.assertEqual(client.head_calls, [])
        replay = store.put(request())
        self.assertEqual(first, replay)
        self.assertEqual(len(client.head_calls), 1)
        self.assertEqual(
            first.version_id,
            "s3-sha256-" + hashlib.sha256(PAYLOAD).hexdigest(),
        )
        call = client.put_calls[-1]
        self.assertEqual(call["IfNoneMatch"], "*")
        self.assertEqual(call["ServerSideEncryption"], "AES256")
        self.assertIn("ChecksumSHA256", call)
        self.assertEqual(
            store.read(first, tenant_id=TENANT, source_id=SOURCE),
            PAYLOAD,
        )
        self.assertNotIn("VersionId", client.get_calls[-1])
        self.assertTrue(store.delete(first, tenant_id=TENANT, source_id=SOURCE))
        self.assertNotIn("VersionId", client.delete_calls[-1])

    def test_bounds_are_enforced_before_storage_io(self):
        oversized = b"x" * 1025
        bounded = S3ArchiveStore(
            bucket="recall-synthetic",
            endpoint_url="https://objects.example.test",
            namespace_key=self.key,
            client=self.s3_client,
            maximum_bytes=1024,
        )
        before = len(self.s3_client.put_calls)
        with self.assertRaisesRegex(ValueError, "byte bound"):
            bounded.put(request(payload=oversized))
        self.assertEqual(len(self.s3_client.put_calls), before)

        with self.assertRaisesRegex(ValueError, "byte bound"):
            bounded.put_stream(
                request(payload=b""),
                io.BytesIO(oversized),
                size_bytes=len(oversized),
                content_sha256=hashlib.sha256(oversized).hexdigest(),
            )


if __name__ == "__main__":
    unittest.main()
