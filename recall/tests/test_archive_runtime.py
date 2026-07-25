from __future__ import annotations

import base64
import unittest

from server.recall_server.archive import ArchiveNotFound, S3ArchiveStore
from server.recall_server.archive_runtime import (
    build_archive_store,
    build_evidence_archive_store,
    probe_archive,
)


class FakeBody:
    def __init__(self, payload: bytes):
        self.payload = payload
        self.offset = 0

    def read(self, size: int) -> bytes:
        chunk = self.payload[self.offset:self.offset + size]
        self.offset += len(chunk)
        return chunk


class FakeR2:
    def __init__(self):
        self.objects = {}

    def put_object(self, **kwargs):
        identity = (kwargs["Bucket"], kwargs["Key"])
        if kwargs.get("IfNoneMatch") == "*" and identity in self.objects:
            raise RuntimeError("precondition failed")
        self.objects[identity] = dict(kwargs)
        return {"ETag": '"synthetic-etag"'}

    def head_object(self, **kwargs):
        try:
            value = self.objects[(kwargs["Bucket"], kwargs["Key"])]
        except KeyError as error:
            raise ArchiveNotFound("archive object not found") from error
        return {
            "ContentLength": value["ContentLength"],
            "Metadata": value["Metadata"],
            "ETag": '"synthetic-etag"',
        }

    def get_object(self, **kwargs):
        value = self.head_object(**kwargs)
        stored = self.objects[(kwargs["Bucket"], kwargs["Key"])]
        return {**value, "Body": FakeBody(stored["Body"])}

    def delete_object(self, **kwargs):
        self.objects.pop((kwargs["Bucket"], kwargs["Key"]), None)
        return {}


class ArchiveRuntimeTest(unittest.TestCase):
    def test_r2_environment_builds_a_bucket_scoped_s3_client(self):
        calls = []
        client = object()
        environment = {
            "RECALL_ARCHIVE_BACKEND": "r2",
            "RECALL_ARCHIVE_BUCKET": "recall-raw-synthetic",
            "RECALL_ARCHIVE_ENDPOINT_URL": (
                "https://0123456789abcdef0123456789abcdef."
                "r2.cloudflarestorage.com"
            ),
            "RECALL_ARCHIVE_REGION": "auto",
            "RECALL_ARCHIVE_ACCESS_KEY_ID": "synthetic-access-id",
            "RECALL_ARCHIVE_SECRET_ACCESS_KEY": "synthetic-secret",
            "RECALL_ARCHIVE_NAMESPACE_KEY": base64.b64encode(b"k" * 32).decode(),
            "CF_R2_API_TOKEN": "must-not-be-used",
        }

        def client_factory(**kwargs):
            calls.append(kwargs)
            return client

        store = build_archive_store(
            environment,
            client_factory=client_factory,
        )

        self.assertIsInstance(store, S3ArchiveStore)
        self.assertEqual(store.compatibility_profile, "r2")
        self.assertIs(store.client.client, client)
        self.assertEqual(
            calls,
            [{
                "service_name": "s3",
                "endpoint_url": environment["RECALL_ARCHIVE_ENDPOINT_URL"],
                "region_name": "auto",
                "aws_access_key_id": "synthetic-access-id",
                "aws_secret_access_key": "synthetic-secret",
            }],
        )
        self.assertNotIn("CF_R2_API_TOKEN", calls[0])

    def test_r2_environment_fails_closed_on_missing_or_invalid_values(self):
        base = {
            "RECALL_ARCHIVE_BACKEND": "r2",
            "RECALL_ARCHIVE_BUCKET": "recall-raw-synthetic",
            "RECALL_ARCHIVE_ENDPOINT_URL": (
                "https://0123456789abcdef0123456789abcdef."
                "r2.cloudflarestorage.com"
            ),
            "RECALL_ARCHIVE_REGION": "auto",
            "RECALL_ARCHIVE_ACCESS_KEY_ID": "synthetic-access-id",
            "RECALL_ARCHIVE_SECRET_ACCESS_KEY": "synthetic-secret",
            "RECALL_ARCHIVE_NAMESPACE_KEY": base64.b64encode(b"k" * 32).decode(),
        }
        for name in base:
            with self.subTest(name=name):
                invalid = dict(base)
                invalid.pop(name)
                with self.assertRaisesRegex(ValueError, "archive configuration"):
                    build_archive_store(invalid, client_factory=lambda **_: object())

        invalid_region = {**base, "RECALL_ARCHIVE_REGION": "us-east-1"}
        with self.assertRaisesRegex(ValueError, "R2 region"):
            build_archive_store(
                invalid_region,
                client_factory=lambda **_: object(),
            )

        invalid_namespace = {**base, "RECALL_ARCHIVE_NAMESPACE_KEY": "not-base64"}
        with self.assertRaisesRegex(ValueError, "namespace key"):
            build_archive_store(
                invalid_namespace,
                client_factory=lambda **_: object(),
            )

    def test_aws_environment_builds_native_versioned_s3_client(self):
        calls = []
        environment = {
            "RECALL_ARCHIVE_BACKEND": "s3",
            "RECALL_ARCHIVE_BUCKET": "recall-archil-test-synthetic",
            "RECALL_ARCHIVE_ENDPOINT_URL": "https://s3.us-west-2.amazonaws.com",
            "RECALL_ARCHIVE_REGION": "us-west-2",
            "RECALL_ARCHIVE_ACCESS_KEY_ID": "synthetic-access-id",
            "RECALL_ARCHIVE_SECRET_ACCESS_KEY": "synthetic-secret",
            "RECALL_ARCHIVE_NAMESPACE_KEY": base64.b64encode(b"s" * 32).decode(),
        }

        store = build_archive_store(
            environment,
            client_factory=lambda **kwargs: calls.append(kwargs) or object(),
        )

        self.assertEqual(store.compatibility_profile, "aws")
        self.assertEqual(store.bucket, "recall-archil-test-synthetic")
        self.assertEqual(calls[0]["region_name"], "us-west-2")
        self.assertEqual(
            calls[0]["endpoint_url"],
            "https://s3.us-west-2.amazonaws.com",
        )

    def test_aws_environment_rejects_cross_region_endpoint(self):
        environment = {
            "RECALL_ARCHIVE_BACKEND": "s3",
            "RECALL_ARCHIVE_BUCKET": "recall-archil-test-synthetic",
            "RECALL_ARCHIVE_ENDPOINT_URL": "https://s3.us-east-1.amazonaws.com",
            "RECALL_ARCHIVE_REGION": "us-west-2",
            "RECALL_ARCHIVE_ACCESS_KEY_ID": "synthetic-access-id",
            "RECALL_ARCHIVE_SECRET_ACCESS_KEY": "synthetic-secret",
            "RECALL_ARCHIVE_NAMESPACE_KEY": base64.b64encode(b"s" * 32).decode(),
        }

        with self.assertRaisesRegex(ValueError, "S3 endpoint"):
            build_archive_store(
                environment,
                client_factory=lambda **_: object(),
            )

    def test_unversioned_aws_environment_is_explicit(self):
        environment = {
            "RECALL_ARCHIVE_BACKEND": "s3-unversioned",
            "RECALL_ARCHIVE_BUCKET": "recall-archil-test-synthetic",
            "RECALL_ARCHIVE_ENDPOINT_URL": "https://s3.us-west-2.amazonaws.com",
            "RECALL_ARCHIVE_REGION": "us-west-2",
            "RECALL_ARCHIVE_ACCESS_KEY_ID": "synthetic-access-id",
            "RECALL_ARCHIVE_SECRET_ACCESS_KEY": "synthetic-secret",
            "RECALL_ARCHIVE_NAMESPACE_KEY": base64.b64encode(b"s" * 32).decode(),
        }

        store = build_archive_store(
            environment,
            client_factory=lambda **_: object(),
        )

        self.assertEqual(store.compatibility_profile, "aws-unversioned")

    def test_evidence_archive_uses_only_its_separate_configuration(self):
        calls = []
        environment = {
            "RECALL_EVIDENCE_ARCHIVE_BACKEND": "r2",
            "RECALL_EVIDENCE_ARCHIVE_BUCKET": "recall-evidence-synthetic",
            "RECALL_EVIDENCE_ARCHIVE_ENDPOINT_URL": (
                "https://0123456789abcdef0123456789abcdef."
                "r2.cloudflarestorage.com"
            ),
            "RECALL_EVIDENCE_ARCHIVE_REGION": "auto",
            "RECALL_EVIDENCE_ARCHIVE_ACCESS_KEY_ID": "evidence-access-id",
            "RECALL_EVIDENCE_ARCHIVE_SECRET_ACCESS_KEY": "evidence-secret",
            "RECALL_EVIDENCE_ARCHIVE_NAMESPACE_KEY": base64.b64encode(
                b"e" * 32
            ).decode(),
            "RECALL_ARCHIVE_BUCKET": "recall-raw-must-not-be-used",
        }

        store = build_evidence_archive_store(
            environment,
            client_factory=lambda **kwargs: calls.append(kwargs) or object(),
        )

        self.assertEqual(store.bucket, "recall-evidence-synthetic")
        self.assertEqual(calls[0]["aws_access_key_id"], "evidence-access-id")
        self.assertNotIn("recall-raw-must-not-be-used", repr(calls))

    def test_probe_proves_write_replay_read_delete_and_absence(self):
        client = FakeR2()
        store = S3ArchiveStore(
            bucket="recall-synthetic",
            endpoint_url=(
                "https://0123456789abcdef0123456789abcdef."
                "r2.cloudflarestorage.com"
            ),
            namespace_key=b"k" * 32,
            client=client,
            compatibility_profile="r2",
        )

        self.assertEqual(
            probe_archive(store),
            {
                "status": "ok",
                "backend": "s3",
                "write": True,
                "replay": True,
                "read": True,
                "delete": True,
                "absent": True,
            },
        )
        self.assertEqual(client.objects, {})


if __name__ == "__main__":
    unittest.main()
