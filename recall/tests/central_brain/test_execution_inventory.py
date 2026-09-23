"""Large authorized inventories travel as data, not shell arguments."""
import hashlib
import json
import unittest
from unittest import mock

from recall_server.deep_inspection import AgentExecObject, ArchilDeepInspector
from recall_server.deep_inspection_runtime import build_deep_inspector


class InventoryArchive:
    def __init__(self):
        self.writes = []
        self.deleted = []

    def put_raw(self, **request):
        self.writes.append(request)
        digest = hashlib.sha256(request["payload"]).hexdigest()
        return {"object_key": "objects/aa/" + digest, "content_sha256": digest}

    def delete_raw(self, reference):
        self.deleted.append(reference)
        return True


class ExecutionInventoryTests(unittest.TestCase):
    def test_large_scan_preserves_every_independent_hash_and_alias(self):
        archive = InventoryArchive()
        objects = tuple(AgentExecObject(
            "objects/aa/" + hashlib.sha256(f"key-{i}".encode()).hexdigest(),
            hashlib.sha256(f"content-{i}".encode()).hexdigest()) for i in range(6000))
        tool = AgentExecObject("objects/bb/" + "b" * 64, "c" * 64)
        aliases = {o.object_key: f"s1000/2026-09/passages-part-{i:05}.parquet"
                   for i, o in enumerate(objects)}
        transport = mock.Mock()
        def post(**kwargs):
            self.assertEqual(len(archive.writes), 1)
            self.assertEqual(archive.deleted, [])
            inventory = json.loads(archive.writes[0]["payload"])
            self.assertEqual(inventory["datasets"], aliases)
            self.assertEqual(inventory["objects"], [
                dict(object_key=o.object_key, content_sha256=o.content_sha256)
                for o in (*objects, tool)])
            self.assertLess(len(kwargs["body"]["command"].encode()), 100_000)
            self.assertTrue(kwargs["body"]["disks"]["evidence"]["readOnly"])
            return {"success": True, "data": {"stdout": "6000", "stderr": "",
                    "exitCode": 0, "timing": {}}}
        transport.post.side_effect = post
        inspector = ArchilDeepInspector(api_key="synthetic", disk_id="dsk-0123456789abcdef",
            region="aws-us-west-2", duckdb_tool=tool, transport=transport,
            execution_archive=archive)
        result = inspector.execute_scan(tenant_id="tenant:test", program="true",
            objects=objects, dataset_aliases=aliases, timeout_seconds=60)
        self.assertEqual(result["stdout"], "6000")
        self.assertEqual(len(archive.deleted), 1)
        self.assertEqual(transport.post.call_count, 1)

    def test_failed_transport_cleans_only_its_own_manifest(self):
        archive = InventoryArchive()
        transport = mock.Mock()
        transport.post.side_effect = TimeoutError()
        inspector = ArchilDeepInspector(api_key="synthetic", disk_id="dsk-0123456789abcdef",
            region="aws-us-west-2", transport=transport, execution_archive=archive)
        obj = AgentExecObject("objects/aa/" + "a" * 64, "b" * 64)
        for _ in range(2):
            with self.assertRaises(TimeoutError):
                inspector.execute(tenant_id="tenant:test", program="true", objects=(obj,),
                    record_spans={"ldoc_" + "a" * 32: ((0, 1),)},
                    routing_receipts={"ldoc_" + "a" * 32: ()}, timeout_seconds=10)
        self.assertEqual(len(archive.deleted), 2)
        self.assertNotEqual(archive.writes[0]["native_id"], archive.writes[1]["native_id"])

    def test_runtime_reuses_existing_evidence_archive(self):
        projection = mock.Mock()
        inspector = build_deep_inspector(projection, {
            "RECALL_DEEP_INSPECTOR": "archil", "ARCHIL_API_KEY": "synthetic",
            "RECALL_ARCHIL_DISK_ID": "dsk-0123456789abcdef",
            "RECALL_ARCHIL_REGION": "aws-us-west-2"})
        self.assertIs(inspector.execution_archive, projection.archive)
