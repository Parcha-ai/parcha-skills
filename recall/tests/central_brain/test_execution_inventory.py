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

    def read_raw_url(self, reference, *, expires_in):
        return "https://synthetic.invalid/inventory?signature=synthetic"

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

    def test_bootstrap_verifies_bytes_and_never_prints_read_capability(self):
        import contextlib
        import io
        import shlex
        from recall_server.deep_inspection import _agent_exec_command
        body = b'{"objects":[],"datasets":{}}'
        inventory = AgentExecObject('objects/aa/' + 'a' * 64, hashlib.sha256(body).hexdigest())
        command = _agent_exec_command(program='true', objects=(), document_aliases={},
            record_spans={}, routing_receipts={}, timeout_seconds=10, inventory=inventory,
            inventory_url='https://synthetic.invalid/PRIVATE-CAPABILITY', inventory_size_bytes=len(body))
        start = command.index('python3 -c ')
        argv = shlex.split(command[start:command.index('\npython3 -c ', start)])
        self.assertIn('NoRedirect', argv[2])
        self.assertNotIn('PRIVATE-CAPABILITY', command[command.index('\nunshare '):])
        for response_body in (body, body + b' ', body[:-1], b'x' * len(body)):
            with self.subTest(response_body=response_body):
                response = mock.MagicMock()
                response.__enter__.return_value.read.return_value = response_body
                opener = mock.Mock()
                opener.open.return_value = response
                path = mock.Mock()
                stderr = io.StringIO()
                with (mock.patch('urllib.request.build_opener', return_value=opener),
                      mock.patch('pathlib.Path', return_value=path),
                      mock.patch('sys.argv', ['bootstrap', argv[3]]),
                      contextlib.redirect_stderr(stderr)):
                    if response_body == body:
                        exec(compile(argv[2], '<bootstrap>', 'exec'), {})
                        path.write_bytes.assert_called_once_with(body)
                    else:
                        with self.assertRaises(SystemExit) as caught:
                            exec(compile(argv[2], '<bootstrap>', 'exec'), {})
                        self.assertEqual(caught.exception.code, 66)
                        path.write_bytes.assert_not_called()
                self.assertEqual(stderr.getvalue(), '')
                response.__enter__.return_value.read.assert_called_once_with(len(body) + 1)

    def test_read_url_is_scoped_to_its_archive_reference(self):
        from recall_server.archive import S3ArchiveStore, ArchiveNotFound
        from recall_server.archive_runtime import BotoS3Client
        from test_archive_runtime import FakeR2
        client = FakeR2()
        client.generate_presigned_url = mock.Mock(return_value='https://synthetic.invalid/read')
        store = S3ArchiveStore(bucket='synthetic-evidence',
            endpoint_url='https://' + 'a' * 32 + '.r2.cloudflarestorage.com',
            namespace_key=b'x' * 32, client=BotoS3Client(client), compatibility_profile='r2')
        reference = store.put_raw(tenant_id='tenant:test', source_id='source:test',
            native_id='manifest:test', payload=b'{}', media_type='application/json',
            created_at='2026-09-23T00:00:00Z')
        self.assertEqual(store.read_raw_url(reference, expires_in=300), 'https://synthetic.invalid/read')
        client.generate_presigned_url.assert_called_once_with('get_object',
            Params={'Bucket': 'synthetic-evidence', 'Key': reference['object_key']}, ExpiresIn=300)
        with self.assertRaises(ArchiveNotFound):
            store.read_raw_url(dict(reference, tenant_id='tenant:other'), expires_in=300)
        self.assertEqual(client.generate_presigned_url.call_count, 1)
