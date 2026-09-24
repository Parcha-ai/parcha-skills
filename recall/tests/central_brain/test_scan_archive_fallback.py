"""Real generated bootstrap, synthetic archive responses, no network."""
import contextlib
import hashlib
import io
import json
from pathlib import Path
import shlex
import tempfile
import unittest
from unittest import mock
import urllib.error
from recall_server.deep_inspection import AgentExecObject, _agent_exec_command

class ScanArchiveFallbackTests(unittest.TestCase):
    def bootstrap(self, *, body=b'PAR1synthetic', returned=None, present=False, status=None, primary_body=None,
                  family='documents'):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        (self.root / 'tmp/recall-agent').mkdir(parents=True)
        digest = hashlib.sha256(body).hexdigest()
        key = 'objects/aa/' + 'a' * 64
        primary = self.root / 'mnt/archil/evidence' / key
        primary.parent.mkdir(parents=True)
        if present:
            primary.write_bytes(body if primary_body is None else primary_body)
        self.fallback = self.root / 'tmp/recall-agent/fallback' / key
        inventory = json.dumps(dict(objects=[dict(object_key=key, content_sha256=digest)],
            datasets={key: f's1/2026-09/{family}-part-00000.parquet'},
            fallbacks={key: dict(url='https://synthetic.invalid/PRIVATE-CAPABILITY', size_bytes=len(body))})).encode()
        ref = AgentExecObject('objects/bb/' + 'b' * 64, hashlib.sha256(inventory).hexdigest())
        command = _agent_exec_command(program='true', objects=(), document_aliases={},
            record_spans={}, routing_receipts={}, timeout_seconds=10, inventory=ref,
            inventory_url='https://synthetic.invalid/inventory', inventory_size_bytes=len(inventory))
        start = command.index('python3 -c ')
        argv = shlex.split(command[start:command.index('\npython3 -c ', start)])
        self.stderr = io.StringIO()
        self.calls = []
        def opened(url, *, timeout):
            self.calls.append(url)
            if url.endswith('/inventory'):
                return io.BytesIO(inventory)
            if status:
                raise urllib.error.HTTPError(url, status, 'PRIVATE-CAPABILITY', {}, None)
            return io.BytesIO(body if returned is None else returned)
        original_path = type(self.root)
        original_resolve, original_is_file = original_path.resolve, original_path.is_file
        def mapped(*args):
            path = original_path(*args)
            return self.root / str(path).lstrip('/') if path.is_absolute() and not path.is_relative_to(self.root) else path

        def no_archil_probe(method):
            def checked(path, *args, **kwargs):
                if family != 'records' and path.is_relative_to(self.root / 'mnt/archil/evidence'):
                    raise AssertionError('bootstrap must download selected catalog bytes without probing Archil')
                return method(path, *args, **kwargs)
            return checked

        with (mock.patch('urllib.request.build_opener') as opener,
              mock.patch('pathlib.Path', side_effect=mapped),
              mock.patch.object(original_path, 'resolve', no_archil_probe(original_resolve)),
              mock.patch.object(original_path, 'is_file', no_archil_probe(original_is_file)),
              mock.patch('sys.argv', ['bootstrap', argv[3]]), contextlib.redirect_stderr(self.stderr)):
            opener.return_value.open.side_effect = opened
            exec(compile(argv[2], '<bootstrap>', 'exec'), {})
            for call in opener.call_args_list:
                self.assertIsNone(call.args[0].redirect_request(None, None, None, None, None, None))

    def test_missing_archil_uses_exact_archive_bytes(self):
        body = b'PAR1' + b'x' * (1024 * 1024 + 7)
        self.bootstrap(body=body)
        self.assertEqual(self.fallback.read_bytes(), body)
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(self.stderr.getvalue(), '')

    def test_visible_archil_does_not_skip_verified_catalog_download(self):
        for family in ('documents', 'passages', 'actors'):
            with self.subTest(family=family):
                self.bootstrap(present=True, primary_body=b'stale Archil bytes', family=family)
                self.assertEqual(len(self.calls), 2)
                self.assertEqual(self.fallback.read_bytes(), b'PAR1synthetic')

    def test_visible_records_remain_lazy_without_downloading(self):
        self.bootstrap(present=True, family='records')
        self.assertEqual(len(self.calls), 1)
        self.assertFalse(self.fallback.exists())

    def test_missing_records_download_verified_bytes(self):
        self.bootstrap(family='records')
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(self.fallback.read_bytes(), b'PAR1synthetic')

    def test_missing_archive_stays_unavailable(self):
        self.bootstrap(status=404)
        self.assertFalse(self.fallback.exists())
        self.assertEqual(self.stderr.getvalue(), '')

    def test_corrupt_or_wrong_size_bytes_fail_closed_without_capability(self):
        for returned in (b'', b'x' * len(b'PAR1synthetic'), b'PAR1syntheticEXTRA'):
            with self.subTest(returned=returned):
                with self.assertRaises(SystemExit) as caught:
                    self.bootstrap(returned=returned)
                self.assertEqual(caught.exception.code, 66)
                self.assertFalse(self.fallback.exists())
                self.assertNotIn('PRIVATE-CAPABILITY', self.stderr.getvalue())

    def test_provider_failure_never_exposes_url(self):
        with self.assertRaises(SystemExit) as caught:
            self.bootstrap(status=403)
        self.assertEqual(caught.exception.code, 66)
        self.assertEqual(self.stderr.getvalue(), '')

    def test_corrupt_download_does_not_fall_back_to_visible_archil(self):
        with self.assertRaises(SystemExit) as caught:
            self.bootstrap(present=True, returned=b'x' * len(b'PAR1synthetic'))
        self.assertEqual(caught.exception.code, 66)
        self.assertFalse(self.fallback.exists())
        self.assertNotIn('PRIVATE-CAPABILITY', self.stderr.getvalue())

    def test_trusted_catalog_signer_binds_scope_without_head(self):
        from recall_server.archive import S3ArchiveStore, ArchiveNotFound
        from recall_server.archive_runtime import BotoS3Client
        from test_archive_runtime import FakeR2
        client = FakeR2()
        client.generate_presigned_url = mock.Mock(return_value='https://synthetic.invalid/read')
        store = S3ArchiveStore(bucket='synthetic-evidence',
            endpoint_url='https://' + 'a' * 32 + '.r2.cloudflarestorage.com',
            namespace_key=b'x' * 32, client=BotoS3Client(client), compatibility_profile='r2')
        ref = store.put_raw(tenant_id='tenant:test', source_id='source:test', native_id='test',
            payload=b'PAR1synthetic', media_type='application/octet-stream', created_at='2026-09-23T00:00:00Z')
        with mock.patch.object(client, 'head_object', side_effect=AssertionError('catalog must not HEAD')):
            self.assertEqual(store.read_catalog_url(ref, tenant_id='tenant:test', source_id='source:test', expires_in=300),
                'https://synthetic.invalid/read')
            for tenant, source in [('tenant:other', 'source:test'), ('tenant:test', 'source:other')]:
                with self.assertRaises(ArchiveNotFound):
                    store.read_catalog_url(ref, tenant_id=tenant, source_id=source, expires_in=300)
        self.assertEqual(client.generate_presigned_url.call_count, 1)

    def test_bootstrap_to_stage_to_result_recovers_or_reports_missing_truthfully(self):
        from tests.central_brain.test_scan_manifest_staging import ScanManifestStagingTests
        from recall_server.deep_inspection import _execution_result
        cases = [(family, present, status)
                 for family in ('documents', 'passages', 'actors', 'records')
                 for present, status in ((False, None), (False, 404), (True, None), (True, 404))]
        for family, present, status in cases:
            with self.subTest(family=family, present=present, status=status):
                self.bootstrap(status=status, present=present, family=family,
                    primary_body=b'stale Archil bytes'
                    if family != 'records' and present and status is None else None)
                helper = ScanManifestStagingTests()
                helper.root = self.root
                helper.objects, helper.mounts, helper.reads, helper.walks = [], [], [], []
                helper.data = AgentExecObject('objects/aa/' + 'a' * 64, hashlib.sha256(b'PAR1synthetic').hexdigest())
                helper.objects.append(helper.data)
                helper.tool = helper.object(b'synthetic duckdb executable')
                # Preserve the signed inventory bootstrap fetched, adding the
                # tool exactly as the production inventory builder does.
                inventory_path = self.root / 'tmp/recall-agent/inventory.json'
                inventory = json.loads(inventory_path.read_bytes())
                inventory['objects'].append(dict(object_key=helper.tool.object_key, content_sha256=helper.tool.content_sha256))
                raw = json.dumps(inventory).encode()
                digest = hashlib.sha256(raw).hexdigest()
                ref = AgentExecObject('objects/bb/' + digest, digest)
                archived_inventory = self.root / 'mnt/archil/evidence' / ref.object_key
                archived_inventory.parent.mkdir(parents=True, exist_ok=True)
                archived_inventory.write_bytes(raw)
                stderr = helper.stage(inventory=ref, datasets=inventory['datasets'], allow_missing=True)
                result = _execution_result(dict(stdout='[]', stderr=stderr, exitCode=0, timing={}))
                available = status is None or present
                self.assertEqual(result['complete'], available)
                self.assertEqual(result['objects_unavailable'], int(not available))
                staged = self.root / 'tmp/recall-authorized' / helper.data.object_key
                if available:
                    self.assertEqual(staged.read_bytes(), b'PAR1synthetic')
                self.assertFalse(inventory_path.exists())
                self.assertFalse((self.root / 'tmp/recall-agent/fallback').exists())
                self.assertNotIn('PRIVATE-CAPABILITY', result['stderr'])

    def test_inventory_signs_only_selected_catalog_refs_and_rejects_mismatch(self):
        from tests.central_brain.test_execution_inventory import InventoryArchive
        from recall_server.deep_inspection import ArchilDeepInspector, DeepInspectionError
        archive = InventoryArchive()
        archive.read_catalog_url = mock.Mock(return_value='https://synthetic.invalid/PRIVATE-CAPABILITY')
        inspector = ArchilDeepInspector(api_key='synthetic', disk_id='dsk-0123456789abcdef',
            region='aws-us-west-2', execution_archive=archive)
        obj = AgentExecObject('objects/aa/' + 'a' * 64, 'b' * 64)
        tool = AgentExecObject('objects/cc/' + 'c' * 64, 'd' * 64)
        aliases = {obj.object_key: 's1/2026-09/documents-part-00000.parquet'}
        ref = dict(tenant_id='tenant:test', source_id='source:test', object_key=obj.object_key,
            content_sha256=obj.content_sha256, size_bytes=12)
        for bad in ({}, {obj.object_key: dict(ref, tenant_id='tenant:other')},
                    {obj.object_key: dict(ref, content_sha256='0' * 64)},
                    {obj.object_key: ref, tool.object_key: dict(ref, object_key=tool.object_key)}):
            with self.subTest(bad=bad), self.assertRaises(DeepInspectionError):
                with inspector._inventory('tenant:test', (obj, tool), aliases, bad):
                    self.fail('invalid catalog accepted')
        archive.read_catalog_url.assert_not_called()
        self.assertEqual(archive.writes, [])
        with inspector._inventory('tenant:test', (obj, tool), aliases, {obj.object_key: ref}):
            payload = json.loads(archive.writes[-1]['payload'])
            self.assertEqual(set(payload['fallbacks']), {obj.object_key})
        archive.read_catalog_url.assert_called_once_with(ref, tenant_id='tenant:test', source_id='source:test', expires_in=300)
        self.assertEqual(len(archive.deleted), 1)
