"""Read-only planning proves existing positions without new body copies."""
import copy
import math
import time
import unittest
from unittest.mock import patch

from tests.central_brain import test_chunk_bodies as fixtures
from tests.central_brain.test_chunk_bodies import digest
from recall_server.db import SearchDeadlineExceeded
from recall_server.locator_backfill_plan import LocatorPlanError, PlanLimits, plan_parent


class LocatorPlanTests(unittest.TestCase):
    def fixture(self):
        base = fixtures.ChunkBodyTests()
        row = base.document('native', 'exact body')
        store, archive = base.fixture([row], [base.record(row, 'exact body', 0)])
        manifest = dict(row.pop('manifest'), tenant_id='tenant', source_id='source',
                        native_parent_id='session', receipt_count=1)
        parts = row.pop('parts')
        row['pg_body_bytes'] = 10
        snapshot = dict(manifest=manifest, parts=parts, queue=None, documents=[row])
        return store, archive, snapshot

    def plan(self, store, archive, snapshot, **options):
        with patch('recall_server.locator_backfill_plan._snapshot', return_value=snapshot):
            return plan_parent(store, archive, tenant_id='tenant', source_id='source',
                               native_parent_id='session', **options)

    def test_null_position_verified_from_existing_part_without_writes(self):
        store, archive, snapshot = self.fixture()
        result = self.plan(store, archive, snapshot)
        self.assertEqual(result['changes'], [dict(document_id=snapshot['documents'][0]['document_id'],
                                                record_ordinal=0, record_count=1)])
        self.assertEqual(result['archive_gets'], 1)
        self.assertNotIn('exact body', str(result))
        self.assertEqual(store.calls, [])

    def test_publication_race_discards_entire_plan(self):
        store, archive, snapshot = self.fixture()
        after = copy.deepcopy(snapshot)
        after['manifest']['revision'] += 1
        with patch('recall_server.locator_backfill_plan._snapshot', side_effect=[snapshot, after]):
            with self.assertRaisesRegex(LocatorPlanError, 'catalog_changed'):
                plan_parent(store, archive, tenant_id='tenant', source_id='source', native_parent_id='session')

    def test_corruption_and_current_hash_mismatch_fail_closed(self):
        for mutation in ('part', 'document', 'chunk'):
            store, archive, snapshot = self.fixture()
            if mutation == 'part':
                archive.payloads[snapshot['parts'][0]['object_key']] = b'bad'
            elif mutation == 'document':
                snapshot['documents'][0]['text_sha256'] = digest('wrong')
            else:
                snapshot['documents'][0]['chunks'][0]['text_sha256'] = digest('wrong')
            with self.subTest(mutation=mutation), self.assertRaises(LocatorPlanError):
                self.plan(store, archive, snapshot)

    def test_unchanged_positions_are_not_proposed_again(self):
        store, archive, snapshot = self.fixture()
        snapshot['documents'][0].update(body_record_ordinal=0, body_record_count=1)
        result = self.plan(store, archive, snapshot)
        self.assertEqual(result['changes'], [])
        self.assertEqual(result['unchanged_locators'], 1)
        snapshot['documents'][0]['body_record_ordinal'] = 9
        with self.assertRaisesRegex(LocatorPlanError, 'existing_position_invalid'):
            self.plan(store, archive, snapshot)

    def test_unsupported_and_noncurrent_bodies_never_become_candidates(self):
        for reason in ('oversized', 'structural', 'not_current'):
            store, archive, snapshot = self.fixture()
            if reason == 'oversized':
                snapshot['documents'][0]['raw_media_type'] = 'application/vnd.recall.oversized-record+gzip'
            elif reason == 'structural':
                snapshot['documents'][0]['structural_types'] = ['token_count']
            else:
                snapshot['documents'] = []
            with self.subTest(reason=reason):
                result = self.plan(store, archive, snapshot)
                self.assertEqual(result['changes'], [])
                self.assertEqual(result['excluded'], {reason: 1})

    def test_archive_byte_budget_rejects_before_fetch(self):
        store, archive, snapshot = self.fixture()
        with self.assertRaisesRegex(LocatorPlanError, 'archive_budget_exceeded'):
            self.plan(store, archive, snapshot, limits=PlanLimits(max_bytes=1))
        self.assertEqual(archive.calls, [])

    def test_failed_get_still_counts_attempted_archive_cost(self):
        store, archive, snapshot = self.fixture()
        archive.payloads[snapshot['parts'][0]['object_key']] = OSError('private internal address')
        with self.assertRaises(LocatorPlanError) as raised:
            self.plan(store, archive, snapshot)
        self.assertEqual(raised.exception.archive_gets, 1)
        self.assertEqual(raised.exception.archive_bytes, snapshot['parts'][0]['size_bytes'])
        self.assertNotIn('private', str(raised.exception))

    def test_deadline_remains_deadline_and_invalid_limits_are_rejected(self):
        store, archive, snapshot = self.fixture()
        with self.assertRaises(SearchDeadlineExceeded):
            self.plan(store, archive, snapshot, deadline_at=time.monotonic() - 1)
        self.assertEqual(archive.calls, [])
        for deadline in (True, math.nan, math.inf, time.monotonic() + 4000):
            with self.subTest(deadline=deadline), self.assertRaisesRegex(LocatorPlanError, 'request_invalid'):
                self.plan(store, archive, snapshot, deadline_at=deadline)
        for values in ({'max_bytes': 0}, {'max_documents': 20001}, {'max_chunks': True}):
            with self.subTest(values=values), self.assertRaisesRegex(LocatorPlanError, 'request_invalid'):
                PlanLimits(**values)

    def test_part_and_whole_manifest_hashes_are_both_required(self):
        store, archive, snapshot = self.fixture()
        snapshot['manifest']['document_content_sha256'] = '0' * 64
        with self.assertRaisesRegex(LocatorPlanError, 'part_invalid'):
            self.plan(store, archive, snapshot)
        snapshot['parts'][0]['revision'] += 1
        archive.calls.clear()
        with self.assertRaisesRegex(LocatorPlanError, 'catalog_invalid'):
            self.plan(store, archive, snapshot)
        self.assertEqual(archive.calls, [])


class LocatorPlanCliTests(unittest.TestCase):
    def module(self):
        import importlib.util
        from pathlib import Path
        path = Path(__file__).resolve().parents[2] / 'server/scripts/plan_body_locators.py'
        spec = importlib.util.spec_from_file_location('locator_plan_cli', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_report_is_private_exclusive_and_stdout_contains_only_counts(self):
        from contextlib import redirect_stdout, redirect_stderr
        from io import StringIO
        import json
        import os
        from pathlib import Path
        import tempfile
        from unittest.mock import MagicMock
        module = self.module()
        parent = dict(source_id='source:private', native_parent_id='private-parent')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'plan.json'
            argv = ['plan', '--tenant', 'tenant:private', '--output', str(path)]
            result = dict(status='verified_dry_run', changes=[dict(document_id='doc_private', record_ordinal=0,
                          record_count=1)], archive_gets=1, archive_bytes=100)
            output = StringIO()
            with patch('sys.argv', argv), patch.dict(os.environ, RECALL_DATABASE_URL='postgresql://synthetic'), \
                 patch.object(module, 'BrainStore', return_value=MagicMock()), \
                 patch.object(module, 'build_evidence_archive_store', return_value=object()), \
                 patch.object(module, 'select_parents', return_value=([parent], False)), \
                 patch.object(module, 'plan_parent', return_value=result), redirect_stdout(output):
                self.assertEqual(module.main(), 0)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(json.loads(path.read_text())['parents'][0]['changes'], result['changes'])
            self.assertNotIn('private', output.getvalue())
            original = path.read_bytes()
            with patch('sys.argv', argv), patch.dict(os.environ, RECALL_DATABASE_URL='postgresql://synthetic'), \
                 redirect_stderr(StringIO()), self.assertRaises(SystemExit):
                module.main()
            self.assertEqual(path.read_bytes(), original)

    def test_no_apply_flag_exists(self):
        from contextlib import redirect_stderr
        from io import StringIO
        module = self.module()
        with patch('sys.argv', ['plan', '--tenant', 'tenant', '--output', '/unused', '--apply']), \
             redirect_stderr(StringIO()), self.assertRaises(SystemExit):
            module.main()
