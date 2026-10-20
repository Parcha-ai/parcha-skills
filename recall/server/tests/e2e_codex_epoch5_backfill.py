#!/usr/bin/env python3
"""Native epoch scheduling with unchanged source and publication proofs."""
from contextlib import contextmanager
from dataclasses import asdict
import json
import time
import unittest
from unittest.mock import patch

from e2e_codex_tool_publication import CodexToolPublication
from contracts.source_units import resolve_codex_tool_source_unit
from recall_server import passage_append, tool_passage_projection


class CodexEpoch5Backfill(CodexToolPublication):
    def setUp(self):
        super().setUp()
        # These fixtures compare the historical epoch 4 and epoch 5 contracts.
        epoch = patch.object(passage_append, 'CODEX_CHECKPOINT_VERSION', 5)
        epoch.start()
        self.addCleanup(epoch.stop)

    def reset_fixture(self):
        # Each subcase owns its pool/archive; release them before the next one.
        self.doCleanups()
        self.setUp()

    @contextmanager
    def old_writer(self):
        def old_resolve(raw, unit):
            if unit.startswith('codex.tool-result.block-text.v1#'):
                raise ValueError('tool_source_unit_invalid')
            return resolve_codex_tool_source_unit(raw, unit)
        with patch.object(passage_append, 'CODEX_CHECKPOINT_VERSION', 4), \
                patch.object(tool_passage_projection, 'resolve_codex_tool_source_unit',
                             old_resolve):
            yield

    def clean_source(self, *, visible=False, empty=False):
        self.ingest('string', result=True, value=dict(type='response_item', payload=dict(
            type='function_call_output', output='' if empty else 'Existing exact result.')))
        self.ingest('list', second=1, value=dict(type='response_item', payload=dict(
            type='custom_tool_call_output', output=[
                dict(type='input_text', text=' ' if empty else 'Existing all-text list.')])))
        self.ingest('unknown', second=2, value=dict(type='response_item',
            payload=dict(type='unchanged_future_event', output='Still unsupported.')))
        if visible:
            self.ingest('visible', second=3, value=dict(type='response_item', payload=dict(
                type='message', role='user', content=[
                    dict(type='input_text', text='Visible original context.')])))

    def seed(self, bounded):
        if not bounded:
            return self.passages.seed_backfill(tenant_id=self.tenant)
        return self.passages.seed_backfill_page(tenant_id=self.tenant,
            source_id=self.source, after=None,
            upper=self.document()['logical_document_id'], limit=1,
            max_source_bytes=1024*1024, deadline_at=time.monotonic()+3)['queued']

    def test_old4_new5_native_outputs_match_for_complete_zero_refusals(self):
        for visible, empty in ((True, False), (False, False), (False, True)):
            with self.subTest(visible=visible, empty=empty):
                self.reset_fixture()
                self.clean_source(visible=visible, empty=empty)
                with self.old_writer():
                    old = self.prepare()
                new = self.passages._prepare(old.candidate)
                try:
                    self.assertEqual([asdict(p) for p in old.passages],
                                     [asdict(p) for p in new.passages])
                    self.assertEqual(old.extraction_coverage, new.extraction_coverage)
                    self.assertNotIn('source_unit_refused',
                                     old.extraction_coverage['dispositions'])
                    self.assertGreater(old.extraction_coverage['dispositions'][
                        'unsupported_provider_event'], 0)
                    self.assertEqual(old.dense_message_count, new.dense_message_count)
                    self.assertEqual(old.dense_message_bytes, new.dense_message_bytes)
                    self.assertEqual(old.append_checkpoint['version'], 4)
                    self.assertEqual(new.append_checkpoint['version'], 5)
                    self.assertEqual(self.passages._commit(old)['status'], 'committed')
                    self.assertEqual(self.document()['extraction_coverage'],
                                     old.extraction_coverage)
                finally:
                    old.close()
                    new.close()

    def test_both_seeds_skip_only_unchanged_complete_zero_refusals(self):
        for bounded in (False, True):
            for visible, empty in ((True, False), (False, False), (False, True)):
                with self.subTest(bounded=bounded, visible=visible, empty=empty):
                    self.reset_fixture()
                    self.clean_source(visible=visible, empty=empty)
                    with self.old_writer():
                        self.assertEqual(self.project()['documents'], 1)
                    before = self.document()
                    with patch.object(self.passages.logical_projection, 'read_part',
                            wraps=self.passages.logical_projection.read_part) as archive_read:
                        started = time.monotonic()
                        self.assertEqual(self.seed(bounded), 0)
                        elapsed_ms = (time.monotonic()-started)*1000
                    self.assertEqual(archive_read.call_count, 0)
                    print(json.dumps(dict(stage='unchanged_epoch4_seed', bounded=bounded,
                        visible=visible, empty=empty, elapsed_ms=round(elapsed_ms, 3),
                        archive_part_reads=archive_read.call_count, queued=0)), flush=True)
                    self.assertEqual(self.document(), before)
                    self.assertEqual(self.passages._pending(tenant_id=self.tenant, limit=1), ())
                    # No checkpoint promotion or relaxed append proof: a real
                    # source update still follows the original full-rebuild path.
                    self.ingest('later', result=True, second=4)
                    prepared = self.prepare()
                    try:
                        self.assertIsNone(prepared.previous_checkpoint)
                        self.assertGreater(prepared.archive_parts_read, 0)
                        self.assertEqual(self.passages._commit(prepared)['status'], 'committed')
                        self.assertEqual(self.document()['append_checkpoint']['version'], 5)
                    finally:
                        prepared.close()

    def test_unknown_coverage_and_binding_drift_remain_rebuild_obligations(self):
        cases = ('null', 'scalar', 'array', 'contract', 'missing_counter', 'string_counter',
                 'missing_dispositions', 'unknown_disposition', 'positive_refusal',
                 'checkpoint_coverage', 'record_count', 'manifest', 'source_hash',
                 'desired_mode', 'zero_events', 'extra_coverage_key', 'checkpoint_revision',
                 'logical_pending')
        for bounded in (False, True):
            for case in cases:
                with self.subTest(bounded=bounded, case=case):
                    self.reset_fixture()
                    self.clean_source()
                    with self.old_writer():
                        self.assertEqual(self.project()['documents'], 1)
                    row = self.document()
                    coverage = row['extraction_coverage']
                    checkpoint = row['append_checkpoint']
                    if case == 'null':
                        coverage = None
                    elif case == 'scalar':
                        coverage = 'not coverage'
                    elif case == 'array':
                        coverage = ['not coverage']
                    elif case == 'contract':
                        coverage['contract'] = 'unknown'
                    elif case == 'missing_counter':
                        del coverage['events']
                    elif case == 'string_counter':
                        coverage['events'] = '3'
                    elif case == 'missing_dispositions':
                        del coverage['dispositions']
                    elif case == 'unknown_disposition':
                        coverage['dispositions']['unknown_future_disposition'] = 1
                    elif case == 'positive_refusal':
                        coverage['dispositions']['source_unit_refused'] = 1
                    elif case == 'zero_events':
                        coverage['events'] = 0
                    elif case == 'extra_coverage_key':
                        coverage['unknown_future_field'] = 0
                    if case != 'checkpoint_coverage':
                        checkpoint['tool_groups']['coverage'] = coverage
                    if case == 'record_count':
                        checkpoint['tool_groups']['record_count'] += 1
                    if case == 'manifest':
                        checkpoint['manifest_sha256'] = 'f'*64
                    if case == 'checkpoint_revision':
                        checkpoint['revision'] += 1
                    if case == 'logical_pending':
                        self.ingest('pending-source-change', result=True, second=4)
                    with self.store.connect() as connection:
                        connection.execute("""UPDATE canonical_passage_documents
                            SET extraction_coverage=%s::jsonb,append_checkpoint=%s::jsonb,
                                source_document_sha256=%s,requested_extraction_version=%s
                            WHERE tenant_id=%s AND source_id=%s""",
                            (json.dumps(coverage), json.dumps(checkpoint),
                             'f'*64 if case == 'source_hash' else row['source_document_sha256'],
                             'legacy' if case == 'desired_mode' else row['requested_extraction_version'],
                             self.tenant, self.source))
                    if case == 'checkpoint_coverage':
                        with self.store.connect() as connection:
                            connection.execute("""UPDATE canonical_passage_documents
                                SET extraction_coverage=jsonb_set(extraction_coverage,
                                    '{events}',to_jsonb(999)) WHERE tenant_id=%s""",
                                (self.tenant,))
                    self.assertEqual(self.seed(bounded), 1)

    def test_explicit_zero_is_not_permission_to_skip_a_future_epoch(self):
        for bounded in (False, True):
            with self.subTest(bounded=bounded):
                self.reset_fixture()
                self.clean_source()
                with self.old_writer():
                    self.assertEqual(self.project()['documents'], 1)
                row = self.document()
                row['extraction_coverage']['dispositions']['source_unit_refused'] = 0
                row['append_checkpoint']['tool_groups']['coverage'] = row['extraction_coverage']
                with self.store.connect() as connection:
                    connection.execute("""UPDATE canonical_passage_documents SET
                        extraction_coverage=%s::jsonb,append_checkpoint=%s::jsonb
                        WHERE tenant_id=%s AND source_id=%s""",
                        (json.dumps(row['extraction_coverage']), json.dumps(row['append_checkpoint']),
                         self.tenant, self.source))
                self.assertEqual(self.seed(bounded), 0)
                with patch.object(passage_append, 'CODEX_CHECKPOINT_VERSION', 6):
                    self.assertEqual(self.seed(bounded), 1)


if __name__ == '__main__':
    suite = unittest.TestSuite(CodexEpoch5Backfill(name) for name in
        CodexEpoch5Backfill.__dict__ if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
