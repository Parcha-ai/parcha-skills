#!/usr/bin/env python3
"""Certified authority skips only the unchanged native receipt proof."""
import hashlib
from pathlib import Path
import time
import unittest
from unittest.mock import patch

from e2e_parent_membership_enrollment import ParentEnrollmentTests
from recall_server import parent_membership_read as reader
from recall_server import parent_membership_shadow as shadow
from recall_server.db import SearchDeadlineExceeded
from recall_server.receipt_membership import lookup_current_search_membership


class ParentMembershipFastReadTests(ParentEnrollmentTests):
    def test_catalog_pipeline_preserves_complete_rows_and_exits_before_membership(self):
        import psycopg
        from psycopg.pq import PipelineStatus
        from recall_server.db import BrainStore
        from recall_server import turbopuffer_retrieval as retrieval

        self.enroll()
        catalogs = (retrieval._SELECTED_AUTHORITY_SQL, retrieval._CERTIFICATE_PRESENCE_SQL)
        execute = psycopg.Connection.execute
        for enabled in (False, True):
            with self.subTest(certificates=enabled):
                if enabled:
                    self.enable()
                with patch.object(self.store, '_execute_bounded_pipeline',
                                  side_effect=BrainStore._execute_bounded):
                    baseline, _ = self.authorize()
                observed = []
                def traced(connection, sql, params=None, **kwargs):
                    if sql in catalogs or str(sql).startswith("SELECT set_config('statement_timeout'"):
                        observed.append((sql in catalogs, connection.info.pipeline_status != PipelineStatus.OFF))
                    return execute(connection, sql, params, **kwargs)
                def membership(*args, **kwargs):
                    self.assertEqual(kwargs['connection'].info.pipeline_status, PipelineStatus.OFF)
                    return lookup_current_search_membership(*args, **kwargs)
                with patch.object(psycopg.Connection, 'execute', traced), patch(
                    'recall_server.receipt_membership.lookup_current_search_membership', membership):
                    actual, _ = self.authorize()
                self.assertCountEqual(actual, baseline)
                index = next(i for i, (catalog, _) in enumerate(observed) if catalog)
                self.assertEqual(observed[index - 1:index + 1], [(False, True), (True, True)])

    def test_catalog_cancellation_rolls_back_before_native_proof_and_connection_reuse(self):
        from psycopg.pq import PipelineStatus, TransactionStatus
        with self.store.connect() as locked, locked.transaction():
            locked.execute('LOCK TABLE canonical_passages IN ACCESS EXCLUSIVE MODE')
            with self.store.connect() as connection:
                with patch('recall_server.receipt_membership.lookup_current_search_membership',
                           wraps=lookup_current_search_membership) as proof:
                    with self.assertRaises(SearchDeadlineExceeded):
                        self.retrieval._authorize_singletons(connection,
                            [(self.source, value) for value in self.ids], time.monotonic() + 0.05)
                proof.assert_not_called()
                self.assertEqual(connection.info.pipeline_status, PipelineStatus.OFF)
                self.assertEqual(connection.info.transaction_status, TransactionStatus.IDLE)
                self.assertEqual(connection.execute('SELECT 7 AS n').fetchone()['n'], 7)

    def setUp(self):
        super().setUp()
        self.store.verified_singleton_receipts = True

    def enable(self, epoch='synthetic-epoch-a'):
        self.retrieval.parent_membership_read = reader.ReadSettings(enabled=True, binding_epoch=epoch)

    def search(self):
        # An explicit caller budget exercises the 1s fallback reserve. Both
        # variants receive the same budget; retained short-deadline tests remain
        # unchanged and the separate reserve test verifies native-only behavior.
        return self.retrieval.search('exact deployment phrase', lexical_query='deployment phrase',
            since=None, until=None, limit=10, include_arms=True,
            deadline_at=time.monotonic() + 3)

    def authorize(self, deadline=None):
        stages = {}
        with self.store.connect() as connection:
            rows = self.retrieval._authorize_singletons(connection,
                [(self.source, value) for value in self.ids],
                time.monotonic() + 3 if deadline is None else deadline, stages=stages)
        return rows, stages

    def test_default_off_preserves_native_inputs_and_does_not_read_certificates(self):
        self.enroll()
        with patch.object(reader, 'partition', side_effect=AssertionError('disabled')) as certificate:
            with patch('recall_server.receipt_membership.lookup_current_search_membership', wraps=lookup_current_search_membership) as native:
                result = self.search()
        self.assertTrue(result['results'])
        certificate.assert_not_called()
        self.assertEqual(native.call_count, 1)
        self.assertEqual(set(native.call_args.kwargs['receipts']), {self.receipt, self.physical_receipt})

    def test_certified_search_matches_native_with_zero_native_receipt_calls(self):
        self.enroll(); before = self.search(); self.enable()
        with patch('recall_server.receipt_membership.lookup_current_search_membership', wraps=lookup_current_search_membership) as native:
            after = self.search()
        self.assertEqual(after['results'], before['results'])
        self.assertEqual(after['arms'], before['arms'])
        native.assert_not_called()
        stages = after['diagnostics']['authority_stages']
        self.assertEqual(stages['native_receipts'], 0)
        self.assertEqual(stages['parent_membership_read']['certified_passages'], len(self.ids))

    def test_missing_or_wrong_epoch_certificates_use_original_receipts_once(self):
        for epoch in ('synthetic-epoch-a', 'other-epoch'):
            self.enable(epoch)
            with patch('recall_server.receipt_membership.lookup_current_search_membership', wraps=lookup_current_search_membership) as native:
                rows, stages = self.authorize()
            self.assertEqual(len(rows), len(self.ids))
            self.assertEqual(native.call_count, 1)
            self.assertEqual(set(native.call_args.kwargs['receipts']), {self.receipt, self.physical_receipt})
            self.assertEqual(stages['parent_membership_read']['certified_passages'], 0)
            self.enroll()

    def test_mixed_certificates_send_only_deduplicated_fallback_union(self):
        # Two distinct real catalog passages with disjoint receipt sets; clone a
        # second fallback range to prove shared receipts are looked up only once.
        clones = tuple('psg_' + hashlib.sha256((self.ids[0] + suffix).encode()).hexdigest()[:32] for suffix in ('a', 'b'))
        with self.store.connect() as c:
            c.execute('UPDATE canonical_passages SET receipts=ARRAY[%s] WHERE tenant_id=%s', (self.receipt, self.tenant))
            for index, passage_id in enumerate(clones, 1):
                c.execute("""INSERT INTO canonical_passages(
                    tenant_id,source_id,logical_document_id,revision,passage_id,ordinal,
                    policy_fingerprint,target_tokens,overlap_tokens,token_count,
                    first_occurred_at,last_occurred_at,roles,receipts,spans,text_redacted,text_sha256)
                    SELECT tenant_id,source_id,logical_document_id,revision,%s,ordinal+%s,
                           policy_fingerprint,target_tokens,overlap_tokens,token_count,
                           first_occurred_at,last_occurred_at,roles,ARRAY[%s],spans,text_redacted,text_sha256
                    FROM canonical_passages WHERE tenant_id=%s AND passage_id=%s""",
                    (passage_id, index * 100, self.physical_receipt, self.tenant, self.ids[0]))
        self.enroll()
        self.ids = (*self.ids, *clones)
        native_rows, _ = self.authorize(); self.enable()
        with patch('recall_server.receipt_membership.lookup_current_search_membership', wraps=lookup_current_search_membership) as native:
            fast_rows, stages = self.authorize()
        # Private authority SQL is unordered; compare every field and duplicate.
        self.assertCountEqual(fast_rows, native_rows)
        self.assertEqual(native.call_count, 1)
        self.assertEqual(native.call_args.kwargs['receipts'], (self.physical_receipt,))
        self.assertEqual(stages['native_receipts'], 1)
        self.assertEqual(stages['unique_receipts'], 2)

    def test_edit_invalidates_certificate_and_stale_vendor_results(self):
        self.enroll(); self.enable(); self.edit()
        with patch('recall_server.receipt_membership.lookup_current_search_membership', wraps=lookup_current_search_membership) as native:
            result = self.search()
        self.assertEqual(result['results'], [])
        self.assertEqual(native.call_count, 1)

    def test_forget_invalidates_before_archive_cleanup_failure(self):
        self.enable()
        super().test_forget_revocation_precedes_archive_cleanup_failure()
        self.assertEqual(self.search()['results'], [])

    def test_source_recreation_does_not_reuse_old_certificates(self):
        self.enable()
        super().test_actual_source_deletion_recreation_retains_generation_and_loses_certificate()

    def test_live_source_grant_revocation_keeps_stale_vendor_out_of_scope(self):
        self.enroll(); self.enable()
        self.assertEqual(self.store.authorized_canonical_source_ids(self.tenant, self.principal), [self.source])
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND principal_id=%s', (self.tenant, self.principal))
        # The MCP authority owner resolves these grants for each request. The
        # retrieval owner receives that scope, never a certificate-derived one.
        self.retrieval.sources = self.store.authorized_canonical_source_ids(self.tenant, self.principal)
        self.assertEqual(self.retrieval.sources, [])
        self.assertEqual(self.search()['results'], [])

    def test_live_brain_grant_revocation_also_removes_scope(self):
        self.enroll(); self.enable()
        with self.store.connect() as c:
            c.execute('DELETE FROM brain_access_grants WHERE tenant_id=%s AND principal_id=%s', (self.tenant, self.principal))
        self.retrieval.sources = self.store.authorized_canonical_source_ids(self.tenant, self.principal)
        self.assertEqual(self.retrieval.sources, [])
        self.assertEqual(self.search()['results'], [])

    def test_metadata_failure_rolls_back_savepoint_and_runs_native_once(self):
        self.enroll(); before = self.search(); self.enable()
        execute = self.store._execute_bounded
        def failed(c, sql, values, deadline):
            if sql == reader._METADATA_SQL:
                return c.execute('SELECT * FROM deliberately_missing_fast_reader_table')
            return execute(c, sql, values, deadline)
        with patch.object(self.store, '_execute_bounded', side_effect=failed):
            with patch('recall_server.receipt_membership.lookup_current_search_membership', wraps=lookup_current_search_membership) as native:
                after = self.search()
        self.assertEqual(after['results'], before['results'])
        self.assertEqual(native.call_count, 1)
        self.assertEqual(after['diagnostics']['authority_stages']['parent_membership_read']['status'], 'unavailable')

    def test_expired_deadline_never_runs_certificate_or_membership(self):
        self.enroll(); self.enable()
        with patch.object(reader, 'partition', side_effect=AssertionError('expired')) as certificate:
            with self.assertRaises(SearchDeadlineExceeded):
                self.authorize(time.monotonic() - 1)
        certificate.assert_not_called()

    def test_metadata_timeout_retains_native_fallback_with_original_deadline(self):
        self.enroll(); self.enable(); execute = self.store._execute_bounded
        def delayed(c, sql, values, deadline):
            if sql == reader._METADATA_SQL:
                return execute(c, 'SELECT pg_sleep(0.2)', (), deadline)
            return execute(c, sql, values, deadline)
        with patch.object(self.store, '_execute_bounded', side_effect=delayed):
            with patch('recall_server.receipt_membership.lookup_current_search_membership', wraps=lookup_current_search_membership) as native:
                deadline = time.monotonic() + 3
                rows, stages = self.authorize(deadline)
                self.assertEqual(len(rows), len(self.ids))
                self.assertEqual(stages['parent_membership_read']['status'], 'deadline')
                self.assertEqual(native.call_count, 1)
                self.assertEqual(native.call_args.kwargs['deadline_at'], deadline)
        # The failed read did not poison the pool or alter any certificate.
        rows, stages = self.authorize()
        self.assertEqual(len(rows), len(self.ids)); self.assertEqual(stages['native_receipts'], 0)

    def test_short_remaining_budget_skips_metadata_and_preserves_native_proof(self):
        self.enroll(); self.enable(); execute = self.store._execute_bounded
        def guarded(c, sql, values, deadline):
            self.assertNotEqual(sql, reader._METADATA_SQL)
            return execute(c, sql, values, deadline)
        with patch.object(self.store, '_execute_bounded', side_effect=guarded):
            rows, stages = self.authorize(time.monotonic() + .5)
        self.assertEqual(len(rows), len(self.ids))
        self.assertEqual(stages['parent_membership_read']['status'], 'serving-reserve')
        self.assertGreater(stages['native_receipts'], 0)

    def test_changed_actual_owner_invalidates_certificate_and_uses_native(self):
        self.enroll(); self.enable()
        with self.store.connect() as c:
            c.execute("UPDATE canonical_events SET native_parent_id='other-parent' WHERE tenant_id=%s", (self.tenant,))
        with patch('recall_server.receipt_membership.lookup_current_search_membership', wraps=lookup_current_search_membership) as native:
            rows, stages = self.authorize()
        self.assertEqual(native.call_count, 1)
        self.assertEqual(stages['parent_membership_read']['certified_passages'], 0)
        self.assertEqual(len(rows), len(self.ids))

    def test_unknown_chunk_profile_stays_on_native_fail_closed_path(self):
        for receipt in (self.receipt, self.physical_receipt):
            self.admit_fixture(self.baseline(receipt)['document_id'])
        self.enroll(); self.enable()
        with self.store.connect() as c:
            c.execute('UPDATE canonical_documents SET chunk_receipt_profile=2 WHERE tenant_id=%s', (self.tenant,))
        with patch('recall_server.receipt_membership.lookup_current_search_membership', wraps=lookup_current_search_membership) as native:
            result = self.search()
        self.assertEqual(native.call_count, 1)
        self.assertEqual(result['results'], [])
        self.assertEqual(result['diagnostics']['authority_status'], 'unavailable')

    def test_missing_generation_and_mutated_bindings_fall_back(self):
        self.enroll(); self.enable()
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_parent_authority WHERE tenant_id=%s', (self.tenant,))
        rows, stages = self.authorize()
        self.assertEqual(len(rows), len(self.ids)); self.assertGreater(stages['native_receipts'], 0)

    def test_read_setting_requires_explicit_epoch(self):
        self.assertFalse(reader.ReadSettings().enabled)
        with self.assertRaises(ValueError):
            reader.ReadSettings(enabled=True)

    def test_concurrent_edit_keeps_one_snapshot_and_next_request_rejects(self):
        self.enroll(); before = self.search(); self.enable()
        original = reader.partition
        def edited(*args, **kwargs):
            self.edit()
            return original(*args, **kwargs)
        with patch.object(reader, 'partition', side_effect=edited):
            after = self.search()
        self.assertEqual(after['results'], before['results'])
        self.assertEqual(self.search()['results'], [])

    def test_concurrent_manifest_publication_uses_snapshot_then_falls_back(self):
        self.enroll(); self.enable(); original = reader.partition
        def published(*args, **kwargs):
            with self.store.connect() as c:
                c.execute('UPDATE canonical_evidence_documents SET manifest_content_sha256=%s WHERE tenant_id=%s', ('a' * 64, self.tenant))
            return original(*args, **kwargs)
        with patch.object(reader, 'partition', side_effect=published):
            rows, stages = self.authorize()
        self.assertEqual(len(rows), len(self.ids)); self.assertEqual(stages['native_receipts'], 0)
        rows, stages = self.authorize()
        self.assertEqual(len(rows), len(self.ids))
        if self.additive_membership_generation:
            self.assertEqual(stages['native_receipts'], 0)
        else:
            self.assertGreater(stages['native_receipts'], 0)
        fast = self.search()
        self.retrieval.parent_membership_read = reader.ReadSettings()
        with patch('recall_server.receipt_membership.lookup_current_search_membership',
                   wraps=lookup_current_search_membership) as proof:
            native_rows, _ = self.authorize()
        self.assertEqual(proof.call_count, 1)
        self.assertEqual(rows, native_rows)
        native = self.search()
        self.assertEqual(fast['results'], native['results'])
        self.assertEqual(fast['arms'], native['arms'])
        self.enable(); self.edit()
        fast_denied = self.search()
        self.retrieval.parent_membership_read = reader.ReadSettings()
        native_denied = self.search()
        self.assertEqual(fast_denied['results'], [])
        self.assertEqual(fast_denied['results'], native_denied['results'])
        self.assertEqual(fast_denied['arms'], native_denied['arms'])

    def test_shadow_still_observes_real_native_proof_when_both_flags_enabled(self):
        self.enroll(); self.enable()
        self.retrieval.parent_membership_shadow = shadow.ShadowSettings(enabled=True, binding_epoch='synthetic-epoch-a', serving_reserve_ms=100)
        with patch('recall_server.receipt_membership.lookup_current_search_membership', wraps=lookup_current_search_membership) as native:
            result = self.search()
        self.assertEqual(native.call_count, 1)
        self.assertEqual(result['diagnostics']['authority_stages']['parent_membership_shadow']['disagreements'], 0)

    def test_missing_certificate_skips_optional_query_but_keeps_native_proof(self):
        before, _ = self.authorize(); self.enable()
        execute = self.store._execute_bounded
        optional = []
        def recorded(c, sql, values, deadline):
            if sql == reader._METADATA_SQL:
                optional.append(values)
            return execute(c, sql, values, deadline)
        with patch.object(self.store, '_execute_bounded', side_effect=recorded), patch(
                'recall_server.receipt_membership.lookup_current_search_membership',
                wraps=lookup_current_search_membership) as native:
            after, stages = self.authorize()
        self.assertEqual(after, before)
        self.assertEqual(native.call_count, 1)
        self.assertEqual(set(native.call_args.kwargs['receipts']), {self.receipt, self.physical_receipt})
        self.assertEqual(stages['parent_membership_read']['fallback_passages'], len(self.ids))
        self.assertEqual(optional, [])

    def test_seventy_nine_rows_only_forty_existing_certificates_are_queried(self):
        import json
        original_ids = self.ids
        with self.store.connect() as c:
            for index in range(79 - len(self.ids)):
                passage_id = 'psg_' + hashlib.sha256(f'cohort-{index}'.encode()).hexdigest()[:32]
                c.execute("""INSERT INTO canonical_passages(
                    tenant_id,source_id,logical_document_id,revision,passage_id,ordinal,
                    policy_fingerprint,target_tokens,overlap_tokens,token_count,
                    first_occurred_at,last_occurred_at,roles,receipts,spans,text_redacted,text_sha256)
                    SELECT tenant_id,source_id,logical_document_id,revision,%s,ordinal+%s,
                           policy_fingerprint,target_tokens,overlap_tokens,token_count,
                           first_occurred_at,last_occurred_at,roles,receipts,spans,text_redacted,text_sha256
                    FROM canonical_passages WHERE tenant_id=%s AND passage_id=%s""",
                    (passage_id, (index + 1) * 100, self.tenant, original_ids[0]))
            self.ids = tuple(r['passage_id'] for r in c.execute(
                'SELECT passage_id FROM canonical_passages WHERE tenant_id=%s ORDER BY passage_id',
                (self.tenant,)).fetchall())
        self.assertEqual(len(self.ids), 79)
        self.scope['passage_ids'] = self.ids[:40]
        self.scope['limits']['passages'] = 79
        self.assertEqual(self.enroll()['status'], 'enrolled')
        before, _ = self.authorize(); self.enable()
        execute = self.store._execute_bounded
        optional, timings = [], []
        def recorded(c, sql, values, deadline):
            start = time.monotonic()
            result = execute(c, sql, values, deadline)
            if sql == reader._METADATA_SQL:
                optional.append(tuple(values[2]))
                timings.append((time.monotonic() - start) * 1000)
            return result
        with patch.object(self.store, '_execute_bounded', side_effect=recorded), patch(
                'recall_server.receipt_membership.lookup_current_search_membership',
                wraps=lookup_current_search_membership) as native:
            after, stages = self.authorize()
        print(json.dumps(dict(fixture='79 actual PG passages,40 certificates',
            optional_query_rows=[len(x) for x in optional], optional_execute_ms=timings,
            catalog_ms=stages['catalog_ms'], native_receipts=stages['native_receipts'],
            certified=stages['parent_membership_read']['certified_passages'])))
        self.assertEqual(after, before)
        self.assertEqual(native.call_count, 1)
        self.assertEqual(set(native.call_args.kwargs['receipts']), {self.receipt, self.physical_receipt})
        self.assertEqual(stages['parent_membership_read']['fallback_passages'], 39)
        self.assertEqual(len(optional), 1)
        self.assertEqual(set(optional[0]), set(self.ids[:40]))

    def test_nullable_certificate_fields_each_keep_native_and_nonnull_invalid_is_checked(self):
        self.enroll(); before, _ = self.authorize(); self.enable()
        for assignment, expects_optional in (
                ('membership_generation=NULL', False),
                ('membership_binding_sha256=NULL', False),
                ("membership_binding_sha256='" + "a" * 64 + "'", True),
                ('membership_generation=membership_generation+1', True)):
            self.enroll()
            with self.store.connect() as c:
                c.execute('UPDATE canonical_passages SET ' + assignment + ' WHERE tenant_id=%s', (self.tenant,))
            with patch.object(reader, 'partition', wraps=reader.partition) as optional, patch(
                    'recall_server.receipt_membership.lookup_current_search_membership',
                    wraps=lookup_current_search_membership) as native:
                rows, stages = self.authorize()
            self.assertEqual(rows, before)
            self.assertEqual(stages['parent_membership_read']['certified_passages'], 0)
            self.assertEqual(native.call_count, 1)
            if expects_optional:
                self.assertEqual(len(optional.call_args.kwargs['selected']), len(self.ids))
            else:
                self.assertFalse(optional.called and optional.call_args.kwargs['selected'])

    def test_certificate_appearance_after_catalog_stays_on_original_snapshot_proof(self):
        before, _ = self.authorize(); self.enable()
        execute = self.store._execute_bounded_pipeline
        class Captured:
            def __init__(inner, rows): inner.rows = rows
            def fetchall(inner): return inner.rows
        changed = []
        def appeared(c, sql, values, deadline):
            result = execute(c, sql, values, deadline)
            if 'passage.spans AS origin_spans' in sql:
                rows = result.fetchall()
                self.assertEqual(self.enroll()['status'], 'enrolled')
                changed.append(True)
                return Captured(rows)
            return result
        with patch.object(self.store, '_execute_bounded_pipeline', side_effect=appeared), patch(
                'recall_server.receipt_membership.lookup_current_search_membership',
                wraps=lookup_current_search_membership) as native:
            after, stages = self.authorize()
        self.assertEqual(changed, [True])
        self.assertEqual(after, before)
        self.assertEqual(native.call_count, 1)
        self.assertEqual(stages['parent_membership_read']['certified_passages'], 0)
        _, next_stages = self.authorize()
        self.assertEqual(next_stages['native_receipts'], 0)

    def test_prefilter_off_shadow_and_malformed_vendor_body_keep_original_output(self):
        from recall_server import turbopuffer_retrieval as retrieval
        self.enroll(); before = self.search(); execute = self.store._execute_bounded_pipeline
        for shadow_enabled in (False, True):
            statements = []
            self.retrieval.parent_membership_read = reader.ReadSettings(
                enabled=shadow_enabled, binding_epoch='synthetic-epoch-a')
            self.retrieval.parent_membership_shadow = shadow.ShadowSettings(
                enabled=shadow_enabled, binding_epoch='synthetic-epoch-a')
            def recorded(c, sql, values, deadline):
                statements.append(sql)
                return execute(c, sql, values, deadline)
            with patch.object(self.store, '_execute_bounded_pipeline', side_effect=recorded):
                after = self.search()
            self.assertEqual(after['results'], before['results'])
            self.assertEqual(after['arms'], before['arms'])
            self.assertIn(retrieval._SELECTED_AUTHORITY_SQL, statements)
            self.assertNotIn(retrieval._CERTIFICATE_PRESENCE_SQL, statements)
        self.retrieval.parent_membership_shadow = shadow.ShadowSettings()
        self.retrieval.parent_membership_read = reader.ReadSettings()
        for row in self.namespace.rows.values(): row['text'] = 'Tampered exact deployment phrase'
        before = self.search(); self.enable(); after = self.search()
        self.assertEqual(after['results'], before['results'])
        self.assertEqual(after['arms'], before['arms'])
        self.assertEqual(after['diagnostics']['hydrated_passages'], 0)

    def test_prefilter_receipt_mutation_still_requires_native_fallback(self):
        self.enroll(); self.enable()
        with self.store.connect() as c:
            c.execute('UPDATE canonical_passages SET receipts=ARRAY[%s] WHERE tenant_id=%s',
                ('recall://' + self.source + '/missing?rev=1#item=0', self.tenant))
        with patch.object(reader, 'partition', wraps=reader.partition) as optional, patch(
                'recall_server.receipt_membership.lookup_current_search_membership',
                wraps=lookup_current_search_membership) as native:
            rows, stages = self.authorize()
        self.assertEqual(len(optional.call_args.kwargs['selected']), len(self.ids))
        self.assertEqual(native.call_count, 1)
        self.assertEqual(rows, [])
        self.assertEqual(stages['parent_membership_read']['certified_passages'], 0)

    def test_presence_projection_only_extends_original_catalog_columns(self):
        from recall_server import turbopuffer_retrieval as retrieval
        extension = ("passage.membership_generation AS _membership_generation,"
                     "passage.membership_binding_sha256 AS _membership_binding_sha256,")
        self.assertEqual(retrieval._CERTIFICATE_PRESENCE_SQL.count(extension), 1)
        self.assertEqual(retrieval._CERTIFICATE_PRESENCE_SQL.replace(extension, ''),
                         retrieval._SELECTED_AUTHORITY_SQL)

    def test_presence_catalog_failure_refuses_without_retry_or_native_bypass(self):
        from recall_server import turbopuffer_retrieval as retrieval
        self.enable(); execute = self.store._execute_bounded_pipeline
        attempts = []
        def absent_column(c, sql, values, deadline):
            if sql == retrieval._CERTIFICATE_PRESENCE_SQL:
                attempts.append(sql)
                return execute(c, 'SELECT missing_certificate_column_for_test FROM canonical_passages', (), deadline)
            return execute(c, sql, values, deadline)
        with patch.object(self.store, '_execute_bounded_pipeline', side_effect=absent_column), patch(
                'recall_server.receipt_membership.lookup_current_search_membership',
                wraps=lookup_current_search_membership) as native:
            result = self.search()
        self.assertEqual(len(attempts), 1)
        self.assertEqual(result['results'], [])
        self.assertEqual(result['diagnostics']['authority_status'], 'unavailable')
        native.assert_not_called()

    def test_original_verifier_and_catalog_sql_are_unchanged(self):
        import inspect
        from recall_server import receipt_membership, turbopuffer_retrieval
        # 095 adds exact archived historical membership. The current-search
        # verifier and its receipt parser retain their original source bytes.
        self.assertIn(hashlib.sha256(Path(receipt_membership.__file__).read_bytes()).hexdigest(), (
            'ecdbee3fbb573c7acb631ab7f026468523c1cb516f6e2bdc47a8e7afcd267fb9',
            '569c04110b0a7821179ff628f4a532c1dd53bfedf6ee532425e3552017ad7639',
        ))
        for function, expected in (
            (receipt_membership.lookup_current_search_membership,
             '39325d79f5c77b25e77decc430e9933dbbad4853822ba3444ec71ee52edf0f33'),
            (receipt_membership._identity,
             'a3a2faccc0f2fca2f3a5668cf0269e699b8e43e56f125554c3739b5eb961316e'),
        ):
            with self.subTest(function=function.__name__):
                self.assertEqual(hashlib.sha256(inspect.getsource(function).encode()).hexdigest(),
                                 expected)
        original_sha = '33cbb2194d20a5a75b67879de1d707910844d41be8c118e90e5de9712de6dac4'
        origin_sha = '7c2ef4b4d8629c8abb042b5de7940b271b133a5f8c7a743e188c816b9e530636'
        projection = '                                   passage.spans AS origin_spans,\n'

        def check_catalog(statement):
            self.assertIn(hashlib.sha256(statement.encode()).hexdigest(), (original_sha, origin_sha))
            # The only accepted extension returns authenticated canonical metadata.
            # Removing that exact projection must restore every original SQL byte,
            # including joins, authority predicates and parameter placeholders.
            self.assertLessEqual(statement.count(projection), 1)
            original = statement.replace(projection, '')
            self.assertEqual(hashlib.sha256(original.encode()).hexdigest(), original_sha)
            return original

        original = check_catalog(turbopuffer_retrieval._SELECTED_AUTHORITY_SQL)
        anchor = '                                   evidence.revision,evidence.manifest_object_key,\n'
        self.assertEqual(original.count(anchor), 1)
        check_catalog(original.replace(anchor, projection + anchor))
        for changed in (original.replace('WHERE', 'WHERE true OR', 1),
                        original.replace('%s', 'NULL', 1),
                        original.replace(anchor, projection + projection + anchor),
                        original.replace(anchor, '                                   passage.text_redacted,\n' + anchor)):
            with self.subTest(statement_sha256=hashlib.sha256(changed.encode()).hexdigest()):
                with self.assertRaises(AssertionError):
                    check_catalog(changed)


if __name__ == '__main__':
    suite = unittest.TestSuite(ParentMembershipFastReadTests(name) for name in ParentMembershipFastReadTests.__dict__ if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
