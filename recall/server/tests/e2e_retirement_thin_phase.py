#!/usr/bin/env python3
"""Ordinary retirement failures identify their bounded native thinning stage."""
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

import psycopg

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
import e2e_retirement_productive_eof as fixtures  # noqa: E402
from recall_server import parent_chunk_window as window  # noqa: E402


class ThinPhase(unittest.TestCase):
    setUpClass = fixtures.ProductiveEOF.__dict__['setUpClass']
    setUp = fixtures.ProductiveEOF.setUp
    ledger = fixtures.ProductiveEOF.ledger
    state = fixtures.ProductiveEOF.state
    prepare_eof = fixtures.ProductiveEOF.prepare_eof
    run_observed = fixtures.ProductiveEOF.run_observed

    def test_native_cancellation_retains_unknown_and_identifies_thinning(self):
        before = self.prepare_eof(count=129, mixed=False, advance=False)
        original = psycopg.Connection.execute
        cancelled = []
        private = 'private source text postgres://credential@example'

        def execute(connection, query, *args, **kwargs):
            if str(query).startswith('WITH requested AS MATERIALIZED ('):
                cancelled.append(True)
                original(connection, "SET LOCAL statement_timeout='20ms'")
                return original(connection, 'SELECT pg_sleep(1) /* ' + private + ' */')
            return original(connection, query, *args, **kwargs)

        with patch.object(psycopg.Connection, 'execute', execute):
            observed = self.run_observed('native-thin-cancellation', 1)
        result = observed['result']
        self.assertEqual(cancelled, [True])
        self.assertTrue(result['commit_outcome_unknown'])
        self.assertEqual(result['error_class'], 'QueryCanceled')
        self.assertEqual(result['errors'], {'parent_retirement_unavailable': 1})
        self.assertEqual(result['failed_phase_counts'], {'parent_thin_native': 1})
        self.assertGreaterEqual(result['failed_phase_ms']['parent_thin_native'], 10)
        self.assertLessEqual(result['failed_phase_ms']['parent_thin_native'], result['parent_wall_ms'])
        self.assertNotIn(private, str(result))
        self.assertEqual(observed['observed']['archive_gets'], 0)
        self.assertEqual(observed['after']['documents'], before['documents'])
        self.assertEqual(observed['after']['catalog'], before['catalog'])
        self.assertEqual(observed['after']['ledger']['scope_epoch'], before['ledger']['scope_epoch'] + 1)
        for field in ('last_record_ordinal', 'cumulative_cleared_documents'):
            self.assertEqual(observed['after']['ledger'][field], before['ledger'][field])

    def cancel_stage(self, stage, fragment, *, bounded=True):
        before = self.state()
        execute, thin = psycopg.Connection.execute, window._thin_before_parent
        diagnostic, cancelled = {}, []

        def observe(*args, **kwargs):
            diagnostic['attempt'] = kwargs['diagnostic']
            return thin(*args, **kwargs)

        def cancel(connection, query, *args, **kwargs):
            if (diagnostic.get('attempt', {}).get('phase') == stage
                    and fragment in str(query) and not cancelled):
                cancelled.append(True)
                execute(connection, "SET LOCAL statement_timeout='20ms'")
                return execute(connection, 'SELECT pg_sleep(1)')
            return execute(connection, query, *args, **kwargs)

        with patch.object(window, '_thin_before_parent', observe), \
                patch.object(psycopg.Connection, 'execute', cancel):
            observed = self.run_observed(stage, 1)
        result = observed['result']
        self.assertEqual(cancelled, [True])
        self.assertEqual(result['failed_phase_counts'], {stage: 1})
        self.assertGreaterEqual(result['failed_phase_ms'][stage], 10)
        self.assertLessEqual(result['failed_phase_ms'][stage], result['parent_wall_ms'])
        self.assertEqual(result['commit_outcome_unknown'], not bounded)
        self.assertEqual(result['errors'], {
            'parent_retirement_deadline_exceeded' if bounded else 'parent_retirement_unavailable': 1})
        self.assertEqual(result['cleared_documents'], 0)
        self.assertEqual(observed['observed']['archive_gets'], 0)
        self.assertEqual(observed['after']['documents'], before['documents'])
        self.assertEqual(observed['after']['catalog'], before['catalog'])
        self.assertEqual(observed['after']['ledger']['last_record_ordinal'], before['ledger']['last_record_ordinal'])
        self.assertNotIn('pg_sleep', str(result))

    def test_bounded_proof_failures_identify_stage_without_changing_outcome(self):
        self.prepare_eof(count=129, mixed=False, advance=False)
        for stage, fragment in (
            ('parent_thin_owner', 'SELECT source.owner_principal_id,grant_row.permission'),
            ('parent_thin_native_locks', 'pg_try_advisory_xact_lock'),
            ('parent_thin_catalog_lock', 'SELECT 1 FROM canonical_evidence_documents'),
            ('parent_thin_catalog', 'SELECT to_jsonb(evidence) AS manifest'),
            ('parent_thin_progress', 'SELECT * FROM canonical_chunk_retirement_progress'),
            ('parent_thin_witnesses', 'SELECT 1 FROM forget_tombstones'),
        ):
            with self.subTest(stage=stage):
                self.cancel_stage(stage, fragment)

    def test_direct_admin_failure_has_its_own_stage_and_keeps_unknown(self):
        self.prepare_eof(count=129, mixed=False, advance=False)
        organization = 'org:' + self.tenant
        with self.store.connect() as connection:
            connection.execute("INSERT INTO brain_organizations VALUES(%s,'company','Thin phase test',now())", (organization,))
            connection.execute("INSERT INTO brain_spaces(tenant_id,organization_id,brain_kind,slug) VALUES(%s,%s,'company','phase-test')", (self.tenant, organization))
            connection.execute("INSERT INTO brain_memberships VALUES(%s,%s,'admin',now())", (organization, self.principal))
            connection.execute("INSERT INTO brain_access_grants VALUES(%s,%s,'admin',now())", (self.tenant, self.principal))
            connection.execute("UPDATE canonical_source_grants SET permission='read' WHERE tenant_id=%s AND principal_id=%s", (self.tenant, self.principal))
        self.cancel_stage('parent_thin_admin', 'SELECT space.organization_id,space.brain_kind,space.slug', bounded=False)


if __name__ == '__main__':
    unittest.main()
