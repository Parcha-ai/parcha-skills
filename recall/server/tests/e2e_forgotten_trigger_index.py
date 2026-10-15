"""The forgotten-identity trigger preserves rejection with an indexed digest lookup."""
import hashlib
import json
import os
from pathlib import Path
import re
import unittest

import psycopg
from psycopg.rows import dict_row

import e2e_retained_passage_receipts as fixture


class ForgottenTriggerIndex(unittest.TestCase):
    def setUp(self):
        self.fixture = fixture.RetainedPassageReceipts(
            'test_metadata_revision_refreshes_retained_receipts_and_search')
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.ingest(1, native='seed', text='Synthetic trigger boundary')
        self.connection = psycopg.connect(os.environ['RECALL_DATABASE_URL'],
                                         autocommit=True, row_factory=dict_row)
        self.addCleanup(self.connection.close)
        self.original = (Path(__file__).resolve().parents[1] / 'schema' /
                         '020_v2_lifecycle_guards.sql').read_text().split(
                             'CREATE OR REPLACE FUNCTION', 1)[1].split(
                                 'DROP TRIGGER', 1)[0]
        self.original = ('CREATE OR REPLACE FUNCTION' + self.original).replace(
            'target_sha256 char(64);', 'target_sha256 text;')
        legacy_body = self.original.split('AS $$', 1)[1].split('$$;', 1)[0].strip()
        self.assertEqual(hashlib.sha256(legacy_body.encode()).hexdigest(),
                         'ff787f738004a4d9a3ffc7a88c482de31d13d3d0da90d0b5847284597a3641d4')
        self.candidate = self.connection.execute(
            "SELECT pg_get_functiondef('recall_v2_reject_forgotten_event'::regproc) AS body"
        ).fetchone()['body']
        self.addCleanup(self.connection.execute, self.candidate)

    def target(self, native, *, tenant=None, source=None):
        return hashlib.sha256(chr(31).join((tenant or self.fixture.tenant,
                                           source or self.fixture.source,
                                           native)).encode()).hexdigest()

    def tombstone(self, native, status, *, target=None):
        self.connection.execute(
            """INSERT INTO forget_tombstones(
                tenant_id,source_id,target_identity_sha256,mode,reason,
                deleted_at,status,completed_at)
               VALUES(%s,%s,%s,'explicit_forget','owner_requested',now(),%s,
                      CASE WHEN %s='deleted' THEN now() ELSE NULL END)""",
            (self.fixture.tenant, self.fixture.source,
             target or self.target(native), status, status))

    def insert(self, native):
        # Use an already-valid artifact/job and only vary the new native identity.
        return self.connection.execute(
            """INSERT INTO canonical_events(
                 tenant_id,source_id,event_id,native_id,native_parent_id,
                 artifact_id,job_id,kind,content_sha256,revision,
                 occurred_at,observed_at,is_tombstone,canonical_redacted,
                 source_ordinal,body_location)
               SELECT tenant_id,source_id,%s,%s,native_parent_id,
                      artifact_id,job_id,kind,%s,1,occurred_at,observed_at,
                      is_tombstone,canonical_redacted,source_ordinal,body_location
               FROM canonical_events
               WHERE tenant_id=%s AND source_id=%s AND native_id='seed'
               LIMIT 1""",
            ('evt_' + self.target(native), native, self.target(native),
             self.fixture.tenant, self.fixture.source))

    def test_repeated_migrate_keeps_the_indexed_trigger_definition(self):
        # Non-retired databases replay pre067 migrations. A recorded081 must not
        # allow020 to restore the original text variable on the next migrate.
        self.assertIn('target_sha256 char(64);', self.candidate)
        versions = {row['version'] for row in self.connection.execute(
            'SELECT version FROM schema_migrations').fetchall()}
        self.assertNotIn(67, versions)
        self.assertIn(81, versions)
        for _ in range(2):
            self.fixture.store.migrate()
            body = self.connection.execute(
                "SELECT pg_get_functiondef('recall_v2_reject_forgotten_event'::regproc) AS body"
            ).fetchone()['body']
            self.assertEqual(body, self.candidate)

    def test_original_and_candidate_reject_pending_and_completed_forget(self):
        for arm, function in (('old', self.original), ('new', self.candidate)):
            self.connection.execute(function)
            for status in ('deleting', 'deleted'):
                for suffix in ('plain', 'unicode-λ-空', 'trailing '):
                    native = f'{arm}-{status}-{suffix}'
                    self.tombstone(native, status)
                    with self.subTest(arm=arm, status=status, native=native):
                        with self.assertRaises(psycopg.errors.CheckViolation) as caught:
                            self.insert(native)
                        self.assertEqual(caught.exception.sqlstate, '23514')
                        self.assertEqual(caught.exception.diag.message_primary,
                                         'canonical identity was forgotten')
                        self.assertEqual(self.connection.execute(
                            'SELECT count(*) n FROM canonical_events WHERE tenant_id=%s AND source_id=%s AND native_id=%s',
                            (self.fixture.tenant, self.fixture.source, native)
                        ).fetchone()['n'], 0)

    def test_original_and_candidate_keep_exact_identity_scope(self):
        for arm, function in (('old', self.original), ('new', self.candidate)):
            self.connection.execute(function)
            native = f'{arm}-survives'
            self.tombstone(native, 'deleted', target=self.target(native, source='other-source'))
            self.tombstone(native, 'deleted', target=self.target(native, tenant='other-tenant'))
            self.tombstone(native, 'deleted', target=self.target(native + '-other'))
            self.assertEqual(self.insert(native).rowcount, 1)
            # A successful insert does not remove or mutate any forget marker.
            self.assertEqual(self.connection.execute(
                'SELECT count(*) n FROM forget_tombstones WHERE tenant_id=%s AND source_id=%s AND target_identity_sha256=ANY(%s::char(64)[])',
                (self.fixture.tenant, self.fixture.source,
                 [self.target(native, source='other-source'),
                  self.target(native, tenant='other-tenant'), self.target(native + '-other')])
            ).fetchone()['n'], 3)

    def test_actual_trigger_digest_type_preserves_point_index_lookup(self):
        c = self.connection
        c.execute("""INSERT INTO forget_tombstones(
            tenant_id,source_id,target_identity_sha256,mode,reason,deleted_at,status,completed_at)
            SELECT %s,%s,md5(i::text)||md5(('salt'||i)::text),
                   'explicit_forget','owner_requested',now(),'deleted',now()
            FROM generate_series(1,20000)i""", (self.fixture.tenant, self.fixture.source))
        c.execute('VACUUM ANALYZE forget_tombstones')
        # Read the actual installed trigger's local variable type. Its predicate
        # remains identical; the query below exposes the resulting index plan.
        body = c.execute("SELECT prosrc FROM pg_proc WHERE oid='recall_v2_reject_forgotten_event'::regproc").fetchone()['prosrc']
        kind = re.search(r'target_sha256\s+(text|char\(64\));', body).group(1)
        plan = c.execute('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT 1 FROM forget_tombstones tombstone WHERE tombstone.tenant_id=%s AND tombstone.source_id=%s AND tombstone.target_identity_sha256=%s::' + kind,
                         (self.fixture.tenant, self.fixture.source, self.target('not-forgotten'))).fetchone()['QUERY PLAN'][0]
        def nodes(node):
            yield node
            for child in node.get('Plans', []):
                yield from nodes(child)
        walked = list(nodes(plan['Plan']))
        self.assertEqual(sum(n.get('Rows Removed by Filter', 0) for n in walked), 0, json.dumps(plan))
        self.assertTrue(any('target_identity_sha256' in n.get('Index Cond', '') for n in walked), json.dumps(plan))


if __name__ == '__main__':
    unittest.main()
