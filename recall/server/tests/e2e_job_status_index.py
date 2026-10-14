"""Actual concurrent DDL preserves job rows, constraints, and rollback shape."""
import importlib.util
import json
import subprocess
import sys
import os
from pathlib import Path
import unittest

import psycopg
from psycopg.rows import dict_row

SPEC = importlib.util.spec_from_file_location('job_index', Path(__file__).parents[1] / 'scripts' / 'retire_job_status_index.py')
OWNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(OWNER)


class JobIndexTest(unittest.TestCase):
    def setUp(self):
        self.connection = psycopg.connect(os.environ['RECALL_DATABASE_URL'], autocommit=True, row_factory=dict_row)
        self.connection.execute('DROP TABLE IF EXISTS public.canonical_events CASCADE')
        self.connection.execute('DROP TABLE IF EXISTS public.canonical_ingest_jobs CASCADE')
        self.connection.execute('''CREATE TABLE public.canonical_ingest_jobs(
            tenant_id text NOT NULL,source_id text NOT NULL,job_id text NOT NULL,
            connector_id text NOT NULL,mode text NOT NULL,status text NOT NULL,
            attempt integer NOT NULL,created_at timestamptz NOT NULL,updated_at timestamptz NOT NULL,
            PRIMARY KEY(tenant_id,source_id,job_id))''')
        self.connection.execute('''CREATE TABLE public.canonical_events(
            tenant_id text,source_id text,event_id text,job_id text,
            PRIMARY KEY(tenant_id,source_id,event_id),
            FOREIGN KEY(tenant_id,source_id,job_id) REFERENCES canonical_ingest_jobs)''')
        self.connection.execute("INSERT INTO canonical_ingest_jobs VALUES('tenant','source','legacy','slack.messages','incremental','committed',1,now(),now())")
        self.connection.execute("INSERT INTO canonical_events VALUES('tenant','source','event','legacy')")
        self.connection.execute(OWNER.CREATE_SQL)
        self.connection.execute('GRANT USAGE,CREATE ON SCHEMA public TO postgres')

    def tearDown(self):
        self.connection.close()

    def test_drop_recreate_preserves_rows_and_foreign_key(self):
        before = self.connection.execute('SELECT * FROM canonical_ingest_jobs').fetchall()
        state = OWNER.inspect(self.connection)
        result = OWNER.execute(self.connection, action='drop', expected=state)
        self.assertEqual(result['status'], 'dropped')
        absent = OWNER.inspect(self.connection)
        self.assertIsNone(absent['index'])
        self.assertEqual(absent['primary'], state['primary'])
        self.assertEqual(before, self.connection.execute('SELECT * FROM canonical_ingest_jobs').fetchall())
        with self.assertRaises(psycopg.errors.ForeignKeyViolation):
            self.connection.execute('DELETE FROM canonical_ingest_jobs')
        result = OWNER.execute(self.connection, action='recreate', expected=absent)
        self.assertEqual(result['status'], 'recreated')
        restored = OWNER.inspect(self.connection)
        self.assertEqual(restored['index']['definition'], state['index']['definition'])
        self.assertEqual(before, self.connection.execute('SELECT * FROM canonical_ingest_jobs').fetchall())

    def test_changed_or_invalid_shape_refuses(self):
        state = OWNER.inspect(self.connection)
        self.connection.execute('DROP INDEX canonical_jobs_status_idx')
        self.connection.execute('CREATE INDEX canonical_jobs_status_idx ON canonical_ingest_jobs(status,tenant_id,updated_at)')
        with self.assertRaisesRegex(RuntimeError, 'catalog_changed|index_shape'):
            OWNER.execute(self.connection, action='drop', expected=state)
        self.assertIsNotNone(OWNER.inspect(self.connection)['index'])
        wrong = OWNER.inspect(self.connection)
        with self.assertRaisesRegex(RuntimeError, 'index_shape'):
            OWNER.execute(self.connection, action='drop', expected=wrong)

    def test_nonsuperuser_ephemeral_owner_actual_job(self):
        from urllib.parse import urlsplit, urlunsplit
        self.connection.execute("CREATE ROLE index_fixture_admin LOGIN SUPERUSER PASSWORD 'synthetic-admin-password'")
        url = urlsplit(os.environ['RECALL_DATABASE_URL'])
        admin_dsn = urlunsplit((url.scheme, 'index_fixture_admin:synthetic-admin-password@'+url.hostname+':'+str(url.port), url.path, url.query, url.fragment))
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute('ALTER ROLE postgres NOSUPERUSER')
            try:
                admin.execute("CREATE ROLE index_fixture_temp LOGIN PASSWORD 'synthetic-temp-password' IN ROLE postgres")
                config = dict(action='drop', commit='a'*40, service_id='fixture-service',
                    credential_key='INDEX_FIXTURE_CREDENTIAL', expected=OWNER.inspect(self.connection),
                    database_identity_sha256=OWNER.digest(dict(host_sha256=OWNER.hashlib.sha256(url.hostname.encode()).hexdigest(), port=url.port, database=url.path)))
                env = dict(os.environ, RENDER_GIT_COMMIT=config['commit'], RENDER_SERVICE_ID=config['service_id'],
                    INDEX_FIXTURE_CREDENTIAL=json.dumps(dict(username='index_fixture_temp', password='synthetic-temp-password')))
                for action in ('drop','recreate'):
                    config['action'] = action
                    config['expected'] = OWNER.contract(OWNER.inspect(self.connection))
                    code = 'CONFIG = '+repr(config)+'\n'+Path(OWNER.__file__).read_text()
                    child = subprocess.run([sys.executable, '-c', code], env=env, capture_output=True, text=True, timeout=300)
                    self.assertEqual(child.returncode, 0, action+' '+child.stderr+' '+child.stdout)
                    result = json.loads(child.stdout)
                    self.assertEqual(result['status'], 'dropped' if action == 'drop' else 'recreated')
                self.assertEqual(OWNER.inspect(self.connection)['index']['owner'], 'postgres')
                admin.execute('DROP ROLE index_fixture_temp')
            finally:
                admin.execute('ALTER ROLE postgres SUPERUSER')
        self.connection.execute('DROP ROLE index_fixture_admin')

    def test_blocked_drop_times_out_without_retry(self):
        self.connection.execute("SET lock_timeout='100ms'")
        state = OWNER.inspect(self.connection)
        with psycopg.connect(os.environ['RECALL_DATABASE_URL']) as blocker:
            blocker.execute('LOCK TABLE canonical_ingest_jobs IN SHARE UPDATE EXCLUSIVE MODE')
            result = OWNER.execute(self.connection, action='drop', expected=state)
            self.assertEqual(result['status'], 'inspect_required')
            self.assertEqual(result['error_class'], 'LockNotAvailable')
            self.assertFalse(result['ddl_acknowledged'])
        unchanged = OWNER.inspect(self.connection)
        self.assertTrue(unchanged['index']['indisvalid'])
        self.assertTrue(unchanged['index']['indisready'])
        self.assertTrue(unchanged['index']['indislive'])

    def test_partial_concurrent_drop_requires_original_intent(self):
        self.connection.execute("SET statement_timeout='250ms'")
        self.connection.execute("SET lock_timeout='1s'")
        before = OWNER.inspect(self.connection)
        with psycopg.connect(os.environ['RECALL_DATABASE_URL']) as blocker:
            blocker.execute('SELECT * FROM canonical_ingest_jobs').fetchall()
            result = OWNER.execute(self.connection, action='drop', expected=before)
            self.assertEqual(result['status'], 'inspect_required')
            self.assertEqual(result['error_class'], 'QueryCanceled')
        partial = OWNER.inspect(self.connection)
        self.assertIsNotNone(partial['index'])
        self.assertFalse(partial['index']['indisvalid'])
        with self.assertRaisesRegex(RuntimeError, 'index_shape'):
            OWNER.execute(self.connection, action='drop', expected=partial)
        with self.assertRaisesRegex((RuntimeError, ValueError), 'original_drop_required|explicit_autocommit'):
            OWNER.execute(self.connection, action='finish_drop', expected=partial)
        original = dict(intent_sha256='a'*64, terminal_job_id='fixture-original-job', state=before)
        wrong = dict(original, intent_sha256='not-an-intent')
        with self.assertRaisesRegex(RuntimeError, 'original_drop_required'):
            OWNER.execute(self.connection, action='finish_drop', expected=partial, original_drop=wrong)
        result = OWNER.execute(self.connection, action='finish_drop', expected=partial, original_drop=original)
        self.assertEqual(result['status'], 'dropped')
        self.assertIsNone(OWNER.inspect(self.connection)['index'])

    def test_unknown_outcome_does_not_replay(self):
        state = OWNER.inspect(self.connection)
        class Lost:
            autocommit = True
            attempts = 0
            def execute(inner, query, *args, **kwargs):
                result = self.connection.execute(query, *args, **kwargs)
                if query == OWNER.DROP_SQL:
                    inner.attempts += 1
                    raise ConnectionError('lost acknowledgement')
                return result
        fake = Lost()
        result = OWNER.execute(fake, action='drop', expected=state)
        self.assertEqual(result['status'], 'inspect_required')
        self.assertFalse(result['ddl_acknowledged'])
        self.assertEqual(fake.attempts, 1)
        self.assertIsNone(OWNER.inspect(self.connection)['index'])


if __name__ == '__main__':
    unittest.main()
