#!/usr/bin/env python3
"""Optional certificate columns preserve native publication and authority."""
from pathlib import Path
import unittest

import psycopg

from e2e_singleton_search_authority import SingletonSearchTests


class ParentCertificateSchemaTests(SingletonSearchTests):
    def migration(self, connection):
        sql=(Path(__file__).resolve().parents[1]/'schema/083_parent_membership_certificate.sql').read_text()
        connection.execute(sql.replace('BEGIN;','',1).rsplit('COMMIT;',1)[0])

    def test_nullable_columns_exist_without_certifying_old_publication(self):
        self.prepare_search()
        with self.store.connect() as c:
            columns=c.execute("""SELECT attname,atttypid::regtype::text AS type,attnotnull,atthasdef
              FROM pg_attribute WHERE attrelid='canonical_passages'::regclass
               AND attname IN ('membership_generation','membership_binding_sha256') AND NOT attisdropped
              ORDER BY attname""").fetchall()
            self.assertEqual([(r['attname'],r['type'],r['attnotnull'],r['atthasdef']) for r in columns],
                [('membership_binding_sha256','text',False,False),('membership_generation','bigint',False,False)])
            self.assertEqual(c.execute('SELECT count(*) AS n FROM schema_migrations WHERE version=83').fetchone()['n'],1)
            self.assertEqual(c.execute('SELECT count(*) AS n FROM canonical_passages WHERE tenant_id=%s AND (membership_generation IS NOT NULL OR membership_binding_sha256 IS NOT NULL)',(self.tenant,)).fetchone()['n'],0)
        self.assertTrue(self.search()['results'])

    def test_replay_keeps_existing_certificate_metadata_and_native_results(self):
        self.prepare_search();before=self.search()['results']
        with self.store.connect() as c:
            c.execute('UPDATE canonical_passages SET membership_generation=7,membership_binding_sha256=%s WHERE tenant_id=%s',('a'*64,self.tenant))
            self.migration(c)
            rows=c.execute('SELECT membership_generation,membership_binding_sha256 FROM canonical_passages WHERE tenant_id=%s',(self.tenant,)).fetchall()
            self.assertTrue(rows)
            self.assertTrue(all(r['membership_generation']==7 and r['membership_binding_sha256']=='a'*64 for r in rows))
        self.assertEqual(self.search()['results'],before)

    def test_missing_generation_prerequisites_refuse_atomically(self):
        for version in (80,82):
            with self.subTest(version=version), self.store.connect() as c:
                with self.assertRaisesRegex(psycopg.errors.RaiseException,'parent_certificate_generation_schema_required'):
                    with c.transaction():
                        c.execute('DELETE FROM schema_migrations WHERE version=%s',(version,))
                        self.migration(c)
                self.assertEqual(c.execute('SELECT count(*) AS n FROM schema_migrations WHERE version=%s',(version,)).fetchone()['n'],1)

    def test_wrong_existing_column_type_is_not_accepted_by_if_not_exists(self):
        with self.store.connect() as c:
            with self.assertRaisesRegex(psycopg.errors.RaiseException,'parent_certificate_columns_invalid'):
                with c.transaction():
                    c.execute('ALTER TABLE canonical_passages ALTER COLUMN membership_generation TYPE text USING membership_generation::text')
                    self.migration(c)
            self.assertEqual(c.execute("SELECT atttypid::regtype::text AS type FROM pg_attribute WHERE attrelid='canonical_passages'::regclass AND attname='membership_generation'").fetchone()['type'],'bigint')


if __name__=='__main__':
    suite=unittest.TestSuite(ParentCertificateSchemaTests(name) for name in ParentCertificateSchemaTests.__dict__ if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
