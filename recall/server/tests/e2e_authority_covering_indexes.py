#!/usr/bin/env python3
"""Real PostgreSQL proof for the guarded authority-index operator operations.

Uses only isolated schemas in the explicitly supplied disposable database.
Does not connect to Recall, vendors, or any configured application runtime.
"""
from pathlib import Path
import ast
import os
import sys
import unittest
import uuid

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVER))
from recall_server.db import BrainStore  # noqa: E402

FILES = ('authority_covering_indexes_concurrent.sql',
         'authority_receipt_constraint.sql')
OLD = 'canonical_chunks_tenant_id_receipt_key'
NEW = 'canonical_chunks_receipt_authority_key'
DOC = 'canonical_documents_live_authority_idx'
BUILD = f'''CREATE UNIQUE INDEX CONCURRENTLY {NEW}
    ON canonical_chunks(tenant_id,receipt)
    INCLUDE(source_id,document_id,deleted_at)'''


class AuthorityIndexOperationTest(unittest.TestCase):
    def setUp(self):
        self.dsn = os.environ['RECALL_DATABASE_URL']
        self.schema = 'authority_index_' + uuid.uuid4().hex
        self.conn = psycopg.connect(self.dsn, autocommit=True, row_factory=dict_row)
        self.conn.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(self.schema)))
        self.conn.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(self.schema)))
        self.conn.execute('''
            CREATE TABLE schema_migrations(version integer PRIMARY KEY);
            INSERT INTO schema_migrations VALUES(70);
            CREATE TABLE canonical_documents(
                tenant_id text NOT NULL,source_id text NOT NULL,document_id text NOT NULL,
                is_current boolean NOT NULL,deleted_at timestamptz,
                PRIMARY KEY(tenant_id,source_id,document_id));
            CREATE TABLE canonical_chunks(
                tenant_id text NOT NULL,source_id text NOT NULL,chunk_id text NOT NULL,
                document_id text NOT NULL,receipt text NOT NULL,deleted_at timestamptz,
                text_redacted text NOT NULL DEFAULT '',
                PRIMARY KEY(tenant_id,source_id,chunk_id),UNIQUE(tenant_id,receipt),
                FOREIGN KEY(tenant_id,source_id,document_id)
                  REFERENCES canonical_documents(tenant_id,source_id,document_id));
            INSERT INTO canonical_documents VALUES
                ('t','s','live',true,NULL),('t','s','retired',true,NULL),
                ('t','s','old',false,NULL),('t','s','deleted',false,now()),
                ('foreign','s','live',true,NULL);
            INSERT INTO canonical_chunks(tenant_id,source_id,chunk_id,document_id,receipt,deleted_at) VALUES
                ('t','s','c1','live','r1',NULL),('t','s','c2','retired','r2',NULL),
                ('t','s','c3','old','r3',NULL),('t','s','c4','deleted','r4',NULL),
                ('t','s','c5','live','r5',now()),('foreign','s','c1','live','r1',NULL);
        ''')
        self.old_oid = self.constraint(OLD)['conindid']
        self.schema_versions = self.conn.execute('SELECT version FROM schema_migrations ORDER BY version').fetchall()

    def tearDown(self):
        self.conn.rollback()
        self.conn.autocommit = True
        self.conn.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(self.schema)))
        self.conn.close()

    def constraint(self, name):
        return self.conn.execute('''SELECT oid,conindid,contype,condeferrable,condeferred
            FROM pg_constraint WHERE conrelid='canonical_chunks'::regclass AND conname=%s''',
            (name,)).fetchone()

    def index(self, name):
        return self.conn.execute('''SELECT i.indexrelid,i.indisvalid,i.indisready,i.indisunique,
            i.indnkeyatts,i.indnatts,pg_get_expr(i.indpred,i.indrelid) AS predicate,
            ARRAY(SELECT a.attname FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum,n)
                  JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum
                  ORDER BY k.n) AS columns
            FROM pg_index i WHERE i.indexrelid=to_regclass(%s)''',(name,)).fetchone()

    def apply(self):
        # With the files absent this intentionally runs the baseline unchanged:
        # behavioral assertions below must fail without the operator implementation.
        for name in FILES:
            path = SERVER/'operations'/name
            if not path.exists():
                continue
            if name.endswith('_concurrent.sql'):
                BrainStore._migrate_concurrently(self.conn, path.read_text())
                self.conn.commit()
                self.conn.autocommit = True
            else:
                self.conn.execute(path.read_text())

    def allowed(self):
        return [r['receipt'] for r in self.conn.execute('''
            SELECT chunk.receipt FROM canonical_chunks chunk
            JOIN canonical_documents document USING(tenant_id,source_id,document_id)
            WHERE chunk.tenant_id='t' AND chunk.source_id='s'
              AND chunk.receipt=ANY(ARRAY['r1','r2','r3','r4','r5','missing',NULL])
              AND chunk.deleted_at IS NULL AND document.is_current
              AND document.deleted_at IS NULL ORDER BY chunk.receipt''').fetchall()]

    def assert_original_preserved(self):
        self.assertEqual(self.constraint(OLD)['conindid'],self.old_oid)
        self.assertIsNone(self.constraint(NEW))
        self.assertEqual(self.conn.execute('SELECT version FROM schema_migrations ORDER BY version').fetchall(),
                         self.schema_versions)

    def test_full_history_uniqueness_liveness_and_repeated_oid_stability(self):
        before = self.allowed()
        self.assertEqual(before,['r1','r2'])
        self.apply()
        new = self.constraint(NEW)
        self.assertIsNotNone(new,'new covering uniqueness constraint was not installed')
        self.assertIsNone(self.constraint(OLD))
        self.assertEqual(new['contype'],'u')
        self.assertFalse(new['condeferrable'])
        self.assertFalse(new['condeferred'])
        covering = self.index(NEW)
        self.assertEqual(covering['columns'],['tenant_id','receipt','source_id','document_id','deleted_at'])
        self.assertEqual((covering['indnkeyatts'],covering['indnatts']),(2,5))
        self.assertTrue(covering['indisunique'] and covering['indisvalid'] and covering['indisready'])
        self.assertIsNone(covering['predicate'])
        self.assertIsNone(self.index(OLD))
        doc = self.index(DOC)
        self.assertEqual(doc['columns'],['tenant_id','source_id','document_id'])
        self.assertFalse(doc['indisunique'])
        self.assertEqual(self.allowed(),before)
        # A deleted receipt remains globally reserved; INCLUDE never changes
        # the key, and foreign tenants may still reuse a receipt.
        for receipt in ('r1','r5'):
            with self.assertRaises(psycopg.errors.UniqueViolation):
                self.conn.execute('''INSERT INTO canonical_chunks
                    (tenant_id,source_id,chunk_id,document_id,receipt)
                    VALUES('t','s',%s,'live',%s)''',('duplicate-'+receipt,receipt))
        self.conn.execute("UPDATE canonical_documents SET is_current=false WHERE tenant_id='t' AND document_id='live'")
        self.assertEqual(self.allowed(),['r2'])
        self.conn.execute("UPDATE canonical_chunks SET deleted_at=now() WHERE tenant_id='t' AND receipt='r2'")
        self.assertEqual(self.allowed(),[])
        self.conn.execute("SET lock_timeout='237ms'")
        for _ in range(2):
            self.apply()
            self.assertEqual(self.constraint(NEW)['conindid'],covering['indexrelid'])
            self.assertEqual(self.index(DOC)['indexrelid'],doc['indexrelid'])
            self.assertEqual(self.conn.execute('SHOW lock_timeout').fetchone()['lock_timeout'],'237ms')
        self.assertEqual(self.conn.execute('SELECT version FROM schema_migrations ORDER BY version').fetchall(),
                         self.schema_versions)

    def test_current_authority_query_uses_covering_indexes_without_heap_fetches(self):
        self.conn.execute("""
            INSERT INTO canonical_documents
              SELECT 't','s','bulk-doc-' || n,true,NULL FROM generate_series(1,2000) AS n;
            INSERT INTO canonical_chunks(tenant_id,source_id,chunk_id,document_id,receipt)
              SELECT 't','s','bulk-chunk-' || n,'bulk-doc-' || n,'bulk-receipt-' || n
                FROM generate_series(1,2000) AS n;
            CREATE TABLE canonical_evidence_documents(
                tenant_id text,source_id text,logical_document_id text,
                PRIMARY KEY(tenant_id,source_id,logical_document_id));
            CREATE TABLE canonical_passage_documents(
                tenant_id text,source_id text,logical_document_id text,policy_fingerprint text,
                PRIMARY KEY(tenant_id,source_id,logical_document_id));
            CREATE TABLE canonical_passages(
                tenant_id text,source_id text,logical_document_id text,passage_id text,
                text_sha256 text,receipts text[],policy_fingerprint text,
                PRIMARY KEY(tenant_id,source_id,passage_id));
        """)
        receipts = {
            'live-retired': ['r1','r2'],
            'partial-forget': ['r1','r5'],
            'old-revision': ['r3'],
            'deleted-document': ['r4'],
            'bulk-live': ['bulk-receipt-123','bulk-receipt-124','bulk-receipt-125'],
        }
        for passage, refs in receipts.items():
            self.conn.execute("INSERT INTO canonical_evidence_documents VALUES('t','s',%s)",(passage,))
            self.conn.execute("INSERT INTO canonical_passage_documents VALUES('t','s',%s,'policy')",(passage,))
            self.conn.execute("INSERT INTO canonical_passages VALUES('t','s',%s,%s,%s,%s,'policy')",
                              (passage,passage,'a'*64,refs))
        module = ast.parse((SERVER/'recall_server/turbopuffer_retrieval.py').read_text())
        statements = [node.value for node in ast.walk(module)
                      if isinstance(node,ast.Constant) and isinstance(node.value,str)
                      and node.value.startswith('WITH selected AS MATERIALIZED')]
        self.assertEqual(len(statements),1)
        query = statements[0]
        params = ('t',['s'],'policy',list(receipts))
        before = self.conn.execute(query,params).fetchall()
        self.assertEqual({row['passage_id'] for row in before},{'live-retired','bulk-live'})
        self.apply()
        for table in ('canonical_chunks','canonical_documents','canonical_passages',
                      'canonical_passage_documents','canonical_evidence_documents'):
            self.conn.execute(sql.SQL('VACUUM ANALYZE {}').format(sql.Identifier(table)))
        after = self.conn.execute(query,params).fetchall()
        self.assertEqual(sorted(before,key=lambda row: row['passage_id']),
                         sorted(after,key=lambda row: row['passage_id']))
        plan = self.conn.execute('EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) '+query,params).fetchone()['QUERY PLAN'][0]
        scans = {}
        def collect(node):
            if node.get('Index Name') in (NEW,DOC):
                scans[node['Index Name']] = node
            for child in node.get('Plans',[]):
                collect(child)
        collect(plan['Plan'])
        self.assertEqual(set(scans),{NEW,DOC},plan)
        for name,node in scans.items():
            self.assertEqual(node['Node Type'],'Index Only Scan',(name,node))
            self.assertEqual(node['Heap Fetches'],0,(name,node))
            self.assertGreater(node['Actual Loops'],0,(name,node))

    def test_wrong_covering_definition_refuses_and_retains_old_constraint(self):
        self.conn.execute(f'CREATE UNIQUE INDEX {NEW} ON canonical_chunks(tenant_id,receipt) INCLUDE(document_id)')
        with self.assertRaises(psycopg.Error):
            self.apply()
        self.conn.rollback()
        self.assert_original_preserved()

    def test_wrong_receipt_key_order_refuses_and_retains_old_constraint(self):
        self.conn.execute(f'''CREATE UNIQUE INDEX {NEW} ON canonical_chunks(receipt,tenant_id)
            INCLUDE(source_id,document_id,deleted_at)''')
        with self.assertRaises(psycopg.Error):
            self.apply()
        self.conn.rollback()
        self.assert_original_preserved()

    def test_wrong_document_predicate_refuses_and_retains_old_constraint(self):
        self.conn.execute(f'''CREATE INDEX {DOC} ON canonical_documents(tenant_id,source_id,document_id)
            WHERE deleted_at IS NULL''')
        with self.assertRaises(psycopg.Error):
            self.apply()
        self.conn.rollback()
        self.assert_original_preserved()

    def test_incoming_receipt_fk_refuses_without_cascade(self):
        self.conn.execute('''CREATE TABLE receipt_child(tenant_id text,receipt text,
            FOREIGN KEY(tenant_id,receipt) REFERENCES canonical_chunks(tenant_id,receipt));
            INSERT INTO receipt_child VALUES('t','r1');''')
        with self.assertRaises(psycopg.Error):
            self.apply()
        self.conn.rollback()
        self.assert_original_preserved()
        self.assertEqual(self.conn.execute('SELECT count(*) AS n FROM receipt_child').fetchone()['n'],1)

    def test_correct_but_invalid_concurrent_index_refuses(self):
        with psycopg.connect(self.dsn) as writer:
            writer.execute(sql.SQL('SET search_path TO {}').format(sql.Identifier(self.schema)))
            writer.execute("UPDATE canonical_chunks SET text_redacted='pending synthetic write' WHERE receipt='r1'")
            self.conn.execute("SET statement_timeout='100ms'")
            try:
                with self.assertRaises(psycopg.errors.QueryCanceled):
                    self.conn.execute(BUILD)
            finally:
                self.conn.execute('RESET statement_timeout')
                writer.rollback()
        invalid = self.index(NEW)
        self.assertIsNotNone(invalid)
        self.assertFalse(invalid['indisvalid'])
        with self.assertRaises(psycopg.Error):
            self.apply()
        self.conn.rollback()
        self.assert_original_preserved()
        self.assertEqual(self.index(NEW)['indexrelid'],invalid['indexrelid'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
