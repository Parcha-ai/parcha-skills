#!/usr/bin/env python3
"""Additive tool-role storage contract; no extractor or enrollment changes."""
import json
from pathlib import Path
import unittest

import psycopg
from psycopg import sql

from e2e_codex_tool_publication import CodexToolPublication


class CodexTextBlockStorage(CodexToolPublication):
    OLD = 'codex.tool-result.string.v1#/payload/output'
    PREFIX = 'codex.tool-result.block-text.v1#/payload/output/'
    COLUMNS = (
        'tenant_id', 'source_id', 'logical_document_id', 'revision', 'passage_id',
        'ordinal', 'policy_fingerprint', 'target_tokens', 'overlap_tokens',
        'token_count', 'first_occurred_at', 'last_occurred_at', 'roles', 'receipts',
        'spans', 'text_redacted', 'text_sha256', 'header_redacted', 'embed_sha256',
    )

    def setUp(self):
        super().setUp()
        self.ingest('old-string', result=True)
        self.assertEqual(self.project()['documents'], 1)
        self.row = self.projected()[0]

    def copy_units(self, units, *, roles=('tool',), source=None):
        # The storage contract checks role/source/unit shape, not source bytes.
        # Actual source opening is covered by the separate resolver contract.
        row = dict(self.row, passage_id='psg_'+'f'*32, ordinal=1000,
                   roles=list(roles), source_id=source or self.source,
                   spans=json.dumps([dict(self.row['spans'][0], source_unit=unit)
                                     for unit in units]))
        with self.store.connect() as connection:
            with connection.transaction(force_rollback=True):
                with connection.cursor() as cursor:
                    with cursor.copy(sql.SQL('COPY canonical_passages ({}) FROM STDIN').format(
                            sql.SQL(',').join(map(sql.Identifier, self.COLUMNS)))) as copy:
                        copy.write_row(tuple(row[name] for name in self.COLUMNS))
                actual = connection.execute(
                    'SELECT roles,spans FROM canonical_passages WHERE tenant_id=%s AND source_id=%s AND passage_id=%s',
                    (self.tenant, row['source_id'], row['passage_id'])).fetchone()
                self.assertEqual(actual, dict(roles=list(roles), spans=json.loads(row['spans'])))

    def test_native_copy_accepts_exact_new_and_old_units(self):
        before = self.grant_and_open(self.row)
        for units in ([self.OLD], [self.PREFIX+'0/text'], [self.PREFIX+'123/text'],
                      [self.OLD, self.PREFIX+'2/text']):
            with self.subTest(units=units):
                self.copy_units(units)
        self.assertEqual(self.grant_and_open(self.row), before)

    def test_old_constraint_rejects_new_unit_and_keeps_old_unit(self):
        original = Path(__file__).parents[1] / 'schema' / '090_codex_tool_extraction.sql'
        roles_ddl = original.read_text().split('ALTER TABLE canonical_passage_documents')[0]
        with self.store.connect() as connection:
            current = connection.execute("""SELECT pg_get_constraintdef(oid) AS definition
                FROM pg_constraint WHERE conrelid='canonical_passages'::regclass
                AND conname='canonical_passages_roles_check1'""").fetchone()['definition']
            connection.execute(roles_ddl.removeprefix('BEGIN;'))
        try:
            self.copy_units([self.OLD])
            with self.assertRaises(psycopg.errors.CheckViolation):
                self.copy_units([self.PREFIX+'0/text'])
        finally:
            with self.store.connect() as connection:
                connection.execute('ALTER TABLE canonical_passages DROP CONSTRAINT canonical_passages_roles_check1')
                connection.execute('ALTER TABLE canonical_passages ADD CONSTRAINT canonical_passages_roles_check1 '+current)

    def test_all_spans_source_roles_and_canonical_index_remain_required(self):
        invalid = ['', '00', '01', '-1', '+1', '١', '１', '1.0', '0/extra']
        units = [self.PREFIX+index+'/text' for index in invalid]
        units += [self.PREFIX+'0/text/extra', self.PREFIX+'0/text\n',
                  'codex.tool-result.block-text.v2#/payload/output/0/text',
                  'codex.tool-input.string.v1#/payload/arguments', None]
        for unit in units:
            with self.subTest(unit=unit):
                with self.assertRaises(psycopg.errors.CheckViolation):
                    self.copy_units([unit])
                with self.assertRaises(psycopg.errors.CheckViolation):
                    self.copy_units([self.OLD, unit])
        for roles in ((), ('assistant', 'tool'), ('user', 'tool'), ('analysis',), ('system',)):
            with self.subTest(roles=roles):
                with self.assertRaises(psycopg.errors.CheckViolation):
                    self.copy_units([self.PREFIX+'0/text'], roles=roles)
        with self.assertRaises(psycopg.errors.CheckViolation):
            self.copy_units([])
        with self.assertRaises(psycopg.errors.CheckViolation):
            self.copy_units([self.PREFIX+'0/text'], source='claude:wrong-provider')

    def test_busy_table_aborts_atomic_migration_and_preserves_old_contract(self):
        schema = Path(__file__).parents[1] / 'schema'
        old = (schema/'090_codex_tool_extraction.sql').read_text().split(
            'ALTER TABLE canonical_passage_documents')[0].removeprefix('BEGIN;')
        new = (schema/'093_codex_text_block_storage.sql').read_text()
        with self.store.connect() as connection:
            connection.execute(old)
            connection.execute('DELETE FROM schema_migrations WHERE version=93')
        try:
            with self.store.connect() as held:
                held.execute('SELECT 1 FROM canonical_passages LIMIT 1').fetchall()
                with self.assertRaises(psycopg.errors.LockNotAvailable):
                    with self.store.connect() as connection:
                        connection.execute(new)
            with self.store.connect() as connection:
                self.assertIsNone(connection.execute(
                    'SELECT version FROM schema_migrations WHERE version=93').fetchone())
            self.copy_units([self.OLD])
            with self.assertRaises(psycopg.errors.CheckViolation):
                self.copy_units([self.PREFIX+'0/text'])
        finally:
            with self.store.connect() as connection:
                connection.execute(new)
        self.copy_units([self.PREFIX+'0/text'])
        with self.store.connect() as connection:
            self.assertFalse(connection.execute("""SELECT convalidated FROM pg_constraint
                WHERE conrelid='canonical_passages'::regclass
                AND conname='canonical_passages_roles_check1'""").fetchone()['convalidated'])


if __name__ == '__main__':
    suite = unittest.TestSuite(CodexTextBlockStorage(name)
        for name in CodexTextBlockStorage.__dict__ if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
