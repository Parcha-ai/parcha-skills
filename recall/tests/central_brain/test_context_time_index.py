import unittest
from unittest.mock import MagicMock, patch

from recall_server.context_time_index import CREATE_SQL, EXPECTED_CLASSES, EXPECTED_KEYS, ensure_context_time_index


def index_row():
    return dict(index_oid=42, index_bytes=8192, index_kind='i', table_schema='public',
                table_name='canonical_events', method='btree', unique=False,
                partial=False, valid=True, ready=True, key_count=5, attribute_count=5,
                keys=list(EXPECTED_KEYS), options=[0]*5, operator_classes=list(EXPECTED_CLASSES),
                collations=[100, 100, 100, 0, 100], declared_collations=[100, 100, 100, 0, 100])


class ContextTimeIndexTests(unittest.TestCase):
    def connection(self, rows):
        connection = MagicMock()
        connection.__enter__.return_value = connection
        connection.execute.return_value.fetchone.side_effect = rows
        return connection

    def test_default_inspection_is_readonly_without_ddl(self):
        connection = self.connection([None])
        with patch('recall_server.context_time_index.psycopg.connect', return_value=connection) as connect:
            result = ensure_context_time_index('synthetic')
        self.assertEqual(result['status'], 'absent')
        self.assertIn('default_transaction_read_only=on', connect.call_args.kwargs['options'])
        self.assertFalse(any(call.args[0] == CREATE_SQL for call in connection.execute.call_args_list))

    def test_build_is_one_concurrent_statement_then_verified(self):
        connection = self.connection([None, index_row()])
        with patch('recall_server.context_time_index.psycopg.connect', return_value=connection) as connect:
            result = ensure_context_time_index('synthetic', apply=True, timeout_seconds=60)
        self.assertEqual(result['status'], 'ready')
        self.assertEqual(result['action'], 'created')
        self.assertTrue(connect.call_args.kwargs['autocommit'])
        self.assertEqual(sum(call.args[0] == CREATE_SQL for call in connection.execute.call_args_list), 1)
        self.assertIn('statement_timeout=60000', connect.call_args.kwargs['options'])

    def test_ready_is_idempotent_without_second_build(self):
        connection = self.connection([index_row()])
        with patch('recall_server.context_time_index.psycopg.connect', return_value=connection):
            result = ensure_context_time_index('synthetic', apply=True)
        self.assertEqual(result['action'], 'already_ready')
        self.assertEqual(connection.execute.call_count, 1)

    def test_invalid_and_incompatible_existing_indexes_are_not_overwritten(self):
        for change in ({'valid': False}, {'keys': ['tenant_id']}, {'partial': True}, {'method': 'hash'},
                       {'collations': [100, 100, 100, 0, 950]},
                       {'operator_classes': list(EXPECTED_CLASSES[:-1])+['pg_catalog.text_pattern_ops']}):
            row = index_row()
            row.update(change)
            connection=self.connection([row])
            with patch('recall_server.context_time_index.psycopg.connect', return_value=connection):
                result=ensure_context_time_index('synthetic', apply=True)
            self.assertIn(result['status'], ('invalid', 'incompatible'))
            self.assertEqual(result['action'], 'refused')
            self.assertEqual(connection.execute.call_count, 1)

    def test_lost_create_ack_never_retries_or_claims_no_mutation(self):
        connection=self.connection([None])
        def execute(sql):
            if sql == CREATE_SQL:
                raise OSError('private credentials or SQL text')
            return MagicMock(fetchone=lambda: None)
        connection.execute.side_effect=execute
        with patch('recall_server.context_time_index.psycopg.connect', return_value=connection):
            result=ensure_context_time_index('synthetic', apply=True)
        self.assertEqual(result['status'], 'inspect_required')
        self.assertEqual(result['outcome'], 'unknown')
        self.assertNotIn('private', str(result))
        self.assertEqual(sum(call.args[0] == CREATE_SQL for call in connection.execute.call_args_list), 1)

    def test_acknowledged_create_but_lost_verification_is_distinguished(self):
        connection=self.connection([None, OSError('private')])
        with patch('recall_server.context_time_index.psycopg.connect', return_value=connection):
            result=ensure_context_time_index('synthetic', apply=True)
        self.assertEqual(result['status'], 'inspect_required')
        self.assertTrue(result['ddl_acknowledged'])

    def test_invalid_timeout_refuses_before_connection(self):
        for value in (0, 1801, True, 1.5):
            with patch('recall_server.context_time_index.psycopg.connect') as connect:
                with self.assertRaises(ValueError):
                    ensure_context_time_index('synthetic', apply=True, timeout_seconds=value)
                connect.assert_not_called()

    def test_empty_dsn_never_falls_back_to_environment_defaults(self):
        for value in ('', ' ', None):
            with patch('recall_server.context_time_index.psycopg.connect') as connect:
                with self.assertRaises(ValueError):
                    ensure_context_time_index(value, apply=True)
                connect.assert_not_called()

    def test_concurrent_ddl_does_not_use_if_not_exists_or_schema_marker(self):
        self.assertIn('CREATE INDEX CONCURRENTLY', CREATE_SQL)
        self.assertNotIn('IF NOT EXISTS', CREATE_SQL)
        self.assertNotIn('schema_migrations', CREATE_SQL)
