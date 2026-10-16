"""Native history proof retains deadline and rollback semantics with pipelined SQL."""
import time
import unittest
from unittest.mock import patch

import psycopg
from psycopg.pq import PipelineStatus, TransactionStatus

import e2e_collector_history_recovery as fixture
from recall_server.db import BrainStore, SearchDeadlineExceeded
from recall_server.retirement_process import RetirementStore


class RecoveryDeadlinePipeline(fixture.CollectorHistoryRecovery):
    def test_history_proof_pairs_deadline_with_query_and_exits_before_writes(self):
        execute = psycopg.Connection.execute
        stats = dict(pipelined_deadlines=0,pipelined_statements=0,normal_deadlines=0)
        def traced(connection, query, params=None, **kwargs):
            setting = str(query).startswith("SELECT set_config('statement_timeout'")
            pipelined = connection.info.pipeline_status != PipelineStatus.OFF
            if setting:
                stats['pipelined_deadlines' if pipelined else 'normal_deadlines'] += 1
            elif pipelined:
                stats['pipelined_statements'] += 1
            return execute(connection,query,params,**kwargs)
        original_write = self.plane._write_authorized_document
        def write(connection, **kwargs):
            self.assertEqual(connection.info.pipeline_status,PipelineStatus.OFF)
            return original_write(connection,**kwargs)
        with patch.object(psycopg.Connection,'execute',traced), patch.object(self.plane,'_write_authorized_document',write):
            result = self.recover()
        self.assertEqual(result['status'],'committed')
        self.assertGreater(stats['pipelined_deadlines'],0)
        self.assertEqual(stats['pipelined_deadlines'],stats['pipelined_statements'])
        self.assertGreater(stats['normal_deadlines'],0)  # Authority helpers retain their existing owner.
        self.assertEqual(RetirementStore._execute_bounded,BrainStore._execute_bounded_pipeline)

    def test_pipeline_exit_cancellation_rolls_back_and_connection_remains_reusable(self):
        with self.store.connect() as connection:
            with self.assertRaises(SearchDeadlineExceeded):
                with connection.transaction():
                    BrainStore._execute_bounded_pipeline(connection,'SELECT pg_sleep(1)',(),time.monotonic()+0.025)
            self.assertEqual(connection.info.pipeline_status,PipelineStatus.OFF)
            self.assertEqual(connection.info.transaction_status,TransactionStatus.IDLE)
            self.assertEqual(connection.execute('SELECT 7 AS n').fetchone()['n'],7)

    def test_expired_deadline_executes_no_query_and_no_deadline_remains(self):
        with self.store.connect() as connection:
            with self.assertRaises(SearchDeadlineExceeded):
                with connection.transaction():
                    BrainStore._execute_bounded_pipeline(connection,'SELECT 1',(),time.monotonic()-1)
            self.assertEqual(connection.info.pipeline_status,PipelineStatus.OFF)
            self.assertEqual(connection.info.transaction_status,TransactionStatus.IDLE)
            self.assertEqual(connection.execute('SELECT 9 AS n').fetchone()['n'],9)

    def test_no_deadline_preserves_normal_executor_without_pipeline(self):
        with self.store.connect() as connection:
            original_pipeline = connection.pipeline
            with patch.object(connection,'pipeline',side_effect=AssertionError('unexpected_pipeline')):
                row = BrainStore._execute_bounded_pipeline(connection,'SELECT 11 AS n',(),None).fetchone()
            self.assertEqual(row['n'],11)
            self.assertEqual(connection.pipeline,original_pipeline)


if __name__=='__main__':
    suite=unittest.TestSuite(RecoveryDeadlinePipeline(name) for name in (
        'test_history_proof_pairs_deadline_with_query_and_exits_before_writes',
        'test_pipeline_exit_cancellation_rolls_back_and_connection_remains_reusable',
        'test_expired_deadline_executes_no_query_and_no_deadline_remains',
        'test_no_deadline_preserves_normal_executor_without_pipeline',
    ))
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
