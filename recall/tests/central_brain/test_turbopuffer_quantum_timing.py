"""Unfinished cooperative publication must expose work without counting pauses."""
import threading
import unittest
from unittest.mock import patch

from . import test_turbopuffer_month_timing as fixtures
from .test_turbopuffer_projection import TENANT, RateLimitError
from recall_server import turbopuffer_projection as owner


class QuantumTimingTest(unittest.TestCase):
    def fixture(self, **kwargs):
        catalog,projector,namespace=fixtures.MonthTimingTest.fixture(self,**kwargs)
        def close_paused():
            while projector._quantum_months:
                projector._quantum_months.popleft()[1].close()
        self.addCleanup(close_paused)
        return catalog,projector,namespace

    def metrics(self, logs):
        lines=[r.getMessage() for r in logs.records
               if r.getMessage().startswith('search plane quantum timing ')]
        self.assertEqual(len(lines),1, 'unfinished search work has no quantum attribution')
        values=dict(token.split('=') for token in lines[0].split()[4:])
        self.assertTrue(all(value.isdecimal() for value in values.values()))
        self.assertNotIn(TENANT, lines[0])
        self.assertNotIn('private',lines[0])
        return {key:int(value) for key,value in values.items()}

    def test_slow_unfinished_claim_reports_each_quantum_and_excludes_pause(self):
        clock=fixtures.Clock()
        catalog,projector,namespace=self.fixture(count=5)
        def delayed(call,seconds):
            def invoke(*args,**kwargs):
                clock.advance(seconds)
                return call(*args,**kwargs)
            return invoke
        execute=catalog.execute
        def execute_with_pending(sql,params=None):
            if 'count(*)' in sql and 'search_projection_outbox' in sql:
                clock.advance(.2)
            return execute(sql,params)
        with patch.object(owner.time,'monotonic',clock), \
             patch.object(projector,'claim_months',delayed(projector.claim_months,.1)), \
             patch.object(projector,'read_watermark',delayed(projector.read_watermark,.3)), \
             patch.object(projector,'passage_page',delayed(projector.passage_page,.4)), \
             patch.object(projector,'finish_month',delayed(projector.finish_month,.7)), \
             patch.object(projector.pacer,'wait_for',lambda _:clock.advance(.5)), \
             patch.object(namespace,'write',delayed(namespace.write,.6)), \
             patch.object(catalog,'execute',execute_with_pending):
            metrics=[]
            for turn in range(3):
                with self.assertLogs(owner.LOG,level='INFO') as logs:
                    result=projector.drain_quantum(tenant_id=TENANT,max_months=1)
                metrics.append(self.metrics(logs))
                self.assertEqual(result['months'],int(turn==2))
                self.assertEqual(len(catalog.outbox),int(turn<2))
                if turn==0:clock.advance(100)  # Coordinator does unrelated work.
        self.assertEqual([m['quantum_ms'] for m in metrics],[2100,1700,2400])
        self.assertEqual([m['claim_ms'] for m in metrics],[100,0,0])
        self.assertEqual([m['catalog_ms'] for m in metrics],[300,0,0])
        self.assertEqual([m['commit_ms'] for m in metrics],[0,0,700])
        self.assertEqual([m['reported_rows'] for m in metrics],[2,2,1])
        for m in metrics:
            self.assertEqual((m['page_ms'],m['pacer_ms'],m['sdk_ms'],m['pending_ms']),
                             (400,500,600,200))
            self.assertEqual((m['page_calls'],m['sdk_calls'],m['pending_calls']),(1,1,1))
            self.assertEqual(m['returned'],1)

    def test_retry_and_failed_step_report_costs_without_early_ack(self):
        clock=fixtures.Clock()
        catalog,projector,namespace=self.fixture(count=3,sleep=clock.advance)
        calls=[]
        def fail(**kwargs):
            calls.append(kwargs)
            clock.advance(.25)
            if len(calls)==1:raise RateLimitError('private rate body')
            raise ValueError('private SDK body')
        with patch.object(owner.time,'monotonic',clock),patch.object(namespace,'write',fail), \
             self.assertLogs(owner.LOG,level='INFO') as logs:
            result=projector.drain_quantum(tenant_id=TENANT,max_months=1)
        m=self.metrics(logs)
        self.assertEqual((m['sdk_ms'],m['sdk_calls'],m['backoff_ms'],m['backoff_calls']),
                         (500,2,1000,1))
        self.assertEqual((m['quantum_ms'],m['failed'],m['commit_calls']),(1500,1,0))
        self.assertEqual(result['failed'],1)
        self.assertEqual(len(catalog.outbox),1)
        self.assertFalse(catalog.shards)

    def test_empty_queue_is_measured_without_month_cost(self):
        catalog,projector,_=self.fixture(count=0)
        catalog.outbox.clear()
        with self.assertLogs(owner.LOG,level='INFO') as logs:
            result=projector.drain_quantum(tenant_id=TENANT,max_months=1)
        m=self.metrics(logs)
        self.assertEqual((m['claim_calls'],m['pending_calls'],m['sdk_calls']),(1,1,0))
        self.assertEqual(result['status'],'complete')

    def test_claim_and_pending_errors_keep_original_exception_and_partial_timings(self):
        for phase in ('claim','pending'):
            with self.subTest(phase=phase):
                clock=fixtures.Clock()
                catalog,projector,_=self.fixture(count=3)
                original=ValueError('private SQL failure')
                claim=projector.claim_months
                execute=catalog.execute
                def fail_claim(*args,**kwargs):
                    if phase=='claim':
                        clock.advance(.375)
                        raise original
                    return claim(*args,**kwargs)
                def fail_pending(sql,params=None):
                    if phase=='pending' and 'count(*)' in sql and 'search_projection_outbox' in sql:
                        clock.advance(.375)
                        raise original
                    return execute(sql,params)
                with patch.object(owner.time,'monotonic',clock), \
                     patch.object(projector,'claim_months',fail_claim), \
                     patch.object(catalog,'execute',fail_pending), \
                     self.assertLogs(owner.LOG,level='INFO') as logs:
                    with self.assertRaises(ValueError) as raised:
                        projector.drain_quantum(tenant_id=TENANT,max_months=1)
                self.assertIs(raised.exception,original)
                m=self.metrics(logs)
                self.assertEqual((m[phase+'_ms'],m[phase+'_calls'],m['returned']),(375,1,0))
                self.assertEqual(len(catalog.outbox),1)
                self.assertFalse(catalog.shards)

    def test_parallel_sdk_sums_overlap_quantum_wall_for_unfinished_page(self):
        clock=fixtures.Clock()
        catalog,projector,namespace=self.fixture(count=5,batch_rows=1,write_concurrency=4)
        entering=threading.Barrier(4)
        leaving=threading.Barrier(4)
        write=namespace.write
        def concurrent_write(**kwargs):
            if entering.wait(timeout=5)==0:clock.advance(.5)
            leaving.wait(timeout=5)
            return write(**kwargs)
        with patch.object(owner.time,'monotonic',clock), \
             patch.object(namespace,'write',concurrent_write), \
             self.assertLogs(owner.LOG,level='INFO') as logs:
            result=projector.drain_quantum(tenant_id=TENANT,max_months=1)
        m=self.metrics(logs)
        self.assertEqual((m['quantum_ms'],m['sdk_ms'],m['sdk_calls']),(500,2000,4))
        self.assertEqual((result['rows'],result['months']),(4,0))
        self.assertEqual(len(catalog.outbox),1)

    def test_logger_failure_preserves_success_and_original_query_failure(self):
        for fail in (False,True):
            with self.subTest(fail=fail):
                catalog,projector,_=self.fixture(count=3)
                original=ValueError('original private claim error')
                with patch.object(owner.LOG,'info',side_effect=RuntimeError('private logger error')):
                    if fail:
                        with patch.object(projector,'claim_months',side_effect=original):
                            with self.assertRaises(ValueError) as raised:
                                projector.drain_quantum(tenant_id=TENANT,max_months=1)
                        self.assertIs(raised.exception,original)
                    else:
                        result=projector.drain_quantum(tenant_id=TENANT,max_months=1)
                        self.assertEqual((result['rows'],result['months']),(2,0))
                self.assertEqual(len(catalog.outbox),1)


if __name__=='__main__':
    unittest.main()
