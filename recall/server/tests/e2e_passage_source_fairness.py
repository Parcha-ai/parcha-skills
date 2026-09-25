#!/usr/bin/env python3
"""Observed passage starvation: source progression before bulk work exhausts."""
from collections import defaultdict
from contextlib import contextmanager
from datetime import timedelta
import json
import threading
import unittest
from unittest.mock import patch

from e2e_passage_set_admission import SetAdmission
from recall_server.logical_evidence import LogicalEvidenceError


class PassageSourceFairness(SetAdmission):
    def populate_bulk(self, count=73581, *, prefix='bulk', age=840):
        self.connection.execute('''INSERT INTO canonical_passage_projection_queue
            SELECT 'a','source:a-cheap',%s||'-'||lpad(n::text,6,'0'),1,7,%s,NULL
            FROM generate_series(1,%s)n''', (prefix,self.now-timedelta(seconds=age),count))
        self.connection.execute('''INSERT INTO canonical_evidence_documents
            (tenant_id,source_id,logical_document_id,revision)
            SELECT tenant_id,source_id,logical_document_id,revision
            FROM canonical_passage_projection_queue q
            WHERE q.source_id='source:a-cheap'
            ON CONFLICT DO NOTHING''')
        self.connection.execute('''INSERT INTO canonical_evidence_document_parts
            (tenant_id,source_id,logical_document_id,revision,part_ordinal,artifact_id,size_bytes)
            SELECT tenant_id,source_id,logical_document_id,revision,0,logical_document_id,941
            FROM canonical_passage_projection_queue q
            WHERE q.source_id='source:a-cheap'
            ON CONFLICT DO NOTHING''')

    def ack(self, candidates):
        for item in candidates:
            self.connection.execute('''DELETE FROM canonical_passage_projection_queue
                WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s
                  AND revision=%s AND generation=%s''',
                (item.tenant_id,item.source_id,item.logical_document_id,item.revision,item.generation))

    def test_thirteen_sources_progress_in_two_turns_per_mode_with_continuing_bulk_arrivals(self):
        self.populate_bulk()
        for source in range(12):
            for ordinal in range(36 if source<9 else 35):
                self.add(f'history-{ordinal:03}',[1112+source],source=f'source:b-{source:02}',
                         age=60444-ordinal)
            if source<6:
                self.add('recent',[900000],source=f'source:b-{source:02}',age=20)
        self.assertEqual(sum(r['key'][1].startswith('source:b-') for r in self.expected),435)
        seen=defaultdict(set)
        expected={'source:a-cheap'}|{f'source:b-{n:02}' for n in range(12)}
        first={}
        for turn in range(4):
            recent=bool(turn%2)
            self.projector._prefer_notification_admission=recent
            rows=self.projector._pending(tenant_id='a',limit=10)
            self.assertEqual(len(rows),10)
            self.assertEqual(len({(r.tenant_id,r.source_id,r.logical_document_id) for r in rows}),10)
            # More eligible sources than slots: all ten are distinct source heads.
            self.assertEqual(len({r.source_id for r in rows}),10,
                'cheap bulk source consumed peer admission while peers were current and ready')
            for row in rows:
                seen[recent].add(row.source_id)
                first.setdefault((recent,row.source_id),row.logical_document_id)
            self.ack(rows)
            self.populate_bulk(50,prefix=f'arrival-{turn}',age=0)
        self.assertEqual(seen[False],expected)
        self.assertEqual(seen[True],expected)
        for source in range(6):
            self.assertEqual(first[False,f'source:b-{source:02}'],'history-000')
            self.assertEqual(first[True,f'source:b-{source:02}'],'recent')
        left=self.connection.execute("SELECT count(*) AS n FROM canonical_passage_projection_queue WHERE source_id='source:a-cheap'").fetchone()['n']
        self.assertGreater(left,73581,'peer progress must not depend on cheap-source exhaustion')
        print(json.dumps(dict(sources=13,batch=10,turns=4,normal_sources=len(seen[False]),
                              recent_sources=len(seen[True]),bulk_remaining=left)),flush=True)

    def test_trusted_notifications_keep_global_fifo_ahead_of_smaller_rotating_peers(self):
        for index in range(13):
            self.add('history',[1],source=f'peer-{index:02}',age=900)
        self.add('first-notification',[500000],source='z',notified=300,age=30)
        self.add('second-notification',[1],source='a',notified=100,age=30)
        self.projector._prefer_notification_admission=True
        first=self.projector._pending(tenant_id='a',limit=1)
        self.assertEqual([r.logical_document_id for r in first],['first-notification'])
        self.ack(first)
        self.projector._prefer_notification_admission=True
        second=self.projector._pending(tenant_id='a',limit=1)
        self.assertEqual([r.logical_document_id for r in second],['second-notification'])
        self.ack(second)
        self.projector._prefer_notification_admission=True
        fallback=self.projector._pending(tenant_id='a',limit=10)
        self.assertEqual(len({r.source_id for r in fallback}),10)

    def test_oldest_large_and_newest_small_both_get_same_source_turns(self):
        self.add('ancient-large',[900000000],age=90000)
        self.add('middle',[100],age=600)
        for turn in range(3):
            self.add(f'new-{turn}',[1],age=-turn)
            self.projector._prefer_notification_admission=True
            recent=self.projector._pending(tenant_id='a',limit=1)
            self.assertEqual([r.logical_document_id for r in recent],[f'new-{turn}'])
            self.ack(recent)
        self.projector._prefer_notification_admission=False
        old=self.projector._pending(tenant_id='a',limit=1)
        self.assertEqual([r.logical_document_id for r in old],['ancient-large'])
        self.assertEqual(old[0].part_references[0]['size_bytes'],900000000)
        self.ack(old)
        self.assertEqual([r.logical_document_id for r in self.projector._pending(tenant_id='a',limit=1)],['middle'])

    def test_invalid_oldest_keeps_queue_but_recent_healthy_sibling_progresses(self):
        self.add('bad-oldest',[],age=90000)
        self.add('healthy-middle',[100],age=600)
        self.add('healthy-recent',[200],age=10)
        self.projector.store.pool_max_size=4
        attempted=[]
        def prepare(candidate):
            attempted.append(candidate.logical_document_id)
            return candidate
        def commit(candidate):
            self.ack([candidate])
            return {'status':'complete','inserted':1,'deleted':0,'retained':0}
        with patch.object(self.projector,'_prepare',prepare),patch.object(self.projector,'_commit',commit):
            # Use the actual coordinator, which must advance even when hydration
            # returned no candidate from a nonempty admitted turn.
            for _ in range(4):
                self.projector.project_pending(tenant_id='a',batch_size=1,max_batches=1,concurrency=1)
        self.assertEqual(attempted,['healthy-recent','healthy-middle'])
        remaining=self.connection.execute('SELECT logical_document_id FROM canonical_passage_projection_queue').fetchall()
        self.assertEqual([r['logical_document_id'] for r in remaining],['bad-oldest'])
        # No silent ACK/retry scheduler: under endless fresh arrivals the bad
        # oldest can still delay middle history within its own source.

    def test_empty_queue_does_not_advance_mode_or_source_cursor(self):
        self.projector.project_pending(tenant_id='a',batch_size=1,max_batches=1,concurrency=1)
        self.assertFalse(self.projector._prefer_notification_admission)
        self.assertEqual(self.projector._ordinary_source_cursor,{False:None,True:None})

    def test_notification_prefix_and_residual_advance_only_last_ordinary_source(self):
        for source in ('a','b','c'):
            for n in range(4):
                self.add(f'parent-{n}',[100-n],source=source,age=100+n)
        self.add('notification',[900000],source='z',notified=300)
        self.projector._prefer_notification_admission=True
        self.assertEqual([r.logical_document_id for r in self.projector._pending(tenant_id='a',limit=1)],['notification'])
        self.assertIsNone(self.projector._ordinary_source_cursor[True])
        self.projector._prefer_notification_admission=True
        rows=self.projector._pending(tenant_id='a',limit=6)
        self.assertEqual(rows[0].logical_document_id,'notification')
        self.assertEqual({r.source_id for r in rows[1:]},{'a','b','c'})
        self.assertEqual(self.projector._ordinary_source_cursor[True],('a','c'))
        self.assertIsNone(self.projector._ordinary_source_cursor[False])
        self.ack(rows)
        self.add('late-source',[1],source='d',age=100)
        self.projector._prefer_notification_admission=True
        self.assertEqual([r.source_id for r in self.projector._pending(tenant_id='a',limit=1)],['d'])

    def test_missing_metadata_rotates_without_acknowledging_invalid_queue(self):
        for n in range(10):
            self.add('bad',[],source=f's-{n:02}',age=900,evidence=n%2==0)
        for n in range(10,13):
            self.add('valid',[10000],source=f's-{n:02}',age=800)
        first=self.projector._pending(tenant_id='a',limit=10)
        self.assertEqual(first,())
        self.projector._prefer_notification_admission=False
        second=self.projector._pending(tenant_id='a',limit=10)
        self.assertEqual({r.source_id for r in second},{'s-10','s-11','s-12'})
        self.assertEqual(self.connection.execute('SELECT count(*) AS n FROM canonical_passage_projection_queue').fetchone()['n'],13)

    def test_unavailable_and_stale_owners_do_not_pin_other_source_admission(self):
        # Real admission/coordinator, two existing owners. Failure paths retain queue;
        # successful commit is represented by an ACK only after the test's preparation.
        for n in range(13):
            self.add('parent',[1000+n],source=f's-{n:02}',age=900)
        self.projector.store.pool_max_size=4
        lock=threading.RLock()
        @contextmanager
        def serialized_connection():
            with lock:
                yield self.connection
        self.projector.store.connect=serialized_connection
        attempted=set()
        def prepare(candidate):
            attempted.add(candidate.source_id)
            ordinal=int(candidate.source_id[2:])
            if ordinal<10 and ordinal%3==0:
                raise LogicalEvidenceError('logical_evidence_unavailable')
            if ordinal<10 and ordinal%3==1:
                raise LogicalEvidenceError('logical_evidence_not_found')
            return candidate
        def commit(candidate):
            if int(candidate.source_id[2:])<10:
                return {'status':'stale'}
            with lock:
                self.ack([candidate])
            return {'status':'complete','inserted':1,'deleted':0,'retained':0}
        with patch.object(self.projector,'_prepare',prepare),patch.object(self.projector,'_commit',commit),patch.object(self.projector,'_requeue_missing',return_value=0):
            for _ in range(4):
                self.projector.project_pending(tenant_id='a',batch_size=10,max_batches=1,concurrency=2)
        self.assertEqual(attempted,{f's-{n:02}' for n in range(13)})
        remaining=self.connection.execute('SELECT source_id FROM canonical_passage_projection_queue ORDER BY source_id').fetchall()
        self.assertEqual([r['source_id'] for r in remaining],[f's-{n:02}' for n in range(10)])


if __name__=='__main__':
    suite=unittest.TestSuite(PassageSourceFairness(name) for name in PassageSourceFairness.__dict__
                             if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
