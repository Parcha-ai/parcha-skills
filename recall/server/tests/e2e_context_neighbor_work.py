#!/usr/bin/env python3
"""Real PostgreSQL parity and EXPLAIN proof that neighbor prose work is bounded."""
import json
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from e2e_logical_evidence_projection import insert_record, insert_source
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.db import BrainStore

COUNT = 3000
ANCHOR = 1500
TENANT, SOURCE, PRINCIPAL = 'tenant:neighbors', 'source:neighbors', 'principal:neighbors'
START = datetime(2026, 9, 21, tzinfo=timezone.utc)


def native(n):
    return f'record:{n:06d}'


def walk(plan):
    yield plan
    for child in plan.get('Plans', ()):
        yield from walk(child)


def main():
    admin_dsn = os.environ['RECALL_DATABASE_URL']
    database = 'recall_neighbor_work_' + uuid.uuid4().hex
    settings = conninfo_to_dict(admin_dsn)
    settings['dbname'] = database
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{database}"')
    store = BrainStore(make_conninfo(**settings), search_deadline_ms=30_000)
    try:
        store.migrate()
        with store.connect() as conn:
            insert_source(conn, TENANT, PRINCIPAL, SOURCE)
            insert_record(conn, tenant=TENANT, source=SOURCE, parent='seed',
                                 native='seed', text='seed', role='assistant', byte_start=0)
            conn.execute('''INSERT INTO canonical_events(
                tenant_id,source_id,event_id,native_id,native_parent_id,artifact_id,job_id,
                kind,content_sha256,revision,occurred_at,observed_at,canonical_redacted)
                SELECT event.tenant_id,event.source_id,'evt_'||md5(n::text),
                       'record:'||lpad(n::text,6,'0'),'large-parent',event.artifact_id,event.job_id,
                       'message',repeat('a',64),1,%s::timestamptz+n*interval '1 second',
                       %s::timestamptz+n*interval '1 second','{}'::jsonb
                  FROM canonical_events event CROSS JOIN generate_series(0,%s) n
                 WHERE event.tenant_id=%s AND event.source_id=%s AND event.native_id='seed' ''',
                         (START, START, COUNT-1, TENANT, SOURCE))
            conn.execute('''INSERT INTO canonical_documents(
                tenant_id,source_id,document_id,event_id,artifact_id,native_id,
                content_sha256,revision,is_current,text_redacted,text_sha256)
                SELECT tenant_id,source_id,'doc_'||substr(event_id,5),event_id,artifact_id,native_id,
                       content_sha256,revision,true,'',repeat('b',64)
                  FROM canonical_events WHERE tenant_id=%s AND native_parent_id='large-parent' ''', (TENANT,))
            conn.execute('''INSERT INTO canonical_chunks(
                tenant_id,source_id,chunk_id,document_id,ordinal,receipt,text_redacted,text_sha256)
                SELECT tenant_id,source_id,'chk_'||md5(document_id||':'||ordinal::text),document_id,ordinal,
                       'recall://'||source_id||'/'||native_id||'?rev=1#item='||ordinal::text,
                       repeat(native_id||':'||ordinal::text||' β ',220),repeat('c',64)
                  FROM canonical_documents CROSS JOIN generate_series(0,7) ordinal
                 WHERE tenant_id=%s AND native_id LIKE 'record:%%' AND native_id<>%s''',
                         (TENANT, native(1501)))
            conn.execute('''UPDATE canonical_chunks SET deleted_at=now()
                            WHERE tenant_id=%s AND receipt LIKE %s''', (TENANT, '%/'+native(1499)+'?%'))
            conn.execute('''UPDATE canonical_documents SET is_current=false,deleted_at=now()
                            WHERE tenant_id=%s AND native_id=%s''', (TENANT, native(1498)))
            conn.execute('''UPDATE canonical_documents SET is_current=false
                            WHERE tenant_id=%s AND native_id=%s''', (TENANT, native(1497)))
            conn.execute('''INSERT INTO canonical_events(
                tenant_id,source_id,event_id,native_id,native_parent_id,artifact_id,job_id,
                kind,content_sha256,revision,occurred_at,observed_at,is_tombstone,canonical_redacted)
                SELECT tenant_id,source_id,'evt_'||md5('later-tombstone'),native_id,native_parent_id,
                       artifact_id,job_id,kind,repeat('f',64),2,occurred_at,observed_at,true,canonical_redacted
                  FROM canonical_events WHERE tenant_id=%s AND native_id=%s''', (TENANT, native(1496)))
            # Same-time neighbors exercise the existing native-ID tie breaker.
            conn.execute('''UPDATE canonical_events SET occurred_at=%s
                            WHERE tenant_id=%s AND native_id=ANY(%s)''',
                         (START+timedelta(seconds=ANCHOR), TENANT, [native(1495), native(1502), native(1503)]))
            for other_tenant, other_source in (('tenant:other', SOURCE), (TENANT, 'source:other')):
                insert_source(conn, other_tenant, PRINCIPAL, other_source)
                insert_record(conn, tenant=other_tenant, source=other_source, parent='large-parent',
                              native='outside', text='must not enter context', role='assistant', byte_start=0)
            for table in ('canonical_events', 'canonical_documents', 'canonical_chunks'):
                conn.execute('ANALYZE ' + table)
        retrieval = BoundCanonicalRetrieval(store, tenant_id=TENANT, principal_id=PRINCIPAL,
                                             authorized_sources=(SOURCE, 'source:other'))
        target = f'recall://{SOURCE}/{native(ANCHOR)}?rev=1#item=3'
        queries = []
        original = store._execute_bounded
        def capture(conn, sql, values, deadline):
            if 'AS chunks' in sql:
                queries.append((sql, values))
            return original(conn, sql, values, deadline)
        store._execute_bounded = capture
        eligible = [n for n in range(COUNT) if n not in (1496,1497,1498,1499,1501)]
        key = lambda n: (ANCHOR if n in (1495,1502,1503) else n, native(n))
        ordered = sorted(eligible, key=key)
        center = ordered.index(ANCHOR)
        for before, after in ((4,4), (0,0), (20,20)):
            actual = retrieval.session_context(target, before=before, after=after)
            expected = ordered[center-before:center] + [ANCHOR] + ordered[center+1:center+after+1]
            assert [event['native_id'] for event in actual['events']] == [native(n) for n in expected]
            for event, n in zip(actual['events'], expected):
                assert event['source_id'] == SOURCE and event['revision'] == 1
                ordinals = [2,3,4] if n == ANCHOR else [0,1]
                assert [chunk['ordinal'] for chunk in event['chunks']] == ordinals
                for chunk, ordinal in zip(event['chunks'], ordinals):
                    full = (native(n)+':'+str(ordinal)+' β ')*220
                    assert chunk['text'] == full[:4096]
                    assert chunk['text_clipped'] == (len(full)>4096)
                    assert chunk['receipt'] == f'recall://{SOURCE}/{native(n)}?rev=1#item={ordinal}'
            assert actual['anchor_receipt'] == target
        measurements = []
        with store.connect() as conn:
            for sql, values in queries:
                result = conn.execute('EXPLAIN (ANALYZE, BUFFERS, VERBOSE, FORMAT JSON) '+sql, values).fetchone()
                report = result['QUERY PLAN'][0]
                body_scans = [node for node in walk(report['Plan'])
                              if node.get('Relation Name') == 'canonical_chunks' and node.get('Alias') == 'bounded']
                assert body_scans, 'EXPLAIN must witness actual chunk body access'
                requested = values[-1]
                visits = sum(node['Actual Loops'] for node in body_scans)
                read_rows = sum(node['Actual Rows']*node['Actual Loops'] for node in body_scans)
                measurements.append(dict(requested=requested, body_scan_loops=visits,
                                         body_rows=read_rows, execution_ms=report['Execution Time']))
                assert visits <= requested, measurements
                assert read_rows <= 2*requested, measurements
        print(json.dumps({'status':'pass', 'parent_events':COUNT,
                          'chunks_per_event':8, 'exact_context_parity':True,
                          'empty_deleted_historical_tombstoned_excluded':True,
                          'measurements':measurements}, sort_keys=True))
    finally:
        store.close()
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE "{database}"')


if __name__ == '__main__':
    main()
