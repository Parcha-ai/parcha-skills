#!/usr/bin/env python3
"""Real PG: context stops eligibility probes after the requested live neighbors."""
from pathlib import Path
import hashlib
import json
import os
import sys
import tempfile
import uuid
from unittest.mock import patch

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
from e2e_logical_evidence_projection import insert_record, insert_source
from e2e_archive_reprojection import fixture
from e2e_logical_source_integrity import TrackedStore
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.chunk_retirement import retire_current_chunks


def eligible_probes(plan):
    """Include sequential/hash work as well as repeated index probes."""
    totals = dict(documents=0, chunks=0, tombstones=0)
    def walk(node):
        relation, alias = node.get('Relation Name'), node.get('Alias')
        key = ('documents' if relation == 'canonical_documents' else
               'chunks' if relation == 'canonical_chunks' else
               'tombstones' if relation == 'canonical_events' and alias == 'later' else None)
        if key:
            examined = node.get('Actual Rows', 0) + node.get('Rows Removed by Filter', 0)
            totals[key] += node.get('Actual Loops', 0) * max(1, examined)
        for child in node.get('Plans', []):
            walk(child)
    walk(plan)
    return totals


def recorded_context(store, reader, receipt):
    execute, captured = store._execute_bounded, []
    def record(connection, sql, args, deadline_at):
        if 'JOIN LATERAL' in sql and 'ORDER BY event.occurred_at' in sql:
            captured.append((sql, args))
        return execute(connection, sql, args, deadline_at)
    with patch.object(store, '_execute_bounded', side_effect=record):
        result = reader.session_context(receipt, before=2, after=2)
    assert result is not None and len(captured) == 2
    plans = []
    with store.connect() as connection:
        for sql, args in captured:
            plan = connection.execute('EXPLAIN (ANALYZE,FORMAT JSON) ' + sql, args).fetchone()['QUERY PLAN'][0]
            plans.append(eligible_probes(plan['Plan']))
    return result, plans


def natives(result):
    return [event['native_id'] for event in result['events']]


def archive_parity(store):
    with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, RECALL_CHUNK_BODY_READS='archive'):
        tenant, source, archive, _, _, _ = fixture(store, Path(directory), count=7)
        scope = dict(tenant_id=tenant, principal_id='principal:reprojection', authorized_sources=(source,))
        pg = BoundCanonicalRetrieval(store, **scope)
        reader = BoundCanonicalRetrieval(store, chunk_body_archive=archive, **scope)
        anchor = f'recall://{source}/event-0003?rev=1#item=0'
        expected = pg.session_context(anchor, before=2, after=2)
        assert reader.session_context(anchor, before=2, after=2) == expected
        with store.connect() as connection:
            documents = connection.execute('SELECT document_id FROM canonical_documents WHERE tenant_id=%s AND source_id=%s', (tenant, source)).fetchall()
        targets = dict(tenant_id=tenant, source_id=source, document_ids=tuple(row['document_id'] for row in documents))
        plan = retire_current_chunks(store, archive, **targets)
        retire_current_chunks(store, archive, **targets, apply=True, reviewed_plan=plan['plan'])
        assert reader.session_context(anchor, before=2, after=2) == expected


def revise_neighbor(store, tenant, source):
    text = 'Exact revised neighbor text α'
    digest = hashlib.sha256(text.encode()).hexdigest()
    event, document, chunk = ('evt_' + digest[:32], 'doc_' + digest[:32], 'chk_' + digest[:32])
    with store.connect() as connection:
        old = connection.execute("SELECT document_id FROM canonical_documents WHERE tenant_id=%s AND source_id=%s AND native_id='event-0001' AND is_current", (tenant, source)).fetchone()['document_id']
        connection.execute('UPDATE canonical_documents SET is_current=false WHERE tenant_id=%s AND source_id=%s AND document_id=%s', (tenant, source, old))
        connection.execute('''INSERT INTO canonical_events(tenant_id,source_id,event_id,native_id,native_parent_id,artifact_id,job_id,kind,content_sha256,revision,occurred_at,observed_at,is_tombstone,canonical_redacted)
            SELECT tenant_id,source_id,%s,native_id,native_parent_id,artifact_id,job_id,kind,%s,2,occurred_at,observed_at,false,canonical_redacted
            FROM canonical_events WHERE tenant_id=%s AND source_id=%s AND native_id='event-0001' AND revision=1''', (event, digest, tenant, source))
        connection.execute('''INSERT INTO canonical_documents(tenant_id,source_id,document_id,event_id,artifact_id,native_id,content_sha256,revision,is_current,text_redacted,text_sha256)
            SELECT tenant_id,source_id,%s,%s,artifact_id,native_id,%s,2,true,%s,%s FROM canonical_documents WHERE tenant_id=%s AND source_id=%s AND document_id=%s''', (document, event, digest, text, digest, tenant, source, old))
        connection.execute('''INSERT INTO canonical_chunks(tenant_id,source_id,chunk_id,document_id,ordinal,receipt,text_redacted,text_sha256)
            SELECT tenant_id,source_id,%s,%s,0,replace(receipt,'?rev=1#','?rev=2#'),%s,%s FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s AND document_id=%s AND ordinal=0''', (chunk, document, text, digest, tenant, source, old))


def main():
    admin_dsn = os.environ['RECALL_DATABASE_URL']
    database = 'recall_context_plan_' + uuid.uuid4().hex
    settings = conninfo_to_dict(admin_dsn) | {'dbname': database}
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{database}"')
    store = TrackedStore(make_conninfo(**settings))
    try:
        store.migrate()
        tenant, principal, source = 'tenant:context-plan', 'principal:context-plan', 'source:context-plan'
        count, middle = 1201, 600
        with store.connect() as connection:
            insert_source(connection, tenant, principal, source)
            # Same timestamps make native-id tie-breaking part of the contract.
            for index in range(count):
                insert_record(connection, tenant=tenant, source=source, parent='session',
                    native=f'event-{index:04}', text=f'Exact context text {index} α', role='assistant', byte_start=index)
            connection.execute('UPDATE canonical_events SET source_ordinal=1201-substring(native_id from 7)::int WHERE tenant_id=%s AND source_id=%s', (tenant, source))
            for table in ('canonical_events', 'canonical_documents', 'canonical_chunks'):
                connection.execute('ANALYZE ' + table)
        reader = BoundCanonicalRetrieval(store, tenant_id=tenant, principal_id=principal, authorized_sources=(source,))
        receipt = f'recall://{source}/event-{middle:04}?rev=1#item=0'
        result, plans = recorded_context(store, reader, receipt)
        assert natives(result) == [f'event-{n:04}' for n in range(middle-2, middle+3)]
        assert all(p['documents'] <= 2 and p['chunks'] <= 4 and p['tombstones'] <= 2 for p in plans), plans
        # Long invalid prefixes must be skipped without any candidate ceiling.
        # Preserve two eligible events at either end, plus the anchor.
        with store.connect() as connection:
            rows = connection.execute('SELECT event_id,native_id FROM canonical_events WHERE tenant_id=%s AND source_id=%s ORDER BY native_id', (tenant, source)).fetchall()
            for index, row in enumerate(rows):
                if index in (0, 1, middle, count-2, count-1):
                    continue
                if index % 4 == 0:
                    connection.execute('UPDATE canonical_documents SET is_current=false WHERE tenant_id=%s AND source_id=%s AND event_id=%s', (tenant, source, row['event_id']))
                elif index % 4 == 1:
                    connection.execute('UPDATE canonical_documents SET is_current=false,deleted_at=now() WHERE tenant_id=%s AND source_id=%s AND event_id=%s', (tenant, source, row['event_id']))
                elif index % 4 == 2:
                    connection.execute('DELETE FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s AND document_id IN(SELECT document_id FROM canonical_documents WHERE tenant_id=%s AND source_id=%s AND event_id=%s)', (tenant, source, tenant, source, row['event_id']))
                else:
                    suffix = hashlib.sha256((row['native_id'] + ':later-tombstone').encode()).hexdigest()
                    connection.execute('''INSERT INTO canonical_events(tenant_id,source_id,event_id,native_id,native_parent_id,artifact_id,job_id,kind,content_sha256,revision,occurred_at,observed_at,is_tombstone,canonical_redacted)
                        SELECT tenant_id,source_id,%s,native_id,native_parent_id,artifact_id,job_id,'tombstone',%s,revision+1,occurred_at,observed_at,true,'{}'::jsonb
                        FROM canonical_events WHERE tenant_id=%s AND source_id=%s AND event_id=%s''', ('evt_' + suffix[:32], suffix, tenant, source, row['event_id']))
        result, _ = recorded_context(store, reader, receipt)
        assert natives(result) == ['event-0000', 'event-0001', 'event-0600', 'event-1199', 'event-1200']
        assert result['anchor_receipt'] == receipt and result['bounds'] == {'before': 2, 'after': 2}
        revise_neighbor(store, tenant, source)
        revised, _ = recorded_context(store, reader, receipt)
        assert natives(revised) == natives(result)
        assert revised['events'][1]['revision'] == 2
        assert revised['events'][1]['chunks'][0]['receipt'] == f'recall://{source}/event-0001?rev=2#item=0'
        assert revised['events'][1]['chunks'][0]['text'] == 'Exact revised neighbor text α'
        assert reader.session_context(f'recall://{source}/event-0001?rev=1#item=0') is None
        denied = BoundCanonicalRetrieval(store, tenant_id=tenant, principal_id=principal, authorized_sources=())
        assert denied.session_context(receipt) is None
        wrong = BoundCanonicalRetrieval(store, tenant_id='tenant:other', principal_id=principal, authorized_sources=(source,))
        assert wrong.session_context(receipt) is None
        # This is a response/plan regression, not a shared-host latency test.
        store.search_deadline_ms = 30_000
        archive_parity(store)
        print(json.dumps(dict(status='pass', bounded_live_eligibility_probes=plans,
            timestamp_ties_exact=True, historical_deleted_missing_chunk_and_later_tombstone_skipped=True,
            no_candidate_ceiling=True, current_revision_receipt_exact=True,
            archive_context_exact_after_retirement=True, tenant_and_source_fences=True)))
    finally:
        store.close()
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE "{database}" WITH (FORCE)')


if __name__ == '__main__':
    main()
