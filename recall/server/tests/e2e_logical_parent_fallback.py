#!/usr/bin/env python3
"""Exact fallback/record parity and bounded TOAST work for the parent cursor."""
from __future__ import annotations

from dataclasses import asdict
import hashlib
import itertools
import json
import os
from pathlib import Path
import re
import sys
import uuid

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER), str(SERVER.parent)]
from e2e_canonical_body_thinning import insert_document  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector  # noqa: E402

LEGACY_SHA = '1e4243233b7bc28eb36890105e51b5139b240e49c96b01575a57464be0fd8d23'
OVERSIZED = 'application/vnd.recall.oversized-record+gzip'
ROOT = re.compile(r'CROSS JOIN LATERAL jsonb_to_record\(.*?\)\s+(?:AS\s+)?root_fields\(role text,\s*type text,\s*content jsonb\)', re.S)


def queries():
    current = next(value for value in CanonicalLogicalEvidenceProjector._prepare_batch_and_upload.__code__.co_consts
                   if isinstance(value, str) and 'fallback_role_values' in value)
    legacy = ROOT.sub('', current)
    for field in ('role', 'type'):
        for nested in ('message', 'payload'):
            legacy = re.sub(r"root_fields\.content\s*#>>\s*'\{" + nested + ',' + field + r"\}'",
                            "event.canonical_redacted #>> '{content," + nested + ',' + field + "}'", legacy)
        legacy = legacy.replace("root_fields.content->>'" + field + "'", "event.canonical_redacted #>> '{content," + field + "}'")
        legacy = legacy.replace('root_fields.' + field, "event.canonical_redacted->>'" + field + "'")
    legacy = legacy.replace('root_fields.content', "event.canonical_redacted->'content'")
    assert hashlib.sha256(re.sub(r'\s+', '', legacy).encode()).hexdigest() == LEGACY_SHA
    return legacy, current


def expression_parity(connection, legacy, current):
    def selected(query):
        return query[query.index('jsonb_build_array('):query.index('END AS oversized_content') + len('END AS oversized_content')]
    lateral = ROOT.search(current)
    statements = ['SELECT ' + selected(query) + " FROM (SELECT %s::jsonb AS canonical_redacted,'event'::text AS event_id,'parent'::text AS native_parent_id,'native'::text AS native_id) event CROSS JOIN (SELECT %s::text AS media_type) artifact CROSS JOIN (SELECT 'event'::text AS event_id) document CROSS JOIN (SELECT 'parent'::text AS native_parent_id) selected " + suffix
                  for query, suffix in ((legacy, ''), (current, lateral.group() if lateral else ''))]
    values = [None, True, False, 0, 1, -2, 1.25, 'text', '', [], [1, 'x'], {}, {'x': 1}]
    roots = [None, [], [{'role': 'hidden'}], 1, True, 'scalar', {}]
    for value in values:
        roots.extend([{'role': value, 'type': value}, {'content': value},
                      {'content': {'role': value, 'type': value}},
                      {'content': {'message': value, 'payload': value}},
                      {'content': {'message': {'role': value, 'type': value}, 'payload': {'role': value, 'type': value}}}])
    roots.extend([{'content': {'role': 'outer', 'message': {'role': 'inner', 'type': 'message'}, 'payload': {'role': 'payload', 'type': 'different'}}},
                  {'content': {'unexpected': {'private': 'synthetic'}}}])
    for root, media in itertools.product(roots, ('application/json', OVERSIZED)):
        rows = [connection.execute(statement, (OVERSIZED, json.dumps(root), media)).fetchone() for statement in statements]
        assert rows[0] == rows[1], 'JSON fallback or oversized-content semantics changed'
    return len(roots) * 2


def digest_rows(rows):
    return hashlib.sha256(json.dumps(rows, sort_keys=True, default=str).encode()).hexdigest()


def main():
    store = BrainStore(os.environ['RECALL_DATABASE_URL'])
    store.migrate()
    tenant = 'tenant:fallback:' + uuid.uuid4().hex
    source, principal = 'source:fallback', 'principal:fallback'
    seed = 'fallback:' + uuid.uuid4().hex
    legacy, current = queries()
    try:
        with store.connect() as connection:
            comparisons = expression_parity(connection, legacy, current)
            insert_document(connection, tenant=tenant, principal=principal, source=source, suffix=seed, text='Synthetic preserved body only')
            insert_document(connection, tenant=tenant, principal=principal, source=source + ':other', suffix=seed + ':other', text='Other source must remain absent')
            connection.execute('UPDATE canonical_events SET native_parent_id=%s WHERE tenant_id=%s AND source_id=%s', ('session:' + seed, tenant, source + ':other'))
            for table, name in (('canonical_events', 'events'), ('canonical_documents', 'docs'), ('canonical_chunks', 'chunks')):
                connection.execute(f'CREATE TEMP TABLE fallback_{name} ON COMMIT DROP AS SELECT * FROM {table} WHERE tenant_id=%s AND source_id=%s', (tenant, source))
            connection.execute("""INSERT INTO canonical_events(tenant_id,source_id,event_id,native_id,native_parent_id,artifact_id,job_id,kind,content_sha256,revision,occurred_at,observed_at,canonical_redacted)
                SELECT tenant_id,source_id,'evt_'||md5('bulk:'||i||':'||rev),'native:'||i,
                       CASE WHEN i=256 THEN NULL WHEN i<=256 THEN 'parent:heavy' ELSE 'parent:'||(i%16) END,
                       artifact_id,job_id,kind,repeat(md5('content:'||i||':'||rev),2),rev,occurred_at,observed_at,
                       jsonb_set(jsonb_set(canonical_redacted,'{content,large_duplicate}',to_jsonb(repeat('synthetic external compressed body ',750))),'{content,noise}',
                         to_jsonb((SELECT string_agg(md5(noise::text),'') FROM generate_series(1,100)noise)))
                FROM fallback_events CROSS JOIN generate_series(1,384)i CROSS JOIN generate_series(1,3)rev""")
            connection.execute('ANALYZE canonical_events')
            connection.execute("""INSERT INTO canonical_documents(tenant_id,source_id,document_id,event_id,artifact_id,native_id,content_sha256,revision,is_current,text_redacted,text_sha256,deleted_at)
                SELECT tenant_id,source_id,'doc_'||md5('bulk:'||i||':'||rev),'evt_'||md5('bulk:'||i||':'||rev),artifact_id,'native:'||i,
                       repeat(md5('content:'||i||':'||rev),2),rev,rev=3 AND i<>1,text_redacted,text_sha256,CASE WHEN i=1 AND rev=3 THEN now() END
                FROM fallback_docs CROSS JOIN generate_series(1,384)i CROSS JOIN generate_series(1,3)rev""")
            connection.execute('ANALYZE canonical_documents')
            connection.execute("""INSERT INTO canonical_chunks(tenant_id,source_id,chunk_id,document_id,ordinal,receipt,text_redacted,text_sha256)
                SELECT tenant_id,source_id,'chk_'||md5('bulk:'||i||':'||rev),'doc_'||md5('bulk:'||i||':'||rev),ordinal,
                       'recall://synthetic/'||i||'/'||rev,text_redacted,text_sha256
                FROM fallback_chunks CROSS JOIN generate_series(1,384)i CROSS JOIN generate_series(1,3)rev""")
            connection.execute('ANALYZE canonical_chunks')
            # Inspect the on-page datum without detoasting: external pointers
            # occupy 18 bytes, while this compressed JSONB value exceeds 4 KiB.
            connection.execute('CREATE EXTENSION IF NOT EXISTS pageinspect')
            storage = connection.execute("SELECT ctid::text AS location,pg_column_size(canonical_redacted) AS stored FROM canonical_events WHERE tenant_id=%s AND source_id=%s AND native_id='native:2' LIMIT 1", (tenant,source)).fetchone()
            block, item = map(int, storage['location'].strip('()').split(','))
            attribute = connection.execute("SELECT attnum FROM pg_attribute WHERE attrelid='canonical_events'::regclass AND attname='canonical_redacted'").fetchone()['attnum']
            pointer = connection.execute("SELECT octet_length(t_attrs[%s]) AS bytes FROM heap_page_item_attrs(get_raw_page('canonical_events',%s),'canonical_events'::regclass,false) WHERE lp=%s", (attribute,block,item)).fetchone()['bytes']
            assert pointer == 18 and storage['stored'] > 4096, 'fixture must retain an external JSONB pointer'
            parents = ['parent:heavy', 'parent:0', 'native:256', 'session:' + seed, 'missing']
            args = (list(range(len(parents))), [tenant] * len(parents), [source] * len(parents), parents, OVERSIZED)
            all_rows = []
            for query in (legacy, current):
                with connection.cursor(name='fallback_parity') as cursor:
                    cursor.itersize = 1000
                    cursor.execute(query, args)
                    all_rows.append(list(cursor))
            assert len(all_rows[0]) == len(all_rows[1]) == 264
            assert digest_rows(all_rows[0]) == digest_rows(all_rows[1])
            assert {row['source_id'] for row in all_rows[1]} == {source}
            assert all(row['document_revision'] == (1 if row['native_id'] == 'native:' + seed else 3) for row in all_rows[1])
            projector = CanonicalLogicalEvidenceProjector(store, None)
            streams = []
            for rows in all_rows:
                streams.append([asdict(record) for _, group in itertools.groupby(rows, key=lambda row: row['candidate_ordinal']) for record in projector._record_stream(group)])
            assert len(streams[0]) == 264 and digest_rows(streams[0]) == digest_rows(streams[1])
            # Warm both paths, then compare actual buffer work, never a wall-time threshold.
            buffers = []
            for query in (legacy, current, legacy, current):
                plan = connection.execute('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) ' + query, args).fetchone()['QUERY PLAN'][0]['Plan']
                buffers.append(plan['Shared Hit Blocks'] + plan['Shared Read Blocks'])
            assert buffers[3] < buffers[2] * .8, f'fallback extraction still repeats external JSONB access: {buffers}'
        print(json.dumps({'status': 'passed', 'json_shape_comparisons': comparisons, 'ordered_rows': 264, 'ordered_records': 264, 'external_pointer_bytes': pointer, 'stored_jsonb_bytes': storage['stored'], 'original_buffers': buffers[2], 'candidate_buffers': buffers[3]}))
    finally:
        store.close()


if __name__ == '__main__':
    main()
