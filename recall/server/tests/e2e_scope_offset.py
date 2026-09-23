#!/usr/bin/env python3
"""Real PostgreSQL: scope pages beyond 10,000 retain tenant/source isolation.

Run only on the disposable E2E database. Catalog-only fixtures need no archive/network.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys
import uuid

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / 'server')]
from recall_server.canonical import CanonicalPlane
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.db import BrainStore


def main():
    store = BrainStore(os.environ['RECALL_DATABASE_URL'], search_deadline_ms=5000)
    try:
        store.migrate()
        nonce = uuid.uuid4().hex
        tenant = 'tenant:scope:' + nonce
        other_tenant = 'tenant:scope-other:' + nonce
        principal = 'principal:scope:' + nonce
        source = 'source:scope:' + nonce
        foreign_source = 'source:scope-foreign:' + nonce
        with store.connect() as con:
            with con.transaction():
                for selected_tenant, selected_source, count, date in (
                    (tenant, source, 10_121, '2026-09-23T00:00:00Z'),
                    (other_tenant, source, 5, '2030-01-01T00:00:00Z'),
                    (tenant, foreign_source, 5, '2030-01-01T00:00:00Z'),
                ):
                    CanonicalPlane.register_source(con, tenant_id=selected_tenant,
                        principal_id=principal, source_id=selected_source)
                    con.execute('''WITH fixture AS (
                        SELECT %s::text AS tenant,%s::text AS source,i,
                               md5(%s::text || ':' || %s::text || ':' || i::text) AS identity,
                               %s::timestamptz-i*interval '1 second' AS occurred
                        FROM generate_series(1,%s) AS i
                    ) INSERT INTO canonical_evidence_documents(
                        tenant_id,source_id,logical_document_id,native_parent_id,revision,
                        evidence_id,manifest_artifact_id,manifest_storage_backend,
                        manifest_object_key,manifest_content_sha256,manifest_size_bytes,
                        manifest_media_type,manifest_encryption,manifest_version_id,
                        document_content_sha256,record_count,receipt_count,part_count,
                        first_occurred_at,last_occurred_at,source_updated_at
                    ) SELECT tenant,source,'ldoc_'||identity,'parent:'||i,1,
                        'evd_'||identity,'art_'||identity,'filesystem',
                        'objects/'||left(identity,2)||'/'||identity||identity,
                        identity||identity,1,
                        'application/vnd.recall.logical-document-manifest+json',
                        'filesystem-owner-only','fixture',identity||identity,1,1,1,
                        occurred,occurred,occurred FROM fixture''',
                        (selected_tenant, selected_source, selected_tenant, selected_source, date, count))
        bound = BoundCanonicalRetrieval(store, tenant_id=tenant,
            principal_id=principal, authorized_sources=(source,))
        page = bound.scope_documents(limit=80, offset=10_000)
        assert len(page['documents']) == 80 and page['complete'] is False
        late = bound.scope_documents(filters={'source_id':source}, limit=80, offset=10_080)
        expected = ['ldoc_' + hashlib.md5(f'{tenant}:{source}:{i}'.encode(), usedforsecurity=False).hexdigest()
                    for i in range(10_081, 10_122)]
        assert [row['logical_document_id'] for row in late['documents']] == expected
        assert all(row['source_id'] == source for row in late['documents'])
        assert late['complete'] is True and late['total_documents'] == 10_121
        denied = bound.scope_documents(filters={'source_id':foreign_source}, offset=10_080)
        assert denied['documents'] == [] and denied['complete'] is True
        other = BoundCanonicalRetrieval(store, tenant_id=other_tenant,
            principal_id=principal, authorized_sources=(source,)).scope_documents(limit=80)
        assert len(other['documents']) == 5 and other['total_documents'] == 5
        assert not ({row['logical_document_id'] for row in other['documents']} & set(expected))
        print(json.dumps({'status':'pass','authorized_documents':10_121,
                          'late_offset':10_080,'late_rows':41,
                          'tenant_isolation':True,'source_isolation':True}))
    finally:
        store.close()


if __name__ == '__main__':
    main()
