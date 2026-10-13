#!/usr/bin/env python3
"""Real PostgreSQL proof that one backfill source cannot monopolize admission.

Uses session-local temporary tables and the production _pending query. No
archive, ingestion, provider, or model calls occur.
"""
from __future__ import annotations

from contextlib import nullcontext
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace

import psycopg
from psycopg.rows import dict_row

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / "server")]
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector, MAX_LOGICAL_ATTEMPTS  # noqa: E402


def main():
    with psycopg.connect(os.environ["RECALL_DATABASE_URL"], row_factory=dict_row) as connection:
        connection.execute("""CREATE TEMP TABLE canonical_evidence_document_queue (
            tenant_id text, source_id text, native_parent_id text,
            generation bigint DEFAULT 1, reason text DEFAULT 'ingest',
            notification_queued_at timestamptz,
            changed_at timestamptz DEFAULT now()-interval '1 hour',
            first_queued_at timestamptz DEFAULT now()-interval '1 hour',
            attempts integer DEFAULT 0, next_attempt_at timestamptz,
            PRIMARY KEY(tenant_id,source_id,native_parent_id))""")
        connection.execute("""CREATE TEMP TABLE canonical_evidence_documents (
            tenant_id text, source_id text, native_parent_id text,
            logical_document_id text, revision bigint, record_count bigint)""")
        connection.execute("""CREATE TEMP TABLE canonical_evidence_document_parts (
            tenant_id text, source_id text, logical_document_id text,
            revision bigint, size_bytes bigint)""")
        store = SimpleNamespace(connect=lambda: nullcontext(connection))
        projector = CanonicalLogicalEvidenceProjector(store, None)

        def pending(limit=3, quiet=0, wait=0, tenant="tenant:test"):
            return projector._pending(tenant_id=tenant, limit=limit,
                quiet_seconds=quiet, max_wait_seconds=wait)

        def add(source, parent, **fields):
            columns = ["tenant_id", "source_id", "native_parent_id", *fields]
            values = ["tenant:test", source, parent, *fields.values()]
            from psycopg import sql
            connection.execute(sql.SQL("INSERT INTO canonical_evidence_document_queue ({}) VALUES ({})").format(
                sql.SQL(",").join(map(sql.Identifier, columns)), sql.SQL(",").join(sql.Placeholder() for _ in values)), values)

        connection.execute("""INSERT INTO canonical_evidence_document_queue
            (tenant_id,source_id,native_parent_id,reason,changed_at,first_queued_at)
            SELECT 'tenant:test','source:bulk',n::text,'backfill',
                   now()-interval '2 days',now()-interval '2 days'
            FROM generate_series(1,37000) n""")
        add("source:claude", "active-claude")
        add("source:codex", "active-codex")
        first = pending()
        assert len(first) == 3
        assert {row.source_id for row in first} == {
            "source:bulk", "source:claude", "source:codex"
        }, [(row.source_id, row.native_parent_id) for row in first]

        connection.execute("""INSERT INTO canonical_evidence_documents VALUES
            ('tenant:test','source:claude','active-claude','document:claude',5,42)""")
        connection.execute("""INSERT INTO canonical_evidence_document_parts VALUES
            ('tenant:test','source:claude','document:claude',5,100),
            ('tenant:test','source:claude','document:claude',5,200)""")
        enriched = next(row for row in pending() if row.source_id == "source:claude")
        assert (enriched.revision, enriched.estimated_records, enriched.estimated_bytes) == (6, 42, 300)

        # Eligibility filtering precedes ranking: blocked candidates cannot
        # consume a source's first slot or hide its ready successor.
        connection.execute("TRUNCATE canonical_evidence_document_queue")
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        add("source:bulk", "quarantined", attempts=MAX_LOGICAL_ATTEMPTS)
        add("source:bulk", "backoff", next_attempt_at=now+timedelta(hours=1))
        add("source:bulk", "debounced", changed_at=now, first_queued_at=now)
        add("source:bulk", "ready", first_queued_at=now-timedelta(days=2))
        add("source:claude", "max-wait", changed_at=now,
            first_queued_at=now-timedelta(hours=1))
        add("source:codex", "forget", reason="forget", changed_at=now, first_queued_at=now)
        add("source:backfill", "backfill", reason="backfill", changed_at=now, first_queued_at=now)
        connection.execute("""INSERT INTO canonical_evidence_document_queue
            (tenant_id,source_id,native_parent_id,reason) VALUES
            ('tenant:other','source:other','other-tenant','forget')""")
        eligible = pending(10, quiet=60, wait=300)
        assert {row.native_parent_id for row in eligible} == {"ready", "max-wait", "forget", "backfill"}
        assert pending(1, quiet=60, wait=300)[0].native_parent_id == "forget"
        assert len(pending(2, quiet=60, wait=300)) == 2
        assert any(row.tenant_id == "tenant:other" for row in pending(10, quiet=60, wait=300, tenant=None))

        # A busy parent's changed_at moving forward cannot erase its age.
        connection.execute("TRUNCATE canonical_evidence_document_queue")
        add("source:one", "old-first", changed_at=now-timedelta(minutes=2), first_queued_at=now-timedelta(days=2))
        add("source:one", "new-first", changed_at=now-timedelta(days=1), first_queued_at=now-timedelta(days=1))
        assert pending(1)[0].native_parent_id == "old-first"

        # Drain finite mixed work across bounded calls, including >1 urgent
        # item on one source. No eligible parent is duplicated or stranded.
        connection.execute("TRUNCATE canonical_evidence_document_queue")
        for number in range(17):
            add("source:bulk", f"bulk-{number}")
        for number in range(3):
            add(f"source:small-{number}", f"small-{number}")
        for number in range(2):
            add("source:urgent", f"forget-{number}", reason="forget")
        seen = set()
        rounds = 0
        while rows := pending(3):
            assert len(rows) <= 3
            rounds += 1
            for row in rows:
                identity = (row.tenant_id, row.source_id, row.native_parent_id)
                assert identity not in seen
                seen.add(identity)
                connection.execute("""DELETE FROM canonical_evidence_document_queue
                    WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s""", identity)
            assert rounds <= 22
        assert len(seen) == 22
    print(json.dumps(dict(status="pass", bulk_queue=37000, eligible_drained=len(seen), rounds=rounds)))


if __name__ == "__main__":
    main()
