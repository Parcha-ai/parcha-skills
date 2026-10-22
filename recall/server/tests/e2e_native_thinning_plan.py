"""Exact native thinning plans stay proportional to selected keys, not their square."""

import os
import sys
import json
from pathlib import Path

W = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(W), str(W / "server"), str(W / "server/tests")]
from recall_server.db import BrainStore  # noqa: E402
from recall_server.canonical_thinning import _thin_selected  # noqa: E402
from e2e_canonical_body_thinning import insert_document  # noqa: E402
from psycopg.types.json import Jsonb  # noqa: E402

store = BrainStore(os.environ["RECALL_DATABASE_URL"])
store.migrate()
tables = [
    "raw_artifacts",
    "canonical_events",
    "canonical_documents",
    "canonical_chunks",
    "canonical_evidence_documents",
    "canonical_evidence_document_queue",
    "canonical_chunk_retirement_progress",
]
with store.connect() as c:
    event, doc = insert_document(
        c,
        tenant="tenant:plan",
        principal="principal:plan",
        source="source:plan",
        suffix="target",
        text="unchanged immutable body",
    )
    c.execute(
        "INSERT INTO canonical_evidence_document_queue(tenant_id,source_id,native_parent_id,generation,reason)VALUES('tenant:plan','source:plan','noise',1,'ingest')"
    )
    columns = {
        t: ",".join(
            r["attname"]
            for r in c.execute(
                "SELECT attname FROM pg_attribute WHERE attrelid=%s::regclass AND attnum>0 AND NOT attisdropped AND attgenerated='' ORDER BY attnum",
                (t,),
            ).fetchall()
        )
        for t in tables
    }
    seeds = {
        t: c.execute(
            "SELECT to_jsonb(t) AS row FROM " + t + " t WHERE tenant_id=%s LIMIT 1",
            ("tenant:plan",),
        ).fetchone()
        for t in tables
    }
    for t in tables:
        c.execute(
            "CREATE TEMP TABLE "
            + t
            + " (LIKE public."
            + t
            + " INCLUDING ALL) ON COMMIT PRESERVE ROWS"
        )
        if seeds[t]:
            c.execute(
                "INSERT INTO "
                + t
                + " ("
                + columns[t]
                + ") SELECT "
                + columns[t]
                + " FROM jsonb_populate_record(NULL::"
                + t
                + ",%s)",
                (Jsonb(seeds[t]["row"]),),
            )
    c.execute("DELETE FROM canonical_evidence_document_queue")
    for t in tables[:-1]:
        seed = seeds[t]["row"]
        c.execute(
            """INSERT INTO """
            + t
            + " ("
            + columns[t]
            + ") SELECT "
            + columns[t]
            + """ FROM generate_series(1,30000)i CROSS JOIN LATERAL jsonb_populate_record(NULL::"""
            + t
            + """,%s::jsonb || jsonb_build_object(
    'tenant_id',CASE WHEN i %% 3=0 THEN 'tenant:foreign' ELSE 'tenant:plan' END,
    'source_id',CASE WHEN i %% 3=1 THEN 'source:other' ELSE 'source:plan' END,
    'artifact_id','art_'||md5(i::text),'object_key','objects/'||substr(md5(i::text),1,2)||'/'||md5(i::text)||md5(i::text),'version_id',i::text,
    'manifest_object_key','objects/'||substr(md5(i::text),1,2)||'/'||md5(i::text)||md5(i::text),'manifest_version_id',i::text,
    'event_id','evt_'||md5(i::text),'document_id','doc_'||md5(i::text),'native_id','native:'||i,'native_parent_id','parent:'||i,
    'chunk_id','chk_'||md5(i::text),'receipt','recall://fixture/'||i,'logical_document_id','ldoc_'||md5(i::text),'evidence_id','evd_'||md5(i::text))) AS r""",
            (Jsonb(seed),),
        )
    for t in tables:
        c.execute("ANALYZE " + t)
    selected = c.execute(
        "SELECT source_id,document_id,event_id FROM canonical_documents WHERE tenant_id='tenant:plan' AND source_id='source:plan' ORDER BY document_id LIMIT 256"
    ).fetchall()
    c.execute(
        "UPDATE canonical_events SET native_parent_id='session:target' WHERE tenant_id='tenant:plan' AND source_id='source:plan' AND event_id=ANY(%s)",
        ([r["event_id"] for r in selected],),
    )
    for t in tables:
        c.execute("ANALYZE " + t)

    # TEMP LIKE retains all current indexes/checks/generated columns. Foreign-key
    # and trigger semantics remain covered by the unchanged native E2E fixtures.
    def nodes(node):
        yield node
        for child in node.get("Plans", []):
            yield from nodes(child)

    class Observe:
        def __init__(self, connection):
            self.connection = connection
            self.plans = []

        def execute(self, query, params):
            self.connection.execute("SAVEPOINT explain_native")
            plan = self.connection.execute(
                "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) " + query, params
            ).fetchone()["QUERY PLAN"][0]
            self.connection.execute("ROLLBACK TO SAVEPOINT explain_native")
            self.plans.append(plan)
            return self.connection.execute(query, params)

    for count in (1, 256):
        keys = selected[:count]
        expected = c.execute(
            """SELECT count(*) AS n,sum(octet_length(d.text_redacted)) AS doc_bytes,
     sum(octet_length(e.canonical_redacted::text)) AS event_bytes
     FROM canonical_documents d JOIN canonical_events e USING(tenant_id,source_id,event_id)
     WHERE d.tenant_id='tenant:plan' AND d.source_id='source:plan' AND d.document_id=ANY(%s)""",
            ([r["document_id"] for r in keys],),
        ).fetchone()
        c.execute("SAVEPOINT native_owner")
        observe = Observe(c)
        result = _thin_selected(observe, "tenant:plan", keys, count)
        assert result == dict(
            candidates=count,
            documents=count,
            events=count,
            document_bytes=expected["doc_bytes"],
            event_bytes=expected["event_bytes"],
        ), result
        remaining = c.execute(
            "SELECT count(*) AS n FROM canonical_documents WHERE body_location='inline'"
        ).fetchone()["n"]
        assert remaining == 30001 - count, remaining
        changed = c.execute(
            """SELECT count(*) AS n FROM canonical_events
      WHERE body_location='raw' AND tenant_id='tenant:plan' AND source_id='source:plan'
      AND event_id=ANY(%s)""",
            ([r["event_id"] for r in keys],),
        ).fetchone()["n"]
        assert changed == count, changed
        c.execute("ROLLBACK TO SAVEPOINT native_owner")
        plan = observe.plans[1]
        visits = sum(
            n.get("Actual Rows", 0) * n.get("Actual Loops", 0)
            for n in nodes(plan["Plan"])
            if n.get("Relation Name") in ("canonical_documents", "canonical_events")
            and n["Node Type"] != "ModifyTable"
        )
        blocks = sum(
            plan["Plan"].get(k, 0)
            for k in (
                "Shared Hit Blocks",
                "Shared Read Blocks",
                "Local Hit Blocks",
                "Local Read Blocks",
            )
        )
        print(
            json.dumps(
                dict(
                    selected=count,
                    actual_counts=result,
                    document_event_tuple_visits=visits,
                    mutation_buffers=blocks,
                    mutation_ms=plan["Execution Time"],
                    lock_ms=observe.plans[0]["Execution Time"],
                )
            ),
            flush=True,
        )
        assert visits <= count * 8, (
            f"native mutation revisits selected set: {visits} tuples for {count} keys"
        )
        assert blocks <= count * 512, (
            f"native mutation excess buffer work: {blocks} for {count} keys"
        )
store.close()
