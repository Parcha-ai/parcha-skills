"""Retirement claims avoid catalog work from unrelated sources."""

# ruff: noqa: E402
import json, os, sys, time, unittest, uuid
from pathlib import Path
from unittest.mock import patch
import psycopg
from psycopg.rows import dict_row
from psycopg.conninfo import make_conninfo

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
from e2e_company_retirement_authority import CompanyAuthorityTests
from recall_server import retirement_runner as runner
from recall_server.chunk_retirement import ChunkRetirementError


BASELINE_SQL = (
    """SELECT progress.source_id,progress.native_parent_id,
            progress.scope_epoch,progress.cumulative_cleared_documents,
            progress.cumulative_cleared_chunks,progress.cumulative_cleared_utf8_bytes,
            to_jsonb(evidence) AS manifest """
    + runner._JOIN
    + " WHERE "
    + runner._ELIGIBLE
    + """
            AND progress.updated_at <= clock_timestamp() - (%s * interval '1 second')
            AND NOT EXISTS(SELECT 1 FROM unnest(%s::text[],%s::text[]) seen(source,parent)
                WHERE seen.source=progress.source_id AND seen.parent=progress.native_parent_id)
            ORDER BY progress.updated_at,progress.source_id,progress.native_parent_id LIMIT 1
            FOR UPDATE OF progress SKIP LOCKED"""
)
TABLES = (
    "canonical_chunk_retirement_progress",
    "canonical_evidence_documents",
    "canonical_evidence_document_queue",
)


def args(tenant, sources, attempted=(), source_parameters=1):
    return (
        tenant,
        *((sources,) * source_parameters),
        60,
        [s for s, p in attempted],
        [p for s, p in attempted],
    )


def catalog_rows_examined(plan):
    own = 0
    if plan.get("Relation Name") == "canonical_evidence_documents":
        own = (plan["Actual Rows"] + plan.get("Rows Removed by Filter", 0)) * plan[
            "Actual Loops"
        ]
    return own + sum(catalog_rows_examined(child) for child in plan.get("Plans", []))


class ClaimSourceTests(CompanyAuthorityTests):
    setUpClass = classmethod(CompanyAuthorityTests.setUpClass.__func__)
    tearDownClass = classmethod(CompanyAuthorityTests.tearDownClass.__func__)
    plan = CompanyAuthorityTests.plan
    state = CompanyAuthorityTests.state

    def setUp(self):
        CompanyAuthorityTests.setUp(self)
        self.current_sql, self.source_parameters = self.capture_native_select()
        self.schema = "claim_" + uuid.uuid4().hex
        self.dsn = make_conninfo(
            os.environ["RECALL_DATABASE_URL"], dbname=self.database
        )
        self.c = psycopg.connect(self.dsn, autocommit=True, row_factory=dict_row)
        with self.store.connect() as c:
            self.e = c.execute(
                "SELECT to_jsonb(e) AS v FROM canonical_evidence_documents e WHERE tenant_id=%s AND source_id=%s LIMIT 1",
                self.key[:2],
            ).fetchone()["v"]
            self.p = c.execute(
                "SELECT to_jsonb(p) AS v FROM canonical_chunk_retirement_progress p WHERE tenant_id=%s AND source_id=%s LIMIT 1",
                self.key[:2],
            ).fetchone()["v"]
        self.c.execute("CREATE SCHEMA " + self.schema)
        for table in TABLES:
            self.c.execute(
                f"CREATE TABLE {self.schema}.{table} (LIKE public.{table} INCLUDING ALL)"
            )
        self.c.execute("SET search_path TO " + self.schema + ",public")
        self.addCleanup(self.cleanup_fixture)

    def capture_native_select(self):
        execute = self.store._execute_bounded
        captured = []
        diagnostic = {}

        class Captured(Exception):
            pass

        def capture(c, statement, values, deadline):
            if diagnostic.get("phase") == "candidate_select":
                captured.append((statement, len(values) - 4))
                raise Captured()
            return execute(c, statement, values, deadline)

        with patch.object(self.store, "_execute_bounded", side_effect=capture):
            with self.assertRaises(Captured):
                runner._claim(
                    self.store,
                    scope=self.admin_scope,
                    limits=runner.RunnerLimits(),
                    deadline_at=time.monotonic() + 5,
                    attempted=(),
                    diagnostic=diagnostic,
                )
        self.assertEqual(len(captured), 1)
        return captured[0]

    def cleanup_fixture(self):
        self.c.execute("DROP SCHEMA " + self.schema + " CASCADE")
        self.c.close()

    def insert(self, tenant, source, count, offset=0, progress=True):
        self.c.execute(
            """INSERT INTO canonical_evidence_documents SELECT r.* FROM generate_series(%s::int,%s::int) g(i)
  CROSS JOIN LATERAL jsonb_populate_record(NULL::canonical_evidence_documents,%s::jsonb||jsonb_build_object(
  'tenant_id',%s::text,'source_id',%s::text,'native_parent_id','parent-'||lpad(i::text,8,'0'),
  'logical_document_id','ldoc_'||md5(%s::text||i::text),'evidence_id','evd_'||md5(%s::text||i::text),
  'manifest_object_key','objects/00/'||repeat('0',32)||md5(%s::text||i::text),
  'manifest_artifact_id','art_'||md5(%s::text||i::text))) r""",
            (
                offset,
                offset + count - 1,
                json.dumps(self.e),
                tenant,
                source,
                tenant + source,
                tenant + source,
                tenant + source,
                tenant + source,
            ),
        )
        if progress:
            self.c.execute(
                """INSERT INTO canonical_chunk_retirement_progress SELECT r.* FROM canonical_evidence_documents e
   CROSS JOIN LATERAL jsonb_populate_record(NULL::canonical_chunk_retirement_progress,%s::jsonb||jsonb_build_object(
   'tenant_id',e.tenant_id,'source_id',e.source_id,'native_parent_id',e.native_parent_id,'logical_document_id',e.logical_document_id,
   'manifest_artifact_id',e.manifest_artifact_id,'enabled',true,'status','pending','updated_at','2020-01-01T00:00:00Z')) r
   WHERE e.tenant_id=%s AND e.source_id=%s""",
                (json.dumps(self.p), tenant, source),
            )

    def select(self, tenant, sources, new=False, attempted=()):
        with self.c.transaction():
            return self.c.execute(
                self.current_sql if new else BASELINE_SQL,
                args(tenant, sources, attempted, self.source_parameters if new else 1),
            ).fetchone()

    def test_predicate_equivalence_multiple_sources_and_holds(self):
        for tenant, source in [
            ("tenant-a", "source-a"),
            ("tenant-a", "source-b"),
            ("tenant-b", "source-a"),
        ]:
            self.insert(tenant, source, 10)
        self.c.execute(
            "UPDATE canonical_chunk_retirement_progress SET status='complete' WHERE native_parent_id IN ('parent-00000001','parent-00000002')"
        )
        self.c.execute(
            "UPDATE canonical_chunk_retirement_progress SET manifest_artifact_id=NULL WHERE native_parent_id='parent-00000002'"
        )
        self.c.execute(
            "UPDATE canonical_chunk_retirement_progress SET enabled=false,status='disabled' WHERE native_parent_id='parent-00000003'"
        )
        self.c.execute(
            "UPDATE canonical_chunk_retirement_progress SET updated_at=clock_timestamp() WHERE native_parent_id='parent-00000004'"
        )
        self.c.execute(
            "UPDATE canonical_chunk_retirement_progress SET logical_document_id=NULL,enabled=false,status='detached' WHERE native_parent_id='parent-00000005'"
        )
        self.c.execute(
            "DELETE FROM canonical_evidence_documents WHERE native_parent_id='parent-00000006'"
        )
        self.c.execute(
            "UPDATE canonical_chunk_retirement_progress SET status='partial' WHERE native_parent_id='parent-00000007'"
        )
        self.c.execute(
            "INSERT INTO canonical_evidence_document_queue(tenant_id,source_id,native_parent_id,reason) SELECT tenant_id,source_id,native_parent_id,'ingest' FROM canonical_chunk_retirement_progress WHERE native_parent_id='parent-00000008'"
        )
        for sources in (
            [],
            ["source-a"],
            ["source-b"],
            ["source-a", "source-b"],
            ["source-a", "source-a"],
            ["source-a", None],
            [None],
            ["absent"],
        ):
            attempted = []
            for _ in range(21):
                a = self.select("tenant-a", sources, attempted=attempted)
                b = self.select("tenant-a", sources, True, attempted)
                self.assertEqual(a, b)
                if a is None:
                    break
                attempted.append((a["source_id"], a["native_parent_id"]))
            else:
                self.fail("enumeration bound")
        self.assertEqual(attempted, [])  # last source absent
        chosen = []
        for i in range(4):
            row = self.select("tenant-a", ["source-a"], True, chosen)
            chosen.append((row["source_id"], row["native_parent_id"]))
        self.assertEqual(
            [p for _, p in chosen],
            [
                "parent-00000000",
                "parent-00000002",
                "parent-00000007",
                "parent-00000009",
            ],
        )

    def test_locked_first_row_skipped_equally_no_epoch_mutation(self):
        self.insert("tenant-a", "source-a", 3)
        before = self.c.execute(
            "SELECT * FROM canonical_chunk_retirement_progress ORDER BY native_parent_id"
        ).fetchall()
        with psycopg.connect(self.dsn) as other:
            other.execute("SET search_path TO " + self.schema + ",public")
            other.execute(
                "SELECT 1 FROM canonical_chunk_retirement_progress WHERE native_parent_id='parent-00000000' FOR UPDATE"
            )
            a = self.select("tenant-a", ["source-a"])
            b = self.select("tenant-a", ["source-a"], True)
            self.assertEqual(a, b)
            self.assertEqual(a["native_parent_id"], "parent-00000001")
        self.assertEqual(
            before,
            self.c.execute(
                "SELECT * FROM canonical_chunk_retirement_progress ORDER BY native_parent_id"
            ).fetchall(),
        )

    def test_native_authority_refusal_still_precedes_candidate(self):
        execute = self.store._execute_bounded
        seen = []

        def candidate(c, sql, params, deadline):
            if sql == self.current_sql:
                seen.append(True)
            return execute(c, sql, params, deadline)

        with self.store.connect() as c:
            c.execute(
                "DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s",
                self.key[:2],
            )
        with patch.object(self.store, "_execute_bounded", side_effect=candidate):
            with self.assertRaises(ChunkRetirementError):
                runner._claim(
                    self.store,
                    scope=self.admin_scope,
                    limits=runner.RunnerLimits(),
                    deadline_at=time.monotonic() + 5,
                    attempted=(),
                )
        self.assertEqual(seen, [])

    def test_catalog_work_excludes_unrelated_source(self):
        self.insert("tenant-a", "a-other", 100000, progress=False)
        self.insert("tenant-a", "z-target", 6000)
        self.c.execute(
            "UPDATE canonical_chunk_retirement_progress SET status='complete' WHERE right(native_parent_id,1)<>'0'"
        )
        for table in TABLES:
            self.c.execute("ANALYZE " + table)
        with self.c.transaction():
            self.c.execute("SET LOCAL statement_timeout='10s'")
            plan = self.c.execute(
                "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) " + self.current_sql,
                args(
                    "tenant-a", ["z-target"], source_parameters=self.source_parameters
                ),
            ).fetchone()["QUERY PLAN"][0]
        examined = catalog_rows_examined(plan["Plan"])
        self.assertGreater(examined, 0)
        self.assertLessEqual(
            examined, 6000, "claim scanned catalog rows outside its source"
        )
        self.assertEqual(
            self.select("tenant-a", ["z-target"]),
            self.select("tenant-a", ["z-target"], True),
        )


if __name__ == "__main__":
    names = sys.argv[1:] or [
        n for n in ClaimSourceTests.__dict__ if n.startswith("test_")
    ]
    r = unittest.TextTestRunner(verbosity=2).run(
        unittest.TestSuite(ClaimSourceTests(n) for n in names)
    )
    raise SystemExit(not r.wasSuccessful())
