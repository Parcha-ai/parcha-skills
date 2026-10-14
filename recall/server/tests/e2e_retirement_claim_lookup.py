#!/usr/bin/env python3
"""Retirement claim keeps exact ownership while avoiding catalog-wide scans."""

# ruff: noqa: E402
import json
import os
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import patch

import psycopg
from psycopg.sql import Literal

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
import e2e_retirement_detachment as fixtures
from recall_server import retirement_runner as runner
from recall_server.db import SearchDeadlineExceeded
from recall_server.chunk_retirement import ChunkRetirementError


class ClaimLookupTests(unittest.TestCase):
    setUpClass = classmethod(fixtures.DetachmentTests.setUpClass.__func__)
    tearDownClass = classmethod(fixtures.DetachmentTests.tearDownClass.__func__)
    setUp = fixtures.DetachmentTests.setUp
    plan = fixtures.DetachmentTests.plan
    state = fixtures.DetachmentTests.state

    def clone_catalog(self, count):
        with self.store.connect() as c:
            template = c.execute(
                """SELECT to_jsonb(e) AS value FROM canonical_evidence_documents e
                WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s""",
                self.key,
            ).fetchone()["value"]
            c.execute(
                """INSERT INTO canonical_evidence_documents
                SELECT r.* FROM generate_series(1,%s) n CROSS JOIN LATERAL
                jsonb_populate_record(NULL::canonical_evidence_documents, %s::jsonb ||
                    jsonb_build_object('native_parent_id','parent-' || lpad(n::text,8,'0'),
                      'logical_document_id','ldoc_' || md5(n::text),
                      'evidence_id','evd_' || md5(n::text),
                      'manifest_version_id','fixture-' || n::text)) r""",
                (count, json.dumps(template)),
            )
            c.execute("ANALYZE canonical_evidence_documents")

    def enroll(self, parents):
        with self.store.connect() as c:
            c.execute(
                """INSERT INTO canonical_chunk_retirement_progress
                (tenant_id,source_id,native_parent_id,logical_document_id,enabled,status,updated_at)
                SELECT tenant_id,source_id,native_parent_id,logical_document_id,true,'pending',
                       '2000-01-01'::timestamptz
                FROM canonical_evidence_documents
                WHERE tenant_id=%s AND source_id=%s AND native_parent_id=ANY(%s)
                ON CONFLICT (tenant_id,source_id,native_parent_id) DO UPDATE
                SET enabled=true,status='pending',updated_at='2000-01-01'::timestamptz""",
                (self.tenant, self.source, parents),
            )
            c.execute("ANALYZE canonical_chunk_retirement_progress")

    def claim(self, attempted=(), deadline=None):
        return runner._claim(
            self.store,
            scope=self.scope,
            limits=runner.RunnerLimits(),
            deadline_at=time.monotonic() + 5 if deadline is None else deadline,
            attempted=attempted,
        )

    def captured_query(self):
        captured = []
        execute = self.store._execute_bounded

        def observe(connection, sql, params, deadline):
            if "to_jsonb(evidence) AS manifest" in sql:
                captured.append((sql, params))
            return execute(connection, sql, params, deadline)

        with patch.object(self.store, "_execute_bounded", side_effect=observe):
            self.claim()
        self.assertEqual(len(captured), 1)
        return captured[0]

    def test_claim_only_visits_catalog_rows_for_scoped_progress(self):
        catalog_count = int(os.environ.get("RECALL_CLAIM_CATALOG_ROWS", "20000"))
        self.assertIn(catalog_count, (20000, 200000))
        self.clone_catalog(catalog_count)
        parents = [
            "parent-" + str(n).zfill(8)
            for n in range(
                catalog_count // 5000, catalog_count + 1, catalog_count // 5000
            )
        ]
        self.enroll(parents)
        sql, params = self.captured_query()
        with self.store.connect() as c:
            c.execute(
                """UPDATE canonical_chunk_retirement_progress SET updated_at='2000-01-01'
                WHERE tenant_id=%s AND source_id=%s""",
                self.key[:2],
            )
        reports = []
        original = sql.replace(
            getattr(runner, "_CLAIM_JOIN", runner._JOIN), runner._JOIN
        )
        for variant, statement in (("original", original), ("candidate", sql)):
            for mode in ("force_custom_plan", "force_generic_plan"):
                with self.store.connect() as c, c.transaction():
                    c.execute("SET LOCAL plan_cache_mode=" + mode)
                    # PREPARE exposes the same generic/custom distinction as a reused client.
                    numbered = statement
                    for i in range(1, len(params) + 1):
                        numbered = numbered.replace("%s", "$" + str(i), 1)
                    c.execute(
                        "PREPARE claim_probe(text,text[],integer,text[],text[]) AS "
                        + numbered
                    )
                    invocation = (
                        "EXECUTE claim_probe ("
                        + ",".join(Literal(value).as_string(c) for value in params)
                        + ")"
                    )
                    plan = c.execute(
                        "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) " + invocation
                    ).fetchone()["QUERY PLAN"][0]

                    def nodes(node):
                        yield node
                        for child in node.get("Plans", []):
                            yield from nodes(child)

                    catalog = [
                        n
                        for n in nodes(plan["Plan"])
                        if n.get("Relation Name") == "canonical_evidence_documents"
                    ]
                    visited = sum(
                        n.get("Actual Loops", 0)
                        * (n.get("Actual Rows", 0) + n.get("Rows Removed by Filter", 0))
                        for n in catalog
                    )
                    reports.append(
                        {
                            "mode": mode,
                            "variant": variant,
                            "catalog_count": catalog_count,
                            "catalog_rows_visited": visited,
                            "catalog_nodes": [n["Node Type"] for n in catalog],
                            "execution_ms": plan["Execution Time"],
                        }
                    )
                    c.execute("DEALLOCATE claim_probe")
        print(json.dumps({"claim_lookup_plans": reports}))
        for report in reports:
            if report["variant"] == "original":
                continue
            self.assertLessEqual(
                report["catalog_rows_visited"], 2 * (len(parents) + 1), report
            )

    def test_full_selection_parity_across_eligibility_and_manifest_changes(self):
        self.clone_catalog(2)
        self.enroll(["session", "parent-00000001", "parent-00000002"])
        sql, params = self.captured_query()
        original = sql.replace(
            getattr(runner, "_CLAIM_JOIN", runner._JOIN), runner._JOIN
        )
        with self.store.connect() as c, c.transaction():
            c.execute(
                "UPDATE canonical_chunk_retirement_progress SET updated_at='2000-01-01' WHERE tenant_id=%s AND source_id=%s",
                self.key[:2],
            )

            def compare(expected):
                old = c.execute(original, params).fetchone()
                new = c.execute(sql, params).fetchone()
                self.assertEqual(new, old)
                self.assertEqual(
                    None if new is None else new["native_parent_id"], expected
                )
                if new is not None:
                    manifest = c.execute(
                        "SELECT to_jsonb(e) AS value FROM canonical_evidence_documents e WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s",
                        (self.tenant, self.source, expected),
                    ).fetchone()["value"]
                    self.assertEqual(new["manifest"], manifest)

            compare("parent-00000001")
            c.execute(
                "UPDATE canonical_chunk_retirement_progress SET status='complete',manifest_artifact_id=(SELECT manifest_artifact_id FROM canonical_evidence_documents WHERE tenant_id=%s AND source_id=%s AND native_parent_id='parent-00000001') WHERE tenant_id=%s AND source_id=%s AND native_parent_id='parent-00000001'",
                self.key[:2] * 2,
            )
            compare("parent-00000002")
            c.execute(
                "UPDATE canonical_chunk_retirement_progress SET manifest_artifact_id='older-manifest' WHERE tenant_id=%s AND source_id=%s AND native_parent_id='parent-00000001'",
                self.key[:2],
            )
            compare("parent-00000001")
            c.execute(
                "INSERT INTO canonical_evidence_document_queue(tenant_id,source_id,native_parent_id,reason) VALUES(%s,%s,'parent-00000001','ingest')",
                self.key[:2],
            )
            compare("parent-00000002")
            c.execute(
                "UPDATE canonical_chunk_retirement_progress SET enabled=false WHERE tenant_id=%s AND source_id=%s AND native_parent_id='parent-00000002'",
                self.key[:2],
            )
            compare("session")
            c.execute(
                "UPDATE canonical_chunk_retirement_progress SET updated_at=clock_timestamp() WHERE tenant_id=%s AND source_id=%s AND native_parent_id='session'",
                self.key[:2],
            )
            compare(None)
            c.execute(
                "UPDATE canonical_chunk_retirement_progress SET updated_at='2000-01-01',enabled=true WHERE tenant_id=%s AND source_id=%s AND native_parent_id='parent-00000002'",
                self.key[:2],
            )
            params = (*params[:3], [self.source], ["parent-00000002"])
            compare(None)
            params = (*params[:3], [], [])
            c.execute(
                "UPDATE canonical_chunk_retirement_progress SET logical_document_id=(SELECT logical_document_id FROM canonical_evidence_documents WHERE tenant_id=%s AND source_id=%s AND native_parent_id='session') WHERE tenant_id=%s AND source_id=%s AND native_parent_id='parent-00000002'",
                self.key[:2] * 2,
            )
            compare(None)

    def test_locked_oldest_is_skipped_and_claim_increments_only_chosen_epoch(self):
        self.clone_catalog(2)
        self.enroll(["parent-00000001", "parent-00000002"])
        with psycopg.connect(self.store.dsn) as lock, lock.transaction():
            lock.execute(
                "SELECT 1 FROM canonical_chunk_retirement_progress WHERE tenant_id=%s AND source_id=%s AND native_parent_id='parent-00000001' FOR UPDATE",
                self.key[:2],
            )
            result = self.claim()
        self.assertEqual(result["native_parent_id"], "parent-00000002")
        self.assertEqual(result["scope_epoch"], 2)
        with self.store.connect() as c:
            rows = c.execute(
                "SELECT native_parent_id,scope_epoch FROM canonical_chunk_retirement_progress WHERE tenant_id=%s AND source_id=%s AND native_parent_id LIKE 'parent-%%' ORDER BY native_parent_id",
                self.key[:2],
            ).fetchall()
        self.assertEqual(
            rows,
            [
                {"native_parent_id": "parent-00000001", "scope_epoch": 1},
                {"native_parent_id": "parent-00000002", "scope_epoch": 2},
            ],
        )

    def test_revoked_authority_and_expired_deadline_leave_claim_unchanged(self):
        self.enroll(["session"])
        before = self.state()
        with self.assertRaises(SearchDeadlineExceeded):
            self.claim(deadline=time.monotonic() - 1)
        self.assertEqual(before, self.state())
        with self.store.connect() as c:
            c.execute(
                "DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s",
                self.key[:2],
            )
        with self.assertRaises(ChunkRetirementError):
            self.claim()
        self.assertEqual(before, self.state())


if __name__ == "__main__":
    unittest.main()
