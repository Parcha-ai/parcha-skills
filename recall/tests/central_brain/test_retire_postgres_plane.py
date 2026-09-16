"""H3-e': retiring the Postgres vector plane behind the turbopuffer search plane.

Every writer that touched ``canonical_passage_embeddings`` or the embedding
ledger is plane-aware: on the turbopuffer plane it neither reads nor writes
them, on the postgres plane it behaves exactly as before. Migration 067 is
applied only on request from a turbopuffer-plane process, and a postgres-plane
store refuses to start once it is recorded.
"""

from __future__ import annotations

import inspect
import io
import json
import os
import re
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "server"))

from recall_server import (  # noqa: E402
    MANDATORY_SCHEMA_VERSION,
    RETIRE_POSTGRES_PLANE_VERSION,
    SCHEMA_VERSION,
)
from recall_server import cli, embedding_ledger  # noqa: E402
from recall_server.canonical import CanonicalPlane  # noqa: E402
from recall_server.db import (  # noqa: E402
    POSTGRES_PLANE_RETIRED,
    RETIRE_POSTGRES_PLANE_REFUSED,
    BrainStore,
    SearchPlaneSchemaError,
    applied_migration_versions,
    migration_version,
)
from recall_server.passage_index import (  # noqa: E402
    CanonicalPassageProjector,
    passage_contract_coverage,
    passage_embed_plan,
)
from recall_server.projection_worker import (  # noqa: E402
    run_embedding_worker,
    run_projection_worker,
)
from recall_server.search_plane_status import namespace_ids, search_plane_reconcile, search_plane_status  # noqa: E402
from recall_server.turbopuffer_plane import TurbopufferSettings  # noqa: E402
from tests.central_brain.fake_turbopuffer import FakeTurbopuffer  # noqa: E402
from tests.central_brain.test_projection_worker import (  # noqa: E402
    _Logical,
    _Passages,
)
from tests.test_passage_stable_ids import (  # noqa: E402
    POLICY,
    FakeConnection,
    FakeStore,
    build,
    candidate_for,
    prepared_for,
    statement_kinds,
    times,
)

SERVER = Path(__file__).resolve().parents[2] / "server"
SCHEMA = SERVER / "schema"
TURBOPUFFER_ENV = {
    "RECALL_SEARCH_PLANE": "turbopuffer",
    "RECALL_TPUF_API_KEY": "synthetic",
    "RECALL_TPUF_CLIENT_FACTORY": "tests.central_brain.fake_turbopuffer:factory",
}
POSTGRES_ENV = {"RECALL_SEARCH_PLANE": "postgres"}
TENANT = "tenant:company:test"
_VERSION_RE = re.compile(r"INSERT INTO schema_migrations\(version\) VALUES \((\d+)\)")


def _store(plane: str) -> BrainStore:
    env = TURBOPUFFER_ENV if plane == "turbopuffer" else POSTGRES_ENV
    with mock.patch.dict(os.environ, env, clear=False):
        for key in ("RECALL_TPUF_KEY_FILE", "RECALL_TPUF_FAKE_STATE"):
            os.environ.pop(key, None)
        return BrainStore("postgresql://synthetic.invalid/recall")


class _RecordingConnection:
    """Executes nothing; records SQL and answers the schema_migrations reads."""

    def __init__(self, versions: set[int], *, has_table: bool = True) -> None:
        self.versions = set(versions)
        self.has_table = has_table
        self.executed: list[str] = []
        self.autocommit = False
        self.commits = 0

    def execute(self, sql: str, params=None):
        self.executed.append(sql)
        cursor = mock.MagicMock()
        folded = " ".join(sql.split())
        if "to_regclass('public.schema_migrations')" in folded:
            cursor.fetchone.return_value = {"value": "schema_migrations" if self.has_table else None}
        elif folded == "SELECT version FROM schema_migrations":
            cursor.fetchall.return_value = [{"version": v} for v in sorted(self.versions)]
        elif _VERSION_RE.search(folded):
            self.versions.add(int(_VERSION_RE.search(folded).group(1)))
            self.has_table = True
        else:
            cursor.fetchone.return_value = {"n": 0, "value": None}
        return cursor

    def commit(self) -> None:
        self.commits += 1

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _migration_files_applied(connection: _RecordingConnection) -> list[int]:
    versions = []
    for sql in connection.executed:
        match = _VERSION_RE.search(" ".join(sql.split()))
        if match:
            versions.append(int(match.group(1)))
    return versions


class MigrationFileTest(unittest.TestCase):
    def test_067_drops_the_vector_plane_idempotently_and_leaves_chunks_alone(self) -> None:
        self.assertEqual(SCHEMA_VERSION, 67)
        self.assertEqual(RETIRE_POSTGRES_PLANE_VERSION, 67)
        self.assertEqual(MANDATORY_SCHEMA_VERSION, 66)
        sql = (SCHEMA / "067_retire_postgres_vector_plane.sql").read_text()
        folded = " ".join(sql.split())
        for statement in (
            "DROP INDEX IF EXISTS canonical_passage_embeddings_hnsw_idx",
            "DROP TABLE IF EXISTS canonical_passage_embeddings",
            "DROP TABLE IF EXISTS canonical_embedding_ledger",
            "DROP INDEX IF EXISTS canonical_passages_search_idx",
            "ALTER TABLE canonical_passages DROP COLUMN IF EXISTS search_vector",
            "INSERT INTO schema_migrations(version) VALUES (67)",
        ):
            self.assertIn(statement, folded)
        # Every DROP is guarded, so a rerun is a no-op.
        for line in sql.splitlines():
            head = line.strip()
            if head.startswith("DROP "):
                self.assertIn("IF EXISTS", head, head)
        self.assertNotIn("canonical_chunks", folded.replace("``canonical_chunks.search_vector``", ""))
        self.assertNotIn("canonical_passage_contexts", folded)
        self.assertEqual(migration_version(SCHEMA / "067_retire_postgres_vector_plane.sql"), 67)
        self.assertIsNone(migration_version(SCHEMA / "060b_stable_projection_keys_concurrent.sql"))


class MigrateRunnerTest(unittest.TestCase):
    def _migrate(self, store: BrainStore, connection: _RecordingConnection, **kwargs):
        store.connect = mock.MagicMock(return_value=connection)
        return store.migrate(**kwargs)

    def test_mandatory_versions_rerun_idempotently_and_067_is_deferred_by_default(self) -> None:
        # Before retirement every file below 067 runs on every call, as it
        # always did (suites delete a version row to replay a repair).
        connection = _RecordingConnection(set(range(1, 66)))
        result = self._migrate(_store("postgres"), connection)
        self.assertEqual(_migration_files_applied(connection), list(range(1, 67)))
        self.assertEqual(result["applied"], [66])
        self.assertEqual(result["deferred"], [67])
        self.assertEqual(result["skipped"], 0)
        self.assertEqual(result["schema_version"], 66)
        self.assertEqual(result["postgres_vector_plane"], "present")
        self.assertFalse(any("DROP TABLE IF EXISTS canonical_passage_embeddings" in sql for sql in connection.executed))
        # The turbopuffer plane defers it too: retirement is an explicit act.
        connection = _RecordingConnection(set(range(1, 67)))
        result = self._migrate(_store("turbopuffer"), connection)
        self.assertEqual(result["applied"], [])
        self.assertEqual(result["deferred"], [67])

    def test_fresh_database_applies_every_mandatory_version_in_order(self) -> None:
        connection = _RecordingConnection(set(), has_table=False)
        result = self._migrate(_store("postgres"), connection)
        self.assertEqual(_migration_files_applied(connection), list(range(1, 67)))
        self.assertEqual(result["applied"], list(range(1, 67)))
        self.assertEqual(result["deferred"], [67])

    def test_retirement_is_refused_on_the_postgres_plane(self) -> None:
        connection = _RecordingConnection(set(range(1, 67)))
        with self.assertRaises(SearchPlaneSchemaError) as raised:
            self._migrate(_store("postgres"), connection, retire_postgres_plane=True)
        self.assertEqual(str(raised.exception), RETIRE_POSTGRES_PLANE_REFUSED)
        self.assertIn("RECALL_SEARCH_PLANE=turbopuffer", str(raised.exception))
        self.assertNotIn(67, _migration_files_applied(connection))
        self.assertFalse(any("DROP TABLE IF EXISTS canonical_passage_embeddings" in sql for sql in connection.executed))

    def test_retirement_applies_once_on_the_turbopuffer_plane(self) -> None:
        connection = _RecordingConnection(set(range(1, 67)))
        store = _store("turbopuffer")
        result = self._migrate(store, connection, retire_postgres_plane=True)
        self.assertEqual(_migration_files_applied(connection)[-1], 67)
        connection.executed.clear()
        self.assertEqual(result["applied"], [67])
        self.assertEqual(result["deferred"], [])
        self.assertEqual(result["postgres_vector_plane"], "retired")
        # Once retired, the files below 067 (041 recreates the table, 063
        # alters it) are skipped and 067 itself is not replayed.
        again = self._migrate(store, connection, retire_postgres_plane=True)
        self.assertEqual(again["applied"], [])
        self.assertEqual(again["skipped"], 67)
        self.assertEqual(_migration_files_applied(connection), [])
        plain = self._migrate(store, connection)
        self.assertEqual((plain["applied"], plain["skipped"], plain["deferred"]), ([], 67, []))

    def test_companions_always_run(self) -> None:
        connection = _RecordingConnection(set(range(1, 68)))
        self._migrate(_store("turbopuffer"), connection)
        self.assertTrue(any("CREATE UNIQUE INDEX CONCURRENTLY" in sql for sql in connection.executed))


class StartupCheckTest(unittest.TestCase):
    def test_postgres_plane_refuses_a_retired_database(self) -> None:
        connection = _RecordingConnection(set(range(1, 68)))
        with self.assertRaises(SearchPlaneSchemaError) as raised:
            _store("postgres").verify_search_plane(connection)
        self.assertEqual(str(raised.exception), POSTGRES_PLANE_RETIRED)
        self.assertIn("RECALL_SEARCH_PLANE=turbopuffer", str(raised.exception))

    def test_turbopuffer_plane_and_unretired_databases_start(self) -> None:
        self.assertEqual(
            _store("turbopuffer").verify_search_plane(_RecordingConnection(set(range(1, 68)))),
            {
                "search_plane": "turbopuffer",
                "schema_version": 67,
                "postgres_vector_plane": "retired",
                "mandatory_schema_version": 66,
            },
        )
        self.assertEqual(
            _store("postgres").verify_search_plane(_RecordingConnection(set(range(1, 67))))["postgres_vector_plane"],
            "present",
        )
        self.assertEqual(
            _store("postgres").verify_search_plane(_RecordingConnection(set(), has_table=False))["schema_version"],
            0,
        )
        self.assertEqual(applied_migration_versions(_RecordingConnection(set(), has_table=False)), set())

    @mock.patch("recall_server.db.ConnectionPool")
    def test_the_check_runs_when_the_pool_opens(self, pool_type) -> None:
        connection = _RecordingConnection(set(range(1, 68)))
        pool_type.return_value.connection.return_value = connection
        store = _store("postgres")
        with self.assertRaises(SearchPlaneSchemaError):
            store.connect()
        pool_type.assert_called_once()


class PassageWriterTest(unittest.TestCase):
    def _commit(self, plane: str):
        before = build(3, revision=1)
        after = build(6, revision=2)
        existing = [
            {"passage_id": p.passage_id, "ordinal": p.ordinal, "revision": 1, **times(p)}
            for p in before
        ]
        connection = FakeConnection(candidate=candidate_for(2), existing=existing)
        store = FakeStore(connection)
        store.search_plane = plane
        projector = CanonicalPassageProjector(store, mock.Mock(), policy=POLICY)
        result = projector._commit(prepared_for(after, 2))
        self.assertEqual(result["status"], "committed")
        self.assertGreater(result["inserted"], 0)
        return connection, result

    def test_commit_on_turbopuffer_never_touches_the_embeddings_table(self) -> None:
        connection, _ = self._commit("turbopuffer")
        kinds = statement_kinds(connection)
        self.assertNotIn("CAPTURE embeddings", kinds)
        self.assertNotIn("REATTACH embeddings", kinds)
        self.assertIn("COPY canonical_passages", kinds)
        for kind, sql, _params in connection.statements:
            self.assertNotIn("canonical_passage_embeddings", sql)

    def test_commit_on_postgres_still_captures_and_reattaches(self) -> None:
        connection, _ = self._commit("postgres")
        kinds = statement_kinds(connection)
        self.assertEqual(kinds[0], "CAPTURE embeddings")
        self.assertIn("REATTACH embeddings", kinds)
        # A store without the attribute (older fakes) is the postgres plane.
        connection = FakeConnection(candidate=candidate_for(2), existing=[])
        CanonicalPassageProjector(FakeStore(connection), mock.Mock(), policy=POLICY)._commit(
            prepared_for(build(2, revision=2), 2)
        )
        self.assertIn("REATTACH embeddings", statement_kinds(connection))

    def _projector(self, plane: str):
        store = mock.MagicMock()
        store.search_plane = plane
        store.semantic_runtime = mock.MagicMock(
            dimensions=512,
            passage_fingerprint="fp",
            passage_fingerprint_v2="fp-v2",
            passage_fingerprint_v1="fp-v1",
        )
        return store, CanonicalPassageProjector(store, mock.Mock(), policy=POLICY)

    def test_embed_pending_is_not_applicable_on_turbopuffer(self) -> None:
        store, projector = self._projector("turbopuffer")
        self.assertEqual(
            projector.embed_pending(tenant_id=TENANT, batch_size=8, max_batches=1),
            {"status": "not-applicable", "processed": 0, "batches": 0, "plane": "turbopuffer"},
        )
        store.connect.assert_not_called()
        store.semantic_runtime.embed_passages.assert_not_called()

    def test_embed_pending_on_postgres_still_opens_the_embedding_path(self) -> None:
        store, projector = self._projector("postgres")
        connection = mock.MagicMock()
        connection.execute.return_value.fetchone.return_value = {"value": False}
        store.connect.return_value.__enter__.return_value = connection
        with mock.patch.object(projector, "backfill_headers", return_value={"updated": 0}):
            result = projector.embed_pending(tenant_id=TENANT, batch_size=8, max_batches=1)
        self.assertEqual(result["status"], "busy")
        store.connect.assert_called()
        source = inspect.getsource(CanonicalPassageProjector.embed_pending)
        self.assertIn("INSERT INTO canonical_passage_embeddings", source)

    def test_coverage_and_plan_are_not_applicable_without_reading(self) -> None:
        connection = mock.MagicMock()
        coverage = passage_contract_coverage(connection, fingerprint="fp", search_plane="turbopuffer")
        self.assertEqual(coverage["status"], "not-applicable")
        self.assertEqual(coverage["coverage"], 1.0)
        self.assertEqual(coverage["total"], 0)
        plan = passage_embed_plan(connection, tenant_id=TENANT, runtime=None, search_plane="turbopuffer")
        self.assertEqual(plan["status"], "not-applicable")
        self.assertEqual(plan["needs_embedding"], 0)
        self.assertEqual(plan["estimated_tokens"], 0)
        self.assertEqual(plan["estimated_cost"], 0.0)
        connection.execute.assert_not_called()
        store, projector = self._projector("turbopuffer")
        self.assertEqual(projector.contract_coverage(tenant_id=TENANT)["status"], "not-applicable")
        self.assertEqual(projector._coverage_probe(), 1.0)
        store.connect.assert_not_called()
        # Postgres: the same helpers still query the embeddings table.
        connection.execute.return_value.fetchone.return_value = {"total": 4, "covered": 2}
        self.assertEqual(passage_contract_coverage(connection, fingerprint="fp")["coverage"], 0.5)
        self.assertIn("canonical_passage_embeddings", connection.execute.call_args.args[0])


class LedgerTest(unittest.TestCase):
    def test_ledger_counters_are_zero_and_silent_on_turbopuffer(self) -> None:
        connection = mock.MagicMock()
        self.assertEqual(embedding_ledger.window_total(connection, search_plane="turbopuffer"), 0)
        self.assertEqual(
            embedding_ledger.count_unembedded_passages(
                connection, passage_fingerprint="fp", search_plane="turbopuffer"
            ),
            0,
        )
        embedding_ledger.record_embedded(connection, tenant_id=TENANT, embedded=5, search_plane="turbopuffer")
        connection.execute.assert_not_called()
        connection.execute.return_value.fetchone.return_value = {"n": 7}
        self.assertEqual(embedding_ledger.window_total(connection), 7)
        embedding_ledger.record_embedded(connection, tenant_id=TENANT, embedded=5)
        self.assertIn("INSERT INTO canonical_embedding_ledger", connection.execute.call_args.args[0])


class ChurnGaugeTest(unittest.TestCase):
    def _connection(self):
        connection = mock.MagicMock()

        def execute(sql: str, params=()):
            cursor = mock.MagicMock()
            folded = " ".join(sql.split()).casefold()
            if "to_regclass" in folded:
                cursor.fetchone.return_value = {
                    "value": None if "schema_migrations" in folded else "present"
                }
            else:
                cursor.fetchone.return_value = {"n": 3, "embedded": 0, "live": 0}
            return cursor

        connection.execute.side_effect = execute
        context = mock.MagicMock()
        context.__enter__.return_value = connection
        return connection, context

    def test_turbopuffer_plane_reports_zero_without_reading_retired_tables(self) -> None:
        store = _store("turbopuffer")
        store.semantic_runtime = mock.MagicMock(passage_fingerprint="fp", fingerprint="fp-docs")
        connection, context = self._connection()
        store.connect = mock.MagicMock(return_value=context)
        metrics = store.service_metrics()
        self.assertEqual(metrics["passages_unembedded"], 0)
        self.assertEqual(metrics["embedding_daily_total"], 0)
        executed = " ".join(call.args[0] for call in connection.execute.call_args_list)
        self.assertNotIn("canonical_passage_embeddings", executed)
        self.assertNotIn("canonical_embedding_ledger", executed)
        self.assertEqual(metrics["passages_total"], 3)

    def test_postgres_plane_still_counts(self) -> None:
        store = _store("postgres")
        store.semantic_runtime = mock.MagicMock(passage_fingerprint="fp", fingerprint="fp-docs")
        connection, context = self._connection()
        store.connect = mock.MagicMock(return_value=context)
        metrics = store.service_metrics()
        self.assertEqual(metrics["passages_unembedded"], 3)
        self.assertEqual(metrics["embedding_daily_total"], 3)
        executed = " ".join(call.args[0] for call in connection.execute.call_args_list)
        self.assertIn("canonical_passage_embeddings", executed)
        self.assertIn("canonical_embedding_ledger", executed)


class WorkerTest(unittest.TestCase):
    def test_projection_worker_skips_embedding_on_turbopuffer(self) -> None:
        calls: list[str] = []
        passages = _Passages(calls, work=1)
        passages.store = mock.MagicMock(search_plane="turbopuffer")
        result = run_projection_worker(
            _Logical(calls, work=1),  # type: ignore[arg-type]
            passages,  # type: ignore[arg-type]
            tenant_id=TENANT,
            logical_batch_size=25,
            passage_batch_size=100,
            embedding_batch_size=128,
            max_batches_per_cycle=10,
            upload_concurrency=2,
            passage_concurrency=4,
            interval_seconds=3,
            once=True,
        )
        self.assertEqual(calls, ["passages", "logical"])
        self.assertEqual(result["embedded"], 0)
        self.assertEqual(result["embed_elapsed_ms"], 0)
        # The postgres plane still embeds by default.
        calls.clear()
        passages.store.search_plane = "postgres"
        run_projection_worker(
            _Logical(calls, work=1),  # type: ignore[arg-type]
            passages,  # type: ignore[arg-type]
            tenant_id=TENANT,
            logical_batch_size=25,
            passage_batch_size=100,
            embedding_batch_size=128,
            max_batches_per_cycle=10,
            upload_concurrency=2,
            passage_concurrency=4,
            interval_seconds=3,
            once=True,
        )
        self.assertEqual(calls, ["embeddings", "passages", "logical"])

    def test_embedding_worker_returns_not_applicable_without_touching_the_database(self) -> None:
        store = mock.MagicMock(search_plane="turbopuffer")
        passages = mock.MagicMock()
        with self.assertLogs("recall_server.projection_worker", level="WARNING") as logs:
            result = run_embedding_worker(
                passages, store, tenant_id=TENANT, batch_size=8,
                max_batches_per_cycle=1, interval_seconds=1, daily_cap=100, once=True,
            )
        self.assertEqual(result["status"], "not-applicable")
        self.assertEqual(result["pending"], 0)
        self.assertEqual(result["embedded"], 0)
        self.assertEqual(result["cap_remaining"], 100)
        store.connect.assert_not_called()
        passages.embed_pending.assert_not_called()
        self.assertTrue(any("suspend this service" in line for line in logs.output))


class SourceStatusTest(unittest.TestCase):
    def _status(self, plane: str):
        store = mock.MagicMock(search_plane=plane)
        connection = mock.MagicMock()
        owner = {"owner_principal_id": "principal:owner"}
        row = {"passages": 4, "passage_embeddings": 0, "missing_passage_embeddings": 0}
        connection.execute.return_value.fetchone.side_effect = [owner, row]
        store.connect.return_value.__enter__.return_value = connection
        plane_object = CanonicalPlane.__new__(CanonicalPlane)
        plane_object.store = store
        with mock.patch.object(CanonicalPlane, "_validate_host_identity", lambda *a, **k: None):
            result = plane_object.source_status(
                tenant_id=TENANT, principal_id="principal:owner", source_id="source:test",
            )
        sql = connection.execute.call_args.args[0]
        return result, sql, connection.execute.call_args.args[1]

    def test_turbopuffer_reports_zero_parity_without_the_table(self) -> None:
        result, sql, params = self._status("turbopuffer")
        self.assertNotIn("canonical_passage_embeddings", sql)
        self.assertIn("0::bigint AS passage_embeddings", sql)
        self.assertEqual(result["passage_embeddings"], 0)
        self.assertEqual(sql.count("%s"), len(params))

    def test_postgres_keeps_the_parity_subqueries(self) -> None:
        _result, sql, params = self._status("postgres")
        self.assertIn("FROM canonical_passage_embeddings", sql)
        self.assertEqual(sql.count("%s"), len(params))


class SearchPlaneStatusTest(unittest.TestCase):
    def _store(self, *, passages: int, pending: int, shards: int) -> mock.MagicMock:
        store = mock.MagicMock(search_plane="turbopuffer")
        connection = mock.MagicMock()

        def execute(sql: str, params=()):
            cursor = mock.MagicMock()
            folded = " ".join(sql.split())
            if "FROM search_projection_outbox" in folded:
                cursor.fetchone.return_value = {"count": pending}
            elif "FROM search_projection_shards" in folded:
                cursor.fetchone.return_value = {"n": shards}
            else:
                self.assertIn("canonical_passage_documents projected", folded)
                self.assertIn("NOT EXISTS", folded)
                self.assertEqual(params, (TENANT, POLICY.fingerprint))
                cursor.fetchone.return_value = {"n": passages}
            return cursor

        connection.execute.side_effect = execute
        store.connect.return_value.__enter__.return_value = connection
        return store

    def test_reports_counts_and_drift_from_the_fake_metadata(self) -> None:
        settings = TurbopufferSettings(api_key="synthetic")
        client = FakeTurbopuffer(settings)
        namespace = client.namespace(settings.namespace(TENANT))
        namespace.write(upsert_rows=[{"id": f"psg_{i:032x}"} for i in range(3)], distance_metric="cosine_distance")
        self.assertEqual(namespace.metadata().approx_row_count, 3)
        status = search_plane_status(
            self._store(passages=5, pending=1, shards=2), settings,
            tenant_id=TENANT, policy_fingerprint=POLICY.fingerprint, client=client,
        )
        self.assertEqual(status["passages"], 5)
        self.assertEqual(status["rows"], 3)
        self.assertEqual(status["drift"], 2)
        self.assertEqual(status["outbox_pending"], 1)
        self.assertEqual(status["shards"], 2)
        self.assertEqual(status["namespace"], settings.namespace(TENANT))
        self.assertNotIn("synthetic", json.dumps(status))
        # Content-free: only counts and identifiers.
        self.assertTrue(all(isinstance(v, (int, str)) for v in status.values()))

    def test_a_namespace_never_written_counts_zero_rows(self) -> None:
        settings = TurbopufferSettings(api_key="synthetic")
        status = search_plane_status(
            self._store(passages=0, pending=0, shards=0), settings,
            tenant_id=TENANT, policy_fingerprint=POLICY.fingerprint, client=FakeTurbopuffer(settings),
        )
        self.assertEqual(status["rows"], 0)
        self.assertEqual(status["drift"], 0)


class CliTest(unittest.TestCase):
    def _run(self, env: dict[str, str], *argv: str) -> tuple[dict, int | None]:
        output = io.StringIO()
        code = None
        with mock.patch.dict(os.environ, {**env, "RECALL_DATABASE_URL": "postgresql://synthetic.invalid/recall"}), \
                mock.patch.object(cli.SemanticRuntime, "from_env", staticmethod(lambda: None)), \
                mock.patch.object(cli, "build_rerank_runtime", lambda: None), \
                mock.patch.object(sys, "argv", ["recall-server", *argv]), \
                redirect_stdout(output):
            try:
                cli.main()
            except SystemExit as exit_:
                code = exit_.code
        lines = output.getvalue().strip().splitlines()
        return (json.loads(lines[-1]) if lines else {}), code

    def test_embedding_worker_exits_with_a_clear_message_on_turbopuffer(self) -> None:
        with mock.patch.object(cli, "run_embedding_worker") as worker:
            result, code = self._run(TURBOPUFFER_ENV, "embedding-worker", "--tenant", TENANT, "--once")
        self.assertEqual(code, 2)
        self.assertEqual(result["status"], "not-applicable")
        self.assertIn("suspend this service", result["error"])
        worker.assert_not_called()

    def test_migrate_flag_reaches_the_store_and_refusal_is_reported(self) -> None:
        with mock.patch.object(cli.BrainStore, "migrate", return_value={"status": "ok", "applied": [], "deferred": [67], "skipped": 66, "schema_version": 66, "postgres_vector_plane": "present"}) as migrate:
            result, code = self._run(POSTGRES_ENV, "migrate")
        self.assertIsNone(code)
        self.assertEqual(migrate.call_args.kwargs, {"retire_postgres_plane": False})
        self.assertEqual(result["deferred"], [67])
        self.assertEqual(result["current_schema_version"], SCHEMA_VERSION)
        with mock.patch.object(cli.BrainStore, "migrate", side_effect=SearchPlaneSchemaError(RETIRE_POSTGRES_PLANE_REFUSED)) as migrate:
            _result, code = self._run(POSTGRES_ENV, "migrate", "--retire-postgres-plane")
        self.assertEqual(code, 2)
        self.assertEqual(migrate.call_args.kwargs, {"retire_postgres_plane": True})

    def test_search_plane_status_is_registered(self) -> None:
        with mock.patch.object(cli, "search_plane_status", return_value={"status": "ok", "drift": 0}) as status:
            result, code = self._run(TURBOPUFFER_ENV, "search-plane-status", "--tenant", TENANT)
        self.assertIsNone(code)
        self.assertEqual(result["drift"], 0)
        self.assertEqual(status.call_args.kwargs["tenant_id"], TENANT)
        self.assertEqual(status.call_args.kwargs["policy_fingerprint"], cli.PassagePolicy(target_tokens=1024, overlap_tokens=128).fingerprint)


if __name__ == "__main__":
    unittest.main()


class SearchPlaneReconcileTest(unittest.TestCase):
    """Exact drift over ids: stale rows go on --apply, missing ids are the outbox's."""

    def _store(self, live_ids: list[str]) -> mock.MagicMock:
        store = mock.MagicMock(search_plane="turbopuffer")
        connection = mock.MagicMock()

        def execute(sql: str, params=()):
            cursor = mock.MagicMock()
            if "passage.passage_id AS passage_id" in sql:
                cursor.fetchall.return_value = [{"passage_id": value} for value in live_ids]
            else:
                cursor.fetchall.return_value = []
            return cursor

        connection.execute.side_effect = execute
        store.connect.return_value.__enter__.return_value = connection
        return store

    def _seed(self, client, settings, ids):
        ns = client.namespace(settings.namespace("tenant:test"))
        ns.write(upsert_rows=[{"id": value, "text": "t", "source_id": "codex:linux:test", "policy_fingerprint": "fp"} for value in ids])
        return ns

    def test_pages_every_id_without_attributes(self) -> None:
        settings = TurbopufferSettings(api_key="synthetic-key")
        client = FakeTurbopuffer(settings)
        ids = [f"psg_{index:032d}" for index in range(7)]
        ns = self._seed(client, settings, ids)
        self.assertEqual(namespace_ids(ns, page_rows=3), set(ids))
        pages = [q for q in ns.queries if q["rank_by"] == ("id", "asc")]
        self.assertEqual(len(pages), 3)
        self.assertEqual(pages[0]["include_attributes"], [])
        self.assertNotIn("filters", pages[0])
        self.assertEqual(pages[1]["filters"], ("id", "Gt", ids[2]))

    def test_reports_stale_and_missing_and_deletes_only_on_apply(self) -> None:
        settings = TurbopufferSettings(api_key="synthetic-key")
        client = FakeTurbopuffer(settings)
        live = [f"psg_{index:032d}" for index in range(4)]
        stale = ["psg_" + "f" * 32, "psg_" + "e" * 32]
        ns = self._seed(client, settings, live[:3] + stale)  # live[3] is missing from the plane
        report = search_plane_reconcile(
            self._store(live), settings, tenant_id="tenant:test", policy_fingerprint="fp", client=client,
        )
        self.assertEqual(
            {k: report[k] for k in ("live_passages", "namespace_rows", "stale", "missing", "applied", "deleted")},
            {"live_passages": 4, "namespace_rows": 5, "stale": 2, "missing": 1, "applied": False, "deleted": 0},
        )
        self.assertEqual(len(ns.rows), 5)
        self.assertFalse(any(isinstance(value, str) and value.startswith("psg_") for value in report.values()))
        class _Projector:
            calls: list[tuple[str, list[str]]] = []

            def upsert_passages(self, tenant_id, passage_ids):
                self.calls.append((tenant_id, list(passage_ids)))
                ns.write(upsert_rows=[{"id": value, "text": "t", "source_id": "codex:linux:test", "policy_fingerprint": "fp"} for value in passage_ids])
                return len(passage_ids)

        applied = search_plane_reconcile(
            self._store(live), settings, tenant_id="tenant:test", policy_fingerprint="fp", client=client,
            apply=True, delete_rows=1, projector=_Projector(),
        )
        self.assertEqual(
            (applied["stale"], applied["deleted"], applied["missing"], applied["written"], applied["applied"]),
            (2, 2, 1, 1, True),
        )
        self.assertEqual(_Projector.calls, [("tenant:test", [live[3]])])
        self.assertEqual(set(ns.rows), set(live))
        with self.assertRaises(ValueError):
            search_plane_reconcile(
                self._store(live + ["psg_" + "9" * 32]), settings, tenant_id="tenant:test", policy_fingerprint="fp",
                client=client, apply=True,
            )

    def test_a_namespace_never_written_reports_everything_missing(self) -> None:
        settings = TurbopufferSettings(api_key="synthetic-key")
        client = FakeTurbopuffer(settings)
        report = search_plane_reconcile(
            self._store(["psg_" + "1" * 32]), settings, tenant_id="tenant:test", policy_fingerprint="fp", client=client,
        )
        self.assertEqual((report["namespace_rows"], report["stale"], report["missing"]), (0, 0, 1))
