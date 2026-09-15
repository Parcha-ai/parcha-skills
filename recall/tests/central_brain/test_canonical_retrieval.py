from __future__ import annotations

import json
import inspect
import sys
import threading
import time
import unittest
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from unittest import mock


SERVER = Path(__file__).resolve().parents[2] / "server"
sys.path.insert(0, str(SERVER))

from recall_server.canonical_retrieval import (  # noqa: E402
    BoundCanonicalRetrieval,
    _informative_query_terms,
)
from recall_server.db import SearchDeadlineExceeded  # noqa: E402
from recall_server.deep_inspection import DeepInspectionError  # noqa: E402
from recall_server.passage_retrieval import PassageHintRetrieval  # noqa: E402


class DeadlineStore:
    search_deadline_ms = 25
    semantic_runtime = None

    def __init__(self) -> None:
        self.deadline_at: float | None = None
        self.deadlines: list[float] = []

    @contextmanager
    def connect(self):
        yield object()

    def _execute_bounded(self, _connection, _sql, _values, deadline_at):
        self.deadline_at = deadline_at
        self.deadlines.append(deadline_at)
        raise SearchDeadlineExceeded("synthetic canonical deadline")


class SemanticRuntime:
    fingerprint = "synthetic-runtime"

    @staticmethod
    def embed_query_bounded(_query):
        return [0.0, 1.0]


class RecordingSemanticRuntime:
    fingerprint = "synthetic-runtime"

    def __init__(self) -> None:
        self.calls: list[str] = []

    def embed_query_bounded(self, query):
        self.calls.append(query)
        return [0.0, 1.0]


class EmptyRows:
    @staticmethod
    def fetchall():
        return []


class RecordingStore:
    search_deadline_ms = 25
    semantic_runtime = None

    def __init__(self) -> None:
        self.sql: list[str] = []
        self.values: list[tuple] = []

    @contextmanager
    def connect(self):
        yield object()

    def _execute_bounded(self, _connection, sql, _values, _deadline_at):
        self.sql.append(" ".join(sql.split()))
        self.values.append(tuple(_values))
        return EmptyRows()


class ActorRecordingStore(RecordingStore):
    def __init__(self, *, exclusive_binding: bool = True) -> None:
        super().__init__()
        self.exclusive_binding = exclusive_binding

    @contextmanager
    def connect(self):
        yield self

    def execute(self, sql, values):
        normalized = " ".join(sql.split())
        self.sql.append(normalized)
        self.values.append(tuple(values))
        if "FROM brain_actors actor" in sql:
            return Rows([{
                "actor_id": "actor_0123456789abcdef0123456789abcdef",
            }])
        return Rows([])

    def _execute_bounded(self, _connection, sql, values, deadline_at):
        self.sql.append(" ".join(sql.split()))
        self.values.append(tuple(values))
        self.deadlines = getattr(self, "deadlines", [])
        self.deadlines.append(deadline_at)
        if "FROM canonical_passage_actors linked" in sql:
            return Rows([{"source_id": "codex:linux:test"}])
        if "FROM canonical_source_actor_bindings binding" in sql:
            return Rows(
                [{"source_id": "codex:linux:test"}]
                if self.exclusive_binding
                else []
            )
        return Rows([])


class AgentExecStore:
    semantic_runtime = None

    def __init__(self, receipt: str, *, verify_receipt: bool = True) -> None:
        self.receipt = receipt
        self.verify_receipt = verify_receipt
        self.calls: list[tuple[str, tuple]] = []

    @contextmanager
    def connect(self):
        yield self

    def execute(self, sql, values):
        normalized = " ".join(sql.split())
        self.calls.append((normalized, tuple(values)))
        if "canonical_evidence_document_parts" in normalized:
            return Rows([
                {
                    "logical_document_id": (
                        "ldoc_0123456789abcdef0123456789abcdef"
                    ),
                    "object_key": "objects/aa/" + "a" * 64,
                    "content_sha256": "b" * 64,
                },
                {
                    "logical_document_id": (
                        "ldoc_0123456789abcdef0123456789abcdef"
                    ),
                    "object_key": "objects/bb/" + "c" * 64,
                    "content_sha256": "d" * 64,
                },
            ])
        return Rows(
            [{"receipt": self.receipt}] if self.verify_receipt else []
        )


class Rows:
    def __init__(self, values):
        self.values = values

    def fetchall(self):
        return self.values


class RecordingExecInspector:
    def __init__(self, receipt: str) -> None:
        self.receipt = receipt
        self.calls: list[dict] = []

    def execute(self, **arguments):
        self.calls.append(arguments)
        record = {
            "content": {"message": "verified"},
            "event_native_id": "event",
            "occurred_at": "2026-07-23T00:00:00Z",
            "ordinal": 1,
            "receipts": [self.receipt],
        }
        return {
            "provider": "synthetic-archil",
            "stdout": (
                json.dumps(record)
                + f"\nRECALL_EVIDENCE {self.receipt}"
            ),
            "stderr": "",
            "exit_code": 0,
            "complete": True,
        }


class ScopeStore(RecordingStore):
    @contextmanager
    def connect(self):
        yield self

    def _execute_bounded(self, _connection, sql, values, deadline_at):
        self.deadlines = getattr(self, "deadlines", [])
        self.deadlines.append(deadline_at)
        self.sql.append(" ".join(sql.split()))
        self.values.append(tuple(values))
        if "FROM brain_actors actor" in sql:
            return Rows([{
                "actor_id": "actor_0123456789abcdef0123456789abcdef",
            }])
        return Rows([{
            "source_id": "codex.jsonl:test",
            "logical_document_id": "ldoc_" + "1" * 32,
            "revision": 1,
            "first_occurred_at": "2026-08-08T00:00:00Z",
            "last_occurred_at": "2026-08-08T01:00:00Z",
            "record_count": 10,
            "part_count": 1,
            "total_documents": 1,
        }])


class PeopleStore(RecordingStore):
    @contextmanager
    def connect(self):
        yield self

    def _execute_bounded(self, _connection, sql, values, deadline_at):
        self.deadlines = getattr(self, "deadlines", [])
        self.deadlines.append(deadline_at)
        self.sql.append(" ".join(sql.split()))
        self.values.append(tuple(values))
        return Rows([
            {
                "actor_id": "actor_" + "1" * 32,
                "display_name": "Alice Example",
                "source_id": "codex:linux:alice",
                "relation": "owner",
                "family": "coding_history",
            },
            {
                "actor_id": "actor_" + "1" * 32,
                "display_name": "Alice Example",
                "source_id": "claude:linux:alice",
                "relation": "owner",
                "family": "coding_history",
            },
        ])


class ParallelExecStore:
    semantic_runtime = None

    def __init__(
        self,
        *,
        omit: str | None = None,
        sizes: dict[str, int] | None = None,
    ) -> None:
        self.omit = omit
        self.sizes = sizes or {}

    @contextmanager
    def connect(self):
        yield self

    def execute(self, _sql, values):
        return Rows([
            {
                "logical_document_id": document_id,
                "size_bytes": self.sizes.get(document_id, 1),
            }
            for document_id in values[2]
            if document_id != self.omit
        ])


class ParallelExecRetrieval(BoundCanonicalRetrieval):
    def __init__(
        self,
        *,
        stdout: str = "",
        stderr: str = "",
        omit: str | None = None,
        sizes: dict[str, int] | None = None,
    ) -> None:
        super().__init__(
            ParallelExecStore(omit=omit, sizes=sizes),
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("codex.jsonl:test",),
        )
        self.active = 0
        self.maximum = 0
        self.lock = threading.Lock()
        self.stdout = stdout
        self.stderr = stderr

    def execute_agent_program(self, _program, *, logical_document_ids, **_kwargs):
        with self.lock:
            self.active += 1
            self.maximum = max(self.maximum, self.active)
        time.sleep(0.1)
        with self.lock:
            self.active -= 1
        return {
            "provider": "synthetic-archil",
            "stdout": self.stdout,
            "stderr": self.stderr,
            "exit_code": 0,
            "complete": True,
            "stopped_reason": "completed",
            "opened_receipts": [],
            "documents_available": len(logical_document_ids),
            "objects_available": len(logical_document_ids),
        }


class CanonicalRetrievalDeadlineTest(unittest.TestCase):
    def test_parquet_scan_stages_only_dataset_families_named_by_program(self) -> None:
        class Inspector:
            calls = []

            @classmethod
            def execute_scan(cls, **arguments):
                cls.calls.append(arguments)
                return {
                    "provider": "synthetic-archil",
                    "stdout": "[]",
                    "stderr": "",
                    "exit_code": 0,
                    "complete": True,
                    "stopped_reason": "completed",
                    "output_truncated": False,
                    "objects_unavailable": 0,
                    "timing": {"totalMs": 1},
                }

        class Retrieval(BoundCanonicalRetrieval):
            def _sources(self, **_arguments):
                return ["source:test"]

            def _parquet_shards(self, _sources, *, since, until):
                return ([
                    {
                        "source_id": "source:test",
                        "bucket_start": date(2026, 8, 1),
                        "dataset": dataset,
                        "shard_index": 0,
                        "object_key": f"objects/{index:02x}/" + f"{index:x}" * 64,
                        "content_sha256": f"{index + 4:x}" * 64,
                    }
                    for index, dataset in enumerate(
                        ("documents", "passages", "records", "actors"),
                        start=1,
                    )
                ], 0)

            def _verify_parquet_receipts(self, *_arguments, **_keywords):
                return None

        result = Retrieval(
            DeadlineStore(),
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("source:test",),
            deep_inspector=Inspector(),
        ).execute_parquet_scan(
            "duckdb -json -c \"select * from read_parquet("
            "'/datasets/*/*/passages-part-*.parquet')\"",
            filters={},
            timeout_seconds=60,
        )
        self.assertEqual(result["datasets_available"], 1)
        self.assertEqual(len(Inspector.calls[-1]["objects"]), 1)
        self.assertIn(
            "passages-part-",
            next(iter(Inspector.calls[-1]["dataset_aliases"].values())),
        )

    def test_parquet_scan_caps_stdout_at_sixteen_kibibytes_truthfully(self) -> None:
        class Inspector:
            @staticmethod
            def execute_scan(**_arguments):
                return {
                    "provider": "synthetic-archil",
                    "stdout": "é" * 20_000,
                    "stderr": "",
                    "exit_code": 0,
                    "complete": True,
                    "stopped_reason": "completed",
                    "output_truncated": False,
                    "timing": {"totalMs": 1},
                }

        class Retrieval(BoundCanonicalRetrieval):
            def _sources(self, **_arguments):
                return ["source:test"]

            def _parquet_shards(self, _sources, *, since, until):
                return ([{
                    "source_id": "source:test",
                    "bucket_start": date(2026, 8, 1),
                    "dataset": "passages",
                    "shard_index": 0,
                    "object_key": "objects/aa/" + "a" * 64,
                    "content_sha256": "b" * 64,
                }], 0)

            def _verify_parquet_receipts(self, *_arguments, **_keywords):
                return None

        result = Retrieval(
            DeadlineStore(),
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("source:test",),
            deep_inspector=Inspector(),
        ).execute_parquet_scan(
            "duckdb -json -c 'select 1'",
            filters={},
            timeout_seconds=60,
        )
        self.assertLessEqual(len(result["stdout"].encode()), 16 * 1024)
        self.assertFalse(result["complete"])
        self.assertTrue(result["output_truncated"])
        self.assertEqual(result["stopped_reason"], "output_limit")
        self.assertEqual(result["opened_receipts"], [])

    def test_uuid_routes_exactly_even_when_the_question_has_other_terms(self) -> None:
        session_id = "8668a658-a6cf-4358-9d7e-c29e5782c1dd"
        self.assertEqual(
            _informative_query_terms(
                f"In session {session_id}, what was verified about ATI?"
            ),
            [session_id],
        )

    def test_query_scaffolding_does_not_dilute_the_domain_concept(self) -> None:
        self.assertEqual(
            _informative_query_terms(
                "Across Codex and Claude coding sessions from July 22 "
                "through July 24, 2026, synthesize ATI harness decisions, "
                "implementation steps, verification evidence, and "
                "unresolved blockers."
            ),
            ["ati", "harness"],
        )

    def test_date_only_filters_are_normalized_to_utc_boundaries(self) -> None:
        self.assertEqual(
            BoundCanonicalRetrieval._filters(
                {"since": "2026-07-23", "until": "2026-07-25"}
            )[-2:],
            ("2026-07-23T00:00:00Z", "2026-07-25T00:00:00Z"),
        )

    def test_source_connector_narrows_only_the_authorized_source_set(self):
        store = RecordingStore()
        retrieval = BoundCanonicalRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=(
                "codex:linux:test",
                "codex:mac:test",
                "claude:linux:test",
            ),
        )

        self.assertEqual(
            retrieval._sources(
                source_id=None,
                source_family=None,
                source_alias=None,
                source_connector="codex",
            ),
            ["codex:linux:test", "codex:mac:test"],
        )

    def test_person_search_routes_to_hints_and_filters_every_arm(self):
        store = ActorRecordingStore()
        retrieval = BoundCanonicalRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=(
                "codex:linux:test",
                "claude:linux:unrelated",
            ),
        )

        result = retrieval.passage_hints(
            "What did Alice write?",
            filters={"person": "Alice", "person_relation": "author"},
        )

        self.assertEqual(result["results"], [])
        self.assertEqual(
            result["diagnostics"]["sparse_status"],
            "skipped-actor-scope",
        )
        resolver_sql = next(
            value for value in store.sql if "FROM brain_actors actor" in value
        )
        self.assertIn("actor.tenant_id=%s", resolver_sql)
        arm_sql = " ".join(
            value
            for value in store.sql
            if "canonical_passage_actors" in value
            or "canonical_evidence_document_actors" in value
        )
        self.assertEqual(arm_sql.count("actor.actor_id=ANY(%s)"), 1)
        retrieval_source = "\n".join(
            inspect.getsource(method)
            for method in (
                PassageHintRetrieval._lexical_query,
                PassageHintRetrieval._sparse_query,
                PassageHintRetrieval._dense_scope_passage_count_uncached,
                PassageHintRetrieval._dense_candidates,
            )
        )
        self.assertGreaterEqual(
            retrieval_source.count("actor.actor_id=ANY(%s)"),
            3,
        )
        self.assertIn(
            "actor_0123456789abcdef0123456789abcdef",
            repr(store.values),
        )
        linked_source_values = next(
            values
            for sql, values in zip(store.sql, store.values, strict=True)
            if "FROM canonical_passage_actors linked" in sql
        )
        self.assertEqual(
            linked_source_values[1],
            ["claude:linux:unrelated", "codex:linux:test"],
        )
        lexical_values = next(
            values
            for sql, values in zip(store.sql, store.values, strict=True)
            if "FROM canonical_passages passage" in sql
        )
        self.assertEqual(lexical_values[1], ["codex:linux:test"])
        self.assertFalse(any(
            "FROM canonical_chunks chunk" in sql
            for sql in store.sql
        ))

    def test_exclusive_person_sources_skip_redundant_passage_actor_filter(
        self,
    ) -> None:
        store = ActorRecordingStore()
        retrieval = BoundCanonicalRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=(
                "codex:linux:test",
                "claude:linux:unrelated",
            ),
        )

        result = retrieval.passage_hints(
            "What did Alice work on?",
            filters={"person": "Alice"},
        )

        self.assertEqual(
            result["diagnostics"]["sparse_status"],
            "skipped-actor-scope",
        )
        lexical_values = next(
            values
            for sql, values in zip(store.sql, store.values, strict=True)
            if "FROM canonical_passages passage" in sql
        )
        self.assertEqual(lexical_values[1], ["codex:linux:test"])
        self.assertIsNone(lexical_values[4])

    def test_shared_person_source_keeps_exact_passage_actor_filter(
        self,
    ) -> None:
        store = ActorRecordingStore(exclusive_binding=False)
        retrieval = BoundCanonicalRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("codex:linux:test",),
        )

        retrieval.passage_hints(
            "What did Alice work on?",
            filters={"person": "Alice"},
        )

        lexical_values = next(
            values
            for sql, values in zip(store.sql, store.values, strict=True)
            if "FROM canonical_passages passage" in sql
        )
        self.assertEqual(
            lexical_values[4],
            ["actor_0123456789abcdef0123456789abcdef"],
        )

    def test_scope_is_content_free_complete_and_actor_time_bounded(self) -> None:
        store = ScopeStore()
        retrieval = BoundCanonicalRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("codex.jsonl:test",),
        )

        result = retrieval.scope_documents(
            filters={
                "person": "Alice",
                "person_relation": "author",
                "since": "2026-08-08T00:00:00Z",
                "until": "2026-08-09T00:00:00Z",
            },
            limit=40,
            offset=0,
        )

        self.assertTrue(result["complete"])
        self.assertEqual(result["total_documents"], 1)
        self.assertEqual(
            set(result["documents"][0]),
            {
                "source_id",
                "logical_document_id",
                "revision",
                "first_occurred_at",
                "last_occurred_at",
                "record_count",
                "part_count",
            },
        )
        self.assertNotIn("text", repr(result))
        self.assertEqual(len(set(store.deadlines)), 1)
        scope_sql = next(
            sql for sql in store.sql
            if "FROM canonical_evidence_documents document" in sql
        )
        self.assertIn("canonical_evidence_document_actors", scope_sql)
        self.assertIn("document.last_occurred_at>=%s", scope_sql)

    def test_people_is_content_free_and_restricted_to_authorized_sources(
        self,
    ) -> None:
        store = PeopleStore()
        retrieval = BoundCanonicalRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=(
                "codex:linux:alice",
                "claude:linux:alice",
            ),
        )

        result = retrieval.list_people()

        self.assertTrue(result["complete"])
        self.assertEqual(len(result["people"]), 1)
        self.assertEqual(result["people"][0]["display_name"], "Alice Example")
        self.assertEqual(len(result["people"][0]["sources"]), 2)
        self.assertNotIn("email", json.dumps(result).casefold())
        self.assertNotIn("alias", json.dumps(result).casefold())
        self.assertEqual(
            store.values[0][1],
            ["codex:linux:alice", "claude:linux:alice"],
        )

    def test_filtered_search_preserves_query_semantics(self) -> None:
        store = ActorRecordingStore()
        retrieval = BoundCanonicalRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("codex.jsonl:test",),
        )

        response = {
            "results": [],
            "diagnostics": {"engine": "lossless-passages-v1"},
        }
        with mock.patch.object(
            PassageHintRetrieval,
            "search",
            return_value=response,
        ) as search:
            result = retrieval.search(
                "What did Alice work on?",
                filters={
                    "person": "Alice",
                    "since": "2026-08-08T00:00:00Z",
                    "until": "2026-08-09T00:00:00Z",
                },
            )

        self.assertEqual(result["diagnostics"]["engine"], "lossless-passages-v1")
        search.assert_called_once()
        self.assertEqual(search.call_args.kwargs["since"], "2026-08-08T00:00:00Z")
        self.assertEqual(search.call_args.kwargs["until"], "2026-08-09T00:00:00Z")

    def test_unfiltered_search_uses_full_document_passages(self) -> None:
        store = ActorRecordingStore()
        retrieval = BoundCanonicalRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("codex.jsonl:test",),
        )
        response = {
            "results": [],
            "diagnostics": {"engine": "lossless-passages-v1"},
        }
        with mock.patch.object(
            PassageHintRetrieval,
            "search",
            return_value=response,
        ) as search:
            result = retrieval.search("Why did we keep the compiled driver?")

        self.assertEqual(result, response)
        search.assert_called_once()

    def test_parallel_exec_fans_out_without_hidden_reduction(self) -> None:
        retrieval = ParallelExecRetrieval()
        document_ids = tuple(f"ldoc_{index:032x}" for index in range(4))
        started = time.monotonic()

        result = retrieval.execute_agent_program_parallel(
            "rg -n --fixed-strings decision /docs",
            logical_document_ids=document_ids,
            document_aliases={
                document_id: f"d{index}"
                for index, document_id in enumerate(document_ids, start=1)
            },
            timeout_seconds=10,
            max_parallel=4,
            shard_size=1,
        )
        elapsed = time.monotonic() - started

        self.assertTrue(result["complete"])
        self.assertEqual(len(result["shards"]), 4)
        self.assertEqual(retrieval.maximum, 4)
        self.assertLess(elapsed, 0.25)
        self.assertNotIn("answer", result)

    def test_parallel_exec_bounds_shards_and_aggregate_output(self) -> None:
        retrieval = ParallelExecRetrieval(
            stdout="x" * 25_000,
            stderr="y" * 3_000,
        )
        document_ids = tuple(f"ldoc_{index:032x}" for index in range(80))

        result = retrieval.execute_agent_program_parallel(
            "printf lots",
            logical_document_ids=document_ids,
            document_aliases={
                document_id: f"d{index}"
                for index, document_id in enumerate(document_ids, start=1)
            },
            timeout_seconds=10,
            max_parallel=8,
            shard_size=1,
        )

        self.assertFalse(result["complete"])
        self.assertEqual(result["stopped_reason"], "partial_failure")
        self.assertEqual(len(result["shards"]), 8)
        self.assertEqual(result["timing"]["effective_shard_size"], 10)
        self.assertTrue(all(
            shard["stopped_reason"] == "output_limit"
            and len(shard["stdout"].encode()) <= 20_000
            and len(shard["stderr"].encode()) <= 2_000
            and shard["opened_receipts"] == []
            for shard in result["shards"]
        ))

    def test_parallel_exec_fails_closed_before_any_unauthorized_shard(self) -> None:
        denied = "ldoc_" + "2" * 32
        retrieval = ParallelExecRetrieval(omit=denied)
        document_ids = ("ldoc_" + "1" * 32, denied)

        with self.assertRaisesRegex(
            DeepInspectionError,
            "deep_inspector_target_invalid",
        ):
            retrieval.execute_agent_program_parallel(
                "rg decision /docs",
                logical_document_ids=document_ids,
                document_aliases={
                    document_ids[0]: "d1",
                    document_ids[1]: "d2",
                },
                timeout_seconds=10,
                max_parallel=2,
                shard_size=1,
            )

        self.assertEqual(retrieval.maximum, 0)

    def test_parallel_exec_byte_balances_uneven_documents(self) -> None:
        document_ids = tuple(f"ldoc_{index:032x}" for index in range(4))
        retrieval = ParallelExecRetrieval(sizes={
            document_ids[0]: 100,
            document_ids[1]: 90,
            document_ids[2]: 10,
            document_ids[3]: 10,
        })

        result = retrieval.execute_agent_program_parallel(
            "rg decision /docs",
            logical_document_ids=document_ids,
            document_aliases={
                document_id: f"d{index}"
                for index, document_id in enumerate(document_ids, start=1)
            },
            timeout_seconds=10,
            max_parallel=2,
            shard_size=2,
        )

        self.assertEqual(
            sorted(shard["input_bytes"] for shard in result["shards"]),
            [100, 110],
        )
        self.assertEqual(result["timing"]["input_bytes"], 210)

    def test_lexical_deadline_degrades_to_optional_semantic_path(self) -> None:
        store = DeadlineStore()
        started = time.monotonic()
        retrieval = BoundCanonicalRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("codex.jsonl:test",),
        )

        result = retrieval._legacy_chunk_search_for_eval(
            "synthetic canonical deadline query"
        )

        self.assertEqual(result["results"], [])
        self.assertEqual(result["diagnostics"]["lexical_mode"], "deadline-exceeded")
        self.assertEqual(result["diagnostics"]["semantic_status"], "disabled")
        self.assertIsNotNone(store.deadline_at)
        assert store.deadline_at is not None
        self.assertGreaterEqual(store.deadline_at, started)
        self.assertLessEqual(store.deadline_at, started + 0.1)

    def test_semantic_and_lexical_queries_share_one_hard_deadline(self) -> None:
        store = DeadlineStore()
        store.semantic_runtime = SemanticRuntime()
        retrieval = BoundCanonicalRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("codex.jsonl:test",),
        )

        result = retrieval._legacy_chunk_search_for_eval(
            "synthetic canonical deadline query"
        )

        self.assertEqual(result["results"], [])
        self.assertEqual(result["diagnostics"]["lexical_mode"], "deadline-exceeded")
        self.assertEqual(
            result["diagnostics"]["semantic_status"],
            "deadline-exceeded",
        )
        self.assertEqual(len(store.deadlines), 2)
        self.assertEqual(len(set(store.deadlines)), 1)
        self.assertIn("elapsed_ms", result["diagnostics"])
        self.assertEqual(result["diagnostics"]["deadline_ms"], 25)
        self.assertEqual(
            {leg["leg"] for leg in result["diagnostics"]["legs"]},
            {"lexical", "semantic"},
        )

    def test_semantic_search_adds_one_domain_noun_probe(self) -> None:
        store = RecordingStore()
        runtime = RecordingSemanticRuntime()
        store.semantic_runtime = runtime
        retrieval = BoundCanonicalRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("codex.jsonl:test",),
        )
        query = (
            "ATI harness decisions implementation verification evidence"
        )

        result = retrieval._legacy_chunk_search_for_eval(query)

        self.assertEqual(runtime.calls, [query, "harness"])
        self.assertEqual(result["diagnostics"]["semantic_probes"], 2)

    def test_session_expansion_uses_the_caller_deadline(self) -> None:
        store = DeadlineStore()
        retrieval = BoundCanonicalRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("codex.jsonl:test",),
        )

        with self.assertRaises(SearchDeadlineExceeded):
            retrieval.session_context(
                "recall://canonical/test",
                _deadline_at=time.monotonic() + 0.025,
            )
        self.assertIsNotNone(store.deadline_at)

    def test_lexical_search_ranks_bounded_chunks_before_metadata_joins(self) -> None:
        store = RecordingStore()
        retrieval = BoundCanonicalRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("codex.jsonl:test",),
        )

        retrieval._legacy_chunk_search_for_eval("ATI harness default runtime")

        self.assertEqual(len(store.sql), 1)
        for sql in store.sql:
            self.assertIn("WITH candidates AS MATERIALIZED", sql)
            self.assertIn("FROM candidates candidate", sql)
            self.assertLess(
                sql.index("LIMIT %s ) SELECT"),
                sql.index("JOIN canonical_documents"),
            )

    def test_empty_strict_search_does_not_issue_a_broad_or_scan(self) -> None:
        store = RecordingStore()
        retrieval = BoundCanonicalRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("codex.jsonl:test",),
        )

        result = retrieval._legacy_chunk_search_for_eval(
            "ATI harness default runtime"
        )

        self.assertEqual(len(store.sql), 1)
        self.assertEqual(result["diagnostics"]["lexical_mode"], "strict-empty")
        self.assertNotIn(" OR ", store.values[0][0])

    def test_strict_lexical_query_treats_uuid_hyphens_as_text(self) -> None:
        store = RecordingStore()
        retrieval = BoundCanonicalRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("codex.jsonl:test",),
        )

        retrieval._legacy_chunk_search_for_eval(
            "8668a658-a6cf-4358-9d7e-c29e5782c1dd"
        )

        self.assertIn(
            "ts_rank_cd( chunk.search_vector, "
            "plainto_tsquery('simple',%s)",
            store.sql[0],
        )
        self.assertIn(
            "chunk.search_vector @@ plainto_tsquery('simple',%s)",
            store.sql[0],
        )

    def test_exact_session_deep_route_is_parent_scoped_and_term_ranked(self):
        store = RecordingStore()
        retrieval = BoundCanonicalRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("claude:test",),
        )
        session_id = "8668a658-a6cf-4358-9d7e-c29e5782c1dd"

        receipts = retrieval._exact_session_receipts(
            f"In session {session_id}, verify ATI harness default runtime",
            {
                "investigations": [{
                    "match": {
                        "source_id": "claude:test",
                        "native_parent_id": "claude-session-hash",
                    },
                }],
            },
            {
                "since": "2026-07-23T00:00:00Z",
                "until": "2026-07-25T00:00:00Z",
            },
            limit=60,
        )

        self.assertEqual(receipts, ())
        self.assertIn(
            "COALESCE( event.native_parent_id,event.native_id )=%s",
            store.sql[0],
        )
        self.assertIn("websearch_to_tsquery('simple',%s)", store.sql[0])
        self.assertIn("matched_term_count DESC", store.sql[0])
        self.assertIn("claude-session-hash", store.values[0])

    def test_agent_exec_stages_only_authorized_documents_and_verifies_receipts(
        self,
    ):
        source = "codex.jsonl:test"
        receipt = f"recall://{source}/event?rev=1#item=0"
        store = AgentExecStore(receipt)
        inspector = RecordingExecInspector(receipt)
        retrieval = BoundCanonicalRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=(source,),
            deep_inspector=inspector,
        )
        document_id = "ldoc_0123456789abcdef0123456789abcdef"

        result = retrieval.execute_agent_program(
            "rg -n verified /mnt/archil/evidence",
            logical_document_ids=(document_id,),
            record_spans={document_id: ((4, 2),)},
            routing_receipts={document_id: (receipt,)},
            timeout_seconds=7,
        )

        self.assertEqual(result["opened_receipts"], [receipt])
        self.assertEqual(result["documents_available"], 1)
        self.assertEqual(result["objects_available"], 2)
        call = inspector.calls[0]
        self.assertEqual(call["tenant_id"], "tenant:test")
        self.assertEqual(call["timeout_seconds"], 7)
        self.assertEqual(call["record_spans"], {document_id: ((4, 2),)})
        self.assertEqual(call["routing_receipts"], {document_id: (receipt,)})
        self.assertEqual(len(call["objects"]), 2)
        self.assertEqual(
            store.calls[0][1],
            (
                "tenant:test",
                [source],
                [document_id],
                "tenant:test",
                [source],
                [document_id],
            ),
        )

    def test_find_and_open_project_only_verified_alias_records(self):
        source = "codex.jsonl:test"
        receipt = f"recall://{source}/event?rev=1#item=0"
        document_id = "ldoc_0123456789abcdef0123456789abcdef"

        class AciInspector(RecordingExecInspector):
            def execute(self, **arguments):
                self.calls.append(arguments)
                record = {
                    "logical_document_id": document_id,
                    "content": "centered verified evidence",
                    "content_start": 900,
                    "content_end": 926,
                    "content_complete": False,
                    "event_native_id": "event",
                    "occurred_at": "2026-07-23T00:00:00Z",
                    "ordinal": 7,
                    "receipts": [receipt],
                }
                page = (
                    "\nRECALL_PAGE "
                    + json.dumps({
                        "complete": True,
                        "emitted_bytes": 400,
                        "next_cursor": None,
                    })
                    if "--cursor" in arguments["program"]
                    else ""
                )
                return {
                    "provider": "synthetic-archil",
                    "stdout": (
                        json.dumps(record)
                        + f"\nRECALL_EVIDENCE {receipt}"
                        + page
                    ),
                    "stderr": "",
                    "exit_code": 0,
                    "complete": True,
                    "stopped_reason": "completed",
                }

        inspector = AciInspector(receipt)
        retrieval = BoundCanonicalRetrieval(
            AgentExecStore(receipt),
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=(source,),
            deep_inspector=inspector,
        )
        common = {
            "record_spans": {document_id: ((4, 2),)},
            "routing_receipts": {document_id: (receipt,)},
            "timeout_seconds": 7,
        }
        found = retrieval.find_documents(
            logical_document_ids=(document_id,),
            document_aliases={document_id: "d1"},
            patterns=("verified evidence",),
            context_chars=800,
            limit=6,
            **common,
        )
        opened = retrieval.open_document(
            logical_document_id=document_id,
            document_alias="d1",
            cursor=None,
            record_ordinal=None,
            page_bytes=4_000,
            **common,
        )
        opened_explicit = retrieval.open_document(
            logical_document_id=document_id,
            document_alias="d1",
            cursor=None,
            record_ordinal=19,
            page_bytes=4_000,
            **common,
        )

        self.assertEqual(found["opened_receipts"], [receipt])
        self.assertEqual(found["matches"][0]["document_alias"], "d1")
        self.assertNotIn("logical_document_id", found["matches"][0])
        self.assertEqual(found["matches"][0]["content_start"], 900)
        self.assertEqual(opened["opened_receipts"], [receipt])
        self.assertEqual(opened["document_alias"], "d1")
        self.assertEqual(opened["records"][0]["content"], "centered verified evidence")
        self.assertTrue(opened["complete"])
        self.assertIsNone(opened["next_cursor"])
        self.assertEqual(opened["start_basis"], "hint")
        self.assertEqual(opened_explicit["start_basis"], "record")
        self.assertIn("--fixed", inspector.calls[0]["program"])
        self.assertIn("--broad", inspector.calls[0]["program"])
        self.assertIn("--cursor 0:0:0", inspector.calls[1]["program"])
        self.assertIn("--start-record 4", inspector.calls[1]["program"])
        self.assertIn("--start-record 19", inspector.calls[2]["program"])
        self.assertNotIn("--one-record", inspector.calls[1]["program"])
        self.assertIn("--one-record", inspector.calls[2]["program"])

    def test_agent_exec_fails_when_any_requested_document_is_absent(self):
        source = "codex.jsonl:test"
        receipt = f"recall://{source}/event?rev=1#item=0"
        retrieval = BoundCanonicalRetrieval(
            AgentExecStore(receipt),
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=(source,),
            deep_inspector=RecordingExecInspector(receipt),
        )
        with self.assertRaisesRegex(
            DeepInspectionError,
            "target_invalid",
        ):
            retrieval.execute_agent_program(
                "true",
                logical_document_ids=(
                    "ldoc_0123456789abcdef0123456789abcdef",
                    "ldoc_fedcba9876543210fedcba9876543210",
                ),
                record_spans={},
                routing_receipts={},
                timeout_seconds=7,
            )

    def test_agent_exec_guessed_and_cross_source_targets_fail_closed_200_of_200(self):
        source = "codex.jsonl:test"
        receipt = f"recall://{source}/event?rev=1#item=0"
        inspector = RecordingExecInspector(receipt)
        retrieval = BoundCanonicalRetrieval(
            AgentExecStore(receipt),
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=(source,),
            deep_inspector=inspector,
        )
        for index in range(200):
            target = f"ldoc_{index + 1:032x}"
            with self.subTest(index=index), self.assertRaises(DeepInspectionError):
                retrieval.execute_agent_program(
                    "true",
                    logical_document_ids=(target,),
                    record_spans={target: ()},
                    routing_receipts={target: ()},
                    timeout_seconds=1,
                )
        self.assertEqual(inspector.calls, [])

    def test_native_inspect_projects_verified_records_without_paths(self):
        source = "codex.jsonl:test"
        receipt = f"recall://{source}/event?rev=1#item=0"
        document_id = "ldoc_0123456789abcdef0123456789abcdef"

        class InspectingInspector(RecordingExecInspector):
            def execute(self, **arguments):
                self.calls.append(arguments)
                record = {
                    "logical_document_id": document_id,
                    "content": '{"message":"verified synthetic decision"}',
                    "event_native_id": "event",
                    "occurred_at": "2026-07-23T00:00:00Z",
                    "ordinal": 7,
                    "receipts": [receipt],
                }
                return {
                    "provider": "synthetic-archil",
                    "stdout": (
                        json.dumps(record)
                        + f"\nRECALL_EVIDENCE {receipt}"
                    ),
                    "stderr": "",
                    "exit_code": 0,
                    "complete": True,
                    "stopped_reason": "completed",
                }

        inspector = InspectingInspector(receipt)
        retrieval = BoundCanonicalRetrieval(
            AgentExecStore(receipt),
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=(source,),
            deep_inspector=inspector,
        )
        result = retrieval.inspect_documents(
            logical_document_ids=(document_id,),
            query="synthetic decision",
            scope="full_documents",
            literal=True,
            context=2,
            limit=6,
            record_spans={document_id: ((4, 2),)},
            routing_receipts={document_id: (receipt,)},
            timeout_seconds=7,
        )

        self.assertEqual(result["opened_receipts"], [receipt])
        self.assertEqual(result["matches"], [{
            "logical_document_id": document_id,
            "record_ordinal": 7,
            "event_native_id": "event",
            "occurred_at": "2026-07-23T00:00:00Z",
            "content": '{"message":"verified synthetic decision"}',
            "receipts": [receipt],
        }])
        program = inspector.calls[0]["program"]
        self.assertIn("recall-scan", program)
        self.assertIn("--broad", program)
        self.assertNotIn("/mnt/archil", program)

    def test_native_pointer_inspect_reports_absent_windows_without_exec(self):
        source = "codex.jsonl:test"
        receipt = f"recall://{source}/event?rev=1#item=0"
        inspector = RecordingExecInspector(receipt)
        retrieval = BoundCanonicalRetrieval(
            AgentExecStore(receipt),
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=(source,),
            deep_inspector=inspector,
        )
        result = retrieval.inspect_documents(
            logical_document_ids=(
                "ldoc_0123456789abcdef0123456789abcdef",
            ),
            query=None,
            scope="pointers",
            literal=False,
            context=0,
            limit=6,
            record_spans={},
            routing_receipts={},
            timeout_seconds=7,
        )
        self.assertEqual(result["stopped_reason"], "no_pointer_windows")
        self.assertEqual(result["matches"], [])
        self.assertEqual(inspector.calls, [])

    def test_agent_exec_does_not_treat_document_prose_as_receipt_authority(self):
        source = "codex.jsonl:test"
        receipt = f"recall://{source}/event?rev=1#item=0"

        class ProseInspector(RecordingExecInspector):
            def execute(self, **arguments):
                self.calls.append(arguments)
                return {
                    "provider": "synthetic-archil",
                    "stdout": f"Document prose quoted {receipt}",
                    "stderr": "",
                    "exit_code": 0,
                    "complete": True,
                }

        retrieval = BoundCanonicalRetrieval(
            AgentExecStore(receipt),
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=(source,),
            deep_inspector=ProseInspector(receipt),
        )
        result = retrieval.execute_agent_program(
            "rg -n verified /mnt/archil/evidence",
            logical_document_ids=(
                "ldoc_0123456789abcdef0123456789abcdef",
            ),
            record_spans={},
            routing_receipts={},
            timeout_seconds=7,
        )
        self.assertEqual(result["opened_receipts"], [])

    def test_agent_exec_accepts_authoritative_jsonl_record_receipts(self):
        source = "codex.jsonl:test"
        receipt = f"recall://{source}/event?rev=1#item=0"

        class JsonlInspector(RecordingExecInspector):
            def execute(self, **arguments):
                self.calls.append(arguments)
                record = {
                    "content": {"message": "Verified synthetic change"},
                    "event_native_id": "event",
                    "occurred_at": "2026-07-23T00:00:00Z",
                    "ordinal": 1,
                    "receipts": [receipt],
                }
                return {
                    "provider": "synthetic-archil",
                    "stdout": (
                        "/mnt/archil/evidence/object:7:"
                        + json.dumps(record)
                    ),
                    "stderr": "",
                    "exit_code": 0,
                    "complete": True,
                }

        retrieval = BoundCanonicalRetrieval(
            AgentExecStore(receipt),
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=(source,),
            deep_inspector=JsonlInspector(receipt),
        )
        result = retrieval.execute_agent_program(
            "rg -n verified /mnt/archil/evidence",
            logical_document_ids=(
                "ldoc_0123456789abcdef0123456789abcdef",
            ),
            record_spans={},
            routing_receipts={},
            timeout_seconds=7,
        )
        self.assertEqual(result["opened_receipts"], [receipt])

    def test_agent_exec_rejects_a_receipt_not_proven_by_admitted_documents(self):
        source = "codex.jsonl:test"
        receipt = f"recall://{source}/foreign?rev=1#item=0"
        store = AgentExecStore(receipt, verify_receipt=False)
        retrieval = BoundCanonicalRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=(source,),
            deep_inspector=RecordingExecInspector(receipt),
        )

        with self.assertRaisesRegex(
            DeepInspectionError,
            "receipt_scope_violation",
        ):
            retrieval.execute_agent_program(
                "rg -n foreign /mnt/archil/evidence",
                logical_document_ids=(
                    "ldoc_0123456789abcdef0123456789abcdef",
                ),
                record_spans={},
                routing_receipts={},
                timeout_seconds=7,
            )


if __name__ == "__main__":
    unittest.main()


class SearchArmCostTests(unittest.TestCase):
    """The arms must not probe canonical_chunks per candidate (PS-80 disk reads)."""

    class _Runtime:
        passage_fingerprint = "fp-runtime"
        dimensions = 512

        def embed_query_bounded(self, _query):
            return [0.0] * 512

    class _Store(ActorRecordingStore):
        def __init__(self, *, scope_count: int) -> None:
            super().__init__()
            self.scope_count = scope_count
            self.semantic_runtime = SearchArmCostTests._Runtime()

        def _execute_bounded(self, connection, sql, values, deadline_at):
            if "sum(projected.passage_count)" in sql:
                self.sql.append(" ".join(sql.split()))
                self.values.append(tuple(values))
                count = self.scope_count

                class _One:
                    @staticmethod
                    def fetchone():
                        return {"count": count}

                return _One()
            return super()._execute_bounded(connection, sql, values, deadline_at)

    def setUp(self) -> None:
        from recall_server import passage_retrieval

        passage_retrieval.reset_scope_count_cache()
        self.addCleanup(passage_retrieval.reset_scope_count_cache)

    def _retrieval(self, store):
        return PassageHintRetrieval(
            store,
            tenant_id="tenant:test",
            sources=["codex:linux:test"],
            policy_fingerprint="fp-policy",
        )

    def test_lexical_arm_checks_liveness_only_on_the_ranked_top_k(self) -> None:
        store = self._Store(scope_count=0)
        self._retrieval(store)._lexical_candidates(
            "deploy failed", since=None, until=None, candidate_limit=80,
            actor_ids=None, actor_relations=None, deadline_at=time.monotonic() + 5,
        )
        sql = store.sql[-1]
        pool, _, outer = sql.partition("SELECT top.source_id")
        self.assertIn("WITH matched AS MATERIALIZED", pool)
        self.assertIn(", top AS MATERIALIZED", pool)
        self.assertNotIn("live_chunk", pool)
        self.assertNotIn("canonical_passage_documents", pool)
        self.assertEqual(outer.count("LEFT JOIN canonical_chunks live_chunk"), 1)
        values = store.values[-1]
        self.assertIn(160, values)
        self.assertIn(80, values)

    def test_ann_dense_arm_never_joins_chunks_inside_the_index_scan(self) -> None:
        from recall_server import passage_retrieval

        store = self._Store(scope_count=passage_retrieval.MAX_EXACT_DENSE_SCOPE_PASSAGES + 1)
        rows, status, strategy, scope = self._retrieval(store)._dense_candidates(
            "deploy failed", since=None, until=None, candidate_limit=80,
            actor_ids=None, actor_relations=None, deadline_at=time.monotonic() + 5,
        )
        self.assertEqual((status, strategy), ("ok", "ann-oversampled"))
        sql = store.sql[-1]
        # The index scan CTE stays a pure ANN scan; text dedupe joins
        # canonical_passages only after the LIMIT, in distinct_texts.
        nearest, _, rest = sql.partition("distinct_texts AS MATERIALIZED")
        self.assertIn("WITH nearest AS MATERIALIZED", nearest)
        self.assertNotIn("canonical_passages", nearest)
        self.assertNotIn("live_chunk", nearest)
        self.assertIn("ranked_documents AS MATERIALIZED", rest)
        self.assertEqual(rest.count("LEFT JOIN canonical_chunks live_chunk"), 1)

    def test_exact_dense_arm_defers_liveness_to_ranked_documents(self) -> None:
        store = self._Store(scope_count=10)
        _, status, strategy, _ = self._retrieval(store)._dense_candidates(
            "deploy failed", since="2026-09-01T00:00:00Z", until=None, candidate_limit=80,
            actor_ids=None, actor_relations=None, deadline_at=time.monotonic() + 5,
        )
        self.assertEqual((status, strategy), ("ok", "exact-scoped"))
        sql = store.sql[-1]
        eligible, _, rest = sql.partition("ranked_documents AS MATERIALIZED")
        self.assertIn("WITH eligible AS MATERIALIZED", eligible)
        self.assertNotIn("live_chunk", eligible)
        self.assertEqual(rest.count("LEFT JOIN canonical_chunks live_chunk"), 1)

    def test_scope_count_is_cached_per_scope_for_a_short_window(self) -> None:
        from recall_server import passage_retrieval

        store = self._Store(scope_count=7)
        retrieval = self._retrieval(store)
        kwargs = dict(since=None, until=None, actor_ids=None, actor_relations=None, deadline_at=time.monotonic() + 5)
        self.assertEqual(retrieval._dense_scope_passage_count(**kwargs), 7)
        self.assertEqual(retrieval._dense_scope_passage_count(**kwargs), 7)
        count_queries = [s for s in store.sql if "sum(projected.passage_count)" in s]
        self.assertEqual(len(count_queries), 1)
        # a different window is a different key
        retrieval._dense_scope_passage_count(**{**kwargs, "since": "2026-09-01T00:00:00Z"})
        self.assertEqual(len([s for s in store.sql if "sum(projected.passage_count)" in s]), 2)
        # expiry
        key = ("tenant:test", ("codex:linux:test",), "fp-policy", None, None, None, None)
        self.assertEqual(passage_retrieval._scope_count_cache_get(key), 7)
        self.assertIsNone(passage_retrieval._scope_count_cache_get(
            key, now=time.monotonic() + passage_retrieval.SCOPE_COUNT_TTL_SECONDS + 1,
        ))
        # deadline failures are not cached
        class Failing(self._Store):
            def _execute_bounded(self, connection, sql, values, deadline_at):
                if "sum(projected.passage_count)" in sql:
                    raise SearchDeadlineExceeded()
                return super()._execute_bounded(connection, sql, values, deadline_at)

        passage_retrieval.reset_scope_count_cache()
        failing = self._retrieval(Failing(scope_count=0))
        self.assertIsNone(failing._dense_scope_passage_count(**kwargs))
        self.assertIsNone(passage_retrieval._scope_count_cache_get(key))

    def test_search_diagnostics_report_elapsed_time_per_arm(self) -> None:
        store = self._Store(scope_count=10)
        response = self._retrieval(store).search(
            "deploy failed", lexical_query="deploy failed", since=None, until=None, limit=10,
        )
        arms = response["diagnostics"]["arm_elapsed_ms"]
        self.assertEqual(set(arms), {"dense", "passage_lexical", "sparse_exact"})
        self.assertTrue(all(isinstance(v, float) and v >= 0 for v in arms.values()))

    def test_fusion_diagnostics_reported(self) -> None:
        from recall_server.fusion import DEFAULT_FUSION_ALPHAS

        store = self._Store(scope_count=10)
        response = self._retrieval(store).search(
            "deploy failed", lexical_query="deploy failed", since=None, until=None, limit=10,
        )
        fusion = response["diagnostics"]["fusion"]
        self.assertEqual(fusion["mode"], "convex")
        self.assertEqual(fusion["alphas"], DEFAULT_FUSION_ALPHAS)
        self.assertEqual(set(fusion["legs"]), {"dense", "passage-lexical", "sparse-exact"})
        for value in fusion["legs"].values():
            self.assertEqual(set(value), {"candidates", "documents", "normalized"})
            self.assertIsInstance(value["candidates"], int)
            self.assertIsInstance(value["normalized"], bool)
        store.fusion_mode = "rrf"
        store.fusion_alphas = {"dense": 0.2, "passage-lexical": 0.3, "sparse-exact": 0.5}
        response = self._retrieval(store).search(
            "deploy failed", lexical_query="deploy failed", since=None, until=None, limit=10,
        )
        self.assertEqual(response["diagnostics"]["fusion"]["mode"], "rrf")
        self.assertEqual(
            response["diagnostics"]["fusion"]["alphas"],
            {"dense": 0.15, "passage-lexical": 0.30, "sparse-exact": 0.55},
        )

    def test_sparse_arm_runs_only_for_identifier_shaped_queries(self) -> None:
        from recall_server.passage_retrieval import sparse_arm_applies

        for prose in ("why did the deploy fail", "what did the team decide about retries", "deploy fail"):
            self.assertFalse(sparse_arm_applies(prose), prose)
        for identifier in (
            "PoolTimeout in recall_server", "48711b38-ce97-47b4-8c88-0987a4adde20",
            "brain_busy 503", "error E063306", "parcha-backend scripts", "FrontalCortexTool",
        ):
            self.assertTrue(sparse_arm_applies(identifier), identifier)
        store = self._Store(scope_count=10)
        retrieval = self._retrieval(store)
        common = dict(since=None, until=None, candidate_limit=80, actor_ids=None,
                      actor_relations=None, deadline_at=time.monotonic() + 5)
        rows, status = retrieval._sparse_candidates("deploy fail", **common)
        self.assertEqual((rows, status), ([], "skipped-prose-query"))
        self.assertFalse(any("phraseto_tsquery" in s for s in store.sql))
        rows, status = retrieval._sparse_candidates("frontalcortextool replacement", original_query="FrontalCortexTool replacement", **common)
        self.assertEqual(status, "ok")
        sparse = [s for s in store.sql if "phraseto_tsquery" in s]
        # only the ranked scan carries the identifier phrases; probes are lexeme queries
        self.assertEqual(len(sparse), 1)
        self.assertIn("FROM canonical_passages passage", sparse[0])
        self.assertNotIn("FROM canonical_chunks chunk", sparse[0])
        # the casefolded and CamelCase forms are one phrase query
        tokens = [v for v in store.values[-1] if isinstance(v, str) and v.casefold() == "frontalcortextool"]
        self.assertEqual(tokens, ["frontalcortextool"] * tokens.count("frontalcortextool"))
        self.assertNotIn("FrontalCortexTool", store.values[-1])
        self.assertNotIn("replacement", store.values[-1])

    def test_sparse_arm_never_reads_canonical_chunks(self) -> None:
        """Every sparse SQL statement scans canonical_passages; chunks appear only in the bounded liveness join."""

        from recall_server.passage_retrieval import identifier_tokens

        self.assertEqual(
            identifier_tokens("brain_busy 503 in parcha-backend", "PoolTimeout brain_busy"),
            ["brain_busy", "503", "parcha-backend", "pooltimeout"],
        )
        self.assertEqual(len(identifier_tokens(" ".join(f"id_{n}" for n in range(40)))), 8)

        store = self._Store(scope_count=10)
        retrieval = self._retrieval(store)
        for order in ("rank", "recent"):
            store.sql.clear()
            store.values.clear()
            retrieval._sparse_query(
                store, ["brain_busy", "503"], lexical_query="brain_busy 503 deploy", order=order,
                since="2026-09-01T00:00:00Z", until=None,
                candidate_limit=80, actor_ids=["actor_" + "0" * 32], actor_relations=["author"],
                deadline_at=time.monotonic() + 5,
            )
            self.assertEqual(len(store.sql), 1)
            sql = store.sql[0]
            pool, _, outer = sql.partition("SELECT top.source_id")
            self.assertIn("WITH matched AS MATERIALIZED", pool)
            self.assertIn("FROM canonical_passages passage", pool)
            self.assertNotIn("canonical_chunks", pool)
            self.assertNotIn("canonical_documents", sql)
            self.assertNotIn("canonical_events", sql)
            # any identifier phrase qualifies on its own; the whole-query cover
            # density only orders the pool
            self.assertIn(
                "passage.search_vector @@ "
                "((phraseto_tsquery('simple',%s) || phraseto_tsquery('simple',%s)))",
                pool,
            )
            self.assertNotIn("plainto_tsquery('simple',%s) && ", pool)
            # scoped exactly like the passage-lexical arm
            self.assertIn("passage.policy_fingerprint=%s", pool)
            self.assertIn("FROM canonical_passage_actors actor", pool)
            self.assertIn("passage.last_occurred_at>=%s", pool)
            self.assertIn("passage.first_occurred_at<=%s", pool)
            # liveness runs once, on the bounded pool, never per match
            self.assertEqual(outer.count("LEFT JOIN canonical_chunks live_chunk"), 1)
            self.assertIn("JOIN canonical_passage_documents projected", outer)
            self.assertIn("top.passage_id,top.passage_ordinal", outer)
            values = store.values[0]
            self.assertEqual(values[:3], ("tenant:test", ["codex:linux:test"], "fp-policy"))
            self.assertIn("brain_busy", values)
            self.assertIn("503", values)
            self.assertIn(160, values)
            self.assertIn(80, values)
            if order == "rank":
                # full-query matches first, then identifier density, then recency
                self.assertIn(
                    "ORDER BY ts_rank_cd(passage.search_vector,plainto_tsquery('simple',%s),32) DESC,"
                    "ts_rank_cd(passage.search_vector,(phraseto_tsquery('simple',%s) || phraseto_tsquery('simple',%s)),32) DESC,"
                    "passage.last_occurred_at DESC,passage.passage_id",
                    pool,
                )
                self.assertEqual(values.count("brain_busy 503 deploy"), 4)  # pool/top/final order, score (match is phrase-only)
            else:
                self.assertEqual(values.count("brain_busy 503 deploy"), 0)  # recent phase: phrase match only
                self.assertNotIn("ts_rank_cd", sql)
                self.assertIn("0.0::real AS score", sql)
        with self.assertRaises(ValueError):
            retrieval._sparse_query(
                store, [], lexical_query="x", order="rank", since=None, until=None, candidate_limit=80,
                actor_ids=None, actor_relations=None, deadline_at=time.monotonic() + 5,
            )

    def test_prose_query_fusion_is_byte_identical_without_the_sparse_arm(self) -> None:
        """A query without identifiers never reaches the sparse arm; fusion sees the same two legs as before."""

        from recall_server import passage_retrieval
        from recall_server.passage_retrieval import collapse_document_candidates

        def row(document, passage, score, when):
            return {
                "source_id": "codex:linux:test", "logical_document_id": document, "revision": 1,
                "native_parent_id": document + "-parent", "first_occurred_at": when, "last_occurred_at": when,
                "manifest_object_key": "k/" + document, "manifest_content_sha256": "c" * 64,
                "passage_id": passage, "passage_ordinal": 0, "spans": [], "receipts": [f"recall://codex:linux:test/{document}?rev=1#item=0"],
                "text_redacted": f"text of {passage}", "passage_first_occurred_at": when, "passage_last_occurred_at": when, "score": score,
            }

        dense_rows = [row("ldoc_a", "psg_a1", 0.9, "2026-09-02"), row("ldoc_b", "psg_b1", 0.8, "2026-09-01")]
        lexical_rows = [row("ldoc_b", "psg_b2", 0.7, "2026-09-01"), row("ldoc_c", "psg_c1", 0.6, "2026-09-03")]

        class Legs(self._Store):
            def _execute_bounded(self, connection, sql, values, deadline_at):
                if "phraseto_tsquery" in sql:
                    raise AssertionError("sparse arm ran for a prose query")
                if "FROM canonical_passages passage" in sql:
                    self.sql.append(" ".join(sql.split()))
                    self.values.append(tuple(values))
                    return Rows(lexical_rows)
                if "WITH eligible AS MATERIALIZED" in sql or "WITH nearest AS MATERIALIZED" in sql:
                    self.sql.append(" ".join(sql.split()))
                    self.values.append(tuple(values))
                    return Rows(dense_rows)
                return super()._execute_bounded(connection, sql, values, deadline_at)

        passage_retrieval.reset_scope_count_cache()
        store = Legs(scope_count=10)
        response = self._retrieval(store).search(
            "why did the deploy fail", lexical_query="deploy fail", since=None, until=None, limit=20,
        )
        self.assertEqual(response["diagnostics"]["sparse_status"], "skipped-prose-query")
        self.assertEqual(response["diagnostics"]["sparse_candidates"], 0)
        legs = tuple(
            (name, passage_retrieval.RRF_LEG_WEIGHTS[name], rows)
            for name, rows in (("dense", dense_rows), ("passage-lexical", lexical_rows), ("sparse-exact", []))
        )
        for mode in ("rrf", "convex"):
            store.fusion_mode = mode
            store.fusion_alphas = dict(passage_retrieval.DEFAULT_FUSION_ALPHAS)
            response = self._retrieval(store).search(
                "why did the deploy fail", lexical_query="deploy fail", since=None, until=None, limit=20,
            )
            self.assertEqual(response["diagnostics"]["fusion"]["mode"], mode)
            expected = collapse_document_candidates(
                legs, limit=20, fusion=mode, alphas=dict(passage_retrieval.DEFAULT_FUSION_ALPHAS),
            )
            self.assertEqual(
                json.dumps(response["results"], sort_keys=True, default=str),
                json.dumps(expected, sort_keys=True, default=str),
                mode,
            )
            self.assertEqual(len(expected), 3)

    def test_search_diagnostics_report_candidate_depth(self) -> None:
        store = self._Store(scope_count=10)
        for limit, depth in ((1, 80), (10, 200), (20, 400), (50, 400)):
            response = self._retrieval(store).search(
                "brain_busy 503", lexical_query="brain_busy 503", since=None, until=None, limit=limit,
            )
            diagnostics = response["diagnostics"]
            self.assertEqual(diagnostics["candidate_depth"], depth)
            self.assertEqual(diagnostics["result_limit"], limit)
            self.assertEqual(diagnostics["sparse_source"], "passages")

    def test_search_diagnostics_report_which_arms_were_truncated(self) -> None:
        from recall_server import passage_retrieval

        passage_retrieval.reset_scope_count_cache()
        store = self._Store(scope_count=10)
        response = self._retrieval(store).search(
            "brain_busy 503", lexical_query="brain_busy 503", since=None, until=None, limit=10,
        )
        self.assertEqual(response["diagnostics"]["arms_truncated"], [])

        class RankedPhasesTimeOut(self._Store):
            def _execute_bounded(self, connection, sql, values, deadline_at):
                if "ts_rank_cd(passage.search_vector" in sql:
                    raise SearchDeadlineExceeded()
                return super()._execute_bounded(connection, sql, values, deadline_at)

        passage_retrieval.reset_scope_count_cache()
        response = self._retrieval(RankedPhasesTimeOut(scope_count=10)).search(
            "brain_busy 503", lexical_query="brain_busy 503", since=None, until=None, limit=10,
        )
        diagnostics = response["diagnostics"]
        self.assertEqual(diagnostics["passage_lexical_status"], "ok-recent-first")
        self.assertEqual(diagnostics["sparse_status"], "ok-recent-first")
        self.assertEqual(diagnostics["dense_status"], "ok")
        self.assertEqual(diagnostics["arms_truncated"], ["passage_lexical", "sparse_exact"])

        class EverythingTimesOut(self._Store):
            def _execute_bounded(self, connection, sql, values, deadline_at):
                if "sum(projected.passage_count)" in sql:
                    return super()._execute_bounded(connection, sql, values, deadline_at)
                raise SearchDeadlineExceeded()

        passage_retrieval.reset_scope_count_cache()
        response = self._retrieval(EverythingTimesOut(scope_count=10)).search(
            "brain_busy 503", lexical_query="brain_busy 503", since=None, until=None, limit=10,
        )
        self.assertTrue(response["diagnostics"]["deadline_exceeded"])
        self.assertEqual(
            response["diagnostics"]["arms_truncated"], ["dense", "passage_lexical", "sparse_exact"],
        )

    def test_sparse_arm_skips_itself_when_the_lexical_bitmap_is_too_wide(self) -> None:
        from recall_server import passage_retrieval

        class Probe(self._Store):
            def __init__(self, *, matches, **kwargs):
                super().__init__(**kwargs)
                self.matches = matches

            def _execute_bounded(self, connection, sql, values, deadline_at):
                if "SELECT count(*) AS n FROM ( SELECT 1 FROM canonical_passages passage" in " ".join(sql.split()):
                    self.sql.append(" ".join(sql.split()))
                    self.values.append(tuple(values))
                    return Rows([{"n": self.matches}])
                return super()._execute_bounded(connection, sql, values, deadline_at)

        common = dict(since="2026-09-01T00:00:00Z", until=None, candidate_limit=80,
                      actor_ids=["actor_" + "0" * 32], actor_relations=["author"],
                      deadline_at=time.monotonic() + 5)
        cap = passage_retrieval.SPARSE_MAX_LEXICAL_MATCHES

        # exactly at the cap: the arm runs; the probe precedes the ranked phase on one connection
        store = Probe(matches=cap, scope_count=10)
        rows, status = self._retrieval(store)._sparse_candidates("brain_busy 503", **common)
        self.assertEqual(status, "ok")
        probe_sql, probe_values = store.sql[0], store.values[0]
        # the probe is a GIN-only lexeme query per identifier (no phrase recheck)
        self.assertIn("passage.search_vector @@ (plainto_tsquery('simple',%s))", probe_sql)
        self.assertNotIn("phraseto_tsquery", probe_sql)
        self.assertNotIn("ts_rank_cd", probe_sql)
        self.assertNotIn("canonical_chunks", probe_sql)
        self.assertIn("LIMIT %s ) probe", probe_sql)
        self.assertEqual(probe_values[-1], cap + 1)
        # scoped exactly like the arm it protects
        self.assertIn("passage.policy_fingerprint=%s", probe_sql)
        self.assertIn("FROM canonical_passage_actors actor", probe_sql)
        self.assertIn("passage.last_occurred_at>=%s", probe_sql)
        self.assertTrue(any(value in ("brain_busy", "503") for value in probe_values))
        self.assertTrue(any("phraseto_tsquery" in q for q in store.sql[1:]))

        # one above the cap: skipped before any phrase recheck
        store = Probe(matches=cap + 1, scope_count=10)
        rows, status = self._retrieval(store)._sparse_candidates("brain_busy 503", **common)
        self.assertEqual((rows, status), ([], "skipped-selectivity"))
        # one probe per identifier token ran; every token was too wide, no scan
        self.assertEqual(len(store.sql), 2)
        self.assertTrue(all("probe" in q for q in store.sql))

        # the probe itself is deadline-bounded
        class SlowProbe(Probe):
            def _execute_bounded(self, connection, sql, values, deadline_at):
                if "AS n FROM" in " ".join(sql.split()):
                    raise SearchDeadlineExceeded()
                return super()._execute_bounded(connection, sql, values, deadline_at)

        rows, status = self._retrieval(SlowProbe(matches=0, scope_count=10))._sparse_candidates("brain_busy 503", **common)
        self.assertEqual((rows, status), ([], "deadline-exceeded"))

        passage_retrieval.reset_scope_count_cache()
        response = self._retrieval(Probe(matches=cap + 1, scope_count=10)).search(
            "brain_busy 503", lexical_query="brain_busy 503", since=None, until=None, limit=10,
        )
        self.assertEqual(response["diagnostics"]["sparse_status"], "skipped-selectivity")
        self.assertEqual(response["diagnostics"]["arms_truncated"], [])

    def test_text_arms_hold_one_pooled_connection_even_when_the_ranked_phase_times_out(self) -> None:
        from recall_server import passage_retrieval

        class Counting(self._Store):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.connects = 0
                self.open = 0
                self.max_open = 0

            @contextmanager
            def connect(self):
                self.connects += 1
                self.open += 1
                self.max_open = max(self.max_open, self.open)
                try:
                    yield self
                finally:
                    self.open -= 1

            def _execute_bounded(self, connection, sql, values, deadline_at):
                if "ts_rank_cd(passage.search_vector" in sql:
                    raise SearchDeadlineExceeded()
                return super()._execute_bounded(connection, sql, values, deadline_at)

        passage_retrieval.reset_scope_count_cache()
        store = Counting(scope_count=10)
        retrieval = self._retrieval(store)
        common = dict(since=None, until=None, candidate_limit=80, actor_ids=None,
                      actor_relations=None, deadline_at=time.monotonic() + 5)
        rows, status = retrieval._lexical_candidates("brain_busy 503", **common)
        self.assertEqual((rows, status), ([], "ok-recent-first"))
        self.assertEqual(store.connects, 1)
        rows, status = retrieval._sparse_candidates("brain_busy 503", **common)
        self.assertEqual((rows, status), ([], "ok-recent-first"))
        self.assertEqual(store.connects, 2)
        self.assertEqual(store.open, 0)

        store.connects = 0
        response = retrieval.search(
            "brain_busy 503", lexical_query="brain_busy 503", since=None, until=None, limit=10,
        )
        self.assertEqual(response["diagnostics"]["arms_truncated"], ["passage_lexical", "sparse_exact"])
        # scope count + three arms, never more than three connections at once
        self.assertLessEqual(store.connects, 4)
        self.assertLessEqual(store.max_open, 3)
        self.assertEqual(store.open, 0)

    def test_pool_timeout_inside_an_arm_is_a_status_not_an_error(self) -> None:
        from psycopg_pool import PoolTimeout

        from recall_server import passage_retrieval

        class Exhausted(self._Store):
            @contextmanager
            def connect(self):
                raise PoolTimeout("pool exhausted")
                yield self  # pragma: no cover

        passage_retrieval.reset_scope_count_cache()
        response = self._retrieval(Exhausted(scope_count=10)).search(
            "brain_busy 503", lexical_query="brain_busy 503", since=None, until=None, limit=10,
        )
        diagnostics = response["diagnostics"]
        self.assertEqual(response["results"], [])
        self.assertEqual(diagnostics["passage_lexical_status"], "pool-exhausted")
        self.assertEqual(diagnostics["sparse_status"], "pool-exhausted")
        self.assertEqual(diagnostics["dense_status"], "pool-exhausted")
        self.assertEqual(diagnostics["arms_truncated"], ["dense", "passage_lexical", "sparse_exact"])
        self.assertFalse(diagnostics["deadline_exceeded"])

    def test_search_admission_bounds_concurrent_searches_and_leaves_pool_headroom(self) -> None:
        from recall_server import passage_retrieval

        class RecordingSemaphore:
            def __init__(self, slots):
                self.inner = threading.BoundedSemaphore(slots)
                self.lock = threading.Lock()
                self.held = 0
                self.max_held = 0
                self.waits = []

            def acquire(self, timeout=None):
                self.waits.append(timeout)
                if not self.inner.acquire(timeout=timeout):
                    return False
                with self.lock:
                    self.held += 1
                    self.max_held = max(self.max_held, self.held)
                return True

            def release(self):
                with self.lock:
                    self.held -= 1
                self.inner.release()

        class Slow(self._Store):
            search_admission = RecordingSemaphore(1)

            def _execute_bounded(self, connection, sql, values, deadline_at):
                if "sum(projected.passage_count)" not in sql:
                    time.sleep(0.05)
                return super()._execute_bounded(connection, sql, values, deadline_at)

        passage_retrieval.reset_scope_count_cache()
        store = Slow(scope_count=10)
        retrieval = self._retrieval(store)
        outcomes = []

        def one():
            outcomes.append(retrieval.search(
                "brain_busy 503", lexical_query="brain_busy 503", since=None, until=None, limit=10,
                deadline_at=time.monotonic() + 5,
            ))

        threads = [threading.Thread(target=one) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(outcomes), 4)
        self.assertTrue(all("reason" not in o["diagnostics"] for o in outcomes))
        self.assertEqual(store.search_admission.max_held, 1)
        self.assertEqual(store.search_admission.held, 0)
        self.assertTrue(all(0 < wait <= 20 for wait in store.search_admission.waits))

        # No slot inside the budget: an explicit reason, no exception, no pool use.
        store.search_admission.inner.acquire()
        try:
            store.sql.clear()
            response = retrieval.search(
                "brain_busy 503", lexical_query="brain_busy 503", since=None, until=None,
                limit=10, deadline_at=time.monotonic() + 0.05,
            )
        finally:
            store.search_admission.inner.release()
        self.assertEqual(response["results"], [])
        self.assertEqual(response["diagnostics"]["reason"], "search-admission-timeout")
        self.assertEqual(response["diagnostics"]["arms_truncated"], ["dense", "passage_lexical", "sparse_exact"])
        self.assertEqual(store.sql, [])

    def test_search_admission_is_released_on_every_exit_path(self) -> None:
        from recall_server import passage_retrieval

        class Guarded(self._Store):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.search_admission = threading.BoundedSemaphore(1)
                self.fail_arms = False

            def _execute_bounded(self, connection, sql, values, deadline_at):
                if self.fail_arms and "sum(projected.passage_count)" not in sql:
                    raise RuntimeError("synthetic arm failure")
                if deadline_at < time.monotonic():
                    raise SearchDeadlineExceeded()
                return super()._execute_bounded(connection, sql, values, deadline_at)

        def slot_is_free(store):
            acquired = store.search_admission.acquire(timeout=0)
            if acquired:
                store.search_admission.release()
            return acquired

        passage_retrieval.reset_scope_count_cache()
        store = Guarded(scope_count=10)
        retrieval = self._retrieval(store)
        search = dict(lexical_query="brain_busy 503", since=None, until=None, limit=10)

        # normal completion
        retrieval.search("brain_busy 503", **search)
        self.assertTrue(slot_is_free(store))

        # an unexpected exception inside an arm propagates, the slot is still released
        store.fail_arms = True
        with self.assertRaises(RuntimeError):
            retrieval.search("brain_busy 503", **search)
        self.assertTrue(slot_is_free(store))
        store.fail_arms = False

        # a deadline that already passed: every arm reports deadline-exceeded, slot released
        response = retrieval.search("brain_busy 503", deadline_at=time.monotonic() - 1, **search)
        self.assertEqual(response["diagnostics"]["arms_truncated"], ["dense", "passage_lexical", "sparse_exact"])
        self.assertTrue(slot_is_free(store))

        # admission timeout never acquired, so it must not release (BoundedSemaphore would raise)
        store.search_admission.acquire()
        try:
            response = retrieval.search("brain_busy 503", deadline_at=time.monotonic() + 0.01, **search)
            self.assertEqual(response["diagnostics"]["reason"], "search-admission-timeout")
        finally:
            store.search_admission.release()
        self.assertTrue(slot_is_free(store))

        # the same holds for a prose query (sparse arm skipped) and for include_arms
        retrieval.search("why did the deploy fail", lexical_query="deploy fail", since=None, until=None, limit=10, include_arms=True)
        self.assertTrue(slot_is_free(store))

    def test_store_search_slots_leave_reserved_connections(self) -> None:
        from recall_server.db import BrainStore

        for pool, slots in ((4, 1), (8, 2), (16, 4), (32, 10)):
            store = BrainStore("postgresql://synthetic/unused", pool_max_size=pool)
            self.assertEqual(store.search_slots, slots, pool)
            self.assertEqual(store.search_admission._value, slots)

    def test_execute_bounded_rolls_back_a_cancelled_statement(self) -> None:
        import psycopg

        from recall_server.db import BrainStore

        class Connection:
            def __init__(self):
                self.calls = []

            def execute(self, sql, values=None):
                self.calls.append(sql)
                if "set_config" not in sql:
                    raise psycopg.errors.QueryCanceled("canceling statement due to statement timeout")

            def rollback(self):
                self.calls.append("ROLLBACK")

        connection = Connection()
        with self.assertRaises(SearchDeadlineExceeded):
            BrainStore._execute_bounded(connection, "SELECT 1", (), time.monotonic() + 1)
        self.assertEqual(connection.calls[-1], "ROLLBACK")

        class InsideTransactionBlock(Connection):
            def rollback(self):
                self.calls.append("ROLLBACK-REFUSED")
                raise psycopg.ProgrammingError(
                    "Explicit rollback() forbidden within a Transaction context."
                )

        connection = InsideTransactionBlock()
        with self.assertRaises(SearchDeadlineExceeded):
            BrainStore._execute_bounded(connection, "SELECT 1", (), time.monotonic() + 1)
        self.assertEqual(connection.calls[-1], "ROLLBACK-REFUSED")

    def test_search_limit_fifty_keeps_candidate_limit_bounded(self) -> None:
        """Depth 50 issues the same bounded scans as depth 20: candidate_limit is already capped at 400."""

        from recall_server import passage_retrieval

        def issued(limit):
            passage_retrieval.reset_scope_count_cache()
            store = self._Store(scope_count=10)
            self._retrieval(store).search(
                "brain_busy 503", lexical_query="brain_busy 503", since=None, until=None, limit=limit,
            )
            return sorted(
                (sql, tuple(v for v in values if isinstance(v, int) and not isinstance(v, bool)))
                for sql, values in zip(store.sql, store.values, strict=True)
            )

        twenty, fifty = issued(20), issued(50)
        # The lexical arm reads pg_stats once per store (cached); drop it.
        twenty = [item for item in twenty if "pg_stats" not in item[0]]
        fifty = [item for item in fifty if "pg_stats" not in item[0]]
        self.assertEqual(twenty, fifty)
        self.assertTrue(any(400 in ints and 800 in ints for _sql, ints in fifty))

    def test_sparse_arm_gets_half_the_remaining_budget(self) -> None:
        store = self._Store(scope_count=10)
        deadline = time.monotonic() + 10.0
        self._retrieval(store)._sparse_candidates(
            "brain_busy 503", since="2026-09-01T00:00:00Z", until=None, candidate_limit=80,
            actor_ids=None, actor_relations=None, deadline_at=deadline,
        )
        # ranked phase: 35% of the arm's half budget (10 s * 0.5 * 0.35 = 1.75 s)
        # ranked phase: 70% of the arm's half budget (10 s * 0.5 * 0.7 = 3.5 s)
        self.assertLessEqual(store.deadlines[-1], deadline - 6.3)
        self.assertGreaterEqual(store.deadlines[-1], deadline - 6.7)

    def test_text_arms_rank_everything_first_then_fall_back_to_a_recent_window(self) -> None:
        from recall_server import passage_retrieval

        class SlowRanking(self._Store):
            def _execute_bounded(self, connection, sql, values, deadline_at):
                if "FROM canonical_passages passage" in sql:
                    self.sql.append(" ".join(sql.split()))
                    self.values.append(tuple(values))
                    self.deadlines = getattr(self, "deadlines", [])
                    self.deadlines.append(deadline_at)
                    # the ranked pool times out; the recency pool succeeds
                    if "ts_rank_cd(passage.search_vector" in sql:
                        raise SearchDeadlineExceeded()
                    return Rows([])
                return super()._execute_bounded(connection, sql, values, deadline_at)

        store = SlowRanking(scope_count=10)
        retrieval = self._retrieval(store)
        deadline = time.monotonic() + 10.0
        rows, status = retrieval._lexical_candidates(
            "deploy fail", since=None, until=None, candidate_limit=80,
            actor_ids=None, actor_relations=None, deadline_at=deadline,
        )
        self.assertEqual((rows, status), ([], "ok-recent-first"))
        lexical = [(q, v, d) for q, v, d in zip(store.sql, store.values, store.deadlines, strict=True) if "canonical_passages passage" in q]
        self.assertEqual(len(lexical), 2)
        first_deadline, second_deadline = lexical[0][2], lexical[1][2]
        self.assertLess(first_deadline, deadline - 2.5)   # ~70% of the budget
        self.assertGreater(second_deadline, deadline - 0.5)  # the full remaining budget
        self.assertIn("ts_rank_cd(passage.search_vector", lexical[0][0])
        self.assertNotIn("ts_rank_cd(passage.search_vector", lexical[1][0])
        self.assertIn("ORDER BY passage.last_occurred_at DESC", lexical[1][0])
        self.assertIn("0.0::real AS score", lexical[1][0])

        rows, status = retrieval._sparse_candidates(
            "brain_busy 503", since=None, until=None, candidate_limit=80,
            actor_ids=None, actor_relations=None, deadline_at=deadline,
        )
        self.assertEqual((rows, status), ([], "ok-recent-first"))
        self.assertTrue(passage_retrieval.DENSE_NEAREST_LIMIT <= 400)

    def test_search_passes_the_original_query_to_the_sparse_arm(self) -> None:
        store = self._Store(scope_count=10)
        response = self._retrieval(store).search(
            "FrontalCortexTool replacement", lexical_query="frontalcortextool replacement",
            since=None, until=None, limit=10,
        )
        self.assertEqual(response["diagnostics"]["sparse_status"], "ok")
        response = self._retrieval(store).search(
            "why did the deploy fail", lexical_query="deploy fail", since=None, until=None, limit=10,
        )
        self.assertEqual(response["diagnostics"]["sparse_status"], "skipped-prose-query")


class TimeClipWindowTests(unittest.TestCase):
    """The time clip only queries receipts of ranges that straddle the window."""

    class _ClipStore(ActorRecordingStore):
        search_deadline_ms = 20000

        def _execute_bounded(self, connection, sql, values, deadline_at):
            self.sql.append(" ".join(sql.split()))
            self.values.append(tuple(values))
            if "FROM canonical_chunks chunk" in sql:
                receipts = values[2]
                return Rows([
                    {"receipt": r, "text_redacted": "inside", "occurred_at": "2026-09-05T00:00:00+00:00"}
                    for r in receipts if r.endswith("#item=0")
                ])
            return Rows([])

    def _response(self):
        return {
            "results": [
                {
                    "logical_document_id": "ldoc_" + "a" * 32,
                    "source_id": "codex:linux:test",
                    "matching_ranges": [
                        {"kind": "dense", "receipts": ["recall://codex:linux:test/x-1?rev=1#item=0"],
                         "text": "t", "text_clipped": False, "spans": [],
                         "passage_window": ["2026-09-03 10:00:00+00", "2026-09-04 10:00:00+00"]},
                        {"kind": "passage-lexical", "receipts": ["recall://codex:linux:test/y-1?rev=1#item=0", "recall://codex:linux:test/y-1?rev=1#item=1"],
                         "text": "t", "text_clipped": False, "spans": [],
                         "passage_window": ["2026-08-30 10:00:00+00", "2026-09-04 10:00:00+00"]},
                    ],
                }
            ],
            "diagnostics": {},
        }

    def _bound(self, store):
        return BoundCanonicalRetrieval(
            store, tenant_id="tenant:test", principal_id="principal:test",
            authorized_sources=("codex:linux:test",),
        )

    def test_ranges_inside_the_window_skip_the_database(self) -> None:
        from recall_server.canonical_retrieval import _window_inside

        self.assertTrue(_window_inside(["2026-09-03T00:00:00Z", "2026-09-04T00:00:00Z"], "2026-09-01T00:00:00Z", None))
        self.assertFalse(_window_inside(["2026-08-30T00:00:00Z", "2026-09-04T00:00:00Z"], "2026-09-01T00:00:00Z", None))
        self.assertFalse(_window_inside(["2026-09-03T00:00:00Z", "2026-09-04T00:00:00Z"], None, "2026-09-03T12:00:00Z"))
        self.assertFalse(_window_inside(None, "2026-09-01T00:00:00Z", None))
        store = self._ClipStore()
        response = self._response()
        response["results"][0]["matching_ranges"].pop(1)
        clipped = self._bound(store)._clip_passage_hints_to_time_window(
            response, sources=["codex:linux:test"], since="2026-09-01T00:00:00Z", until=None,
        )
        self.assertFalse(any("FROM canonical_chunks chunk" in s for s in store.sql))
        self.assertEqual(clipped["diagnostics"]["time_clip_status"], "ok")
        self.assertEqual(clipped["diagnostics"]["time_clip_ranges_inside_window"], 1)
        self.assertEqual(len(clipped["results"][0]["matching_ranges"]), 1)
        self.assertNotIn("passage_window", clipped["results"][0]["matching_ranges"][0])

    def test_only_straddling_ranges_are_looked_up(self) -> None:
        store = self._ClipStore()
        clipped = self._bound(store)._clip_passage_hints_to_time_window(
            self._response(), sources=["codex:linux:test"], since="2026-09-01T00:00:00Z", until=None,
        )
        lookups = [v for s, v in zip(store.sql, store.values, strict=True) if "FROM canonical_chunks chunk" in s]
        self.assertEqual(len(lookups), 1)
        self.assertEqual(lookups[0][2], ["recall://codex:linux:test/y-1?rev=1#item=0", "recall://codex:linux:test/y-1?rev=1#item=1"])
        ranges = clipped["results"][0]["matching_ranges"]
        self.assertEqual(len(ranges), 2)
        self.assertEqual(ranges[0]["kind"], "dense")
        self.assertNotIn("time_clipped", ranges[0])
        self.assertTrue(ranges[1]["time_clipped"])
        self.assertEqual(ranges[1]["receipts"], ["recall://codex:linux:test/y-1?rev=1#item=0"])
        self.assertTrue(all("passage_window" not in r for r in ranges))
        self.assertEqual(clipped["diagnostics"]["time_clip_ranges_inside_window"], 1)

    def test_deadline_keeps_inside_ranges_and_drops_unverified_ones(self) -> None:
        class Slow(self._ClipStore):
            def _execute_bounded(self, connection, sql, values, deadline_at):
                if "FROM canonical_chunks chunk" in sql:
                    raise SearchDeadlineExceeded()
                return super()._execute_bounded(connection, sql, values, deadline_at)

        clipped = self._bound(Slow())._clip_passage_hints_to_time_window(
            self._response(), sources=["codex:linux:test"], since="2026-09-01T00:00:00Z", until=None,
        )
        self.assertEqual(clipped["diagnostics"]["time_clip_status"], "deadline-exceeded")
        ranges = clipped["results"][0]["matching_ranges"]
        self.assertEqual([r["kind"] for r in ranges], ["dense"])
        self.assertNotIn("passage_window", ranges[0])


class DensePoolDepthTests(unittest.TestCase):
    def test_prose_queries_pull_the_full_dense_pool(self) -> None:
        from recall_server import passage_retrieval
        # candidate_limit 20 x DENSE_PROSE_OVERSAMPLE reaches DENSE_NEAREST_LIMIT,
        # so one large session cannot crowd a small one out of the pool.
        self.assertGreaterEqual(20 * passage_retrieval.DENSE_PROSE_OVERSAMPLE, passage_retrieval.DENSE_NEAREST_LIMIT)
        self.assertLessEqual(passage_retrieval.DENSE_NEAREST_LIMIT, 400)


class DensePoolShapeTests(unittest.TestCase):
    def test_dense_pool_dedupes_text_and_sets_ef_search(self) -> None:
        import inspect
        from recall_server import passage_retrieval
        source = inspect.getsource(passage_retrieval.PassageHintRetrieval._dense_candidates)
        self.assertIn("DISTINCT ON (passage.text_sha256)", source)
        self.assertIn("FROM distinct_texts nearest", source)
        self.assertIn("set_config('hnsw.ef_search'", source)
        self.assertGreaterEqual(passage_retrieval.DENSE_EF_SEARCH, passage_retrieval.DENSE_NEAREST_LIMIT)


class RerankWiringTests(unittest.TestCase):
    """H2-c: the fused passage pool is reranked before the per-document collapse."""

    class _FakeRerank:
        max_candidates = 50
        fingerprint = "fake-rerank-fingerprint"

        def __init__(self, scores=None, *, error=None, sleep=0.0) -> None:
            self.scores = scores or {}
            self.error = error
            self.sleep = sleep
            self.calls: list[dict] = []

        def rerank(self, query, documents, *, top_k=None, deadline_seconds=None):
            from recall_server.rerank import RerankUnavailable

            self.calls.append({
                "query": query, "documents": list(documents), "deadline_seconds": deadline_seconds,
            })
            if self.sleep:
                time.sleep(self.sleep)
            if self.error:
                raise RerankUnavailable(self.error)
            # Documents arrive as "<context line>\n\n<text window>"; score by the text.
            scored = [
                (index, self.scores.get(doc.rsplit("\n\n", 1)[-1], 0.0))
                for index, doc in enumerate(documents)
            ]
            scored.sort(key=lambda item: (-item[1], item[0]))
            return scored

    class _Store(ActorRecordingStore):
        search_deadline_ms = 20000
        rerank_runtime = None
        rerank_min_budget_seconds = 1.0
        rerank_blend = 1.0  # these tests exercise the pure rerank order

    @staticmethod
    def _rows(kind: str, pairs: list[tuple[str, float]]) -> list[dict]:
        from tests.central_brain.test_passage_fusion import candidate

        return [candidate(document, kind, score) for document, score in pairs]

    def _retrieval(self, store):
        retrieval = PassageHintRetrieval(
            store,
            tenant_id="tenant:test",
            sources=["codex:linux:test"],
            policy_fingerprint="fp-policy",
        )
        dense = self._rows("dense", [("a", 0.91), ("b", 0.88), ("c", 0.87), ("d", 0.80)])
        lexical = self._rows("passage-lexical", [("c", 0.42), ("e", 0.40), ("a", 0.31)])
        retrieval._dense_candidates = lambda query, **kwargs: (dense, "ok", "prose-pool", 10)
        retrieval._lexical_candidates = lambda query, **kwargs: (lexical, "ok")
        retrieval._sparse_candidates = lambda query, original_query=None, **kwargs: ([], "skipped-prose-query")
        return retrieval

    @staticmethod
    def _order(results: list[dict]) -> list[str]:
        return [row["logical_document_id"][5:].rstrip("0") for row in results]

    def _search(self, retrieval, **kwargs):
        return retrieval.search(
            "why did the deploy fail", lexical_query="deploy fail", since=None, until=None,
            **{"limit": 10, **kwargs},
        )

    def test_search_is_identical_when_rerank_runtime_is_disabled(self) -> None:
        store = self._Store()
        baseline = self._search(self._retrieval(store))
        store.rerank_runtime = None
        again = self._search(self._retrieval(store))
        for response in (baseline, again):
            response["diagnostics"].pop("elapsed_ms")
            response["diagnostics"]["arm_elapsed_ms"] = {
                key: 0.0 for key in response["diagnostics"]["arm_elapsed_ms"]
            }
        self.assertEqual(baseline, again)
        self.assertEqual(baseline["diagnostics"]["rerank_status"], "skipped-disabled")
        self.assertEqual(
            set(baseline["diagnostics"]["arm_elapsed_ms"]),
            {"dense", "passage_lexical", "sparse_exact"},
        )
        for key in ("rerank_elapsed_ms", "rerank_candidates", "rerank_model"):
            self.assertNotIn(key, baseline["diagnostics"])
        self.assertTrue(all("rerank_score" not in row for row in baseline["results"]))
        self.assertEqual(self._order(baseline["results"]), ["c", "e", "a", "b", "d"])

    def test_rerank_reorders_top_passages_before_collapse(self) -> None:
        store = self._Store()
        store.rerank_runtime = self._FakeRerank({"d": 0.99, "e": 0.9, "a": 0.5, "b": 0.2, "c": 0.1})
        response = self._search(self._retrieval(store))
        self.assertEqual(self._order(response["results"]), ["d", "e", "a", "b", "c"])
        call = store.rerank_runtime.calls[0]
        self.assertEqual(call["query"], "why did the deploy fail")
        # One passage per document in fused order; raw text_redacted, not the
        # bounded snippet. ``c`` and ``a`` reach the pool under a passage id
        # (dense) and a receipt (lexical) with the same text: sent once.
        self.assertEqual([doc.rsplit("\n\n", 1)[-1] for doc in call["documents"]], ["c", "e", "a", "b", "d"])
        # Each document carries its context line (source, first day) ahead of the text.
        self.assertTrue(all(doc.startswith("source: ") for doc in call["documents"]), call["documents"][0])
        self.assertGreater(call["deadline_seconds"], 1.0)
        top = response["results"][0]
        self.assertEqual(top["rerank_score"], 0.99)
        self.assertEqual(top["matching_ranges"][0]["rerank_score"], 0.99)
        self.assertIn("arm_scores", top)
        self.assertNotIn("rerank", top["matching_ranges"][0]["text"])
        diagnostics = response["diagnostics"]
        self.assertEqual(diagnostics["rerank_status"], "ok")
        self.assertEqual(diagnostics["rerank_candidates"], 5)
        self.assertEqual(diagnostics["rerank_model"], "fake-rerank-fingerprint")

    def test_rerank_scores_reorder_ranges_within_a_document(self) -> None:
        from recall_server.passage_retrieval import apply_rerank_scores

        results = [{
            "logical_document_id": "ldoc_a",
            "rank": 0.5,
            "matching_ranges": [
                {"kind": "dense", "passage_id": "p1", "score": 0.9, "receipts": ["r1"]},
                {"kind": "passage-lexical", "passage_id": "p2", "score": 0.4, "receipts": ["r2"]},
                {"kind": "sparse-exact", "receipts": ["r3"], "score": 0.3},
            ],
        }]
        reranked = apply_rerank_scores(results, {"p2": 0.8, "p1": 0.1})
        self.assertEqual(
            [item.get("passage_id") or item["receipts"][0] for item in reranked[0]["matching_ranges"]],
            ["p2", "p1", "r3"],
        )
        self.assertEqual(reranked[0]["rerank_score"], 0.8)
        self.assertEqual(reranked[0]["rank"], 0.5)
        self.assertNotIn("rerank_score", reranked[0]["matching_ranges"][2])

    def test_rerank_skipped_when_remaining_budget_below_minimum(self) -> None:
        store = self._Store()
        store.rerank_runtime = self._FakeRerank({"d": 0.99})
        retrieval = self._retrieval(store)
        response = retrieval.search(
            "why did the deploy fail", lexical_query="deploy fail", since=None, until=None,
            limit=10, deadline_at=time.monotonic() + 0.5,
        )
        self.assertEqual(store.rerank_runtime.calls, [])
        self.assertEqual(self._order(response["results"]), ["c", "e", "a", "b", "d"])
        diagnostics = response["diagnostics"]
        self.assertEqual(diagnostics["rerank_status"], "skipped-budget")
        self.assertEqual(diagnostics["rerank_elapsed_ms"], 0.0)
        self.assertEqual(diagnostics["rerank_candidates"], 0)
        self.assertEqual(diagnostics["rerank_model"], "fake-rerank-fingerprint")
        self.assertNotIn("rerank", diagnostics["arm_elapsed_ms"])
        self.assertTrue(all("rerank_score" not in row for row in response["results"]))
        # The threshold is the store's configured minimum budget.
        store.rerank_min_budget_seconds = 0.1
        response = retrieval.search(
            "why did the deploy fail", lexical_query="deploy fail", since=None, until=None,
            limit=10, deadline_at=time.monotonic() + 0.5,
        )
        self.assertEqual(response["diagnostics"]["rerank_status"], "ok")
        self.assertEqual(len(store.rerank_runtime.calls), 1)

    def test_rerank_failure_preserves_fused_results(self) -> None:
        store = self._Store()
        store.rerank_runtime = self._FakeRerank(error="rerank_transport_error")
        response = self._search(self._retrieval(store))
        store.rerank_runtime = None
        fused = self._search(self._retrieval(store))
        self.assertEqual(response["results"], fused["results"])
        diagnostics = response["diagnostics"]
        self.assertEqual(diagnostics["rerank_status"], "unavailable")
        self.assertEqual(diagnostics["rerank_error"], "rerank_transport_error")
        self.assertEqual(diagnostics["rerank_candidates"], 5)
        self.assertIn("rerank", diagnostics["arm_elapsed_ms"])
        self.assertFalse(diagnostics["deadline_exceeded"])
        self.assertTrue(diagnostics["partial_results_preserved"])

    def test_rerank_elapsed_reported_in_arm_elapsed_ms(self) -> None:
        store = self._Store()
        store.rerank_runtime = self._FakeRerank({"a": 1.0}, sleep=0.02)
        response = self._search(self._retrieval(store))
        diagnostics = response["diagnostics"]
        self.assertEqual(diagnostics["rerank_status"], "ok")
        self.assertGreaterEqual(diagnostics["rerank_elapsed_ms"], 20.0)
        self.assertEqual(diagnostics["arm_elapsed_ms"]["rerank"], diagnostics["rerank_elapsed_ms"])
        self.assertEqual(
            set(diagnostics["arm_elapsed_ms"]),
            {"dense", "passage_lexical", "sparse_exact", "rerank"},
        )
        self.assertGreaterEqual(diagnostics["elapsed_ms"], diagnostics["rerank_elapsed_ms"])

    def test_rerank_pool_reaches_past_the_result_limit(self) -> None:
        store = self._Store()
        store.rerank_runtime = self._FakeRerank({"e": 1.0})
        response = self._search(self._retrieval(store), limit=2)
        # ``e`` is fused fifth; with limit=2 it is still in the reranked pool
        # and wins, while the response is trimmed to the requested limit.
        self.assertEqual(self._order(response["results"]), ["e", "c"])
        self.assertEqual(response["diagnostics"]["rerank_candidates"], 5)

    def test_rerank_candidate_selection_round_robins_documents(self) -> None:
        from recall_server.passage_retrieval import select_rerank_candidates

        results = [
            {"matching_ranges": [{"passage_id": "a1"}, {"passage_id": "a2"}, {"passage_id": "a3"}]},
            {"matching_ranges": [{"passage_id": "b1"}, {"receipts": ["b2"]}]},
            {"matching_ranges": [{"passage_id": "c1"}]},
        ]
        self.assertEqual(
            select_rerank_candidates(results, max_candidates=50),
            [(0, "a1"), (1, "b1"), (2, "c1"), (0, "a2"), (1, "b2"), (0, "a3")],
        )
        self.assertEqual(
            select_rerank_candidates(results, max_candidates=4),
            [(0, "a1"), (1, "b1"), (2, "c1"), (0, "a2")],
        )
        self.assertEqual(select_rerank_candidates([], max_candidates=4), [])


class RerankBlendTests(unittest.TestCase):
    def _rows(self):
        return [
            {"logical_document_id": "ldoc_a", "rank": 0.90, "matching_ranges": [{"kind": "dense", "passage_id": "a1", "score": 0.9, "receipts": ["ra"]}]},
            {"logical_document_id": "ldoc_b", "rank": 0.10, "matching_ranges": [{"kind": "dense", "passage_id": "b1", "score": 0.1, "receipts": ["rb"]}]},
            {"logical_document_id": "ldoc_c", "rank": 0.50, "matching_ranges": [{"kind": "dense", "passage_id": "c1", "score": 0.5, "receipts": ["rc"]}]},
        ]

    def test_pure_rerank_orders_by_rerank_score(self) -> None:
        from recall_server.passage_retrieval import apply_rerank_scores
        out = apply_rerank_scores(self._rows(), {"a1": 0.2, "b1": 0.9, "c1": 0.5}, blend=1.0)
        self.assertEqual([r["logical_document_id"] for r in out], ["ldoc_b", "ldoc_c", "ldoc_a"])
        self.assertNotIn("blended_score", out[0])

    def test_blend_keeps_a_strongly_fused_document_ahead(self) -> None:
        from recall_server.passage_retrieval import apply_rerank_scores
        out = apply_rerank_scores(self._rows(), {"a1": 0.2, "b1": 0.9, "c1": 0.5}, blend=0.5)
        # a: rerank_norm 0, fused_norm 1 -> 0.5; b: 1, 0 -> 0.5 (tie broken by fused order: a first); c: 0.43, 0.5 -> 0.46
        self.assertEqual([r["logical_document_id"] for r in out], ["ldoc_a", "ldoc_b", "ldoc_c"])
        self.assertIn("blended_score", out[0])
        self.assertEqual(out[0]["rank"], 0.90)

    def test_blend_env_parsing(self) -> None:
        from recall_server.rerank import rerank_blend_from_env, DEFAULT_RERANK_BLEND
        with mock.patch.dict("os.environ", {"RECALL_RERANK_BLEND": ""}):
            self.assertEqual(rerank_blend_from_env(), DEFAULT_RERANK_BLEND)
        with mock.patch.dict("os.environ", {"RECALL_RERANK_BLEND": "0.25"}):
            self.assertEqual(rerank_blend_from_env(), 0.25)
        for bad in ("1.5", "-0.1", "x"):
            with mock.patch.dict("os.environ", {"RECALL_RERANK_BLEND": bad}), self.assertRaises(ValueError):
                rerank_blend_from_env()


class RerankPoolNominationTests(unittest.TestCase):
    def _row(self, doc, score, kind="dense", rank=1):
        return {
            "logical_document_id": doc, "source_id": "s", "revision": 1,
            "native_parent_id": doc, "first_occurred_at": "2026-01-01",
            "last_occurred_at": "2026-01-01", "manifest_object_key": "k",
            "manifest_content_sha256": "h", "score": score,
            "text_redacted": f"text of {doc}", "passage_id": f"{doc}-{kind}",
            "passage_ordinal": 0, "spans": [], "receipts": [f"r-{doc}"],
        }

    def _legs(self):
        dense = [self._row(f"d{i}", 1.0 - i * 0.01) for i in range(30)]
        lexical = [self._row("lex-top", 0.9, "passage-lexical"), self._row("d3", 0.5, "passage-lexical")]
        return (("dense", 0.65, dense), ("passage-lexical", 0.1, lexical), ("sparse-exact", 0.25, []))

    def test_collapse_without_nomination_drops_the_lexical_top_hit(self) -> None:
        from recall_server.passage_retrieval import collapse_document_candidates
        out = collapse_document_candidates(self._legs(), limit=5, fusion="convex", alphas={"dense": 0.65, "passage-lexical": 0.1, "sparse-exact": 0.25})
        self.assertNotIn("lex-top", [r["logical_document_id"] for r in out])
        self.assertEqual(len(out), 5)

    def test_collapse_appends_arm_nominated_documents_after_the_fused_head(self) -> None:
        from recall_server.passage_retrieval import collapse_document_candidates
        alphas = {"dense": 0.65, "passage-lexical": 0.1, "sparse-exact": 0.25}
        plain = collapse_document_candidates(self._legs(), limit=5, fusion="convex", alphas=alphas)
        out = collapse_document_candidates(self._legs(), limit=5, fusion="convex", alphas=alphas, nominate_per_arm=10)
        # The fused head is byte-identical; the lexical #1 follows it, flagged.
        self.assertEqual(out[:5], plain)
        self.assertEqual([r["logical_document_id"] for r in out[5:]], [f"d{i}" for i in range(5, 10)] + ["lex-top"])
        self.assertTrue(all(r["nominated"] for r in out[5:]))
        self.assertTrue(all("nominated" not in r for r in out[:5]))

    def test_nominated_documents_enter_the_rerank_pool_first_pass(self) -> None:
        from recall_server.passage_retrieval import select_rerank_candidates
        results = [{"matching_ranges": [{"passage_id": f"h{i}"}]} for i in range(6)]
        results.append({"matching_ranges": [{"passage_id": "nom"}], "nominated": True})
        chosen = [key for _index, key in select_rerank_candidates(results, max_candidates=4)]
        self.assertIn("nom", chosen)
        self.assertEqual(chosen, ["h0", "h1", "h2", "nom"])
        # Without the flag the pool is the fused head only.
        results[-1].pop("nominated")
        self.assertEqual([k for _i, k in select_rerank_candidates(results, max_candidates=4)], ["h0", "h1", "h2", "h3"])

    def test_focus_window_centres_on_the_matching_sentence(self) -> None:
        from recall_server.passage_retrieval import focus_terms, focus_window
        text = "a " * 3000 + "junction tables expert_skills and expert_mcp_tools " + "b " * 3000
        window = focus_window(text, focus_terms("junction tables expert_skills expert_mcp_tools"), 2000)
        self.assertEqual(len(window), 2000)
        self.assertIn("expert_mcp_tools", window)
        self.assertEqual(focus_window(text, ["zzz"], 100), text[:100])
        self.assertEqual(focus_window("short", ["short"], 100), "short")
        self.assertEqual(focus_terms("What is the P2 #6076 greptile fix?"), ["6076", "greptile"])
        # The head wins unless the window covers clearly more terms.
        head = "junction tables expert_skills " + "c " * 3000 + "expert_mcp_tools " + "d " * 3000
        self.assertEqual(focus_window(head, focus_terms("junction tables expert_skills expert_mcp_tools"), 2000), head[:2000])

    def test_search_reranks_a_nominated_lexical_hit(self) -> None:
        from recall_server.passage_retrieval import RERANK_NOMINATE_PER_ARM
        self.assertEqual(RERANK_NOMINATE_PER_ARM, 5)


class LexicalMinShouldMatchTests(unittest.TestCase):
    def test_plan_keeps_the_plain_and_for_short_or_all_common_queries(self) -> None:
        from recall_server.passage_retrieval import lexical_match_plan
        common = frozenset({"grep", "tools", "expert"})
        plan = lexical_match_plan("deploy fail", common)
        self.assertEqual((plan["min_match"], plan["conjunctions"], plan["relaxed"]), (2, ["deploy fail"], False))
        plan = lexical_match_plan("grep tools expert", common)
        self.assertEqual((plan["required"], plan["conjunctions"], plan["relaxed"]), ([], ["grep tools expert"], False))

    def test_plan_makes_common_terms_optional_and_relaxes_long_queries(self) -> None:
        from recall_server.passage_retrieval import lexical_match_plan
        common = frozenset({"grep", "tools", "expert"})
        plan = lexical_match_plan("codex Grep hydration parity", common)
        self.assertEqual(plan["required"], ["codex", "hydration", "parity"])
        self.assertEqual((plan["min_match"], plan["conjunctions"], plan["relaxed"]), (3, ["codex hydration parity"], True))
        plan = lexical_match_plan("a b c d e", frozenset())
        self.assertEqual(plan["min_match"], 4)
        self.assertEqual(plan["conjunctions"], ["a b c d", "a b c e", "a b d e", "a c d e", "b c d e"])
        plan = lexical_match_plan(" ".join("t%d" % i for i in range(13)), frozenset())
        self.assertEqual((len(plan["required"]), plan["min_match"], len(plan["conjunctions"])), (10, 8, 45))
        # Duplicates collapse, case is preserved for the query text.
        self.assertEqual(lexical_match_plan("Alpha alpha beta", frozenset())["terms"], ["Alpha", "beta"])

    def test_lexical_sql_ors_the_conjunctions_and_ranks_every_term(self) -> None:
        from recall_server.passage_retrieval import PassageHintRetrieval, _COMMON_LEXEMES_CACHE

        class _Cursor:
            def __init__(self, rows): self.rows = rows
            def fetchall(self): return self.rows

        class _Store:
            database_url = "postgresql://stats-test"
            def __init__(self):
                self.sql = []; self.values = []
            def _execute_bounded(self, connection, sql, values, deadline_at):
                self.sql.append(sql); self.values.append(values)
                if "pg_stats" in sql:
                    return _Cursor([{"elems": ["grep", "tools"]}])
                return _Cursor([])

        _COMMON_LEXEMES_CACHE.pop("postgresql://stats-test", None)
        store = _Store()
        retrieval = PassageHintRetrieval(store, tenant_id="tenant:test", sources=["codex:linux:test"], policy_fingerprint="fp")
        rows = retrieval._lexical_query(
            object(), "grep triage step schemas workflow tools", order="rank", since=None, until=None,
            candidate_limit=40, actor_ids=None, actor_relations=None, deadline_at=time.monotonic() + 5,
        )
        self.assertEqual(rows, [])
        sql = store.sql[-1]
        values = store.values[-1]
        # required = triage step schemas workflow (4) -> any 3 of 4: four conjunctions OR-ed
        match_clause = sql.split("search_vector @@", 1)[1].split("AND (%s::timestamptz", 1)[0]
        self.assertEqual(match_clause.count("plainto_tsquery('simple',%s)"), 4)
        self.assertEqual(match_clause.count(" || "), 3)
        self.assertIn("triage step schemas", values)
        self.assertIn("step schemas workflow", values)
        # the rank query ORs all six terms
        # matched ORDER BY, top ORDER BY, final ORDER BY, and the score column
        self.assertEqual(sql.count(" || ".join(["plainto_tsquery('simple',%s)"] * 6)), 4)
        self.assertEqual(retrieval.lexical_plan, {"terms": 6, "required": 4, "min_match": 3, "conjunctions": 4, "relaxed": True})
        # Second call within the TTL does not re-read pg_stats.
        stats_calls = sum(1 for item in store.sql if "pg_stats" in item)
        retrieval._lexical_query(
            object(), "deploy fail", order="rank", since=None, until=None,
            candidate_limit=40, actor_ids=None, actor_relations=None, deadline_at=time.monotonic() + 5,
        )
        self.assertEqual(sum(1 for item in store.sql if "pg_stats" in item), stats_calls)
        self.assertFalse(retrieval.lexical_plan["relaxed"])
        self.assertIn("passage.search_vector @@\n                              plainto_tsquery('simple',%s)", store.sql[-1])


class RerankContextTests(unittest.TestCase):
    def test_stored_header_leads_the_rerank_document(self) -> None:
        from recall_server.passage_retrieval import rerank_document
        from recall_server import passage_retrieval
        row = {"header_redacted": "source family: codex\nharness: codex\n", "text_redacted": "x " * 3000 + "needle here"}
        doc = rerank_document(row, ["needle"], 2000)
        self.assertTrue(doc.startswith("source family: codex\nharness: codex\n\n"))
        # Focus window off: the head follows the context (the runtime trims the tail).
        self.assertEqual(doc, "source family: codex\nharness: codex\n\n" + row["text_redacted"])
        with mock.patch.object(passage_retrieval, "RERANK_FOCUS_WINDOW", True):
            doc = rerank_document(row, ["needle", "here"], 2000)
            self.assertIn("needle here", doc)
            self.assertLessEqual(len(doc), 2000)

    def test_fallback_context_uses_source_and_first_day(self) -> None:
        from recall_server.passage_retrieval import rerank_context, rerank_document
        row = {"source_id": "codex:linux:greppy3", "passage_first_occurred_at": "2026-07-08 12:00:00+00", "text_redacted": "body"}
        self.assertEqual(rerank_context(row), "source: codex:linux:greppy3\nwhen: 2026-07-08")
        self.assertEqual(rerank_document(row, [], 2000), "source: codex:linux:greppy3\nwhen: 2026-07-08\n\nbody")
        self.assertEqual(rerank_document({"text_redacted": "body"}, [], 2000), "body")
        self.assertEqual(rerank_document({"header_redacted": "h" * 1990, "text_redacted": "body"}, [], 2000), "body")


class IdentifierSigilTests(unittest.TestCase):
    def test_hash_prefixed_numbers_are_identifiers(self) -> None:
        from recall_server.passage_retrieval import identifier_tokens
        tokens = identifier_tokens("what did Greptile flag on PR #6076 around May 2-4?")
        self.assertIn("6076", tokens)
        self.assertIn("2-4", tokens)
        self.assertNotIn("#6076", tokens)
        self.assertEqual(identifier_tokens("@alice's $HOME_DIR."), ["home_dir"])
