from __future__ import annotations

import json
import inspect
import sys
import threading
import time
import unittest
from contextlib import contextmanager
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
        self.assertEqual(arm_sql.count("actor.actor_id=ANY(%s)"), 2)
        self.assertGreaterEqual(
            inspect.getsource(PassageHintRetrieval.search).count(
                "actor.actor_id=ANY(%s)"
            ),
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
        self.assertEqual(lexical_values[2], ["codex:linux:test"])

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
