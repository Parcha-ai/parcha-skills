from __future__ import annotations

import json
import sys
import time
import unittest
from contextlib import contextmanager
from pathlib import Path


SERVER = Path(__file__).resolve().parents[2] / "server"
sys.path.insert(0, str(SERVER))

from recall_server.canonical_retrieval import (  # noqa: E402
    BoundCanonicalRetrieval,
    _informative_query_terms,
)
from recall_server.db import SearchDeadlineExceeded  # noqa: E402
from recall_server.deep_inspection import DeepInspectionError  # noqa: E402


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

    def test_lexical_deadline_degrades_to_optional_semantic_path(self) -> None:
        store = DeadlineStore()
        started = time.monotonic()
        retrieval = BoundCanonicalRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("codex.jsonl:test",),
        )

        result = retrieval.search("synthetic canonical deadline query")

        self.assertEqual(result["results"], [])
        self.assertEqual(result["diagnostics"]["lexical_mode"], "deadline-exceeded")
        self.assertEqual(result["diagnostics"]["semantic_status"], "disabled")
        self.assertIsNotNone(store.deadline_at)
        assert store.deadline_at is not None
        self.assertGreaterEqual(store.deadline_at, started)
        self.assertLessEqual(store.deadline_at, started + 0.1)

    def test_semantic_database_query_gets_an_independent_hard_deadline(self) -> None:
        store = DeadlineStore()
        store.semantic_runtime = SemanticRuntime()
        retrieval = BoundCanonicalRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("codex.jsonl:test",),
        )

        result = retrieval.search("synthetic canonical deadline query")

        self.assertEqual(result["results"], [])
        self.assertEqual(result["diagnostics"]["lexical_mode"], "deadline-exceeded")
        self.assertEqual(
            result["diagnostics"]["semantic_status"],
            "deadline-exceeded",
        )
        self.assertEqual(len(store.deadlines), 2)
        self.assertGreaterEqual(store.deadlines[1], store.deadlines[0])

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

        result = retrieval.search(query)

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

        retrieval.search("ATI harness default runtime")

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

        result = retrieval.search("ATI harness default runtime")

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

        retrieval.search("8668a658-a6cf-4358-9d7e-c29e5782c1dd")

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
