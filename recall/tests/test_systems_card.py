"""Systems card: probes run against a fake brain and produce a content-free card."""
from __future__ import annotations

import io
import json
import tempfile
import unittest
import urllib.error
from pathlib import Path

from evals.systems_card import accuracy, corpus, forget, latency
from evals.systems_card.availability import AvailabilityProbe
from evals.systems_card.mcp_client import McpClient, McpClientError, load_profile
from evals.systems_card.model import Gate, ProbeResult, dimension_status, percentile, summarize_latency
from evals.systems_card.probes import ProbeContext, timed
from evals.systems_card.render import render_html
from evals.systems_card.runner import PROBES, build_card, history_row, parser
from tests.test_agentic_truth import truth_cases


class _Response:
    def __init__(self, payload: dict, status: int = 200) -> None:
        self._raw = json.dumps(payload).encode()
        self.status = status

    def read(self, n: int = -1) -> bytes:
        return self._raw[: n if n > 0 else None]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeBrain:
    """Answers tools/call from a table; records every call name."""

    def __init__(self, tools: dict, *, http_error: int | None = None) -> None:
        self.tools = tools
        self.calls: list[tuple[str, dict]] = []
        self.http_error = http_error

    def __call__(self, request, timeout):
        if self.http_error:
            raise urllib.error.HTTPError(request.full_url, self.http_error, "busy", {"Retry-After": "30"}, io.BytesIO(b"{}"))
        message = json.loads(request.data)
        if message["method"] == "ping":
            return _Response({"jsonrpc": "2.0", "id": message["id"], "result": {}})
        name = message["params"]["name"]
        arguments = message["params"]["arguments"]
        self.calls.append((name, arguments))
        handler = self.tools[name]
        structured = handler(arguments) if callable(handler) else handler
        if structured is None:
            return _Response({"jsonrpc": "2.0", "id": message["id"], "result": {"isError": True, "content": []}})
        return _Response({"jsonrpc": "2.0", "id": message["id"], "result": {"structuredContent": structured, "isError": False}})


def search_result(query_seed: int = 0) -> dict:
    hits = []
    for i in range(3):
        identity = query_seed * 2 + i
        hits.append({
            "logical_document_id": f"ldoc_{identity:032x}",
            "source_id": f"synthetic:source:{i % 2}",
            "revision": 1,
            "rank": 0.1,
            "matching_ranges": [{"receipts": [f"recall://synthetic:source:{i % 2}/record-{identity}?rev=1#item=0"]}],
        })
    return {
        "results": hits,
        "diagnostics": {"elapsed_ms": 120.0, "deadline_exceeded": False, "dense_status": "ok", "dense_strategy": "exact-scoped",
                        "dense_candidates": 5, "passage_lexical_candidates": 8, "sparse_candidates": 2},
    }


SCAN_FRESHNESS = json.dumps([
    {"source_id": "codex:linux:a", "passages": 10, "docs": 4, "newest": "2099-01-01 00:00:00+00", "oldest": "2026-01-01 00:00:00+00"},
    {"source_id": "claude:linux:b", "passages": 5, "docs": 2, "newest": "2026-01-01 00:00:00+00", "oldest": "2026-01-01 00:00:00+00"},
])


def scan_handler(arguments: dict) -> dict:
    program = arguments["program"]
    base = {"exit_code": 0, "complete": True, "projection_pending": 0, "objects_unavailable": 0, "sources_available": 2,
            "buckets_available": 3, "stderr": "", "timing": {"totalMs": 900.0, "queueMs": 50.0, "executeMs": 800.0, "phases": {"program_start_to_program_endMs": 300.0}}}
    if arguments.get("filters", {}).get("source_id", "").startswith("systemscard:"):
        return {**base, "sources_available": 0, "stdout": "0\n"}
    if "newest" in program:
        return {**base, "stdout": SCAN_FRESHNESS}
    if "secret_" in program:
        row = {"passages": 100, "report_email": 3, "report_phone_us": 0}
        for name in corpus.SECRET_PATTERNS:
            row[f"secret_{name}"] = 0
        return {**base, "stdout": json.dumps([row])}
    if "count(DISTINCT logical_document_id) AS docs" in program:
        return {**base, "stdout": json.dumps([{"docs": 6}])}
    return {**base, "stdout": json.dumps([{"passages": 15, "docs": 6}])}


def scope_handler(arguments: dict) -> dict:
    if arguments.get("filters", {}).get("source_id", "").startswith("systemscard:"):
        return {"documents": [], "total_documents": 0, "offset": 0, "complete": True}
    docs = [{"logical_document_id": f"ldoc_{i:032x}", "source_id": "synthetic:source:0"} for i in range(6)]
    return {"documents": docs, "total_documents": None, "offset": 0, "complete": True}


def default_tools() -> dict:
    return {
        "recall_search": lambda a: {"results": [], "diagnostics": {"reason": "no-authorized-sources"}} if a.get("filters", {}).get("source_id", "").startswith("systemscard:") or a.get("filters", {}).get("person") else search_result(),
        "recall_scope": scope_handler,
        "recall_people": {"people": [], "complete": True},
        "recall_session_context": {"records": []},
        "recall_show": {"content": []},
        "recall_exec": {"exit_code": 0, "stdout": "3\n", "stderr": "", "complete": True, "timing": {"totalMs": 500}},
        "recall_scan": scan_handler,
    }


def make_context(brain: FakeBrain, **options) -> ProbeContext:
    client = McpClient("https://brain.invalid/mcp", "synthetic", opener=brain)
    return ProbeContext(client=client, base_url="https://brain.invalid/mcp", options=options, http_get=lambda url, t: (200, b'{"status":"ready"}'))


class ModelTest(unittest.TestCase):
    def test_percentile_and_summary(self):
        self.assertEqual(percentile([1, 2, 3, 4], 50), 2.5)
        self.assertEqual(percentile([7], 95), 7)
        self.assertIsNone(percentile([], 50))
        summary = summarize_latency([100.0, 200.0, 300.0])
        self.assertEqual(summary["n"], 3)
        self.assertEqual(summary["p50_ms"], 200.0)

    def test_gate_semantics(self):
        self.assertTrue(Gate("m", "<=", 5).evaluate(5).passed)
        self.assertFalse(Gate("m", ">=", 5).evaluate(4.9).passed)
        self.assertIsNone(Gate("m", "==", 0).evaluate(None).passed)
        with self.assertRaises(ValueError):
            Gate("m", "!=", 0).evaluate(1)

    def test_dimension_status_rolls_up_worst_case(self):
        ok = ProbeResult("a", "d", "ok")
        bad_gate = ProbeResult("b", "d", "ok", gates=[Gate("m", "<=", 1).evaluate(2)])
        self.assertEqual(dimension_status([ok]), "ok")
        self.assertEqual(dimension_status([ok, bad_gate]), "degraded")
        self.assertEqual(dimension_status([ok, ProbeResult("c", "d", "failed")]), "failed")
        self.assertEqual(dimension_status([ProbeResult("c", "d", "skipped")]), "skipped")


class ClientTest(unittest.TestCase):
    def test_tool_call_parses_structured_content_and_times_it(self):
        brain = FakeBrain(default_tools())
        client = McpClient("https://brain.invalid/mcp", "tok", opener=brain)
        outcome = client.call_tool("recall_people", {})
        self.assertTrue(outcome.ok)
        self.assertEqual(outcome.result, {"people": [], "complete": True})
        self.assertGreaterEqual(outcome.elapsed_ms, 0.0)
        self.assertEqual(brain.calls, [("recall_people", {})])

    def test_http_error_is_content_free_and_never_raises(self):
        brain = FakeBrain(default_tools(), http_error=503)
        client = McpClient("https://brain.invalid/mcp", "tok", opener=brain)
        outcome = client.call_tool("recall_people", {})
        self.assertFalse(outcome.ok)
        self.assertEqual(outcome.error, "http:503")
        self.assertEqual(outcome.http_status, 503)

    def test_tool_error_result_is_reported(self):
        brain = FakeBrain({"recall_people": None})
        client = McpClient("https://brain.invalid/mcp", "tok", opener=brain)
        outcome = client.call_tool("recall_people", {})
        self.assertFalse(outcome.ok)
        self.assertEqual(outcome.error, "tool_error")

    def test_profile_requires_private_files(self):
        with tempfile.TemporaryDirectory() as directory:
            token = Path(directory) / "token.json"
            token.write_text('{"token": "abc"}')
            token.chmod(0o644)
            with self.assertRaises(McpClientError):
                load_profile(url="https://brain.invalid/mcp", token_file=str(token))
            token.chmod(0o600)
            self.assertEqual(load_profile(url="https://brain.invalid/mcp", token_file=str(token)), ("https://brain.invalid/mcp", "abc"))
            with self.assertRaises(McpClientError):
                load_profile(url="http://brain.invalid/mcp", token_file=str(token))


class ProbeTest(unittest.TestCase):
    def test_availability_probe_counts_endpoints_and_ping(self):
        context = make_context(FakeBrain(default_tools()))
        result = AvailabilityProbe(samples=2, interval_seconds=0).run(context)
        self.assertEqual(result.status, "ok")
        self.assertEqual(result.metrics["readyz_success_rate"], 1.0)
        self.assertEqual(result.metrics["mcp_ping_success_rate"], 1.0)
        self.assertTrue(all(g.passed for g in result.gates))

    def test_tool_latency_probe_measures_every_tool_once_per_repetition(self):
        brain = FakeBrain(default_tools())
        context = make_context(brain, latency_repetitions=2, queries=["q1", "q2"])
        result = latency.ToolLatencyProbe().run(context)
        self.assertEqual(result.status, "ok")
        self.assertEqual(result.metrics["recall_search.n"], 4)
        self.assertEqual(result.metrics["recall_scan.n"], 2)
        self.assertEqual(result.metrics["recall_exec.n"], 2)
        self.assertEqual(result.metrics["error_rate"], 0.0)
        self.assertIsNotNone(context.options["_shared_receipt"])
        first = result.metrics["recall_search.first_call_diagnostics"]
        self.assertEqual(first["elapsed_ms"], 120.0)
        self.assertEqual(first["dense_strategy"], "exact-scoped")
        self.assertNotIn("results", json.dumps(first))

    def test_search_stage_probe_reads_server_diagnostics(self):
        context = make_context(FakeBrain(default_tools()), queries=["q1", "q2", "q3"])
        result = latency.SearchStageProbe().run(context)
        self.assertEqual(result.status, "ok")
        self.assertEqual(result.metrics["deadline_exceeded_rate"], 0.0)
        self.assertEqual(result.metrics["dense_ok_rate"], 1.0)
        self.assertEqual(result.metrics["server_p50_ms"], 120.0)
        self.assertEqual(result.metrics["dense_strategy.exact-scoped"], 3)

    def test_archil_phase_probe_summarizes_timing(self):
        context = make_context(FakeBrain(default_tools()), archil_repetitions=2)
        result = latency.ArchilPhaseProbe().run(context)
        self.assertEqual(result.status, "ok")
        self.assertEqual(result.metrics["queueMs.p95"], 50.0)
        self.assertEqual(result.metrics["phase.program_start_to_program_endMs.p50"], 300.0)

    def test_freshness_probe_hashes_sources_and_measures_age(self):
        context = make_context(FakeBrain(default_tools()))
        result = corpus.FreshnessProbe().run(context)
        self.assertEqual(result.metrics["sources"], 2)
        self.assertEqual(result.metrics["newest_age_hours_min"], 0.0)
        self.assertEqual(result.metrics["projection_pending"], 0)
        for key in result.metrics["per_source"]:
            self.assertNotIn(":", key)  # hashed, never the source id

    def test_scan_consistency_probe_compares_scope_and_scan(self):
        context = make_context(FakeBrain(default_tools()))
        result = corpus.ScanConsistencyProbe().run(context)
        self.assertEqual(result.status, "ok")
        self.assertEqual(result.metrics["scope_enumerated_documents"], 6)
        self.assertEqual(result.metrics["scan_distinct_documents"], 6)
        self.assertEqual(result.metrics["scope_scan_agreement"], 1.0)

    def test_authorization_probe_flags_leaks(self):
        context = make_context(FakeBrain(default_tools()))
        result = corpus.AuthorizationProbe().run(context)
        self.assertEqual(result.status, "ok")
        self.assertEqual(result.metrics["leaks"], 0)
        self.assertEqual(result.metrics["unauthorized_source_reason"], "no-authorized-sources")
        leaky = default_tools()
        leaky["recall_search"] = search_result()
        result = corpus.AuthorizationProbe().run(make_context(FakeBrain(leaky)))
        self.assertEqual(result.status, "failed")
        self.assertGreaterEqual(result.metrics["leaks"], 2)

    def test_secret_scan_probe_gates_on_zero_hits(self):
        context = make_context(FakeBrain(default_tools()))
        result = corpus.SecretScanProbe().run(context)
        self.assertEqual(result.status, "ok")
        self.assertEqual(result.metrics["secret_hits_total"], 0)
        self.assertEqual(result.metrics["report_email"], 3)
        program = corpus.secret_scan_program()
        self.assertIn("regexp_matches(text", program)
        self.assertNotIn("\n", program)

    def test_timed_wraps_probe_exceptions(self):
        class Broken:
            name = "x.broken"
            dimension = "latency"

            def run(self, context):
                raise RuntimeError("boom")

        result = timed(Broken(), make_context(FakeBrain(default_tools())))
        self.assertEqual(result.status, "failed")
        self.assertIn("RuntimeError", result.notes[0])


class AccuracyTest(unittest.TestCase):
    def test_truth_probe_scores_validation_split_through_search(self):
        cases = truth_cases()
        gold = {case["id"]: case for case in cases}

        def search(arguments: dict) -> dict:
            # Return the gold boundary for answerable questions, junk otherwise.
            for case in cases:
                if case["question"] == arguments["query"]:
                    hits = [
                        {"logical_document_id": b["logical_document_id"], "source_id": b["source_id"], "revision": 1,
                         "matching_ranges": [{"receipts": b["receipts"]}]}
                        for b in case["gold_boundaries"]
                    ]
                    hits.append({"logical_document_id": "ldoc_" + "f" * 32, "source_id": "synthetic:source:9", "revision": 2, "matching_ranges": []})
                    return {"results": hits, "diagnostics": {"elapsed_ms": 10.0}}
            return {"results": [], "diagnostics": {}}

        tools = default_tools()
        tools["recall_search"] = search
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            private = root / "private"
            private.mkdir(mode=0o700)
            truth = private / "truth.jsonl"
            truth.write_text("".join(json.dumps(c, sort_keys=True) + "\n" for c in cases))
            truth.chmod(0o600)
            out = private / "out"
            context = make_context(FakeBrain(tools), truth_path=str(truth), truth_split="validation", accuracy_pointer_checks=2)
            context.private_dir = str(out)
            result = accuracy.TruthBoundaryProbe().run(context)
            self.assertEqual(result.metrics["cases"], 15)
            self.assertEqual(result.metrics["boundary_recall@20"], 1.0)
            self.assertEqual(result.metrics["boundary_mrr"], 1.0)
            self.assertEqual(result.metrics["negative_false_hit_rate"], 1.0)
            self.assertEqual(result.metrics["authorization_violation_rate"], 0.0)
            self.assertEqual(result.metrics["receipt_resolution_checks"], 2)
            self.assertEqual(result.metrics["stratum.exact-document.boundary_recall@20"], 1.0)
            saved = list(out.glob("systems-card-boundaries-validation-*.jsonl"))
            self.assertEqual(len(saved), 1)
            self.assertEqual(oct(saved[0].stat().st_mode & 0o777), "0o600")
            rendered = json.dumps(result.as_dict())
            for case in cases:
                self.assertNotIn(case["question"], rendered)
                for boundary in case["gold_boundaries"]:
                    self.assertNotIn(boundary["receipts"][0], rendered)
        self.assertTrue(gold)

    def test_truth_probe_skips_without_truth(self):
        result = accuracy.TruthBoundaryProbe().run(make_context(FakeBrain(default_tools())))
        self.assertEqual(result.status, "skipped")

    def test_candidates_dedupe_and_cap(self):
        hits = [{"logical_document_id": "ldoc_" + "a" * 32, "source_id": "s", "revision": 3}] * 3
        self.assertEqual(len(accuracy.candidates_from_search({"results": hits})), 1)
        self.assertEqual(accuracy.candidates_from_search({"results": hits})[0]["revision"], 3)


class _FakeClock:
    """Sleeping advances the clock; nothing else does, so polls are deterministic."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.slept: list[float] = []

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds

    def __call__(self) -> float:
        return self.now


class ForgetBrainState:
    """Memory becomes searchable after `visible_after` polls and disappears `absent_after` polls after forget."""

    RECEIPT = "recall://manual:capture:owner/mem-0123456789abcdef?rev=1#item=0"

    def __init__(self, *, visible_after: int | None = 2, absent_after: int | None = 3, capture_ok: bool = True, forget_ok: bool = True, resolves_after_forget: bool = False) -> None:
        self.visible_after = visible_after
        self.absent_after = absent_after
        self.capture_ok = capture_ok
        self.forget_ok = forget_ok
        self.resolves_after_forget = resolves_after_forget
        self.captured: dict | None = None
        self.forgotten = False
        self.searches = 0
        self.searches_after_forget = 0

    def _visible(self) -> bool:
        if self.captured is None:
            return False
        if self.forgotten:
            return self.absent_after is not None and self.searches_after_forget <= self.absent_after
        return self.visible_after is not None and self.searches > self.visible_after

    def capture(self, arguments: dict) -> dict | None:
        if not self.capture_ok:
            return None
        self.captured = arguments
        return {"receipt": self.RECEIPT, "status": "accepted", "duplicate": False}

    def forget(self, arguments: dict) -> dict | None:
        if not self.forget_ok:
            return None
        self.forgotten = arguments["receipt"] == self.RECEIPT
        return {"status": "forgotten", "deleted_receipt": self.RECEIPT.split("#", 1)[0]}

    def search(self, arguments: dict) -> dict:
        self.searches += 1
        if self.forgotten:
            self.searches_after_forget += 1
        hits = [{"logical_document_id": "ldoc_" + "9" * 32, "source_id": "other", "revision": 1, "matching_ranges": [{"receipts": ["recall://other/record-1?rev=1#item=0"]}]}]
        if self._visible():
            body = self.captured["body"]
            hits.insert(0, {"logical_document_id": "ldoc_" + "1" * 32, "source_id": "manual:capture:owner", "revision": 1,
                            "matching_ranges": [{"receipts": [self.RECEIPT], "text": body}]})
        return {"results": hits, "diagnostics": {"elapsed_ms": 5.0}}

    def resolve(self, arguments: dict) -> dict | None:
        if self.captured is None or arguments["target"].split("#", 1)[0] != self.RECEIPT.split("#", 1)[0]:
            return None
        if self.forgotten and not self.resolves_after_forget:
            return None
        return {"records": [], "content": []}

    def tools(self) -> dict:
        value = default_tools()
        value.update({
            "recall_capture": self.capture,
            "recall_forget": self.forget,
            "recall_search": self.search,
            "recall_show": self.resolve,
            "recall_session_context": self.resolve,
        })
        return value


def forget_context(state: ForgetBrainState, clock: _FakeClock, **options) -> tuple[FakeBrain, ProbeContext]:
    brain = FakeBrain(state.tools())
    context = make_context(brain, forget_probe=True, _sleep=clock.sleep, _clock=clock, **options)
    return brain, context


class ForgetLatencyTest(unittest.TestCase):
    def test_skipped_unless_flag(self):
        brain = FakeBrain(ForgetBrainState().tools())
        result = forget.ForgetLatencyProbe().run(make_context(brain))
        self.assertEqual(result.status, "skipped")
        self.assertEqual(brain.calls, [])
        self.assertIn("--forget-probe", result.notes[0])

    def test_measures_visible_and_forgotten_latency(self):
        state = ForgetBrainState(visible_after=2, absent_after=3)
        clock = _FakeClock()
        brain, context = forget_context(state, clock)
        result = forget.ForgetLatencyProbe(poll_interval_s=15).run(context)
        self.assertEqual(result.status, "ok", result.notes)
        metrics = result.metrics
        self.assertTrue(metrics["capture_ok"])
        self.assertTrue(metrics["forget_ok"])
        self.assertEqual(metrics["capture_visible_after_s"], 30.0)  # 2 misses -> 2 sleeps
        self.assertEqual(metrics["forgotten_after_s"], 45.0)  # 3 hits after forget -> 3 sleeps
        self.assertTrue(metrics["receipt_unresolvable"])
        self.assertEqual(metrics["polls"], 3 + 4)
        self.assertEqual(metrics["search_errors"], 0)
        self.assertTrue(all(g.passed for g in result.gates), [(g.metric, g.passed) for g in result.gates])
        self.assertEqual({g.metric for g in result.gates}, {"capture_ok", "capture_visible_after_s", "forget_ok", "forgotten_after_s", "receipt_unresolvable"})
        self.assertEqual(clock.slept, [15.0] * 5)
        # The capture went through the documented write schema; forget used the returned receipt.
        capture_args = next(a for n, a in brain.calls if n == "recall_capture")
        self.assertEqual(capture_args["schema_version"], 1)
        self.assertEqual(capture_args["title"], "systems card forget probe")
        self.assertEqual(capture_args["provenance"], {"uri": "manual://systems-card/forget-probe"})
        self.assertEqual(capture_args["tags"], ["systems-card"])
        self.assertIn("systemscard-forget-probe", capture_args["body"])
        self.assertEqual(next(a for n, a in brain.calls if n == "recall_forget"), {"receipt": ForgetBrainState.RECEIPT})
        searches = [a for n, a in brain.calls if n == "recall_search"]
        self.assertTrue(all(a["limit"] == 5 and a["query"] in capture_args["body"] for a in searches))
        self.assertEqual([n for n, _ in brain.calls][-2:], ["recall_show", "recall_session_context"])
        # Nothing content-bearing reaches the card.
        rendered = json.dumps(result.as_dict())
        self.assertNotIn(ForgetBrainState.RECEIPT, rendered)
        self.assertNotIn(ForgetBrainState.RECEIPT.split("#", 1)[0], rendered)
        self.assertNotIn(capture_args["body"], rendered)
        nonce = capture_args["body"].split("Marker: ", 1)[1].split(".", 1)[0]
        self.assertNotIn(nonce, rendered)
        self.assertNotIn(nonce.split(" ")[-1], rendered)

    def test_never_visible_is_degraded_but_still_forgets(self):
        state = ForgetBrainState(visible_after=None, absent_after=None)
        clock = _FakeClock()
        brain, context = forget_context(state, clock)
        result = forget.ForgetLatencyProbe(poll_interval_s=100, visible_timeout_s=300, absent_timeout_s=300).run(context)
        self.assertEqual(result.status, "degraded")
        self.assertTrue(state.forgotten)
        self.assertIsNone(result.metrics["capture_visible_after_s"])
        self.assertIsNone(result.metrics["forgotten_after_s"])
        self.assertTrue(result.metrics["receipt_unresolvable"])
        self.assertIsNone(next(g for g in result.gates if g.metric == "forgotten_after_s").passed)
        self.assertEqual(result.metrics["polls"], 4 + 4)  # polls at 0,100,200,300 in each phase
        self.assertNotIn(ForgetBrainState.RECEIPT, json.dumps(result.as_dict()))

    def test_still_searchable_after_timeout_fails(self):
        state = ForgetBrainState(visible_after=0, absent_after=10_000)
        clock = _FakeClock()
        _, context = forget_context(state, clock)
        result = forget.ForgetLatencyProbe(poll_interval_s=60, absent_timeout_s=120).run(context)
        self.assertEqual(result.status, "failed")
        self.assertIsNone(result.metrics["forgotten_after_s"])
        self.assertIn("still searchable", result.notes[0])
        self.assertNotIn(ForgetBrainState.RECEIPT, json.dumps(result.as_dict()))

    def test_receipt_that_still_resolves_fails(self):
        state = ForgetBrainState(visible_after=0, absent_after=0, resolves_after_forget=True)
        _, context = forget_context(state, _FakeClock())
        result = forget.ForgetLatencyProbe().run(context)
        self.assertEqual(result.status, "failed")
        self.assertFalse(result.metrics["receipt_unresolvable"])
        self.assertFalse(next(g for g in result.gates if g.metric == "receipt_unresolvable").passed)

    def test_capture_failure_writes_nothing_more(self):
        state = ForgetBrainState(capture_ok=False)
        brain, context = forget_context(state, _FakeClock())
        result = forget.ForgetLatencyProbe().run(context)
        self.assertEqual(result.status, "failed")
        self.assertFalse(result.metrics["capture_ok"])
        self.assertEqual([n for n, _ in brain.calls], ["recall_capture"])

    def test_forget_failure_is_reported(self):
        state = ForgetBrainState(visible_after=0, forget_ok=False)
        brain, context = forget_context(state, _FakeClock())
        result = forget.ForgetLatencyProbe().run(context)
        self.assertEqual(result.status, "failed")
        self.assertFalse(result.metrics["forget_ok"])
        self.assertIn("may remain", result.notes[0])
        self.assertNotIn("recall_show", [n for n, _ in brain.calls])

    def test_receipt_matching_and_result_receipts(self):
        captured = "recall://s/x?rev=1#item=3"
        self.assertTrue(forget.receipt_matches(captured, captured))
        self.assertTrue(forget.receipt_matches("recall://s/x?rev=1#item=0", captured))
        self.assertTrue(forget.receipt_matches("recall://s/x?rev=1", captured))
        self.assertFalse(forget.receipt_matches("recall://s/x?rev=10#item=0", captured))
        self.assertEqual(forget.result_receipts({"results": [{"receipt": "a", "matching_ranges": [{"receipts": ["b", 1]}, "junk"]}, 5]}), ["a", "b"])
        self.assertEqual(forget.result_receipts({}), [])

    def test_registered_in_runner_and_history(self):
        self.assertIn(forget.ForgetLatencyProbe, PROBES["integrity"])
        args = parser().parse_args(["run", "--output-dir", "/tmp/x"])
        self.assertFalse(args.forget_probe)
        self.assertTrue(parser().parse_args(["run", "--output-dir", "/tmp/x", "--forget-probe"]).forget_probe)
        results = [ProbeResult("integrity.forget_latency", "integrity", "ok", metrics={"forgotten_after_s": 12.0, "capture_visible_after_s": 3.0, "polls": 4})]
        with tempfile.TemporaryDirectory() as directory:
            card = build_card(results, base_url="https://brain.invalid/mcp", started_at=0.0, repo_root=Path(directory), options={})
        row = history_row(card)
        self.assertEqual(row["integrity.forget_latency.forgotten_after_s"], 12.0)
        self.assertEqual(row["integrity.forget_latency.capture_visible_after_s"], 3.0)
        self.assertNotIn("integrity.forget_latency.polls", row)


class CardTest(unittest.TestCase):
    def _results(self) -> list[ProbeResult]:
        return [
            ProbeResult("availability.endpoints", "availability", "ok", metrics={"mcp_ping_p95_ms": 200.0, "readyz_success_rate": 1.0}, gates=[Gate("mcp_ping_p95_ms", "<=", 2000).evaluate(200.0)]),
            ProbeResult("latency.tools", "latency", "degraded", metrics={"recall_search.p95_ms": 9000.0, "error_rate": 0.0}, gates=[Gate("recall_search.p95_ms", "<=", 5000).evaluate(9000.0)]),
            ProbeResult("cost.planetscale", "cost", "skipped", notes=["not configured"]),
        ]

    def test_build_card_rolls_up_gates_and_status(self):
        with tempfile.TemporaryDirectory() as directory:
            card = build_card(self._results(), base_url="https://brain.invalid/mcp", started_at=0.0, repo_root=Path(directory), options={"since": None, "until": None})
        self.assertEqual(card["schema_version"], "recall.systems-card.v1")
        self.assertEqual(card["overall"]["status"], "degraded")
        self.assertEqual(card["overall"]["gates_total"], 2)
        self.assertEqual(card["overall"]["gates_failed"], 1)
        self.assertEqual(card["dimensions"]["cost"]["status"], "skipped")
        self.assertEqual(card["dimensions"]["accuracy"]["status"], "skipped")
        row = history_row(card)
        self.assertEqual(row["latency.tools.recall_search.p95_ms"], 9000.0)
        self.assertEqual(row["overall"], "degraded")

    def test_render_html_is_self_contained_and_escapes(self):
        with tempfile.TemporaryDirectory() as directory:
            card = build_card(self._results(), base_url="https://brain.invalid/mcp", started_at=0.0, repo_root=Path(directory), options={})
        card["target"]["mcp_url"] = "https://brain.invalid/mcp?<script>"
        page = render_html(card, [history_row(card), history_row(card)])
        self.assertIn("<title>Recall Systems Card</title>", page)
        self.assertIn("&lt;script&gt;", page)
        self.assertNotIn("<script>", page)
        self.assertIn("svg", page)
        self.assertNotIn("http://", page.replace("https://", ""))

    def test_cli_parser_defaults(self):
        args = parser().parse_args(["run", "--output-dir", "/tmp/x"])
        self.assertEqual(args.truth_split, "validation")
        self.assertEqual(args.repetitions, 3)
        self.assertIsNone(args.dimensions)


if __name__ == "__main__":
    unittest.main()
