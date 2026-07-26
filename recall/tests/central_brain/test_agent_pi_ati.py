from __future__ import annotations

import hashlib
import json
import os
import stat
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

SERVER = Path(__file__).resolve().parents[2] / "server"
sys.path.insert(0, str(SERVER))

from recall_server.agent import (  # noqa: E402
    AgentExecutionError,
    RecallAgentService,
    ScriptedAgentRunner,
    service_from_env,
)
from recall_server.agent_pi_ati import (  # noqa: E402
    CEREBRAS_API_BASE_URL,
    MODEL_PROXY_PLACEHOLDER_KEY,
    PiAtiRunner,
    ProviderKey,
    SubprocessBrainTurnTransport,
    _load_provider_key,
    _model_tool_error_message,
)


TENANT = "tenant:synthetic:company"
PRINCIPAL = "principal:synthetic:member"
SOURCE = "source:synthetic:company"
OTHER_SOURCE = "source:synthetic:personal"
HINT = f"recall://{SOURCE}/hint?rev=1#item=0"
DECISION = f"recall://{SOURCE}/decision?rev=1#item=0"
IMPLEMENTATION = f"recall://{SOURCE}/implementation?rev=1#item=0"
REQUEST = {
    "contract": "recall.agent-request.v1",
    "schema_version": 1,
    "request_id": "req_0123456789abcdef",
    "idempotency_key": "synthetic-pi-ati-1",
    "question": "What changed in Project Aurora during July 23?",
    "depth": "deep",
    "since": "2026-07-23T00:00:00Z",
    "until": "2026-07-24T00:00:00Z",
}


def principal() -> dict:
    return {
        "credential_kind": "mcp",
        "tenant_id": TENANT,
        "principal_id": PRINCIPAL,
        "role": "member",
        "audience": "recall-mcp",
        "authorized_sources": [SOURCE],
    }


class SyntheticRetrieval:
    """Multi-document corpus with an ingest-time decoy."""

    def __init__(self, *, fail_deep: bool = False):
        self.calls: list[str] = []
        self.filters: list[dict] = []
        self.map_batches: list[list[str]] = []
        self.fail_deep = fail_deep

    def investigate(self, question, *, filters, depth):
        self.calls.append("recall_investigate")
        self.filters.append(dict(filters))
        return {
            "question_interpretation": {"time_basis": "occurred_at"},
            "routing_hints": [
                {"receipt": DECISION},
                {"receipt": IMPLEMENTATION},
            ],
            "investigations": [{
                "match": {
                    "receipt": HINT,
                    "occurred_at": "2026-07-10T12:00:00Z",
                    "ingested_at": "2026-07-23T18:00:00Z",
                    "text": "Old decoy ingested inside the requested window.",
                },
                "context": {"events": []},
            }],
            "coverage": {"sessions": 3, "sources": [SOURCE]},
        }

    def deep_search(self, question, *, filters, depth):
        self.calls.append("recall_deep_search")
        self.filters.append(dict(filters))
        if self.fail_deep:
            raise RuntimeError("synthetic provider body must not escape")
        return {
            "findings": [
                {
                    "receipt": DECISION,
                    "occurred_at": "2026-07-23T09:00:00Z",
                    "text": "The team selected the bounded agent bridge.",
                },
                {
                    "receipt": IMPLEMENTATION,
                    "occurred_at": "2026-07-23T15:00:00Z",
                    "text": "The bridge passed its receipt-grounding check.",
                },
            ],
            "coverage": {
                "sessions": 2,
                "sources": [SOURCE],
                "provider": "synthetic-archil",
                "complete": True,
            },
        }

    def map_reduce_search(self, question, *, maps, depth):
        self.calls.append("recall_map_reduce")
        self.map_batches.append([item["map_id"] for item in maps])
        self.filters.extend(dict(item["filters"]) for item in maps)
        rendered_maps = []
        for index, item in enumerate(maps):
            implementation = (
                "implementation" in item["map_id"]
                or "verification" in item["map_id"]
                or index > 0
            )
            rendered_maps.append({
                "map_id": item["map_id"],
                "objective": item["objective"],
                "query": item["query"],
                "filters": item["filters"],
                "status": "complete",
                "findings": [{
                    "receipt": IMPLEMENTATION if implementation else DECISION,
                    "occurred_at": (
                        "2026-07-23T15:00:00Z"
                        if implementation
                        else "2026-07-23T09:00:00Z"
                    ),
                    "text": (
                        "The bridge passed its receipt-grounding check."
                        if implementation
                        else "The team selected the bounded agent bridge."
                    ),
                }],
                "coverage": {"complete": True},
                "uncertainty": [],
            })
        return {
            "contract": "recall.agentic-map-reduce.v1",
            "question": question,
            "maps": rendered_maps,
            "coverage": {
                "maps": len(maps),
                "complete_maps": len(maps),
                "complete": True,
                "unique_receipts": len({
                    finding["receipt"]
                    for item in rendered_maps
                    for finding in item["findings"]
                }),
            },
            "diagnostics": {
                "engine": "synthetic-agentic-map-reduce",
                "parallelism": len(maps),
                "reducer": "agent",
            },
        }

    def show(self, target):
        self.calls.append("recall_show")
        return {
            "resolved_receipt": target,
            "occurred_at": "2026-07-23T15:00:00Z",
            "text": "Exact evidence opened.",
        }

    def session_context(self, target, *, before, after):
        self.calls.append("recall_session_context")
        return {"anchor_receipt": target, "events": []}


class ScriptedTransport:
    def __init__(self, script):
        self.script = script
        self.start = None

    def run(self, start, invoke, *, timeout_seconds):
        self.start = start
        for name, arguments in self.script:
            invoke(name, arguments)
        return {
            "terminal": {
                "status": "complete",
                "model_attestation": {
                    "model_alias": "gemma-4-31b",
                    "route_kind": "private_broker",
                    "provider": "broker",
                    "route_identity": "10.23.45.67",
                },
            },
            "usage": {},
        }


class WaveRetrieval(SyntheticRetrieval):
    def map_reduce_search(self, question, *, maps, depth):
        result = super().map_reduce_search(
            question,
            maps=maps,
            depth=depth,
        )
        if len(self.map_batches) == 1:
            result["maps"] = result["maps"][:1]
            result["maps"][0]["coverage"]["complete"] = False
            result["maps"][0]["uncertainty"] = ["Implementation proof is missing."]
            result["coverage"] = {
                "maps": 2,
                "complete_maps": 1,
                "complete": False,
                "unique_receipts": 1,
            }
        return result


def success_script():
    filters = {
        "since": REQUEST["since"],
        "until": REQUEST["until"],
    }
    return [
        (
            "recall_investigate",
            {
                "question": REQUEST["question"],
                "filters": filters,
                "depth": "deep",
            },
        ),
        (
            "recall_deep_search",
            {
                "question": REQUEST["question"],
                "filters": filters,
                "depth": "deep",
            },
        ),
        ("recall_show", {"target": IMPLEMENTATION}),
        (
            "evidence_finish",
            {
                "status": "complete",
                "answer": (
                    "On July 23, Aurora selected the bounded agent bridge and "
                    "then passed its receipt-grounding check."
                ),
                "claims": [
                    {
                        "statement": "The bounded bridge was selected on July 23.",
                        "receipts": [DECISION],
                    },
                    {
                        "statement": "Its grounding check passed later that day.",
                        "receipts": [IMPLEMENTATION],
                    },
                ],
                "citations": [DECISION, IMPLEMENTATION],
                "gaps": [],
            },
        ),
    ]


def map_reduce_script():
    filters = {
        "since": REQUEST["since"],
        "until": REQUEST["until"],
    }
    return [
        (
            "recall_investigate",
            {
                "question": REQUEST["question"],
                "filters": filters,
                "depth": "deep",
            },
        ),
        (
            "recall_map_reduce",
            {
                "question": REQUEST["question"],
                "maps": [
                    {
                        "map_id": "decision",
                        "objective": "Find the decision and its rationale.",
                        "query": "Project Aurora bounded bridge decision",
                        "filters": filters,
                        "seed_receipts": [DECISION],
                    },
                    {
                        "map_id": "verification",
                        "objective": "Find implementation and verification.",
                        "query": "Project Aurora bridge grounding check",
                        "filters": filters,
                        "seed_receipts": [IMPLEMENTATION],
                    },
                ],
                "depth": "deep",
            },
        ),
        (
            "evidence_finish",
            {
                "status": "complete",
                "answer": (
                    "On July 23, Aurora selected the bounded agent bridge and "
                    "then passed its receipt-grounding check."
                ),
                "claims": [
                    {
                        "statement": "The bounded bridge was selected on July 23.",
                        "receipts": [DECISION],
                    },
                    {
                        "statement": "Its grounding check passed later that day.",
                        "receipts": [IMPLEMENTATION],
                    },
                ],
                "citations": [DECISION, IMPLEMENTATION],
                "gaps": [],
            },
        ),
    ]


def service(transport) -> RecallAgentService:
    fixed = datetime(2026, 7, 25, 10, 0, tzinfo=timezone.utc)
    ticks = iter([10.0, 10.25])
    return RecallAgentService(
        PiAtiRunner(transport),
        clock=lambda: fixed,
        monotonic=lambda: next(ticks),
    )


class PiAtiGroundingTest(unittest.TestCase):
    def test_model_tool_errors_are_actionable_but_content_free(self):
        budget = AgentExecutionError(
            "private failure details",
            code="agent_tool_budget_exhausted",
        )
        finish = AgentExecutionError(
            "private failure details",
            code="agent_finish_invalid",
        )
        self.assertIn("do not retry", _model_tool_error_message(budget))
        self.assertIn("exactly", _model_tool_error_message(finish))
        self.assertNotIn(
            "private failure details",
            _model_tool_error_message(budget),
        )

    def test_agent_uses_hint_deep_inspection_exact_open_and_grounded_finish(self):
        transport = ScriptedTransport(success_script())
        retrieval = SyntheticRetrieval()
        bundle = service(transport).use_recall(
            principal(),
            REQUEST,
            retrieval,
        )
        self.assertEqual(
            retrieval.calls,
            ["recall_investigate", "recall_deep_search", "recall_show"],
        )
        self.assertEqual(bundle["result"]["status"], "complete")
        self.assertEqual(
            bundle["result"]["citations"],
            [DECISION, IMPLEMENTATION],
        )
        self.assertIn("July 23", bundle["result"]["answer"])
        stages = [event["stage"] for event in bundle["trace"]]
        self.assertEqual(stages[:2], ["authorize", "plan"])
        self.assertIn("retrieve", stages)
        self.assertIn("inspect", stages)
        self.assertEqual(stages[-3:], ["synthesize", "verify", "complete"])
        self.assertEqual(bundle["trace"][-1]["elapsed_ms"], 250.0)
        self.assertEqual(bundle["trace"][0]["elapsed_ms"], 0.0)
        self.assertEqual(bundle["trace"][1]["elapsed_ms"], 0.0)
        self.assertEqual(bundle["trace"][-2]["elapsed_ms"], 0.0)
        rendered_trace = json.dumps(bundle["trace"])
        self.assertNotIn(REQUEST["question"], rendered_trace)
        self.assertNotIn(bundle["result"]["answer"], rendered_trace)
        self.assertNotIn("Exact evidence opened", rendered_trace)
        self.assertEqual(
            transport.start["data"]["model"],
            {
                "alias": "gemma-4-31b",
                "thinking": "low",
                "tool_choice": "required",
            },
        )
        self.assertEqual(
            {tool["name"] for tool in transport.start["data"]["tools"]},
            {
                "recall_investigate",
                "recall_deep_search",
                "recall_map_reduce",
                "recall_session_context",
                "recall_show",
                "evidence_finish",
            },
        )
        finish = next(
            tool
            for tool in transport.start["data"]["tools"]
            if tool["name"] == "evidence_finish"
        )
        self.assertIs(finish["terminate_turn"], True)

    def test_every_model_tool_schema_is_strict_compatible(self):
        transport = ScriptedTransport(success_script())
        service(transport).use_recall(
            principal(),
            REQUEST,
            SyntheticRetrieval(),
        )

        def assert_strict_schema(schema):
            if not isinstance(schema, dict):
                return
            if schema.get("type") == "object":
                properties = schema.get("properties", {})
                self.assertIs(schema.get("additionalProperties"), False)
                self.assertEqual(
                    set(schema.get("required", [])),
                    set(properties),
                )
                for child in properties.values():
                    assert_strict_schema(child)
            if schema.get("type") == "array":
                assert_strict_schema(schema.get("items"))

        for tool in transport.start["data"]["tools"]:
            assert_strict_schema(tool["input_schema"])

    def test_agentic_map_reduce_decomposes_then_reduces_grounded_evidence(self):
        transport = ScriptedTransport(map_reduce_script())
        retrieval = SyntheticRetrieval()
        bundle = service(transport).use_recall(
            principal(),
            REQUEST,
            retrieval,
        )
        self.assertEqual(
            retrieval.calls,
            ["recall_investigate", "recall_map_reduce"],
        )
        self.assertEqual(
            retrieval.filters,
            [
                {"since": REQUEST["since"], "until": REQUEST["until"]},
                {"since": REQUEST["since"], "until": REQUEST["until"]},
                {"since": REQUEST["since"], "until": REQUEST["until"]},
            ],
        )
        self.assertEqual(bundle["result"]["status"], "complete")
        self.assertEqual(
            bundle["result"]["citations"],
            [DECISION, IMPLEMENTATION],
        )
        map_event = next(
            event
            for event in bundle["trace"]
            if event["tool"] == "recall.map_reduce"
        )
        self.assertEqual(map_event["stage"], "inspect")
        self.assertEqual(map_event["receipt_count"], 2)

    def test_map_reduce_cannot_widen_explicit_time_or_source_scope(self):
        script = map_reduce_script()
        call = next(
            arguments
            for name, arguments in script
            if name == "recall_map_reduce"
        )
        call["maps"][0]["filters"]["since"] = "2026-01-01T00:00:00Z"
        with self.assertRaises(AgentExecutionError) as caught:
            service(ScriptedTransport(script)).use_recall(
                principal(),
                REQUEST,
                SyntheticRetrieval(),
            )
        self.assertEqual(caught.exception.code, "agent_query_scope_violation")

        scoped_request = {
            **REQUEST,
            "source_families": ["codex"],
            "idempotency_key": "synthetic-pi-ati-source-scope",
        }
        source_script = map_reduce_script()
        next(
            arguments
            for name, arguments in source_script
            if name == "recall_map_reduce"
        )["maps"][0]["filters"]["source_family"] = "slack"
        with self.assertRaises(AgentExecutionError) as caught:
            service(ScriptedTransport(source_script)).use_recall(
                principal(),
                scoped_request,
                SyntheticRetrieval(),
            )
        self.assertEqual(caught.exception.code, "agent_query_scope_violation")

    def test_map_reduce_seed_must_come_from_a_prior_hint_call(self):
        script = [
            item
            for item in map_reduce_script()
            if item[0] != "recall_investigate"
        ]
        with self.assertRaises(AgentExecutionError) as caught:
            service(ScriptedTransport(script)).use_recall(
                principal(),
                REQUEST,
                SyntheticRetrieval(),
            )
        self.assertEqual(caught.exception.code, "agent_map_seed_not_opened")

    def test_incomplete_map_supports_one_targeted_second_wave(self):
        script = map_reduce_script()
        filters = {
            "since": REQUEST["since"],
            "until": REQUEST["until"],
        }
        script.insert(
            2,
            (
                "recall_map_reduce",
                {
                    "question": REQUEST["question"],
                    "maps": [{
                        "map_id": "implementation_retry",
                        "objective": "Close the missing implementation proof gap.",
                        "query": "Project Aurora exact grounding verification",
                        "filters": filters,
                        "seed_receipts": [IMPLEMENTATION],
                    }],
                    "depth": "deep",
                },
            ),
        )
        retrieval = WaveRetrieval()
        bundle = service(ScriptedTransport(script)).use_recall(
            principal(),
            REQUEST,
            retrieval,
        )
        self.assertEqual(
            retrieval.map_batches,
            [["decision", "verification"], ["implementation_retry"]],
        )
        self.assertEqual(bundle["result"]["status"], "complete")
        self.assertEqual(
            [
                event["tool"]
                for event in bundle["trace"]
                if event["tool"] == "recall.map_reduce"
            ],
            ["recall.map_reduce", "recall.map_reduce"],
        )

    def test_semantic_runner_beats_scripted_generic_baseline(self):
        pi = service(ScriptedTransport(success_script())).use_recall(
            principal(), REQUEST, SyntheticRetrieval()
        )
        baseline = RecallAgentService(
            ScriptedAgentRunner(),
            clock=lambda: datetime(
                2026, 7, 25, 10, 0, tzinfo=timezone.utc
            ),
            monotonic=lambda: 10.0,
        ).use_recall(principal(), REQUEST, SyntheticRetrieval())
        def rubric(bundle):
            return sum([
                bundle["result"]["status"] == "complete",
                "bounded agent bridge" in bundle["result"]["answer"],
                "July 23" in bundle["result"]["answer"],
                len(bundle["result"]["claims"]) >= 2,
            ])
        self.assertEqual(rubric(pi), 4)
        self.assertEqual(rubric(baseline), 0)

    def test_hint_only_receipt_cannot_be_cited_as_proof(self):
        script = success_script()
        del script[1:3]
        script[-1] = (
            "evidence_finish",
            {
                "status": "complete",
                "answer": "The old hint proves the change.",
                "claims": [{"statement": "Hint claim", "receipts": [HINT]}],
                "citations": [HINT],
                "gaps": [],
            },
        )
        with self.assertRaisesRegex(
            AgentExecutionError, "did not open"
        ) as caught:
            service(ScriptedTransport(script)).use_recall(
                principal(), REQUEST, SyntheticRetrieval()
            )
        self.assertEqual(caught.exception.code, "agent_citation_not_opened")

    def test_cross_brain_citation_fails_closed(self):
        other = f"recall://{OTHER_SOURCE}/item?rev=1#item=0"
        script = success_script()
        script[-1] = (
            "evidence_finish",
            {
                "status": "complete",
                "answer": "Invented.",
                "claims": [{"statement": "Invented", "receipts": [other]}],
                "citations": [other],
                "gaps": [],
            },
        )
        with self.assertRaises(AgentExecutionError) as caught:
            service(ScriptedTransport(script)).use_recall(
                principal(), REQUEST, SyntheticRetrieval()
            )
        self.assertEqual(caught.exception.code, "agent_citation_not_opened")

    def test_missing_finish_and_post_finish_tool_call_fail_closed(self):
        with self.assertRaises(AgentExecutionError) as missing:
            service(ScriptedTransport(success_script()[:-1])).use_recall(
                principal(), REQUEST, SyntheticRetrieval()
            )
        self.assertEqual(missing.exception.code, "agent_finish_missing")
        script = success_script() + [
            ("recall_show", {"target": IMPLEMENTATION}),
        ]
        with self.assertRaises(AgentExecutionError) as post:
            service(ScriptedTransport(script)).use_recall(
                principal(), REQUEST, SyntheticRetrieval()
            )
        self.assertEqual(post.exception.code, "agent_post_finish_tool_call")

    def test_finish_status_and_claim_receipt_set_must_be_truthful(self):
        cases = {
            "complete_with_gap": lambda finish: finish["gaps"].append(
                "Material evidence is missing."
            ),
            "unclaimed_citation": lambda finish: finish["claims"].pop(),
        }
        for label, mutate in cases.items():
            with self.subTest(label=label):
                script = success_script()
                mutate(script[-1][1])
                with self.assertRaises(AgentExecutionError) as caught:
                    service(ScriptedTransport(script)).use_recall(
                        principal(), REQUEST, SyntheticRetrieval()
                    )
                self.assertIn(
                    caught.exception.code,
                    {"agent_finish_invalid", "agent_claim_not_grounded"},
                )

    def test_archil_failure_cannot_turn_into_an_unsupported_answer(self):
        script = success_script()
        with self.assertRaises(AgentExecutionError) as caught:
            service(ScriptedTransport(script)).use_recall(
                principal(), REQUEST, SyntheticRetrieval(fail_deep=True)
            )
        self.assertEqual(caught.exception.code, "agent_evidence_tool_failed")
        self.assertNotIn("synthetic provider body", str(caught.exception))

    def test_transport_failure_codes_are_preserved(self):
        class Failure:
            def run(self, *_args, **_kwargs):
                raise AgentExecutionError(
                    "provider unavailable",
                    code="agent_model_failed",
                )

        with self.assertRaises(AgentExecutionError) as caught:
            service(Failure()).use_recall(
                principal(), REQUEST, SyntheticRetrieval()
            )
        self.assertEqual(caught.exception.code, "agent_model_failed")

    def test_source_family_and_explicit_time_scope_are_host_enforced(self):
        family_request = {
            **REQUEST,
            "source_families": ["coding"],
        }
        script = success_script()
        for _name, arguments in script[:2]:
            arguments["filters"]["source_family"] = "coding"
        retrieval = SyntheticRetrieval()
        service(ScriptedTransport(script)).use_recall(
            principal(),
            family_request,
            retrieval,
        )
        self.assertEqual(
            retrieval.filters,
            [
                {
                    "since": REQUEST["since"],
                    "until": REQUEST["until"],
                    "source_family": "coding",
                },
                {
                    "since": REQUEST["since"],
                    "until": REQUEST["until"],
                    "source_family": "coding",
                },
            ],
        )
        for label, mutate in (
            (
                "family",
                lambda arguments: arguments["filters"].update({
                    "source_family": "gmail"
                }),
            ),
            (
                "time",
                lambda arguments: arguments["filters"].update({
                    "since": "2020-01-01T00:00:00Z"
                }),
            ),
        ):
            with self.subTest(label=label):
                escaped = success_script()
                for _name, arguments in escaped[:2]:
                    arguments["filters"]["source_family"] = "coding"
                mutate(escaped[0][1])
                with self.assertRaises(AgentExecutionError) as caught:
                    service(ScriptedTransport(escaped)).use_recall(
                        principal(),
                        family_request,
                        SyntheticRetrieval(),
                    )
                self.assertEqual(
                    caught.exception.code,
                    "agent_query_scope_violation",
                )


class PiAtiSubprocessBoundaryTest(unittest.TestCase):
    def test_render_managed_secret_symlink_is_narrowly_accepted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "managed-secrets"
            root.mkdir(mode=0o700)
            target = Path(directory) / "runtime-secret"
            target.write_text("synthetic-provider-key-value\n")
            target.chmod(0o640)
            link = root / "cerebras-api-key"
            link.symlink_to(target)

            with patch(
                "recall_server.agent_pi_ati.os.getuid",
                return_value=os.getuid() + 10_000,
            ):
                key = _load_provider_key(
                    str(link),
                    _managed_secret_root=root,
                    _managed_secret_group=os.getgid(),
                )
            self.assertNotIn("synthetic-provider-key-value", repr(key))

            with self.assertRaisesRegex(RuntimeError, "not private"):
                _load_provider_key(
                    str(link),
                    _managed_secret_root=root,
                    _managed_secret_group=os.getgid(),
                )

            root.chmod(0o777)
            with patch(
                "recall_server.agent_pi_ati.os.getuid",
                return_value=os.getuid() + 10_000,
            ):
                key = _load_provider_key(
                    str(link),
                    _managed_secret_root=root,
                    _managed_secret_group=os.getgid(),
                )
            self.assertNotIn("synthetic-provider-key-value", repr(key))

    def test_unmanaged_provider_key_symlink_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            managed_root = Path(directory) / "managed-secrets"
            managed_root.mkdir(mode=0o700)
            target = Path(directory) / "runtime-secret"
            target.write_text("synthetic-provider-key-value\n")
            target.chmod(0o600)
            link = Path(directory) / "cerebras-api-key"
            link.symlink_to(target)
            with self.assertRaisesRegex(RuntimeError, "not private"):
                _load_provider_key(
                    str(link),
                    _managed_secret_root=managed_root,
                )

    def test_private_broker_passes_only_non_secret_placeholder(self):
        child = r"""
import json,os,sys
assert os.environ["LITELLM_API_KEY"]=="not-a-secret"
assert os.environ["LITELLM_BASE_URL"]=="http://10.23.45.67:9420"
assert os.environ["ATI_MODEL_ROUTE_KIND"]=="private_broker"
assert os.environ["ATI_MODEL_PROVIDER"]=="broker"
start=json.loads(sys.stdin.readline())
print(json.dumps({"v":"ati.brain.turn.v1","turn_id":start["turn_id"],"seq":0,"type":"terminal.complete","at":"2026-07-25T10:00:00Z","data":{"status":"complete","unresolved_call_ids":[],"model_attestation":{"model_alias":"gemma-4-31b","thinking":"low","route_kind":"private_broker","provider":"broker","route_identity":"10.23.45.67"}}}),flush=True)
"""
        transport = SubprocessBrainTurnTransport(
            (sys.executable, "-c", child),
            model_base_url="http://10.23.45.67:9420",
            route_kind="private_broker",
            provider="broker",
            expected_route_identity="10.23.45.67",
            environment={"PATH": os.environ["PATH"], "FORBIDDEN_SECRET": "no"},
        )
        outcome = transport.run(
            {
                "turn_id": "turn_broker",
                "data": {"model": {"alias": "gemma-4-31b"}},
            },
            lambda *_args: {},
            timeout_seconds=3,
        )
        self.assertEqual(
            outcome["terminal"]["model_attestation"]["route_identity"],
            "10.23.45.67",
        )
        self.assertEqual(transport.route_kind, "private_broker")
        self.assertNotIn("FORBIDDEN_SECRET", transport.child_environment)
        self.assertEqual(MODEL_PROXY_PLACEHOLDER_KEY, "not-a-secret")

    def test_private_broker_rejects_public_url_and_key_sources(self):
        with self.assertRaisesRegex(RuntimeError, "must be private"):
            SubprocessBrainTurnTransport(
                (sys.executable, "-c", "pass"),
                model_base_url="https://litellm.example",
                route_kind="private_broker",
                provider="broker",
                expected_route_identity="litellm.example",
            )
        with self.assertRaisesRegex(RuntimeError, "credential mode"):
            SubprocessBrainTurnTransport(
                (sys.executable, "-c", "pass"),
                model_base_url="http://10.23.45.67:9420",
                route_kind="private_broker",
                provider="broker",
                provider_key=ProviderKey(
                    value="synthetic-provider-key-value",
                ),
                expected_route_identity="10.23.45.67",
            )

    def test_direct_cerebras_transport_and_attestation(self):
        child = r"""
import json,os,sys
assert os.environ["LITELLM_API_KEY"]=="synthetic-provider-key-value"
assert os.environ["LITELLM_BASE_URL"]=="https://api.cerebras.ai/v1"
assert os.environ["ATI_MODEL_ROUTE_KIND"]=="direct_provider"
assert os.environ["ATI_MODEL_PROVIDER"]=="cerebras"
start=json.loads(sys.stdin.readline())
turn=start["turn_id"]
def send(seq,kind,data):
 print(json.dumps({"v":"ati.brain.turn.v1","turn_id":turn,"seq":seq,"type":kind,"at":"2026-07-25T10:00:00Z","data":data}),flush=True)
send(0,"tool.invoke",{"call_id":"call-1","name":"recall_show","arguments":{"target":"recall://source:synthetic:company/item?rev=1#item=0"},"parent_event_id":"event-1","effect":"read","approval":"never","timeout_hint_ms":1000,"idempotency":"none","readback":"result"})
result=json.loads(sys.stdin.readline())
assert result["data"]["status"]=="ok"
send(1,"terminal.complete",{"status":"complete","unresolved_call_ids":[],"model_attestation":{"model_alias":"gemma-4-31b","thinking":"low","route_kind":"direct_provider","provider":"cerebras","route_identity":"api.cerebras.ai"}})
"""
        key = ProviderKey(
            value="synthetic-provider-key-value",
        )
        transport = SubprocessBrainTurnTransport(
            (sys.executable, "-c", child),
            model_base_url=CEREBRAS_API_BASE_URL,
            route_kind="direct_provider",
            provider="cerebras",
            provider_key=key,
            expected_route_identity="api.cerebras.ai",
            environment={"PATH": os.environ["PATH"], "FORBIDDEN_SECRET": "no"},
        )
        seen = []
        outcome = transport.run(
            {
                "turn_id": "turn_synthetic",
                "data": {
                    "session_id": "session",
                    "run_id": "run",
                    "model": {"alias": "gemma-4-31b"},
                },
            },
            lambda name, arguments: seen.append((name, arguments)) or {
                "resolved_receipt": arguments["target"]
            },
            timeout_seconds=3,
        )
        self.assertEqual(seen[0][0], "recall_show")
        self.assertEqual(
            outcome["terminal"]["model_attestation"]["route_kind"],
            "direct_provider",
        )
        self.assertNotIn("FORBIDDEN_SECRET", transport.child_environment)
        self.assertNotIn("LITELLM_API_KEY", transport.child_environment)
        self.assertNotIn("synthetic-provider-key-value", repr(key))

    def test_direct_cerebras_accepts_silent_success_terminal(self):
        child = r"""
import json,sys
start=json.loads(sys.stdin.readline())
print(json.dumps({"v":"ati.brain.turn.v1","turn_id":start["turn_id"],"seq":0,"type":"terminal.complete","at":"2026-07-25T10:00:00Z","data":{"status":"silent","unresolved_call_ids":[],"model_attestation":{"model_alias":"gemma-4-31b","thinking":"low","route_kind":"direct_provider","provider":"cerebras","route_identity":"api.cerebras.ai"}}}),flush=True)
"""
        transport = SubprocessBrainTurnTransport(
            (sys.executable, "-c", child),
            model_base_url=CEREBRAS_API_BASE_URL,
            route_kind="direct_provider",
            provider="cerebras",
            provider_key=ProviderKey(
                value="synthetic-provider-key-value",
            ),
            expected_route_identity="api.cerebras.ai",
            environment={"PATH": os.environ["PATH"]},
        )
        outcome = transport.run(
            {
                "turn_id": "turn_silent",
                "data": {"model": {"alias": "gemma-4-31b"}},
            },
            lambda *_args: {},
            timeout_seconds=3,
        )
        self.assertEqual(outcome["terminal"]["status"], "silent")

    def test_transport_rejects_malformed_unattested_and_timed_out_children(self):
        key = ProviderKey(
            value="synthetic-provider-key-value",
        )
        cases = {
            "malformed": (
                "print('not-json',flush=True)",
                "agent_transport_frame_invalid",
            ),
            "unattested": (
                """
import json,sys
s=json.loads(sys.stdin.readline())
print(json.dumps({"v":"ati.brain.turn.v1","turn_id":s["turn_id"],"seq":0,"type":"terminal.complete","at":"2026-07-25T10:00:00Z","data":{"status":"complete","unresolved_call_ids":[],"model_attestation":{"model_alias":"gemma-4-31b","route_kind":"direct_provider","provider":"cerebras","route_identity":"wrong"}}}),flush=True)
""",
                "agent_model_attestation_invalid",
            ),
            "invalid-success-status": (
                """
import json,sys
s=json.loads(sys.stdin.readline())
print(json.dumps({"v":"ati.brain.turn.v1","turn_id":s["turn_id"],"seq":0,"type":"terminal.complete","at":"2026-07-25T10:00:00Z","data":{"status":"waiting","unresolved_call_ids":[],"model_attestation":{"model_alias":"gemma-4-31b","route_kind":"direct_provider","provider":"cerebras","route_identity":"api.cerebras.ai"}}}),flush=True)
""",
                "agent_terminal_status_invalid",
            ),
            "unresolved-tools": (
                """
import json,sys
s=json.loads(sys.stdin.readline())
print(json.dumps({"v":"ati.brain.turn.v1","turn_id":s["turn_id"],"seq":0,"type":"terminal.complete","at":"2026-07-25T10:00:00Z","data":{"status":"complete","unresolved_call_ids":["call-pending"],"model_attestation":{"model_alias":"gemma-4-31b","route_kind":"direct_provider","provider":"cerebras","route_identity":"api.cerebras.ai"}}}),flush=True)
""",
                "agent_unresolved_tool_calls",
            ),
            "timeout": (
                "import time; time.sleep(2)",
                "agent_model_timeout",
            ),
            "partial-frame-timeout": (
                (
                    "import sys,time; "
                    "sys.stdout.write('{\"v\":'); sys.stdout.flush(); "
                    "time.sleep(2)"
                ),
                "agent_model_timeout",
            ),
        }
        for label, (child, expected) in cases.items():
            with self.subTest(label=label):
                transport = SubprocessBrainTurnTransport(
                    (sys.executable, "-c", child),
                    model_base_url=CEREBRAS_API_BASE_URL,
                    route_kind="direct_provider",
                    provider="cerebras",
                    provider_key=key,
                    expected_route_identity="api.cerebras.ai",
                    environment={"PATH": os.environ["PATH"]},
                )
                with self.assertRaises(AgentExecutionError) as caught:
                    transport.run(
                        {
                            "turn_id": "turn_synthetic",
                            "data": {
                                "model": {"alias": "gemma-4-31b"},
                            },
                        },
                        lambda *_args: {},
                        timeout_seconds=0.1,
                    )
                self.assertEqual(caught.exception.code, expected)

    def test_transport_write_is_bounded_when_child_stops_reading(self):
        child = r"""
import json,sys,time
start=json.loads(sys.stdin.readline())
print(json.dumps({
  "v":"ati.brain.turn.v1",
  "turn_id":start["turn_id"],
  "seq":0,
  "type":"tool.invoke",
  "at":"2026-07-25T10:00:00Z",
  "data":{
    "call_id":"call-1",
    "name":"recall_show",
    "arguments":{"target":"recall://source:synthetic:company/item?rev=1#item=0"},
    "parent_event_id":"event-1",
    "effect":"read",
    "approval":"never",
    "timeout_hint_ms":1000,
    "idempotency":"none",
    "readback":"result"
  }
}),flush=True)
time.sleep(2)
"""
        transport = SubprocessBrainTurnTransport(
            (sys.executable, "-c", child),
            model_base_url=CEREBRAS_API_BASE_URL,
            route_kind="direct_provider",
            provider="cerebras",
            provider_key=ProviderKey(
                value="synthetic-provider-key-value",
            ),
            expected_route_identity="api.cerebras.ai",
            environment={"PATH": os.environ["PATH"]},
        )
        with self.assertRaises(AgentExecutionError) as caught:
            transport.run(
                {
                    "turn_id": "turn_synthetic",
                    "data": {"model": {"alias": "gemma-4-31b"}},
                },
                lambda *_args: {"text": "x" * 200_000},
                timeout_seconds=0.1,
            )
        self.assertEqual(caught.exception.code, "agent_model_timeout")

    def test_configuration_has_one_explicit_route_and_private_secret_file(self):
        with tempfile.TemporaryDirectory() as directory:
            key_path = Path(directory) / "cerebras.key"
            key_path.write_text("synthetic-provider-key-value\n")
            key_path.chmod(stat.S_IRUSR | stat.S_IWUSR)
            artifact_path = Path(directory) / "ati-runner.mjs"
            artifact_path.write_text("export {};\n")
            artifact_path.chmod(0o444)
            artifact_sha256 = hashlib.sha256(
                artifact_path.read_bytes()
            ).hexdigest()
            environment = {
                "RECALL_AGENT_RUNNER": "pi-ati",
                "RECALL_ATI_COMMAND_JSON": json.dumps([
                    sys.executable, str(artifact_path)
                ]),
                "RECALL_ATI_ARTIFACT_PATH": str(artifact_path),
                "RECALL_ATI_ARTIFACT_SHA256": artifact_sha256,
                "RECALL_AGENT_MODEL_ROUTE": "direct-provider:cerebras",
                "RECALL_AGENT_MODEL_KEY_FILE": str(key_path),
                "RECALL_AGENT_MODEL_ALIAS": "gpt-oss-120b",
            }
            self.assertIsInstance(
                service_from_env(environment).runner,
                PiAtiRunner,
            )
            self.assertNotIn(
                "synthetic-provider-key-value",
                repr(service_from_env(environment).runner.transport.provider_key),
            )
            environment["RECALL_AGENT_MODEL_ROUTE"] = "direct-provider:other"
            with self.assertRaisesRegex(RuntimeError, "route is invalid"):
                service_from_env(environment)
            environment["RECALL_AGENT_MODEL_ROUTE"] = "direct-provider:cerebras"
            environment["RECALL_ATI_ARTIFACT_SHA256"] = "0" * 64
            with self.assertRaisesRegex(RuntimeError, "does not match"):
                service_from_env(environment)
            environment["RECALL_ATI_ARTIFACT_SHA256"] = artifact_sha256
            key_path.write_text("contains whitespace")
            with self.assertRaisesRegex(RuntimeError, "invalid"):
                service_from_env(environment)
            key_path.write_text("synthetic-provider-key-value\n")
            key_path.chmod(0o644)
            with self.assertRaisesRegex(RuntimeError, "not private"):
                service_from_env(environment)

            key_path.chmod(0o600)
            broker_environment = {
                **environment,
                "RECALL_AGENT_MODEL_ROUTE": "private-broker",
                "RECALL_AGENT_MODEL_BASE_URL": "http://10.23.45.67:9420",
            }
            broker_environment.pop("RECALL_AGENT_MODEL_KEY_FILE")
            self.assertIsInstance(
                service_from_env(broker_environment).runner,
                PiAtiRunner,
            )
            broker_environment["RECALL_AGENT_MODEL_BASE_URL"] = (
                "https://litellm.example"
            )
            with self.assertRaisesRegex(RuntimeError, "must be private"):
                service_from_env(broker_environment)

    def test_hostile_direct_provider_configurations_fail_closed(self):
        common = {
            "RECALL_AGENT_RUNNER": "pi-ati",
            "RECALL_ATI_COMMAND_JSON": json.dumps([sys.executable, "-c", "pass"]),
            "RECALL_ATI_ARTIFACT_PATH": "/tmp/missing-artifact",
            "RECALL_ATI_ARTIFACT_SHA256": "0" * 64,
        }
        for environment in (
            {
                **common,
                "RECALL_AGENT_MODEL_ROUTE": "direct-provider:cerebras",
            },
            {
                **common,
                "RECALL_AGENT_MODEL_ROUTE": "private-broker",
                "RECALL_AGENT_MODEL_BASE_URL": "https://api.cerebras.ai/v1",
            },
            {
                **common,
                "RECALL_AGENT_MODEL_ROUTE": "private-broker",
                "RECALL_AGENT_MODEL_BASE_URL": "http://10.23.45.67:9420?next=x",
            },
        ):
            with self.subTest(environment=environment):
                with self.assertRaises(RuntimeError):
                    service_from_env(environment)


if __name__ == "__main__":
    unittest.main()
