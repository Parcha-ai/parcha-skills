from __future__ import annotations

import copy
import json
import unittest
from pathlib import Path

from contracts.agent_v1 import (
    AGENT_CONTRACTS,
    derive_run_id,
    validate_agent_contract,
    validate_agent_exchange,
)
from contracts.v2 import ContractError


RECALL = Path(__file__).resolve().parents[2]
CATALOG = RECALL / "contracts/recall_agent_v1.json"
PRINCIPAL = "principal:synthetic:member"
TENANT = "tenant:synthetic:company"
REQUEST_ID = "req_0123456789abcdef"
TRACE_ID = "trc_0123456789abcdef"
RECEIPT = "recall://source:synthetic:company/item-1?rev=1#item=0"


def fixtures() -> tuple[dict, dict, dict, list[dict], dict]:
    authority = {
        "contract": "recall.principal-authority.v1",
        "schema_version": 1,
        "tenant_id": TENANT,
        "principal_id": PRINCIPAL,
        "subject": "synthetic@example.test",
        "audience": "recall-agent",
        "scopes": ["recall:answer"],
        "source_ids": ["source:synthetic:company"],
        "expires_at": "2026-07-25T12:00:00Z",
    }
    request = {
        "contract": "recall.agent-request.v1",
        "schema_version": 1,
        "request_id": REQUEST_ID,
        "idempotency_key": "synthetic-retry-1",
        "question": "What changed in the synthetic project?",
        "depth": "normal",
    }
    run_id = derive_run_id(PRINCIPAL, request["idempotency_key"])
    run = {
        "contract": "recall.agent-run.v1",
        "schema_version": 1,
        "run_id": run_id,
        "request_id": REQUEST_ID,
        "tenant_id": TENANT,
        "principal_id": PRINCIPAL,
        "trace_id": TRACE_ID,
        "status": "complete",
        "attempt": 1,
        "created_at": "2026-07-25T10:00:00Z",
        "updated_at": "2026-07-25T10:00:02Z",
        "completed_at": "2026-07-25T10:00:02Z",
    }
    trace = [
        {
            "contract": "recall.agent-trace-event.v1",
            "schema_version": 1,
            "trace_id": TRACE_ID,
            "run_id": run_id,
            "sequence": index,
            "occurred_at": "2026-07-25T10:00:01Z",
            "stage": stage,
            "outcome": "ok",
            "elapsed_ms": index * 10,
            "receipt_count": 1 if index else 0,
            "source_count": 1 if index else 0,
            "session_count": 1 if index else 0,
            "tool": tool,
        }
        for index, (stage, tool) in enumerate((
            ("authorize", "recall.authorization"),
            ("retrieve", "recall.investigate"),
            ("verify", "recall.grounding"),
            ("complete", "recall.agent"),
        ))
    ]
    result = {
        "contract": "recall.agent-result.v1",
        "schema_version": 1,
        "run_id": run_id,
        "request_id": REQUEST_ID,
        "tenant_id": TENANT,
        "principal_id": PRINCIPAL,
        "trace_id": TRACE_ID,
        "status": "complete",
        "answer": "The synthetic project changed.",
        "claims": [{
            "statement": "The synthetic project changed.",
            "receipts": [RECEIPT],
        }],
        "citations": [RECEIPT],
        "gaps": [],
        "completed_at": "2026-07-25T10:00:02Z",
    }
    return authority, request, run, trace, result


class AgentContractTest(unittest.TestCase):
    def test_catalog_is_closed_and_matches_runtime(self) -> None:
        catalog = json.loads(CATALOG.read_text())
        names = {
            item["$ref"].rsplit("/", 1)[-1]
            for item in catalog["oneOf"]
        }
        self.assertEqual(names, {"request", "run", "trace_event", "result"})
        contracts = {
            catalog["$defs"][name]["properties"]["contract"]["const"]
            for name in names
        }
        self.assertEqual(contracts, AGENT_CONTRACTS)
        for name in names | {"claim"}:
            self.assertFalse(catalog["$defs"][name]["additionalProperties"])

    def test_receipt_backed_fake_success_validates(self) -> None:
        values = fixtures()
        validated = validate_agent_exchange(*values)
        self.assertEqual(validated[1]["request_id"], REQUEST_ID)
        self.assertEqual(validated[4]["citations"], [RECEIPT])

    def test_request_cannot_select_tenant_principal_or_brain(self) -> None:
        request = fixtures()[1]
        for field in ("tenant_id", "principal_id", "brain_id", "source_ids"):
            mutant = copy.deepcopy(request)
            mutant[field] = "tenant:personal"
            with self.subTest(field=field), self.assertRaisesRegex(
                ContractError, "incomplete or unknown"
            ):
                validate_agent_contract(mutant)

    def test_cross_brain_result_is_denied(self) -> None:
        authority, request, run, trace, result = fixtures()
        result["tenant_id"] = "tenant:synthetic:personal"
        with self.assertRaisesRegex(ContractError, "result authority mismatch"):
            validate_agent_exchange(authority, request, run, trace, result)

    def test_personal_receipt_leakage_is_denied(self) -> None:
        authority, request, run, trace, result = fixtures()
        personal = "recall://source:synthetic:personal/item-1?rev=1#item=0"
        result["citations"] = [personal]
        result["claims"][0]["receipts"] = [personal]
        with self.assertRaisesRegex(ContractError, "citation source scope mismatch"):
            validate_agent_exchange(authority, request, run, trace, result)

    def test_unsupported_answer_claim_is_denied(self) -> None:
        result = fixtures()[4]
        result["claims"][0]["receipts"] = []
        with self.assertRaisesRegex(ContractError, "no supporting receipts"):
            validate_agent_contract(result)

    def test_partial_answer_is_also_receipt_backed(self) -> None:
        result = fixtures()[4]
        result["status"] = "partial"
        result["claims"] = []
        result["citations"] = []
        with self.assertRaisesRegex(ContractError, "not receipt-backed"):
            validate_agent_contract(result)

    def test_claim_receipt_must_be_declared(self) -> None:
        result = fixtures()[4]
        result["citations"] = []
        with self.assertRaisesRegex(ContractError, "undeclared receipt"):
            validate_agent_contract(result)

    def test_trace_rejects_content_secrets_and_transcripts(self) -> None:
        event = fixtures()[3][0]
        for field in ("prompt", "query", "answer", "transcript", "token", "payload"):
            mutant = copy.deepcopy(event)
            mutant[field] = "synthetic forbidden content"
            with self.subTest(field=field), self.assertRaisesRegex(
                ContractError, "incomplete or unknown"
            ):
                validate_agent_contract(mutant)

    def test_trace_must_be_contiguous_and_bound_to_run(self) -> None:
        authority, request, run, trace, result = fixtures()
        trace[1]["sequence"] = 7
        with self.assertRaisesRegex(ContractError, "not contiguous"):
            validate_agent_exchange(authority, request, run, trace, result)
        trace = fixtures()[3]
        trace[1]["run_id"] = "run_ffffffffffffffff"
        with self.assertRaisesRegex(ContractError, "lineage mismatch"):
            validate_agent_exchange(authority, request, run, trace, result)

    def test_trace_has_authorize_to_complete_lifecycle(self) -> None:
        authority, request, run, trace, result = fixtures()
        trace[0]["stage"] = "plan"
        with self.assertRaisesRegex(ContractError, "no closed lifecycle"):
            validate_agent_exchange(authority, request, run, trace, result)

    def test_run_and_result_status_must_agree(self) -> None:
        authority, request, run, trace, result = fixtures()
        result["status"] = "partial"
        with self.assertRaisesRegex(ContractError, "status mismatch"):
            validate_agent_exchange(authority, request, run, trace, result)

    def test_inverted_time_window_is_denied(self) -> None:
        request = fixtures()[1]
        request["since"] = "2026-07-26T00:00:00Z"
        request["until"] = "2026-07-25T00:00:00Z"
        with self.assertRaisesRegex(ContractError, "window is inverted"):
            validate_agent_contract(request)

    def test_same_retry_key_has_one_stable_run_identity(self) -> None:
        first = derive_run_id(PRINCIPAL, "synthetic-retry-1")
        second = derive_run_id(PRINCIPAL, "synthetic-retry-1")
        other_principal = derive_run_id(
            "principal:synthetic:other",
            "synthetic-retry-1",
        )
        self.assertEqual(first, second)
        self.assertNotEqual(first, other_principal)

    def test_exchange_rejects_fabricated_retry_identity(self) -> None:
        authority, request, run, trace, result = fixtures()
        run["run_id"] = "run_ffffffffffffffff"
        result["run_id"] = run["run_id"]
        for event in trace:
            event["run_id"] = run["run_id"]
        with self.assertRaisesRegex(ContractError, "idempotency identity mismatch"):
            validate_agent_exchange(authority, request, run, trace, result)


if __name__ == "__main__":
    unittest.main()
