#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SERVER = ROOT / "server"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SERVER))

from evals.runner import evaluate_store, load_jsonl, load_synthetic_corpus, public_report, safe_pins, write_json  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402
from recall_server.semantic import SemanticRuntime  # noqa: E402


FIXTURES = ROOT / "tests/central_brain/retrieval_eval_v2"


def request(base: str, path: str, body: dict) -> dict:
    encoded = json.dumps(body).encode()
    value = urllib.request.Request(
        base + path,
        data=encoded,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        # A cold semantic query includes a bounded planner call before local
        # embedding and database retrieval. Keep the transport outside that
        # server-side budget so the E2E measures the response instead of
        # abandoning valid work at an arbitrary five-second client cutoff.
        with urllib.request.urlopen(value, timeout=45) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as exc:
        raise AssertionError(f"HTTP {exc.code}: {exc.read().decode(errors='replace')[:200]}") from None


class HttpStore:
    def __init__(self, base: str):
        self.base = base

    def search(self, query, filters, limit, authorized_source):
        if authorized_source is not None:
            raise AssertionError("the unauthenticated E2E transport cannot emulate a source-scoped credential")
        return request(self.base, "/v1/search", {"query": query, "filters": filters, "limit": limit})

    def show(self, target, *, around, authorized_source):
        if authorized_source is not None:
            raise AssertionError("the unauthenticated E2E transport cannot emulate a source-scoped credential")
        return request(self.base, "/v1/show", {"target": target, "around": around})


def main() -> None:
    dsn = os.environ["RECALL_DATABASE_URL"]
    port = int(os.environ.get("RECALL_RETRIEVAL_E2E_PORT", "18791"))
    base = f"http://127.0.0.1:{port}"
    store = BrainStore(dsn, semantic_runtime=SemanticRuntime.from_env())
    store.migrate()
    with store.connect() as connection:
        connection.execute(
            "TRUNCATE session_export_cursors,chunks,entities,items,sessions,projection_watermarks,"
            "source_events,ingest_batches,source_profiles,source_grants,sources,dead_letters,audit_events "
            "RESTART IDENTITY CASCADE"
        )
    ingest = load_synthetic_corpus(store, load_jsonl(FIXTURES / "corpus.jsonl"))
    semantic_enabled = store.semantic_runtime is not None
    embedding = (
        store.embed_pending(batch_size=128, max_batches=None)
        if semantic_enabled
        else {"processed": 0, "batches": 0}
    )
    embedding_metrics = store.service_metrics()
    # Source-scoped auth cases use BrainStore directly because this E2E server deliberately runs
    # without fabricated credentials. Every other case traverses the real HTTP boundary.
    all_cases = load_jsonl(FIXTURES / "queries-dev.jsonl") + load_jsonl(FIXTURES / "queries-holdout.jsonl")
    http_cases = [case for case in all_cases if case["stratum"] != "privacy-auth"]

    with tempfile.TemporaryDirectory(prefix="recall-retrieval-eval-") as temporary:
        log_path = Path(temporary) / "server.log"
        with log_path.open("w") as log:
            environment = os.environ | {
                "RECALL_LEGACY_WRITES": "1",
                "RECALL_LEGACY_READS": "1",
                "PYTHONPATH": str(SERVER),
                "RECALL_DATABASE_URL": dsn,
                "RECALL_PORT": str(port),
            }
            process = subprocess.Popen(
                [sys.executable, "-m", "recall_server.app"],
                env=environment,
                stdout=log,
                stderr=log,
            )
            try:
                for _ in range(50):
                    try:
                        with urllib.request.urlopen(base + "/healthz", timeout=1) as response:
                            if response.status == 200:
                                break
                    except OSError:
                        time.sleep(0.1)
                else:
                    raise AssertionError("server did not become healthy")

                first_http = evaluate_store(HttpStore(base), http_cases, source_routing_supported=True)
                second_http = evaluate_store(HttpStore(base), http_cases, source_routing_supported=True)
            finally:
                process.terminate()
                process.wait(timeout=5)

    first = evaluate_store(store, all_cases, source_routing_supported=True)
    second = evaluate_store(store, all_cases, source_routing_supported=True)
    changed_http = sorted(
        case_id for case_id, ranking in first_http["rankings"].items()
        if ranking != second_http["rankings"].get(case_id)
    )
    changed_direct = sorted(
        case_id for case_id, ranking in first["rankings"].items()
        if ranking != second["rankings"].get(case_id)
    )
    assert not changed_http, f"HTTP rankings changed for {len(changed_http)} cases: {','.join(changed_http)}"
    assert not changed_direct, f"direct rankings changed for {len(changed_direct)} cases: {','.join(changed_direct)}"
    assert first["strata"]["exact-identifier"]["hit@1"] == 1.0
    assert first["behavior"]["session_reconstruction_accuracy"] == 1.0
    assert first["behavior"]["deletion_resurrection_rate"] == 0.0
    assert ingest["deduplication_accuracy"] == 1.0
    assert first["aggregate"]["unauthorized_hit_rate"] == 0.0
    assert first["strata"]["source-routed"]["backend_error_rate"] == 0.0
    assert first["strata"]["source-routed"]["recall@5"] == 1.0
    if semantic_enabled:
        for report in (first, first_http):
            assert report["strata"]["semantic-paraphrase"]["recall@5"] >= 0.8
            assert report["strata"]["workflow-gotcha"]["recall@5"] >= 0.8
            assert report["strata"]["cross-session-synthesis"]["recall@5"] >= 0.8
    assert all(
        metrics["backend_error_rate"] == 0.0
        for stratum, metrics in first["strata"].items()
        if stratum != "source-routed"
    )
    numeric = first["numeric_values"]
    assert numeric and all(math.isfinite(value) for value in numeric)

    first["behavior"]["deduplication_accuracy"] = ingest["deduplication_accuracy"]
    first["pins"] = safe_pins(ROOT.parent, FIXTURES / "corpus.jsonl", FIXTURES / "queries-holdout.jsonl")
    output_path = os.environ.get("RECALL_EVAL_OUTPUT")
    if output_path:
        write_json(Path(output_path), public_report(first, aggregate_only=True), private=False)
    print(json.dumps({
        "status": "pass",
        "queries": len(all_cases),
        "exact_hit_at_1": first["strata"]["exact-identifier"]["hit@1"],
        "semantic_recall_at_5": first["strata"]["semantic-paraphrase"]["recall@5"],
        "workflow_recall_at_5": first["strata"]["workflow-gotcha"]["recall@5"],
        "cross_session_recall_at_5": first["strata"]["cross-session-synthesis"]["recall@5"],
        "http_semantic_recall_at_5": first_http["strata"]["semantic-paraphrase"]["recall@5"],
        "http_workflow_recall_at_5": first_http["strata"]["workflow-gotcha"]["recall@5"],
        "http_cross_session_recall_at_5": first_http["strata"]["cross-session-synthesis"]["recall@5"],
        "backfilled_items": embedding["processed"],
        "embedded_items": embedding_metrics["embedded_items"],
        "embedding_lag": embedding_metrics["embedding_lag"],
        "source_routing_backend_error_rate": first["strata"]["source-routed"]["backend_error_rate"],
        "session_reconstruction_accuracy": first["behavior"]["session_reconstruction_accuracy"],
        "deduplication_accuracy": ingest["deduplication_accuracy"],
        "unauthorized_hit_rate": first["aggregate"]["unauthorized_hit_rate"],
    }, sort_keys=True))


if __name__ == "__main__":
    main()
