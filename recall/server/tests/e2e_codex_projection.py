#!/usr/bin/env python3
"""Synthetic C6A proof for current Codex projection through HTTP and PostgreSQL."""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parents[3]
SERVER = ROOT / "recall/server"
RECALL = ROOT / "recall"
sys.path.insert(0, str(SERVER))
sys.path.insert(0, str(RECALL))

from client.mac import BrainClient, MemoryClient
from recall_server.db import BrainStore
from recall_server.projectors import canonical_json


def envelope(*, source: str, native: str, parent: str, harness: str, content: dict) -> dict:
    value = {
        "schema_version": 1,
        "source_id": source,
        "native_id": native,
        "native_parent_id": parent,
        "kind": "transcript_record",
        "occurred_at": "2026-07-13T00:00:00Z",
        "observed_at": "2026-07-13T00:00:01Z",
        "principal_id": "owner",
        "visibility": "private",
        "content_type": "application/json",
        "content": content,
        "provenance": {
            "harness": harness,
            "original_path": f"/synthetic/{harness}/session.jsonl",
        },
    }
    value["content_sha256"] = hashlib.sha256(canonical_json(content)).hexdigest()
    return value


def wait_healthy(client: BrainClient) -> None:
    for _ in range(50):
        try:
            client.doctor()
            return
        except (OSError, urllib.error.URLError):
            time.sleep(0.1)
    raise AssertionError("authenticated server did not become healthy")


def rank_for(result: dict, receipt: str) -> int:
    for rank, item in enumerate(result["results"], 1):
        if item["receipt"].split("#", 1)[0] == receipt:
            return rank
    raise AssertionError("committed receipt was not returned by source-scoped search")


def main() -> None:
    dsn = os.environ["RECALL_DATABASE_URL"]
    port = int(os.environ.get("RECALL_E2E_PORT", "18789"))
    endpoint = f"http://127.0.0.1:{port}"
    store = BrainStore(dsn)
    store.migrate()
    with store.connect() as connection:
        connection.execute(
            "TRUNCATE chunks,items,sessions,projection_watermarks,source_events,"
            "ingest_batches,collector_credentials,source_grants,sources,dead_letters,"
            "audit_events RESTART IDENTITY CASCADE"
        )

    codex_source = "codex:mac:c6a"
    claude_source = "claude:mac:c6a"
    codex_credential = store.create_collector_token(
        "c6a-codex", codex_source, ["read", "write"]
    )
    claude_credential = store.create_collector_token(
        "c6a-claude", claude_source, ["read", "write"]
    )
    codex = BrainClient(
        endpoint=endpoint, token=codex_credential["token"], source_id=codex_source
    )
    claude = BrainClient(
        endpoint=endpoint, token=claude_credential["token"], source_id=claude_source
    )

    with tempfile.TemporaryDirectory(prefix="recall-c6a-e2e-") as temporary:
        log_path = Path(temporary) / "server.log"
        with log_path.open("w") as log:
            environment = os.environ | {
                "PYTHONPATH": os.pathsep.join((str(RECALL), str(SERVER))),
                "RECALL_DATABASE_URL": dsn,
                "RECALL_PORT": str(port),
                "RECALL_AUTH_REQUIRED": "1",
            }
            process = subprocess.Popen(
                [sys.executable, "-m", "recall_server.app"],
                env=environment, stdout=log, stderr=log,
            )
            try:
                wait_healthy(codex)
                codex_marker = "c6a-codex-projector-ember-4f18"
                codex_event = envelope(
                    source=codex_source,
                    native="codex-current:1",
                    parent="codex-current",
                    harness="codex",
                    content={
                        "timestamp": "2026-07-13T00:00:00Z",
                        "type": "event_msg",
                        "payload": {
                            "type": "agent_message",
                            "message": codex_marker,
                            "phase": "final_answer",
                            "memory_citation": None,
                        },
                        "_recall_collector_generation": 0,
                    },
                )
                acknowledgements = [
                    codex._request(
                        "/v1/ingest/batches", body={"events": [codex_event]},
                        idempotency_key=f"c6a-codex-delivery-{delivery}",
                    )
                    for delivery in range(3)
                ]
                receipt = acknowledgements[0]["receipts"][0]
                assert acknowledgements[0]["inserted"] == 1
                assert [ack["duplicate_events"] for ack in acknowledgements] == [0, 1, 1]
                codex_rank = rank_for(codex.search(codex_marker, limit=5), receipt)
                resolved = codex.resolve(receipt)
                assert resolved["items"] and resolved["items"][0]["text_redacted"] == codex_marker

                claude_marker = "c6a-claude-parity-sable-91b0"
                claude_event = envelope(
                    source=claude_source,
                    native="claude-current:1",
                    parent="claude-current",
                    harness="claude",
                    content={
                        "timestamp": "2026-07-13T00:00:00Z",
                        "type": "assistant",
                        "message": {"content": claude_marker},
                        "_recall_collector_generation": 0,
                    },
                )
                claude_ack = claude.ingest([claude_event])
                claude_rank = rank_for(
                    claude.search(claude_marker, limit=5), claude_ack["receipts"][0]
                )

                with store.connect() as connection:
                    assert connection.execute(
                        "SELECT count(*) AS n FROM source_events WHERE source_id=%s",
                        (codex_source,),
                    ).fetchone()["n"] == 1
                    assert connection.execute(
                        "SELECT count(*) AS n FROM items WHERE source_id=%s AND deleted_at IS NULL",
                        (codex_source,),
                    ).fetchone()["n"] == 1

                MemoryClient(
                    endpoint=endpoint, token=codex_credential["token"], source_id=codex_source
                ).delete(receipt)
                assert codex.search(codex_marker, limit=5)["results"] == []
                try:
                    codex.resolve(receipt)
                except urllib.error.HTTPError as error:
                    assert error.code == 404
                else:
                    raise AssertionError("tombstoned Codex receipt still resolves")

                result = {
                    "status": "pass",
                    "runtime": {
                        "python": sys.version.split()[0],
                        "postgres": "17-alpine",
                        "psycopg": psycopg.__version__,
                    },
                    "summary": {
                        "codex_deliveries": 3,
                        "codex_canonical_events": 1,
                        "codex_live_items_before_delete": 1,
                        "codex_rank": codex_rank,
                        "codex_receipt_exact": True,
                        "codex_deleted_search_and_receipt": True,
                        "claude_rank": claude_rank,
                        "claude_receipt_exact": True,
                    },
                }
                rendered = json.dumps(result, sort_keys=True)
                if output := os.environ.get("RECALL_E2E_OUT"):
                    Path(output).write_text(rendered + "\n")
                print(rendered)
            finally:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    main()
