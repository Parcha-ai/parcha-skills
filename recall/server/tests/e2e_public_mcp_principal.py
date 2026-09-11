#!/usr/bin/env python3
"""Fresh-PostgreSQL proof for principal-scoped public MCP retrieval."""

from __future__ import annotations

import hashlib
import http.client
import json
import os
import sys
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SERVER = ROOT / "recall/server"
sys.path.insert(0, str(ROOT / "recall"))
sys.path.insert(0, str(SERVER))

from recall_server.app import Handler
from recall_server.db import BrainStore
from recall_server.projectors import canonical_json


def envelope(source_id: str, principal_id: str, native_id: str, marker: str) -> dict:
    content = {
        "role": "user",
        "text": f"synthetic cobalt retrieval marker {marker}",
    }
    return {
        "schema_version": 1,
        "source_id": source_id,
        "native_id": native_id,
        "native_parent_id": f"{source_id}:session",
        "kind": "message",
        "occurred_at": "2026-07-17T20:00:00Z",
        "observed_at": "2026-07-17T20:00:01Z",
        "principal_id": principal_id,
        "visibility": "private",
        "content_type": "application/json",
        "content": content,
        "content_sha256": hashlib.sha256(canonical_json(content)).hexdigest(),
        "provenance": {
            "harness": "codex",
            "cwd": "/synthetic/public-mcp",
            "branch": "test/principal-isolation",
        },
    }


def rpc(server: ThreadingHTTPServer, token: str, method: str, params: dict) -> dict:
    payload = json.dumps({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }).encode()
    connection = http.client.HTTPConnection(
        "127.0.0.1", server.server_port, timeout=5,
    )
    connection.request(
        "POST",
        "/mcp",
        body=payload,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
            "Content-Length": str(len(payload)),
            "MCP-Protocol-Version": "2025-11-25",
        },
    )
    response = connection.getresponse()
    raw = response.read()
    connection.close()
    assert response.status == 200
    return json.loads(raw)


def tool(server: ThreadingHTTPServer, token: str, name: str, arguments: dict) -> dict:
    response = rpc(
        server,
        token,
        "tools/call",
        {"name": name, "arguments": arguments},
    )
    return response


def main() -> None:
    store = BrainStore(os.environ["RECALL_DATABASE_URL"])
    store.migrate()
    owner_sources = ["synthetic:owner:a", "synthetic:owner:b"]
    outsider_source = "synthetic:outsider"
    with store.connect() as connection:
        connection.execute(
            "TRUNCATE collector_credentials,session_export_cursors,chunks,items,"
            "sessions,projection_watermarks,source_events,ingest_batches,"
            "source_grants,sources,dead_letters,audit_events RESTART IDENTITY CASCADE"
        )
    events = [
        envelope(owner_sources[0], "owner-one", "owner-a:1", "owner-a"),
        envelope(owner_sources[1], "owner-one", "owner-b:1", "owner-b"),
        envelope(outsider_source, "owner-two", "outsider:1", "outsider"),
    ]
    store.ingest("synthetic-public-mcp-principals", events)
    credential = store.create_collector_token(
        "synthetic-owner-one-mcp",
        "synthetic:owner:capture",
        ["read", "write"],
        principal_id="owner-one",
        capture_origin="grep-agent",
    )
    no_grants = store.create_collector_token(
        "synthetic-no-grants-mcp",
        None,
        ["read"],
        principal_id="owner-without-grants",
    )

    original_environment = {
        key: os.environ.get(key)
        for key in (
            "RECALL_AUTH_REQUIRED",
            "RECALL_HTTP_PROFILE",
            "RECALL_TRUST_TAILSCALE_HEADERS",
            "RECALL_LEGACY_WRITES",
        )
    }
    os.environ.update({
        "RECALL_AUTH_REQUIRED": "1",
        "RECALL_HTTP_PROFILE": "public-mcp",
        "RECALL_TRUST_TAILSCALE_HEADERS": "0",
        # v1 store contract proof; canonical-mode capture is covered by
        # e2e_webhook_ingest.py (H1-T6).
        "RECALL_LEGACY_WRITES": "1",
    })
    Handler.store = store
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        searched = tool(
            server,
            credential["token"],
            "recall_search",
            {"query": "synthetic cobalt retrieval marker", "limit": 10},
        )
        results = searched["result"]["structuredContent"]["results"]
        result_sources = {row["source_id"] for row in results}
        assert result_sources == set(owner_sources)
        assert outsider_source not in json.dumps(searched)

        outsider_receipt = store.search(
            "outsider",
            authorized_source=outsider_source,
        )["results"][0]["receipt"]
        shown = tool(
            server,
            credential["token"],
            "recall_show",
            {"target": outsider_receipt},
        )
        assert shown["error"] == {
            "code": -32602,
            "message": "receipt not found",
        }

        denied = tool(
            server,
            no_grants["token"],
            "recall_search",
            {"query": "synthetic cobalt retrieval marker"},
        )
        assert denied["result"]["structuredContent"]["results"] == []

        capture_arguments = {
            "schema_version": 1,
            "title": "Synthetic hosted memory",
            "body": (
                "keep the bounded amethyst queue\n"
                "api_key=synthetic-public-mcp-secret-canary"
            ),
            "occurred_at": "2026-07-17T21:00:00Z",
            "tags": ["synthetic", "decision"],
            "provenance": {"uri": "manual://grep-agent"},
        }
        captured = tool(
            server,
            credential["token"],
            "recall_capture",
            capture_arguments,
        )
        capture_result = captured["result"]["structuredContent"]
        assert "synthetic-public-mcp-secret-canary" not in json.dumps(captured)
        receipt = capture_result["receipt"]
        replayed = tool(
            server,
            credential["token"],
            "recall_capture",
            capture_arguments,
        )["result"]["structuredContent"]
        assert replayed["receipt"] == receipt
        assert replayed["replay"] is True
        with store.connect() as connection:
            capture_events = connection.execute(
                """SELECT kind,principal_id,envelope
                   FROM source_events WHERE source_id=%s ORDER BY revision""",
                ("synthetic:owner:capture",),
            ).fetchall()
        assert len(capture_events) == 1
        assert capture_events[0]["kind"] == "capture"
        assert capture_events[0]["principal_id"] == "owner-one"
        rendered_envelope = json.dumps(capture_events[0]["envelope"])
        assert "synthetic-public-mcp-secret-canary" not in rendered_envelope
        assert capture_events[0]["envelope"]["content"]["origin"] == "grep-agent"
        capture_search = store.search(
            "bounded amethyst queue",
            authorized_source="synthetic:owner:capture",
        )["results"]
        assert len(capture_search) == 1
        assert capture_search[0]["receipt"] == receipt

        wrong_forget = tool(
            server,
            credential["token"],
            "recall_forget",
            {"receipt": outsider_receipt},
        )
        assert "error" in wrong_forget
        forgotten = tool(
            server,
            credential["token"],
            "recall_forget",
            {"receipt": receipt},
        )["result"]["structuredContent"]
        assert forgotten["receipt"].endswith("?rev=2")
        assert store.search(
            "bounded amethyst queue",
            authorized_source="synthetic:owner:capture",
        )["results"] == []

        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_port, timeout=5,
        )
        connection.request(
            "GET",
            "/v1/doctor",
            headers={"Authorization": f"Bearer {credential['token']}"},
        )
        hidden = connection.getresponse()
        hidden.read()
        connection.close()
        assert hidden.status == 404

        print(json.dumps({
            "status": "pass",
            "authorized_sources": len(result_sources),
            "cross_principal_search_hits": 0,
            "cross_principal_show_hits": 0,
            "empty_grant_search_hits": 0,
            "capture_events_after_replay": 1,
            "capture_secret_canary_rows": 0,
            "cross_source_forget_events": 0,
            "live_capture_hits_after_forget": 0,
            "hidden_rest_status": hidden.status,
        }, sort_keys=True))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        for key, value in original_environment.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        with store.connect() as connection:
            connection.execute(
                "TRUNCATE collector_credentials,session_export_cursors,chunks,items,"
                "sessions,projection_watermarks,source_events,ingest_batches,"
                "source_grants,sources,dead_letters,audit_events "
                "RESTART IDENTITY CASCADE"
            )


if __name__ == "__main__":
    main()
