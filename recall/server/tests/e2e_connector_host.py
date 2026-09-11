#!/usr/bin/env python3
"""Fresh-PostgreSQL E2E for the private two-connector supervisor host."""

from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SERVER = ROOT / "recall/server"
RECALL = ROOT / "recall"
sys.path.insert(0, str(RECALL))
sys.path.insert(0, str(SERVER))

from client.mac import BrainClient, MemoryClient
from connectors.grep_ai import GrepAIResponse
from connectors.host import build_host, load_host_config
from recall_server.db import BrainStore


EXPORT_SOURCE = "chatgpt:synthetic:host-e2e"
GREP_SOURCE = "grep-ai:synthetic:host-e2e"
EXPORT_MARKER = "supervised export cobalt marker"
GREP_MARKER = "Safe report citation"
KEY = "parcha-synthetic-" + "b" * 32
FIXTURES = RECALL / "tests/grep_ai_v2/corpus.jsonl"


def private_file(path: Path, value: str) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
        output.write(value)


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def grep_fixtures() -> tuple[dict, dict]:
    row = next(json.loads(line) for line in FIXTURES.read_text().splitlines()
               if json.loads(line)["case"] == "complete")
    item = row["list"]["items"][0]
    return {"items": [item], "next_cursor": None, "has_more": False}, row["details"][item["job_id"]]


class GrepState:
    def __init__(self):
        self.page, self.detail = grep_fixtures()
        self.requests = 0
        self.fail_once = True


class GrepHandler(BaseHTTPRequestHandler):
    state: GrepState

    def log_message(self, _format, *_args):
        return

    def do_GET(self):
        self.state.requests += 1
        parsed = urllib.parse.urlparse(self.path)
        if self.headers.get("Authorization") != "Bearer " + KEY:
            body = json.dumps({"error": {"code": "unauthenticated"}}).encode(); status = 401
        elif parsed.path == "/api/v2/research":
            body = json.dumps(self.state.page).encode(); status = 200
        elif parsed.path.startswith("/api/v2/research/"):
            body = json.dumps(self.state.detail).encode(); status = 200
        else:
            body = b"{}"; status = 404
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers(); self.wfile.write(body)


class LocalGrepTransport:
    def __init__(self, state: GrepState, base: str):
        self.state = state
        self.base = base

    def request(self, *, path, query, headers, timeout, max_bytes):
        if self.state.fail_once:
            self.state.fail_once = False
            raise OSError("synthetic transient transport")
        url = self.base + path + (("?" + urllib.parse.urlencode(query)) if query else "")
        request = urllib.request.Request(url, headers=dict(headers), method="GET")
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return GrepAIResponse(
                    response.status,
                    {key.casefold(): value for key, value in response.headers.items()},
                    response.read(max_bytes + 1), "https://api.grep.ai" + path,
                )
        except urllib.error.HTTPError as error:
            return GrepAIResponse(
                error.code, {key.casefold(): value for key, value in error.headers.items()},
                error.read(max_bytes + 1), "https://api.grep.ai" + path,
            )


def schedule(key: str, connector_id: str, *, interval: int, base: int) -> dict:
    return {
        "schema_version": 1, "job_key": key, "connector_id": connector_id,
        "generation": 1, "enabled": True, "interval_seconds": interval,
        "jitter_seconds": 0, "transient_base_seconds": base,
        "max_backoff_seconds": 20, "lease_seconds": 30,
        "max_rate_limit_seconds": 60,
    }


def truncate(store: BrainStore) -> None:
    with store.connect() as connection:
        connection.execute(
            "TRUNCATE chunks,items,sessions,projection_watermarks,source_events,"
            "ingest_batches,source_grants,sources,dead_letters,audit_events RESTART IDENTITY CASCADE"
        )


def main() -> None:
    database = os.environ["RECALL_DATABASE_URL"]
    store = BrainStore(database); store.migrate(); truncate(store)
    brain_port = free_port(); grep_port = free_port()
    grep_state = GrepState(); GrepHandler.state = grep_state
    grep_server = ThreadingHTTPServer(("127.0.0.1", grep_port), GrepHandler)
    grep_thread = threading.Thread(target=grep_server.serve_forever, daemon=True); grep_thread.start()

    with tempfile.TemporaryDirectory(prefix="recall-host-e2e-") as temporary:
        root = Path(temporary)
        private = root / "private"; private.mkdir(mode=0o700)
        state = root / "state"; state.mkdir(mode=0o700)
        inbox = root / "inbox"; inbox.mkdir(mode=0o700)
        export_brain = private / "export-brain.json"
        grep_brain = private / "grep-brain.json"
        grep_key = private / "grep.key"
        private_file(export_brain, json.dumps({"token": "synthetic-export-authority"}))
        private_file(grep_brain, json.dumps({"token": "synthetic-grep-authority"}))
        private_file(grep_key, KEY + "\n")
        (inbox / "cowork.jsonl").write_text(json.dumps({
            "conversation_id": "synthetic-host-conversation",
            "message_id": "synthetic-host-message", "parent_message_id": None,
            "create_time": "2026-07-14T08:30:00Z", "role": "assistant",
            "content": {"content_type": "text", "parts": [EXPORT_MARKER]},
        }) + "\n")
        config_path = private / "host.json"
        config = {
            "schema_version": 1,
            "jobs": [
                {
                    "schedule": schedule("2" * 64, "grep.ai", interval=100, base=1),
                    "source_id": GREP_SOURCE, "endpoint": f"http://127.0.0.1:{brain_port}",
                    "brain_authority": {"kind": "file", "path": str(grep_brain)},
                    "privacy_mode": "scrub",
                    "connector": {
                        "source_authority": {"kind": "file", "path": str(grep_key)},
                        "spool": str(state / "grep.db"), "max_pages": 10,
                        "page_size": 10, "timeout_seconds": 5,
                    },
                },
                {
                    "schedule": schedule("1" * 64, "openai.export-inbox", interval=100, base=1),
                    "source_id": EXPORT_SOURCE, "endpoint": f"http://127.0.0.1:{brain_port}",
                    "brain_authority": {"kind": "file", "path": str(export_brain)},
                    "privacy_mode": "scrub",
                    "connector": {
                        "inbox": str(inbox), "catalog": str(state / "export-catalog.db"),
                        "spool": str(state / "export.db"), "page_size": 100,
                    },
                },
            ],
        }
        private_file(config_path, json.dumps(config, sort_keys=True, separators=(",", ":")))
        log = (private / "server.log").open("w")
        # The host posts to /v1/ingest/batches and BrainClient reads /v1/search;
        # this script pins the v1 store contract (H1-T6 canonical-mode coverage
        # lives in e2e_webhook_ingest.py).
        env = os.environ | {
            "PYTHONPATH": str(SERVER), "RECALL_DATABASE_URL": database,
            "RECALL_PORT": str(brain_port),
            "RECALL_LEGACY_WRITES": "1", "RECALL_LEGACY_READS": "1",
        }
        process = subprocess.Popen([sys.executable, "-m", "recall_server.app"], env=env, stdout=log, stderr=log)
        base = f"http://127.0.0.1:{brain_port}"
        try:
            for _ in range(50):
                try:
                    with urllib.request.urlopen(base + "/healthz", timeout=1):
                        break
                except OSError:
                    time.sleep(0.1)
            else:
                raise AssertionError("Brain service did not become ready")

            loaded = load_host_config(config_path)
            transport = LocalGrepTransport(grep_state, f"http://127.0.0.1:{grep_port}")
            first_host = build_host(loaded, state_path=state / "supervisor.db", grep_transport=transport)
            first = first_host.supervisor.tick(first_host.jobs, now=0)
            first_host.close()
            assert first["outcomes"] == {"success": 1, "transient": 1}
            assert BrainClient(
                endpoint=base, token="synthetic", source_id=EXPORT_SOURCE,
            ).search(EXPORT_MARKER)["results"]

            recovered_host = build_host(loaded, state_path=state / "supervisor.db", grep_transport=transport)
            second = recovered_host.supervisor.tick(recovered_host.jobs, now=1)
            assert second["outcomes"] == {"success": 1}
            export_client = BrainClient(endpoint=base, token="synthetic", source_id=EXPORT_SOURCE)
            grep_client = BrainClient(endpoint=base, token="synthetic", source_id=GREP_SOURCE)
            export_hit = export_client.search(EXPORT_MARKER)["results"][0]
            grep_hit = grep_client.search(GREP_MARKER)["results"][0]
            assert export_client.resolve(export_hit["receipt"])
            assert grep_client.resolve(grep_hit["receipt"])
            with store.connect() as connection:
                before_repeat = connection.execute("SELECT count(*) AS n FROM source_events").fetchone()["n"]
            third = recovered_host.supervisor.tick(recovered_host.jobs, now=101)
            recovered_host.close()
            assert third["outcomes"] == {"success": 2}
            with store.connect() as connection:
                after_repeat = connection.execute("SELECT count(*) AS n FROM source_events").fetchone()["n"]
            assert before_repeat == after_repeat == 2

            MemoryClient(endpoint=base, token="synthetic", source_id=EXPORT_SOURCE).delete(export_hit["receipt"])
            MemoryClient(endpoint=base, token="synthetic", source_id=GREP_SOURCE).delete(grep_hit["receipt"])
            assert export_client.search(EXPORT_MARKER)["results"] == []
            assert grep_client.search(GREP_MARKER)["results"] == []
            with store.connect() as connection:
                live_after_forget = connection.execute(
                    "SELECT count(*) AS n FROM items WHERE deleted_at IS NULL"
                ).fetchone()["n"]
            assert live_after_forget == 0
            truncate(store)
            print(json.dumps({
                "status": "pass", "configured_sources": 2,
                "isolated_progress_after_transient": 1, "restart_recovery": 1,
                "searchable_sources": 2, "resolved_sources": 2,
                "duplicate_acknowledged_pages": 0, "inferred_tombstones": 0,
                "cross_source_deletes": 0, "live_after_forget": live_after_forget,
                "credential_bytes_rendered": False, "private_content_rendered": False,
            }, sort_keys=True))
        finally:
            process.terminate(); process.wait(timeout=5); log.close()
            grep_server.shutdown(); grep_server.server_close(); grep_thread.join(timeout=2)


if __name__ == "__main__":
    main()
