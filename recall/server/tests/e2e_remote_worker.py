#!/usr/bin/env python3
"""Fresh-PostgreSQL restart and failure-isolation proof for the remote worker."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from collections import deque
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SERVER = ROOT / "recall/server"
RECALL = ROOT / "recall"
sys.path.insert(0, str(RECALL))
sys.path.insert(0, str(SERVER))

from client.mac import BrainClient
from connectors.host import build_host
from connectors.remote_api import RemoteApiError
from connectors.remote_worker import load_remote_worker_config
from recall_server.db import BrainStore


def private_file(path: Path, value: str) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
        output.write(value)


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def schedule(key: str, connector_id: str) -> dict:
    return {
        "schema_version": 1,
        "job_key": key,
        "connector_id": connector_id,
        "generation": 1,
        "enabled": True,
        "interval_seconds": 300,
        "jitter_seconds": 0,
        "transient_base_seconds": 5,
        "max_backoff_seconds": 300,
        "lease_seconds": 60,
        "max_rate_limit_seconds": 3600,
    }


class Rail:
    def __init__(self, responses):
        self.responses = deque(responses)
        self.calls = 0

    def request(self, _operation, **_parameters):
        self.calls += 1
        if not self.responses:
            raise AssertionError("unexpected synthetic remote operation")
        value = self.responses.popleft()
        if isinstance(value, Exception):
            raise value
        return value


def main() -> None:
    database = os.environ["RECALL_DATABASE_URL"]
    store = BrainStore(database)
    store.migrate()
    with store.connect() as connection:
        connection.execute(
            "TRUNCATE chunks,items,sessions,projection_watermarks,source_events,"
            "ingest_batches,source_grants,sources,dead_letters,audit_events "
            "RESTART IDENTITY CASCADE"
        )
    port = free_port()
    endpoint = f"http://127.0.0.1:{port}"
    github_source = "synthetic:remote:github:e2e"
    slack_source = "synthetic:remote:slack:e2e"
    canary = "".join(("remote-worker-", "private-canary"))
    github_page = [{
        "number": 1,
        "title": "remote worker github marker",
        "body": f"api_key={canary}",
        "state": "open",
        "created_at": "2026-07-18T01:00:00Z",
        "updated_at": "2026-07-18T02:00:00Z",
        "html_url": "https://github.example.invalid/o/r/issues/1",
        "user": {"login": "synthetic-user"},
        "labels": [],
    }]
    slack_page = {
        "ok": True,
        "messages": [{
            "type": "message",
            "ts": "1784332800.000100",
            "user": "U1",
            "text": f"remote worker slack marker api_key={canary}",
        }],
        "has_more": False,
        "response_metadata": {"next_cursor": ""},
    }
    slack_users = {
        "ok": True,
        "members": [],
        "response_metadata": {"next_cursor": ""},
    }
    slack_join = {"ok": True, "channel": {"id": "C123"}}
    rails = {
        "github.activity": Rail((github_page, github_page)),
        "slack.messages": Rail((
            RemoteApiError("upstream_unavailable"),
            slack_users,
            slack_join,
            slack_page,
        )),
    }
    with tempfile.TemporaryDirectory(prefix="recall-remote-worker-e2e-") as directory:
        root = Path(directory)
        private = root / "private"
        private.mkdir(mode=0o700)
        state = root / "state"
        state.mkdir(mode=0o700)
        brain_github = private / "brain-github"
        brain_slack = private / "brain-slack"
        source_github = private / "source-github"
        source_slack = private / "source-slack"
        for path, value in (
            (brain_github, json.dumps({"token": "synthetic-github-authority"})),
            (brain_slack, json.dumps({"token": "synthetic-slack-authority"})),
            (source_github, "synthetic-source-authority"),
            (source_slack, "synthetic-source-authority"),
        ):
            private_file(path, value)
        jobs = [
            {
                "schedule": schedule("1" * 64, "github.activity"),
                "source_id": github_source,
                "endpoint": endpoint,
                "brain_authority": {"kind": "file", "path": str(brain_github)},
                "privacy_mode": "scrub",
                "connector": {
                    "source_authority": {"kind": "file", "path": str(source_github)},
                    "spool": str(state / "github.db"),
                    "page_size": 10,
                    "timeout_seconds": 10,
                    "selectors": {
                        "owner": "synthetic-org",
                        "repository": "synthetic-repo",
                    },
                },
            },
            {
                "schedule": schedule("2" * 64, "slack.messages"),
                "source_id": slack_source,
                "endpoint": endpoint,
                "brain_authority": {"kind": "file", "path": str(brain_slack)},
                "privacy_mode": "scrub",
                "connector": {
                    "source_authority": {"kind": "file", "path": str(source_slack)},
                    "spool": str(state / "slack.db"),
                    "page_size": 10,
                    "timeout_seconds": 10,
                    "selectors": {
                        "workspace_id": "T123", "channel_ids": ["C123"],
                        "owner_user_ids": ["U111"],
                    },
                },
            },
        ]
        config_path = private / "worker.json"
        private_file(config_path, json.dumps({"schema_version": 1, "jobs": jobs}))
        log_path = private / "brain.log"
        log = log_path.open("w")
        process = subprocess.Popen(
            [sys.executable, "-m", "recall_server.app"],
            env={
                **os.environ,
                "PYTHONPATH": str(SERVER),
                "RECALL_DATABASE_URL": database,
                "RECALL_PORT": str(port),
            },
            stdout=log,
            stderr=log,
        )
        try:
            for _ in range(50):
                try:
                    with urllib.request.urlopen(endpoint + "/healthz", timeout=1):
                        break
                except OSError:
                    time.sleep(0.1)
            else:
                raise AssertionError("synthetic Brain did not become ready")
            config = load_remote_worker_config(config_path)
            first = build_host(
                config,
                state_path=state / "supervisor.db",
                remote_rails=rails,
            )
            first_result = first.supervisor.tick(first.jobs, now=0)
            first.close()
            assert first_result["outcomes"] == {"success": 1, "transient": 1}

            second = build_host(
                config,
                state_path=state / "supervisor.db",
                remote_rails=rails,
            )
            second_result = second.supervisor.tick(second.jobs, now=5)
            second.close()
            assert second_result["outcomes"] == {"success": 1}

            third = build_host(
                config,
                state_path=state / "supervisor.db",
                remote_rails=rails,
            )
            third_result = third.supervisor.tick(third.jobs, now=305)
            third.close()
            assert third_result["outcomes"] == {"success": 2}, third_result
            github = BrainClient(
                endpoint=endpoint,
                token="synthetic",
                source_id=github_source,
            )
            slack = BrainClient(
                endpoint=endpoint,
                token="synthetic",
                source_id=slack_source,
            )
            assert github.search("remote worker github marker")["results"]
            assert slack.search("remote worker slack marker")["results"]
            assert github.search(canary)["results"] == []
            assert slack.search(canary)["results"] == []
            with store.connect() as connection:
                events = connection.execute(
                    "SELECT count(*) AS n FROM source_events"
                ).fetchone()["n"]
            assert events == 2
            assert all(
                canary.encode() not in path.read_bytes()
                for path in (state / "github.db", state / "slack.db")
            )
            print(json.dumps({
                "status": "pass",
                "configured_sources": 2,
                "restart_cycles": 2,
                "isolated_progress_after_failure": 1,
                "searchable_sources": 2,
                "duplicate_acknowledged_versions": 0,
                "canary_search_hits": 0,
                "spool_canary_hits": 0,
                "credential_bytes_rendered": False,
                "private_content_rendered": False,
            }, sort_keys=True))
        finally:
            process.terminate()
            process.wait(timeout=5)
            log.close()


if __name__ == "__main__":
    main()
