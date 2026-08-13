#!/usr/bin/env python3
"""Fresh-PostgreSQL E2E for GitHub, Linear, Slack, and Notion connectors."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
from collections import defaultdict, deque
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "recall"))
sys.path.insert(0, str(ROOT / "recall/server"))

from connectors.sdk import ConnectorRunner
from connectors.work_apis import (
    GitHubActivityConnector,
    LinearActivityConnector,
    NotionWorkspaceConnector,
    SlackMessagesConnector,
)
from privacy.policy import PrivacyPolicy
from recall_server.db import BrainStore
from recall_server.projectors import canonical_json


class Rail:
    def __init__(self):
        self.responses = defaultdict(deque)

    def add(self, operation, value):
        self.responses[operation].append(value)

    def request(self, operation, **_parameters):
        if not self.responses[operation]:
            raise AssertionError("unexpected synthetic provider operation")
        return self.responses[operation].popleft()


class StoreWriter:
    def __init__(self, store):
        self.store = store

    def ingest(self, events):
        key = "work-api-e2e-" + hashlib.sha256(canonical_json(events)).hexdigest()
        acknowledgement, replay = self.store.ingest(key, events)
        return {**acknowledgement, "replay": replay}


def main() -> None:
    store = BrainStore(os.environ["RECALL_DATABASE_URL"])
    store.migrate()
    with store.connect() as connection:
        connection.execute(
            "TRUNCATE chunks,items,sessions,projection_watermarks,source_events,"
            "ingest_batches,source_grants,sources,dead_letters,audit_events "
            "RESTART IDENTITY CASCADE"
        )

    rails = [Rail() for _ in range(4)]
    rails[0].add("issues.list", [{
        "number": 1,
        "title": "work github marker",
        "body": "api_key=work-api-e2e-private-canary",
        "state": "open",
        "created_at": "2026-07-18T01:00:00Z",
        "updated_at": "2026-07-18T02:00:00Z",
        "html_url": "https://github.example.invalid/o/r/issues/1",
        "user": {"login": "synthetic-user"},
        "labels": [],
    }])
    rails[1].add("issues.list", {"data": {"issues": {
        "nodes": [{
            "id": "linear-1",
            "identifier": "SYN-1",
            "title": "work linear marker",
            "description": "api_key=work-api-e2e-private-canary",
            "createdAt": "2026-07-18T01:00:00Z",
            "updatedAt": "2026-07-18T02:00:00Z",
            "state": {"name": "Open"},
            "labels": {"nodes": []},
        }],
        "pageInfo": {"hasNextPage": False, "endCursor": None},
    }}})
    rails[2].add("messages.history", {
        "ok": True,
        "messages": [{
            "type": "message",
            "ts": "1784332800.000100",
            "user": "U1",
            "text": "work slack marker api_key=work-api-e2e-private-canary",
        }],
        "has_more": False,
        "response_metadata": {"next_cursor": ""},
    })
    rails[3].add("search.list", {
        "object": "list",
        "results": [{
            "object": "page",
            "id": "page-1",
            "created_time": "2026-07-18T01:00:00Z",
            "last_edited_time": "2026-07-18T02:00:00Z",
            "in_trash": False,
            "properties": {"Name": {"type": "title", "title": [{
                "plain_text": "work notion marker api_key=work-api-e2e-private-canary",
            }]}},
        }],
        "has_more": False,
        "next_cursor": None,
    })

    sources = {
        "github": "synthetic:github:work:e2e",
        "linear": "synthetic:linear:work:e2e",
        "slack": "synthetic:slack:work:e2e",
        "notion": "synthetic:notion:work:e2e",
    }
    connectors = (
        GitHubActivityConnector(
            rail=rails[0],
            source_id=sources["github"],
            owner="synthetic-org",
            repository="synthetic-repo",
        ),
        LinearActivityConnector(
            rail=rails[1],
            source_id=sources["linear"],
            team_id="team-synthetic",
        ),
        SlackMessagesConnector(
            rail=rails[2],
            source_id=sources["slack"],
            workspace_id="T123",
            channel_id="C123",
        ),
        NotionWorkspaceConnector(
            rail=rails[3],
            source_id=sources["notion"],
        ),
    )

    with tempfile.TemporaryDirectory(prefix="recall-work-api-e2e-") as directory:
        spools = []
        acked = 0
        for connector in connectors:
            spool = Path(directory) / f"{connector.connector_id}.db"
            runner = ConnectorRunner(
                connector=connector,
                brain=StoreWriter(store),
                spool_path=spool,
                privacy=PrivacyPolicy(mode="scrub"),
            )
            result = runner.run_once()
            assert result["acked"] == 1
            assert runner.doctor()["checkpointed"]
            assert runner.doctor()["pending"] == 0
            acked += result["acked"]
            spools.append(spool)
            runner.close()
        for provider, source in sources.items():
            assert store.search(
                f"work {provider} marker",
                authorized_source=source,
            )["results"]
            assert store.search(
                "work-api-e2e-private-canary",
                authorized_source=source,
            )["results"] == []
        assert all(
            b"work-api-e2e-private-canary" not in spool.read_bytes()
            for spool in spools
        )
    print(json.dumps({
        "status": "pass",
        "connectors": len(connectors),
        "records_acked": acked,
        "typed_search_hits": 4,
        "canary_search_hits": 0,
        "spool_canary_hits": 0,
        "inferred_tombstones": 0,
        "live_grants": 0,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
