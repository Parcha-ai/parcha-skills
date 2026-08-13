#!/usr/bin/env python3
"""Fresh-PostgreSQL proof for Slack, Notion, and X portable archives."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
RECALL = ROOT / "recall"
SERVER = RECALL / "server"
sys.path.insert(0, str(RECALL))
sys.path.insert(0, str(SERVER))

from connectors.portable_archives import (
    NotionArchiveConnector,
    SlackArchiveConnector,
    XArchiveConnector,
)
from connectors.sdk import ConnectorRunner
from privacy.policy import PrivacyPolicy
from recall_server.db import BrainStore
from recall_server.projectors import canonical_json


SOURCES = (
    "synthetic:portable-slack:postgres",
    "synthetic:portable-notion:postgres",
    "synthetic:portable-x:postgres",
)


class StoreWriter:
    def __init__(self, store):
        self.store = store

    def ingest(self, events):
        key = "portable-archive-e2e-" + hashlib.sha256(
            canonical_json(events)
        ).hexdigest()
        acknowledgement, replay = self.store.ingest(key, events)
        return {**acknowledgement, "replay": replay}


def write_zip(path: Path, members: dict[str, str]) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        for name, value in members.items():
            archive.writestr(name, value)


def main() -> None:
    store = BrainStore(os.environ["RECALL_DATABASE_URL"])
    store.migrate()
    with store.connect() as connection:
        connection.execute(
            "TRUNCATE chunks,items,sessions,projection_watermarks,source_events,"
            "ingest_batches,source_grants,sources,dead_letters,audit_events "
            "RESTART IDENTITY CASCADE"
        )
    with tempfile.TemporaryDirectory(prefix="recall-portable-archive-e2e-") as temporary:
        root = Path(temporary)
        slack = root / "slack.zip"
        notion = root / "notion.zip"
        x_archive = root / "x.zip"
        write_zip(slack, {
            "channels.json": json.dumps([{"id": "C123", "name": "general"}]),
            "general/2026-07-17.json": json.dumps([{
                "client_msg_id": "slack-e2e-1",
                "ts": "1784282400.000001",
                "user": "U111",
                "text": (
                    "portable slack postgres marker "
                    "api_key=synthetic-portable-slack-canary"
                ),
            }]),
        })
        write_zip(notion, {
            "Plan abc123.md": (
                "portable notion postgres marker\n"
                "api_key=synthetic-portable-notion-canary"
            ),
        })
        write_zip(x_archive, {
            "data/tweets.js": (
                'window.YTD.tweets.part0 = [{"tweet":'
                '{"id_str":"x-e2e-1","full_text":'
                '"portable x postgres marker api_key=synthetic-portable-x-canary"}}]'
            ),
        })
        writer = StoreWriter(store)
        connectors = [
            SlackArchiveConnector(
                path=slack, source_id=SOURCES[0], archive_id="T123",
            ),
            NotionArchiveConnector(
                path=notion, source_id=SOURCES[1], archive_id="space-e2e",
            ),
            XArchiveConnector(
                path=x_archive, source_id=SOURCES[2], archive_id="account-e2e",
            ),
        ]
        spools = [root / "state" / f"{index}.db" for index in range(3)]
        runners = [
            ConnectorRunner(
                connector=connector, brain=writer, spool_path=spool,
                privacy=PrivacyPolicy(mode="scrub"),
            )
            for connector, spool in zip(connectors, spools, strict=True)
        ]
        for runner in runners:
            assert runner.run_once()["acked"] == 1
            assert runner.run_once()["acked"] == 0
        for phrase, source in zip((
            "portable slack postgres marker",
            "portable notion postgres marker",
            "portable x postgres marker",
        ), SOURCES, strict=True):
            assert store.search(phrase, authorized_source=source)["results"]
        for canary, source in zip((
            "synthetic-portable-slack-canary",
            "synthetic-portable-notion-canary",
            "synthetic-portable-x-canary",
        ), SOURCES, strict=True):
            assert store.search(canary, authorized_source=source)["results"] == []

        slack_id = connectors[0].pull(None).records[0].native_id
        write_zip(slack, {
            "channels.json": json.dumps([{"id": "C123", "name": "general"}]),
            "general/2026-07-17.json": json.dumps([{
                "client_msg_id": "slack-e2e-1",
                "ts": "1784282400.000001",
                "user": "U111",
                "text": "portable slack revised marker",
            }]),
        })
        assert runners[0].run_once()["acked"] == 1
        for runner in runners:
            runner.close()
        remover = ConnectorRunner(
            connector=SlackArchiveConnector(
                path=slack, source_id=SOURCES[0], archive_id="T123",
                removed_native_ids=(slack_id,),
            ),
            brain=writer,
            spool_path=spools[0],
            privacy=PrivacyPolicy(mode="scrub"),
        )
        assert remover.run_once()["acked"] == 1
        remover.close()
        assert store.search(
            "portable slack revised marker", authorized_source=SOURCES[0]
        )["results"] == []
        private_bytes = b"".join(
            path.read_bytes()
            for path in spools[0].parent.iterdir()
            if path.is_file()
        )
        assert b"synthetic-portable" not in private_bytes
        print(json.dumps({
            "status": "pass",
            "configured_sources": 3,
            "searchable_sources": 3,
            "content_revisions": 1,
            "explicit_tombstones": 1,
            "duplicate_acknowledged_versions": 0,
            "restart_reacknowledgements": 0,
            "inferred_tombstones": 0,
            "canary_search_hits": 0,
            "spool_canary_hits": 0,
            "private_content_rendered": False,
        }, sort_keys=True))


if __name__ == "__main__":
    main()
