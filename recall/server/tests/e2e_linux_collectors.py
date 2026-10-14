#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sqlite3
import ssl
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import psycopg

from client.mac import MemoryClient


SAMPLES = 20


def token(path: str) -> str:
    return json.loads(Path(path).read_text())["token"]


def percentile95(values: list[float]) -> float:
    return sorted(values)[max(0, int(len(values) * 0.95 + 0.999999) - 1)]


def fixture(harness: str, marker: str, timestamp: str) -> dict:
    if harness == "claude":
        return {"type": "user", "timestamp": timestamp, "message": {"content": marker}}
    return {
        "type": "response_item",
        "timestamp": timestamp,
        "payload": {"type": "message", "role": "user", "content": [{"type": "input_text", "text": marker}]},
    }


def query_spool(spool: Path, prefix: str, state: str) -> list[sqlite3.Row]:
    db = sqlite3.connect(f"file:{spool}?mode=ro", uri=True, timeout=5)
    db.row_factory = sqlite3.Row
    try:
        return list(db.execute(
            "SELECT path,start_offset,end_offset,native_id,receipt,queued_at,acked_at FROM outbox WHERE path LIKE ? AND state=? ORDER BY id",
            (prefix + "%", state),
        ))
    finally:
        db.close()


def wait_for(predicate, timeout: float, description: str):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(2)
    raise AssertionError("timeout waiting for " + description)


def resolve(endpoint: str, receipt: str, bearer: str) -> dict:
    url = endpoint.rstrip("/") + "/v1/receipts/resolve?" + urllib.parse.urlencode({"receipt": receipt})
    request = urllib.request.Request(url, headers={"Authorization": "Bearer " + bearer})
    with urllib.request.urlopen(request, timeout=10, context=ssl.create_default_context()) as response:
        assert response.status == 200
        return json.loads(response.read())


def main() -> None:
    run_id = os.environ.get("RECALL_E2E_RUN_ID", str(int(time.time())))
    endpoint = os.environ["RECALL_ENDPOINT"]
    dsn = os.environ["RECALL_DATABASE_URL"]
    configurations = {
        "claude": {
            "root": Path(os.environ["RECALL_CLAUDE_ROOT"]),
            "spool": Path(os.environ["RECALL_CLAUDE_SPOOL"]),
            "source": os.environ["RECALL_CLAUDE_SOURCE"],
            "token": token(os.environ["RECALL_CLAUDE_TOKEN_FILE"]),
        },
        "codex": {
            "root": Path(os.environ["RECALL_CODEX_ROOT"]),
            "spool": Path(os.environ["RECALL_CODEX_SPOOL"]),
            "source": os.environ["RECALL_CODEX_SOURCE"],
            "token": token(os.environ["RECALL_CODEX_TOKEN_FILE"]),
        },
    }
    started = time.time()
    paths: dict[str, list[Path]] = {}
    prefixes: dict[str, str] = {}
    try:
        timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        for harness, config in configurations.items():
            directory = config["root"] / f"recall-c3-e2e-{run_id}"
            directory.mkdir(parents=True, exist_ok=False)
            paths[harness] = []
            prefixes[harness] = str(directory) + "/"
            for index in range(SAMPLES):
                name = f"rollout-e2e-{index:02d}.jsonl" if harness == "codex" else f"e2e-{index:02d}.jsonl"
                path = directory / name
                marker = f"recall-c3-{harness}-{run_id}-{index:02d}"
                path.write_text(json.dumps(fixture(harness, marker, timestamp)) + "\n")
                paths[harness].append(path)

        acked: dict[str, list[sqlite3.Row]] = {}
        for harness, config in configurations.items():
            acked[harness] = wait_for(
                lambda c=config, p=prefixes[harness]: (rows if len(rows := query_spool(c["spool"], p, "acked")) == SAMPLES else None),
                150,
                f"{harness} live append acknowledgements",
            )

        evidence = {"status": "pass", "run_id": run_id, "samples_per_harness": SAMPLES, "harnesses": {}}
        with psycopg.connect(dsn) as database:
            for harness, config in configurations.items():
                rows = acked[harness]
                latencies = [row["acked_at"] - started for row in rows]
                p95 = percentile95(latencies)
                assert p95 < 120, p95
                central = database.execute(
                    "SELECT count(*),count(DISTINCT native_id) FROM source_events WHERE source_id=%s AND envelope->'provenance'->>'original_path' LIKE %s AND NOT is_tombstone",
                    (config["source"], prefixes[harness] + "%"),
                ).fetchone()
                assert central == (SAMPLES, SAMPLES), central
                sample = rows[-1]
                window = Path(sample["path"]).read_bytes()[sample["start_offset"]:sample["end_offset"]].decode()
                resolved = resolve(endpoint, sample["receipt"], config["token"])
                expected_marker = f"recall-c3-{harness}-{run_id}-{SAMPLES - 1:02d}"
                assert expected_marker in window
                assert expected_marker in json.dumps(resolved)
                evidence["harnesses"][harness] = {
                    "live_append_p95_seconds": round(p95, 3),
                    "acked": len(rows),
                    "central_events": central[0],
                    "central_distinct_native_ids": central[1],
                    "receipt_resolved": True,
                    "local_byte_window_reopened": True,
                }
    finally:
        for harness_paths in paths.values():
            for path in harness_paths:
                path.unlink(missing_ok=True)
            if harness_paths:
                harness_paths[0].parent.rmdir()

    cleanup_at = time.time()
    for harness, config in configurations.items():
        def scanned_after_cleanup():
            with sqlite3.connect(f"file:{config['spool']}?mode=ro", uri=True) as local:
                row = local.execute("SELECT value FROM meta WHERE key='last_scan_at'").fetchone()
                return row is not None and float(row[0]) > cleanup_at
        wait_for(scanned_after_cleanup, 150, f"{harness} post-cleanup scan")
        native_ids = [row["native_id"] for row in acked[harness]]
        with psycopg.connect(dsn) as database:
            tombstones = database.execute(
                "SELECT count(*) FROM source_events WHERE source_id=%s AND native_id=ANY(%s) AND is_tombstone",
                (config["source"], native_ids),
            ).fetchone()[0]
        assert tombstones == 0, tombstones
        assert len(query_spool(config["spool"], prefixes[harness], "acked")) == SAMPLES
        assert resolve(endpoint, acked[harness][-1]["receipt"], config["token"])
        evidence["harnesses"][harness]["local_cleanup_retained_records"] = SAMPLES
        MemoryClient(endpoint=endpoint, token=config["token"], source_id=config["source"],
                     principal_id="owner", visibility="private").delete_many(
                         [row["receipt"] for row in acked[harness]])
        with psycopg.connect(dsn) as database:
            tombstones = database.execute(
                "SELECT count(*) FROM source_events WHERE source_id=%s AND native_id=ANY(%s) AND is_tombstone",
                (config["source"], native_ids),
            ).fetchone()[0]
        assert tombstones == SAMPLES, tombstones
        evidence["harnesses"][harness]["tombstones_committed"] = tombstones

    rendered = json.dumps(evidence, indent=2, sort_keys=True) + "\n"
    if os.environ.get("RECALL_E2E_OUT"):
        Path(os.environ["RECALL_E2E_OUT"]).write_text(rendered)
    print(json.dumps(evidence, sort_keys=True))


if __name__ == "__main__":
    main()
