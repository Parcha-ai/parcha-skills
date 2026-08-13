#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import statistics
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from skills.recall.scripts.codex_identity import resolve_codex_session_identity
from collector.collector import Collector


SESSION_ID = "019f1111-2222-7333-8444-555555555555"


def _line(value: dict) -> str:
    return json.dumps(value, sort_keys=True) + "\n"


def _rollout(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        _line({
            "timestamp": "2026-08-10T00:00:00Z",
            "type": "session_meta",
            "payload": {"id": SESSION_ID, "cwd": "/synthetic"},
        })
        + _line({
            "timestamp": "2026-08-10T00:00:01Z",
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "synthetic marker"}],
            },
        })
    )


def _collector(
    root: Path,
    spool: Path,
    *,
    archive_root: Path | None = None,
) -> Collector:
    return Collector(
        root=root,
        harness="codex",
        source_id="codex:synthetic:baseline",
        spool_path=spool,
        endpoint="http://127.0.0.1:1",
        token="unused",
        max_scan_records=10_000,
        max_scan_seconds=60,
        archive_root=archive_root,
    )


def measure() -> dict:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        active = root / "sessions"
        archived = root / "archived_sessions"
        filename = (
            "rollout-2026-08-10T00-00-00-" + SESSION_ID + ".jsonl"
        )
        active_path = active / "2026" / "08" / "10" / filename
        archived_path = archived / filename
        _rollout(active_path)
        stable = resolve_codex_session_identity(active_path)

        active_collector = _collector(active, root / "active.db")
        initial = active_collector.scan()
        initial_envelopes = active_collector.pending_envelopes()
        active_parent = initial_envelopes[0]["native_parent_id"]
        active_ids = [item["native_id"] for item in initial_envelopes]
        archived_path.parent.mkdir(parents=True)
        active_path.replace(archived_path)
        moved = active_collector.scan()
        active_collector.close()

        archive_collector = _collector(archived, root / "archive.db")
        archive_discovered = archive_collector.discover()
        archive_scan = archive_collector.scan()
        archive_envelopes = archive_collector.pending_envelopes()
        archive_parent = archive_envelopes[0]["native_parent_id"]
        archive_ids = [item["native_id"] for item in archive_envelopes]
        archive_collector.close()

        multi_active = root / "multi" / "sessions"
        multi_archived = root / "multi" / "archived_sessions"
        multi_active_path = multi_active / "2026" / "08" / "10" / filename
        multi_archived_path = multi_archived / filename
        _rollout(multi_active_path)
        multi_archived.mkdir(parents=True)
        multi = _collector(
            multi_active,
            root / "multi.db",
            archive_root=multi_archived,
        )
        multi.scan()
        multi_before = multi.pending_envelopes()
        multi_parent = multi_before[0]["native_parent_id"]
        multi_ids = [item["native_id"] for item in multi_before]
        multi_archived_path.parent.mkdir(parents=True, exist_ok=True)
        multi_active_path.replace(multi_archived_path)
        multi_move = multi.scan()
        multi_after = multi.pending_envelopes()
        no_op_samples = []
        for _ in range(20):
            started = time.perf_counter()
            multi.scan()
            no_op_samples.append((time.perf_counter() - started) * 1000)
        multi.close()

        return {
            "contract": "recall.codex-archive-baseline.v1",
            "status": "baseline",
            "fixture_sessions": 1,
            "fixture_records": 2,
            "stable_identity_coverage": int(stable.status == "resolved"),
            "archive_discovery_coverage": (
                len(archive_discovered) / 1
            ),
            "current_single_root_archive_coverage": 0.0,
            "single_root_move_tombstones": moved["tombstones_queued"],
            "single_root_parent_identity_invariant": active_parent == archive_parent,
            "single_root_record_identity_invariant": active_ids == archive_ids,
            "move_tombstones": multi_move["tombstones_queued"],
            "move_parent_identity_invariant": (
                multi_parent == multi_after[0]["native_parent_id"]
            ),
            "move_record_identity_invariant": (
                multi_ids == [item["native_id"] for item in multi_after]
            ),
            "initial_records_queued": initial["records_queued"],
            "archive_records_queued_fresh_spool": archive_scan["records_queued"],
            "noop_scan_ms": {
                "p50": round(statistics.median(no_op_samples), 3),
                "p95": round(sorted(no_op_samples)[18], 3),
            },
        }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output")
    args = parser.parse_args()
    result = measure()
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        Path(args.output).write_text(rendered)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
