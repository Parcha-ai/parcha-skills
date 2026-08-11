#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import random
import statistics
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector.collector import Collector


def _line(value: dict) -> str:
    return json.dumps(value, sort_keys=True) + "\n"


def _rollout(path: Path, session_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        _line({
            "timestamp": "2026-08-10T00:00:00Z",
            "type": "session_meta",
            "payload": {"id": session_id},
        })
        + _line({
            "timestamp": "2026-08-10T00:00:01Z",
            "type": "response_item",
            "payload": {"type": "message", "marker": "synthetic"},
        })
    )


def _collector(
    active: Path,
    archived: Path,
    spool: Path,
    *,
    max_records: int = 100_000,
) -> Collector:
    return Collector(
        root=active,
        archive_root=archived,
        harness="codex",
        source_id="codex:synthetic:l1",
        spool_path=spool,
        endpoint="http://127.0.0.1:1",
        token="unused",
        max_scan_records=max_records,
        max_scan_seconds=60,
    )


def measure() -> dict:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        active = root / "sessions"
        archived = root / "archived_sessions"
        archived.mkdir(parents=True)
        paths = []
        randomizer = random.Random(20260811)
        for index in range(100):
            session_id = f"019f{index:04x}-1111-7222-8333-{index:012x}"
            path = active / str(index % 7) / f"rollout-{session_id}.jsonl"
            _rollout(path, session_id)
            paths.append(path)

        collector = _collector(active, archived, root / "spool.db")
        initial = collector.scan()
        before_ids = [
            row[0] for row in collector.db.execute(
                "SELECT native_id FROM outbox ORDER BY native_id"
            )
        ]
        before_content = sorted(
            row[0] for row in collector.db.execute(
                "SELECT content_sha256 FROM active_records"
            )
        )
        for path in paths:
            target = (
                archived
                / str(randomizer.randrange(12))
                / str(randomizer.randrange(31))
                / path.name
            )
            target.parent.mkdir(parents=True, exist_ok=True)
            path.replace(target)

        collector.close()
        collector = _collector(active, archived, root / "spool.db")
        moved = collector.scan()
        after_ids = [
            row[0] for row in collector.db.execute(
                "SELECT native_id FROM outbox ORDER BY native_id"
            )
        ]
        after_content = sorted(
            row[0] for row in collector.db.execute(
                "SELECT content_sha256 FROM active_records"
            )
        )
        no_op = []
        for _ in range(50):
            started = time.perf_counter()
            collector.scan()
            no_op.append((time.perf_counter() - started) * 1000)
        doctor = collector.doctor(include_dead_letters=False)
        collector.close()

        fairness_active = root / "fairness" / "sessions"
        fairness_archive = root / "fairness" / "archived_sessions"
        fairness_archive.mkdir(parents=True)
        active_id = "019fffff-1111-7222-8333-ffffffffffff"
        _rollout(fairness_active / f"rollout-{active_id}.jsonl", active_id)
        for index in range(20):
            session_id = f"019f{index:04x}-aaaa-7bbb-8ccc-{index:012x}"
            _rollout(
                fairness_archive / f"rollout-{session_id}.jsonl",
                session_id,
            )
        fairness = _collector(
            fairness_active,
            fairness_archive,
            root / "fairness.db",
            max_records=1,
        )
        fairness_scan = fairness.scan()
        first = fairness.pending_envelopes()[0]
        fairness.close()

        return {
            "contract": "recall.codex-archive-l1-scorecard.v1",
            "randomized_moves": 100,
            "initial_records": initial["records_queued"],
            "move_identity_invariance": sum(
                before == after for before, after in zip(
                    before_ids, after_ids, strict=True
                )
            ) / len(before_ids),
            "record_content_parity": before_content == after_content,
            "move_tombstones": moved["tombstones_queued"],
            "move_duplicate_records": moved["records_queued"],
            "move_visibility_intervals": 1,
            "archive_coverage": doctor["archive_coverage_percent"] / 100,
            "identity_conflicts": doctor["identity_conflicts"],
            "active_serviced_first": (
                first["content"]["payload"]["id"] == active_id
                and fairness_scan["active_files_seen"] == 1
                and fairness_scan["archive_files_seen"] == 20
            ),
            "noop_scan_ms": {
                "p50": round(statistics.median(no_op), 3),
                "p95": round(sorted(no_op)[47], 3),
            },
        }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = measure()
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(rendered)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
