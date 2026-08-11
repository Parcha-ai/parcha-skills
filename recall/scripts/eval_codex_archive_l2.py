#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import random
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
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


class _AckServer(BaseHTTPRequestHandler):
    acknowledgements: dict[str, dict] = {}

    def log_message(self, *_args) -> None:
        pass

    def do_POST(self) -> None:
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        key = self.headers["Idempotency-Key"]
        acknowledgement = type(self).acknowledgements.get(key)
        replay = acknowledgement is not None
        if acknowledgement is None:
            acknowledgement = {
                "batch_id": "synthetic-" + str(len(type(self).acknowledgements) + 1),
                "status": "committed",
                "inserted": len(body["events"]),
                "duplicate_events": 0,
                "receipts": [
                    f"recall://{item['source_id']}/{item['native_id']}?rev=1"
                    for item in body["events"]
                ],
            }
            type(self).acknowledgements[key] = acknowledgement
        rendered = json.dumps({**acknowledgement, "replay": replay}).encode()
        self.send_response(200 if replay else 201)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(rendered)))
        self.end_headers()
        self.wfile.write(rendered)


def _collector(
    active: Path,
    archived: Path | None,
    spool: Path,
    endpoint: str,
    *,
    max_records: int = 100_000,
) -> Collector:
    return Collector(
        root=active,
        archive_root=archived,
        harness="codex",
        source_id="codex:synthetic:l2",
        spool_path=spool,
        endpoint=endpoint,
        token="synthetic-test-token",
        max_scan_records=max_records,
        max_scan_seconds=60,
    )


def measure() -> dict:
    _AckServer.acknowledgements = {}
    server = ThreadingHTTPServer(("127.0.0.1", 0), _AckServer)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            active = root / "sessions"
            archived = root / "archived_sessions"
            archived.mkdir(parents=True)
            spool = root / "spool.db"
            randomizer = random.Random(20260811)
            paths: list[Path] = []
            for index in range(100):
                session_id = f"019f{index:04x}-1111-7222-8333-{index:012x}"
                path = active / str(index % 7) / f"rollout-{session_id}.jsonl"
                _rollout(path, session_id)
                paths.append(path)
            endpoint = f"http://127.0.0.1:{server.server_port}"

            initial = _collector(active, archived, spool, endpoint)
            assert initial.scan()["records_queued"] == 200
            assert initial.flush()["acked"] == 200
            native_ids = {
                row[0] for row in initial.db.execute(
                    "SELECT native_id FROM active_records"
                )
            }
            initial.close()

            for path in paths:
                target = (
                    archived
                    / str(randomizer.randrange(12))
                    / str(randomizer.randrange(31))
                    / path.name
                )
                target.parent.mkdir(parents=True, exist_ok=True)
                path.replace(target)

            obsolete = _collector(active, None, spool, endpoint)
            tombstoned = obsolete.scan()
            assert obsolete.flush()["acked"] == 200
            obsolete.close()

            partial = _collector(
                active,
                archived,
                spool,
                endpoint,
                max_records=73,
            )
            first_restore = partial.scan()
            partial.close()
            resumed = _collector(active, archived, spool, endpoint)
            second_restore = resumed.scan()
            pending = list(resumed.db.execute(
                """SELECT native_id,COUNT(*) AS copies
                     FROM outbox WHERE state='pending'
                     GROUP BY native_id ORDER BY native_id"""
            ))
            generations = {
                row[0] for row in resumed.db.execute(
                    "SELECT DISTINCT generation FROM record_generations"
                )
            }
            backlog_before_ack = resumed.doctor()["archive_backlog"]
            acked = resumed.flush()["acked"]
            doctor = resumed.doctor(include_dead_letters=False)
            no_op = resumed.scan()
            resumed.close()

            restored = (
                first_restore["restored_records_queued"]
                + second_restore["restored_records_queued"]
            )
            return {
                "contract": "recall.codex-archive-l2-scorecard.v1",
                "randomized_restores": 100,
                "initial_records": 200,
                "operational_tombstones": tombstoned["tombstones_queued"],
                "restored_records": restored,
                "restore_identity_invariance": (
                    len({row["native_id"] for row in pending} & native_ids)
                    / len(native_ids)
                ),
                "restore_duplicate_records": sum(
                    max(0, row["copies"] - 1) for row in pending
                ),
                "restore_generation": min(generations) if generations else -1,
                "crash_resume_records": second_restore["restored_records_queued"],
                "archive_backlog_before_ack": backlog_before_ack,
                "archive_backlog_after_ack": doctor["archive_backlog"],
                "restore_acked": acked,
                "restore_noop_records": no_op["records_queued"],
                "restore_noop_tombstones": no_op["tombstones_queued"],
                "identity_conflicts": doctor["identity_conflicts"],
            }
    finally:
        server.shutdown()
        server.server_close()


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
