#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from collector.codex_identity import resolve_codex_session_identity


def _digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def audit_roots(
    *,
    source_id: str,
    active_root: Path,
    archive_root: Path,
    parquet_queue_depth: int | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    locations: dict[str, list[Path]] = defaultdict(list)
    roots: dict[str, dict[str, Any]] = {}
    total_files = 0
    total_bytes = 0

    for lifecycle, root in (("active", active_root), ("archived", archive_root)):
        statuses: Counter[str] = Counter()
        file_count = 0
        byte_count = 0
        if root.is_dir():
            for path in root.rglob("rollout-*.jsonl"):
                if not path.is_file():
                    continue
                file_count += 1
                byte_count += path.stat().st_size
                identity = resolve_codex_session_identity(path)
                statuses[identity.status] += 1
                if identity.native_session_id is not None:
                    locations[identity.native_session_id].append(path)
        roots[lifecycle] = {
            "root_present": root.is_dir(),
            "files": file_count,
            "bytes": byte_count,
            "identity_status": dict(sorted(statuses.items())),
        }
        total_files += file_count
        total_bytes += byte_count

    duplicate_groups = [paths for paths in locations.values() if len(paths) > 1]
    identical = 0
    divergent = 0
    for paths in duplicate_groups:
        if len({_digest(path) for path in paths}) == 1:
            identical += 1
        else:
            divergent += 1

    resolved = sum(len(paths) for paths in locations.values())
    elapsed = max(time.perf_counter() - started, 1e-9)
    return {
        "contract": "recall.codex-root-inventory.v1",
        "source_id": source_id,
        "roots": roots,
        "stable_identity_coverage": round(
            resolved / total_files if total_files else 1.0,
            6,
        ),
        "duplicates": {
            "session_ids": len(duplicate_groups),
            "byte_identical": identical,
            "divergent": divergent,
        },
        "totals": {"files": total_files, "bytes": total_bytes},
        "measurement": {
            "elapsed_ms": round(elapsed * 1000, 3),
            "files_per_second": round(total_files / elapsed, 3),
        },
        "parquet_queue_depth": parquet_queue_depth,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Emit content-free Codex active/archive coverage metrics."
    )
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--active-root", type=Path, required=True)
    parser.add_argument("--archive-root", type=Path, required=True)
    parser.add_argument("--parquet-queue-depth", type=int)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = audit_roots(
        source_id=args.source_id,
        active_root=args.active_root.expanduser(),
        archive_root=args.archive_root.expanduser(),
        parquet_queue_depth=args.parquet_queue_depth,
    )
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(rendered)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
