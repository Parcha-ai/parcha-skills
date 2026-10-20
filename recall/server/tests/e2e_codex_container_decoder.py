#!/usr/bin/env python3
"""Decode the native rollout fixture inside the actual read-only image."""
from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess


def main() -> None:
    image = os.environ.get("RECALL_CORE_IMAGE") or os.environ["RECALL_REMOTE_WORKER_IMAGE"]
    fixture = Path(__file__).resolve().parents[2] / "tests/fixtures/codex-rollout.jsonl.zst"
    raw = (
        b'{"payload": {"id": "019f1111-2222-7333-8444-555555555555"}, '
        b'"timestamp": "2026-08-10T00:00:00Z", "type": "session_meta"}\n'
        b'{"payload": {"marker": "compressed synthetic work", "type": "message"}, '
        b'"timestamp": "2026-08-10T00:00:01Z", "type": "response_item"}\n'
    )
    program = f"""
import base64, hashlib, json, os, tempfile, time
from pathlib import Path
from collector.collector import Collector
from skills.recall.scripts.codex_rollout import decoded_snapshot
assert os.getuid() == 10001
with tempfile.TemporaryDirectory() as directory:
    source = Path(directory) / 'rollout.jsonl.zst'
    source.write_bytes(base64.b64decode({base64.b64encode(fixture.read_bytes()).decode()!r}))
    with decoded_snapshot(source, deadline=time.monotonic()+5) as snapshot:
        data = snapshot.path.read_bytes()
        assert len(data) == {len(raw)}
        assert hashlib.sha256(data).hexdigest() == {hashlib.sha256(raw).hexdigest()!r}
        assert snapshot.size == len(data)
        assert snapshot.path.stat().st_mode & 0o777 == 0o600
        snapshot.verify_current()
        temporary = snapshot.path
    assert not temporary.exists()
print(json.dumps(dict(status='pass', decoded_bytes={len(raw)}, private_snapshot=True)))
"""
    result = subprocess.run(
        [
            "docker", "run", "--rm", "--read-only", "--network", "none",
            "--memory", "256m", "--memory-swap", "256m", "--cpus", "1",
            "--pids-limit", "64",
            "--tmpfs", "/tmp:rw,noexec,nosuid,size=16m,uid=10001,gid=10001,mode=0700",
            "--entrypoint", "python", image, "-c", program,
        ],
        check=False, capture_output=True, text=True,
    )
    if result.returncode:
        raise RuntimeError(f"container decoder refused: {result.stderr.strip()}")
    proof = json.loads(result.stdout)
    assert proof == dict(status="pass", decoded_bytes=len(raw), private_snapshot=True)
    print(json.dumps(dict(image=image, **proof), sort_keys=True))


if __name__ == "__main__":
    main()
