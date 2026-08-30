"""Durable inbound journal for the Tether gateway plugin.

Persists one row per observed inbound event — idempotent by event key —
under <hermes home>/plugin-data/tether/, the per-plugin data directory
Hermes preserves across plugin update and removal. Never the install dir.

In shadow mode this journal is pure evidence: the recorded decision is
what Tether WOULD have done. In active mode (a later slice) the same
journal write becomes the durable admission record made before any
directive is returned to the gateway.
"""

from __future__ import annotations

import contextlib
import json
import os
import sqlite3
import stat
import time
from pathlib import Path
from typing import Any


_SCHEMA = """
CREATE TABLE IF NOT EXISTS shadow_events (
  event_key TEXT PRIMARY KEY,
  received_at TEXT NOT NULL,
  platform TEXT NOT NULL,
  workspace TEXT,
  channel TEXT,
  thread TEXT,
  actor TEXT,
  verdict TEXT NOT NULL,
  reason TEXT NOT NULL,
  binding_ref TEXT,
  decision_json TEXT NOT NULL
)
"""


class JournalError(RuntimeError):
    pass


def _secure_directory(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid():
        raise JournalError(f"journal directory is not privately owned: {path}")
    if stat.S_IMODE(info.st_mode) & 0o077:
        os.chmod(path, 0o700)


class DurableJournal:
    def __init__(self, directory: Path):
        directory = Path(directory)
        _secure_directory(directory.parent)
        _secure_directory(directory)
        self.path = directory / "shadow.db"
        existed = self.path.exists()
        self._db = sqlite3.connect(self.path, timeout=10)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.execute(_SCHEMA)
        self._db.commit()
        if not existed:
            os.chmod(self.path, 0o600)
        for sidecar in (f"{self.path}-wal", f"{self.path}-shm"):
            with contextlib.suppress(FileNotFoundError):
                os.chmod(sidecar, 0o600)

    def record(self, event_key: str, decision: dict[str, Any], **fields: Any) -> bool:
        """Insert one decision; returns False when the key was a replay."""
        if not event_key:
            raise JournalError("an event key is required")
        cursor = self._db.execute(
            """
            INSERT OR IGNORE INTO shadow_events(
              event_key,received_at,platform,workspace,channel,thread,actor,
              verdict,reason,binding_ref,decision_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                event_key,
                time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
                str(fields.get("platform") or ""),
                fields.get("workspace"),
                fields.get("channel"),
                fields.get("thread"),
                fields.get("actor"),
                str(decision.get("verdict") or "unknown"),
                str(decision.get("reason") or ""),
                decision.get("binding_ref"),
                json.dumps(decision, sort_keys=True, separators=(",", ":")),
            ),
        )
        self._db.commit()
        return cursor.rowcount == 1

    def summary(self) -> dict[str, Any]:
        counts = {
            str(row["verdict"]): int(row["n"])
            for row in self._db.execute(
                "SELECT verdict,COUNT(*) AS n FROM shadow_events GROUP BY verdict"
            )
        }
        last = self._db.execute(
            "SELECT MAX(received_at) FROM shadow_events"
        ).fetchone()[0]
        return {
            "events": sum(counts.values()),
            "verdicts": counts,
            "last_received_at": last,
        }

    def close(self) -> None:
        with contextlib.suppress(sqlite3.Error):
            self._db.close()
