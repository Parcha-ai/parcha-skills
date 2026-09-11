"""Batched writer for allowed authorization audit rows.

Denied decisions never pass through here: they are written synchronously so
the row is durable before the 403 leaves the process. Allowed decisions are
queued and flushed by a daemon thread every ``batch_seconds`` or once
``batch_rows`` rows are waiting, whichever comes first. The queue is bounded;
when it is full the caller writes its row synchronously instead, so no row is
ever dropped. ``flush()`` is public so shutdown, tests, and e2e scripts can
force every queued row to disk.
"""

from __future__ import annotations

import logging
import os
import threading
from collections import deque
from collections.abc import Callable, Sequence
from typing import Any

LOG = logging.getLogger("recall.audit")

DEFAULT_BATCH_ROWS = 500
DEFAULT_BATCH_SECONDS = 2.0
MAX_BATCH_ROWS = 10_000
MAX_BATCH_SECONDS = 60.0
QUEUE_CAPACITY_MULTIPLIER = 4

AUTHORIZATION_AUDIT_INSERT = """INSERT INTO authorization_audit_events(
       principal_kind,principal_id,tenant_id,action,
       decision,reason,policy_version
   ) VALUES (%s,%s,%s,%s,%s,%s,%s)"""

AuditRow = tuple[str, str, str, str, str, str, str]


def batch_settings_from_env() -> tuple[int, float]:
    """Return ``(batch_rows, batch_seconds)``; ``batch_rows=0`` disables batching."""
    try:
        rows = int(os.environ.get("RECALL_AUDIT_BATCH_ROWS", str(DEFAULT_BATCH_ROWS)))
        seconds = float(
            os.environ.get("RECALL_AUDIT_BATCH_SECONDS", str(DEFAULT_BATCH_SECONDS))
        )
    except ValueError as exc:
        raise ValueError("authorization audit batch settings are invalid") from exc
    if not 0 <= rows <= MAX_BATCH_ROWS:
        raise ValueError(
            f"RECALL_AUDIT_BATCH_ROWS must be between 0 and {MAX_BATCH_ROWS}"
        )
    if not 0 < seconds <= MAX_BATCH_SECONDS:
        raise ValueError(
            f"RECALL_AUDIT_BATCH_SECONDS must be between 0 and {MAX_BATCH_SECONDS}"
        )
    return rows, seconds


class AuthorizationAuditBatcher:
    """Bounded queue plus daemon flusher for allowed authorization audit rows."""

    def __init__(
        self,
        connect: Callable[[], Any],
        *,
        batch_rows: int = DEFAULT_BATCH_ROWS,
        batch_seconds: float = DEFAULT_BATCH_SECONDS,
        queue_capacity: int | None = None,
    ):
        if isinstance(batch_rows, bool) or not isinstance(batch_rows, int):
            raise ValueError("audit batch rows must be an integer")
        if not 0 <= batch_rows <= MAX_BATCH_ROWS:
            raise ValueError(
                f"audit batch rows must be between 0 and {MAX_BATCH_ROWS}"
            )
        if not 0 < batch_seconds <= MAX_BATCH_SECONDS:
            raise ValueError(
                f"audit batch seconds must be between 0 and {MAX_BATCH_SECONDS}"
            )
        self._connect = connect
        self.batch_rows = batch_rows
        self.batch_seconds = float(batch_seconds)
        self.queue_capacity = (
            queue_capacity
            if queue_capacity is not None
            else max(1, batch_rows) * QUEUE_CAPACITY_MULTIPLIER
        )
        if self.queue_capacity < max(1, batch_rows):
            raise ValueError("audit queue capacity must hold at least one batch")
        self._queue: deque[AuditRow] = deque()
        self._condition = threading.Condition()
        self._thread: threading.Thread | None = None
        self._closed = False
        self._flush_lock = threading.Lock()
        self.queued_total = 0
        self.flushed_total = 0
        self.overflow_sync_total = 0
        self.flush_failures = 0

    @property
    def enabled(self) -> bool:
        return self.batch_rows > 0

    def __len__(self) -> int:
        with self._condition:
            return len(self._queue)

    # -- write paths -----------------------------------------------------

    def write_sync(self, rows: Sequence[AuditRow]) -> None:
        """Write ``rows`` in one transaction on a fresh pooled connection."""
        if not rows:
            return
        with self._connect() as conn:
            with conn.transaction():
                if len(rows) == 1:
                    conn.execute(AUTHORIZATION_AUDIT_INSERT, rows[0])
                else:
                    with conn.cursor() as cursor:
                        cursor.executemany(AUTHORIZATION_AUDIT_INSERT, list(rows))

    def enqueue(self, row: AuditRow) -> bool:
        """Queue ``row``; returns False when it was written synchronously instead."""
        if not self.enabled:
            self.write_sync([row])
            return False
        with self._condition:
            if self._closed or len(self._queue) >= self.queue_capacity:
                overflow = not self._closed
            else:
                self._queue.append(row)
                self.queued_total += 1
                self._ensure_thread_locked()
                if len(self._queue) >= self.batch_rows:
                    self._condition.notify()
                return True
        if overflow:
            self.overflow_sync_total += 1
        self.write_sync([row])
        return False

    def flush(self) -> int:
        """Write every queued row now; returns the number written.

        Rows are removed from the queue only after the insert commits, so a
        failed flush leaves them queued for the next attempt.
        """
        with self._flush_lock:
            with self._condition:
                pending = list(self._queue)
            if not pending:
                return 0
            try:
                self.write_sync(pending)
            except Exception:
                self.flush_failures += 1
                raise
            with self._condition:
                for _ in range(len(pending)):
                    self._queue.popleft()
                self.flushed_total += len(pending)
            return len(pending)

    def close(self) -> None:
        """Stop the flusher and write everything that is still queued."""
        with self._condition:
            self._closed = True
            thread = self._thread
            self._condition.notify_all()
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=max(5.0, self.batch_seconds * 2))
        self.flush()

    # -- background flusher ----------------------------------------------

    def _ensure_thread_locked(self) -> None:
        if self._thread is None or not self._thread.is_alive():
            self._thread = threading.Thread(
                target=self._run,
                name="recall-audit-flush",
                daemon=True,
            )
            self._thread.start()

    def _run(self) -> None:
        while True:
            with self._condition:
                if self._closed:
                    return
                if len(self._queue) < self.batch_rows:
                    self._condition.wait(timeout=self.batch_seconds)
                if self._closed:
                    return
                if not self._queue:
                    continue
            try:
                self.flush()
            except Exception:
                LOG.exception(
                    "authorization audit flush failed queued=%d", len(self)
                )
                with self._condition:
                    self._condition.wait(timeout=self.batch_seconds)
