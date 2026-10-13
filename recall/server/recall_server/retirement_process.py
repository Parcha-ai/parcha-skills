"""A single retirement invocation with one database connection, then exit."""

from __future__ import annotations

import json
import os
from pathlib import Path
import resource
import signal
import sys
import time

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .archive_runtime import build_evidence_archive_store
from .db import BrainStore
from .retirement_maintenance import RetirementMaintenance


class RetirementStore:
    _execute_bounded = staticmethod(BrainStore._execute_bounded)
    _set_statement_deadline = staticmethod(BrainStore._set_statement_deadline)

    def __init__(self, dsn):
        self.pool = ConnectionPool(
            dsn,
            kwargs={
                "row_factory": dict_row,
                "connect_timeout": 3,
                "application_name": "recall-retirement",
            },
            min_size=0,
            max_size=1,
            timeout=5,
            max_idle=30,
            open=True,
            name="recall-retirement",
        )

    def connect(self):
        return self.pool.connection()

    def close(self):
        self.pool.close(timeout=2)


def peak_rss_kib():
    # getrusage().ru_maxrss can retain the parent's pre-exec high-water mark.
    # /proc describes this Linux worker process's current executable lifetime.
    for line in Path("/proc/self/status").read_text().splitlines():
        if line.startswith("VmHWM:"):
            _, value, unit = line.split()
            if unit == "kB":
                return int(value)
    raise RuntimeError("retirement process memory measurement unavailable")


def main():
    raw = sys.stdin.buffer.read(16385)
    if len(raw) > 16384:
        raise ValueError("retirement configuration too large")
    configuration = json.loads(raw)
    if set(configuration) != {"dsn", "tenant_id", "seconds"}:
        raise ValueError("retirement configuration invalid")
    seconds = configuration["seconds"]
    if type(seconds) not in (int, float) or not 0 < seconds <= 300:
        raise ValueError("retirement work budget invalid")
    if os.environ.get("RECALL_CHUNK_BODY_READS") != "archive":
        raise ValueError("archive body reads required")

    def expired(*_args):
        raise TimeoutError("retirement child deadline")

    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, seconds + 5)
    started = time.monotonic()
    store = RetirementStore(configuration["dsn"])
    try:
        archive = build_evidence_archive_store(deadline_reads=True)
        if archive is None:
            raise ValueError("retirement archive not configured")
        owner = RetirementMaintenance(
            store,
            archive,
            tenant_id=configuration["tenant_id"],
            enabled=True,
            seconds=seconds,
            max_archive_bytes=64 * 1024**3,
            catalog_archive_budget=True,
        )
        result = owner.cycle(busy=False)
        usage = resource.getrusage(resource.RUSAGE_SELF)
        result.update(
            elapsed_ms=round((time.monotonic() - started) * 1000),
            peak_rss_kib=peak_rss_kib(),
            cpu_seconds=usage.ru_utime + usage.ru_stime,
        )
        print(json.dumps(result), flush=True)
    finally:
        store.close()
        signal.setitimer(signal.ITIMER_REAL, 0)


if __name__ == "__main__":
    main()
