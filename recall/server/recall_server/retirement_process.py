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
from .db import BrainStore, singleton_receipt_capability
from .turbopuffer_plane import search_plane_from_env
from .retirement_maintenance import RetirementMaintenance


class RetirementStore:
    _set_statement_deadline = staticmethod(BrainStore._set_statement_deadline)
    _execute_bounded = staticmethod(BrainStore._execute_bounded_pipeline)
    _execute_bounded_pipeline = staticmethod(BrainStore._execute_bounded_pipeline)

    def __init__(self, dsn):
        self.verified_singleton_receipts = singleton_receipt_capability()
        self.search_plane = search_plane_from_env()
        pool = ConnectionPool(
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
        try:
            if self.verified_singleton_receipts:
                with pool.connection() as connection:
                    BrainStore.verify_search_plane(self, connection)
        except BaseException:
            pool.close(timeout=2)
            raise
        # No caller can obtain the pool until optional-schema validation passes.
        self.pool = pool

    def connect(self, *, timeout=None):
        return self.pool.connection(timeout=timeout)

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


def _failure(error, phase):
    return dict(
        status="commit_outcome_unknown",
        cleared_documents=0,
        cleared_utf8_bytes=0,
        commit_outcome_unknown=True,
        error_class=type(error).__name__,
        phase=phase,
    )


def main():
    started = time.monotonic()
    store = None
    phase = "configuration"
    failed = False
    try:
        raw = sys.stdin.buffer.read(16385)
        if len(raw) > 16384:
            raise ValueError("retirement configuration too large")
        configuration = json.loads(raw)
        if set(configuration) not in ({"dsn", "tenant_id", "seconds"}, {"dsn", "tenant_id", "seconds", "metadata_mode"}):
            raise ValueError("retirement configuration invalid")
        metadata_mode = configuration.get("metadata_mode", "keep")
        if metadata_mode not in ("keep", "singleton"):
            raise ValueError("invalid retirement metadata mode")
        seconds = configuration["seconds"]
        if type(seconds) not in (int, float) or not 0 < seconds <= 300:
            raise ValueError("retirement work budget invalid")
        if os.environ.get("RECALL_CHUNK_BODY_READS") != "archive":
            raise ValueError("archive body reads required")

        def expired(*_args):
            raise TimeoutError("retirement child deadline")

        signal.signal(signal.SIGALRM, expired)
        signal.setitimer(signal.ITIMER_REAL, seconds + 5)
        phase = "database_setup"
        store = RetirementStore(configuration["dsn"])
        phase = "archive_setup"
        archive = build_evidence_archive_store(deadline_reads=True)
        if archive is None:
            raise ValueError("retirement archive not configured")
        phase = "maintenance"
        owner = RetirementMaintenance(
            store,
            archive,
            tenant_id=configuration["tenant_id"],
            enabled=True,
            seconds=seconds,
            max_parents=1000,
            max_clear_bytes=512 * 1024**2,
            max_archive_bytes=64 * 1024**3,
            catalog_archive_budget=True,
            **({"metadata_mode": metadata_mode} if metadata_mode != "keep" else {}),
        )
        result = owner.cycle(busy=False)
    except Exception as error:
        result, failed = _failure(error, phase), True
    finally:
        try:
            if store is not None:
                store.close()
        except Exception as error:
            result, failed = _failure(error, "shutdown"), True
        signal.setitimer(signal.ITIMER_REAL, 0)
    try:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        result.update(
            elapsed_ms=round((time.monotonic() - started) * 1000),
            peak_rss_kib=peak_rss_kib(),
            cpu_seconds=usage.ru_utime + usage.ru_stime,
        )
    except Exception as error:
        result, failed = _failure(error, "metrics"), True
    print(json.dumps(result), flush=True)
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
