#!/usr/bin/env python3
"""Review/enroll a finite existing cohort, or run bounded enabled-only maintenance.

Enrollment never replaces disabled rows. Run is metadata-only unless --apply is
explicit. Invoke run repeatedly from an existing scheduler; this installs no cron.
"""

import argparse
import json
import math
import os
from pathlib import Path
import signal
import sys
import time

RECALL = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(RECALL), str(RECALL / "server")]
from recall_server.archive_runtime import build_evidence_archive_store
from recall_server.chunk_retirement import (
    ChunkRetirementError,
    ParentRetirementLimits,
    read_private_plan,
    write_private_plan,
)
from recall_server.db import BrainStore
from recall_server.retirement_runner import (
    RetirementScope,
    RunnerLimits,
    enroll_cohort,
    plan_cohort,
    run_retirement,
)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("enroll", "run"))
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--principal-id", required=True)
    parser.add_argument("--source-id", action="append", required=True)
    parser.add_argument("--plan-file", type=Path)
    parser.add_argument(
        "--after-plan-file",
        type=Path,
        help="Prior private enrollment page supplies next_cursor",
    )
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--max-parents", type=int, default=10)
    parser.add_argument("--max-archive-bytes", type=int, default=256 * 1024**2)
    parser.add_argument("--max-clear-bytes", type=int, default=64 * 1024**2)
    parser.add_argument("--max-parent-documents", type=int, default=2_000_000)
    parser.add_argument("--max-parent-chunks", type=int, default=8_000_000)
    parser.add_argument("--max-spool-bytes", type=int, default=1024**3)
    parser.add_argument("--max-batches", type=int, default=1000)
    parser.add_argument("--cooldown-seconds", type=int, default=60)
    parser.add_argument("--timeout-seconds", type=float, default=300)
    args = parser.parse_args(argv)
    if args.operation == "enroll" and args.plan_file is None:
        parser.error("--plan-file is required for enrollment")
    if (
        args.operation == "run"
        and (args.plan_file or args.after_plan_file)
        or args.apply
        and args.after_plan_file
    ):
        parser.error("enrollment paging is only available during metadata planning")
    store = None
    stopped = False

    def stop(*_):
        nonlocal stopped
        stopped = True

    handlers = {}
    try:
        if (
            not math.isfinite(args.timeout_seconds)
            or not 0 < args.timeout_seconds <= 3600
        ):
            raise ChunkRetirementError("parent_retirement_deadline_invalid")
        scope = RetirementScope(
            args.tenant_id, args.principal_id, tuple(args.source_id)
        )
        limits = RunnerLimits(
            args.max_parents,
            args.max_archive_bytes,
            args.max_clear_bytes,
            args.cooldown_seconds,
        )
        parent_limits = ParentRetirementLimits(
            max_documents=args.max_parent_documents,
            max_chunks=args.max_parent_chunks,
            max_spool_bytes=args.max_spool_bytes,
            max_batches=args.max_batches,
        )
        deadline_at = time.monotonic() + args.timeout_seconds
        store = BrainStore(os.environ["RECALL_DATABASE_URL"])
        if args.operation == "enroll":
            if args.apply:
                result = enroll_cohort(
                    store,
                    scope=scope,
                    reviewed_plan=read_private_plan(args.plan_file),
                    deadline_at=deadline_at,
                )
            else:
                previous = (
                    read_private_plan(args.after_plan_file)
                    if args.after_plan_file
                    else None
                )
                if previous is not None and (
                    previous.get("scope")
                    != dict(
                        tenant_id=scope.tenant_id,
                        principal_id=scope.principal_id,
                        source_ids=list(scope.source_ids),
                    )
                    or not previous.get("more")
                ):
                    raise ChunkRetirementError("retirement_runner_request_invalid")
                plan = plan_cohort(
                    store,
                    scope=scope,
                    after=previous["next_cursor"] if previous else None,
                    limit=args.page_size,
                    deadline_at=deadline_at,
                )
                write_private_plan(args.plan_file, plan)
                result = dict(
                    status="enrollment_review",
                    parents=len(plan["parents"]),
                    more=plan["more"],
                    proof_sha256=plan["proof_sha256"],
                )
        else:
            for signum in (signal.SIGINT, signal.SIGTERM):
                handlers[signum] = signal.signal(signum, stop)
            archive = (
                build_evidence_archive_store(deadline_reads=True)
                if args.apply
                else None
            )
            result = run_retirement(
                store,
                archive,
                scope=scope,
                apply=args.apply,
                limits=limits,
                parent_limits=parent_limits,
                deadline_at=deadline_at,
                should_stop=lambda: stopped,
            )
        print(json.dumps(result, sort_keys=True))
        return int(
            bool(
                result.get("errors")
                or result.get("failed_parents")
                or result.get("commit_outcome_unknown")
            )
        )
    except ChunkRetirementError as error:
        print(json.dumps(dict(error=error.error_code)), file=sys.stderr)
        return 1
    except Exception:
        print(json.dumps(dict(error="retirement_runner_unavailable")), file=sys.stderr)
        return 1
    finally:
        for signum, handler in handlers.items():
            signal.signal(signum, handler)
        if store is not None:
            store.close()


if __name__ == "__main__":
    raise SystemExit(main())
