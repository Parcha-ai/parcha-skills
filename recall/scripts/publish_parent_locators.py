#!/usr/bin/env python3
"""Prove one explicit parent and publish only NULL archive positions.

Dry proof is the default. --apply repeats proof before bounded metadata writes.
Retirement must be absent or disabled. Wait at least 60 seconds after the final
publication before separately enabling body retirement. This command never
clears bodies, enrolls a parent, or enables retirement.
"""

import argparse
import json
import math
import os
from pathlib import Path
import sys
import time

RECALL = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(RECALL), str(RECALL / "server")]
from recall_server.archive_runtime import build_evidence_archive_store
from recall_server.chunk_retirement import (
    ParentRetirementLimits,
    read_private_plan,
    write_private_plan,
)
from recall_server.db import BrainStore
from recall_server.streaming_locators import (
    LocatorPublicationError,
    publish_parent_locators,
)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("tenant-id", "source-id", "native-parent-id", "owner-principal-id"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--reviewed-report",
        type=Path,
        help="Optional prior private report identity; fresh proof is still required",
    )
    parser.add_argument(
        "--report-file",
        type=Path,
        help="New private0600 report; advisory, never accepted as proof",
    )
    parser.add_argument("--batch-documents", type=int, default=64)
    parser.add_argument("--max-batches", type=int, default=1000)
    parser.add_argument("--max-documents", type=int, default=2_000_000)
    parser.add_argument("--max-chunks", type=int, default=8_000_000)
    parser.add_argument("--max-archive-bytes", type=int, default=8 * 1024**3)
    parser.add_argument("--max-spool-bytes", type=int, default=1024**3)
    parser.add_argument("--timeout-seconds", type=float, default=300)
    args = parser.parse_args(argv)
    store = None
    try:
        if (
            not math.isfinite(args.timeout_seconds)
            or not 0 < args.timeout_seconds <= 3600
        ):
            raise LocatorPublicationError("locator_publication_deadline_invalid")
        if args.report_file is not None and args.report_file.exists():
            raise LocatorPublicationError("locator_publication_report_exists")
        limits = ParentRetirementLimits(
            batch_documents=args.batch_documents,
            max_batches=args.max_batches,
            max_documents=args.max_documents,
            max_chunks=args.max_chunks,
            max_archive_bytes=args.max_archive_bytes,
            max_spool_bytes=args.max_spool_bytes,
        )
        store = BrainStore(os.environ["RECALL_DATABASE_URL"])
        archive = build_evidence_archive_store(deadline_reads=True)
        result = publish_parent_locators(
            store,
            archive,
            tenant_id=args.tenant_id,
            source_id=args.source_id,
            native_parent_id=args.native_parent_id,
            owner_principal_id=args.owner_principal_id,
            apply=args.apply,
            reviewed_plan=(
                read_private_plan(args.reviewed_report)["plan"]
                if args.reviewed_report is not None
                else None
            ),
            limits=limits,
            deadline_at=time.monotonic() + args.timeout_seconds,
        )
        public = {key: value for key, value in result.items() if key != "plan"}
        public["proof_sha256"] = result["plan"]["proof_sha256"]
        print(json.dumps(public, sort_keys=True))
        if args.report_file is not None:
            write_private_plan(args.report_file, result)
        return 0 if result["complete"] else 2
    except LocatorPublicationError as error:
        print(
            json.dumps(
                dict(
                    error=str(error),
                    committed=error.committed,
                    commit_unknown=error.commit_unknown,
                )
            ),
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            json.dumps(dict(error="locator_publication_unavailable")), file=sys.stderr
        )
        return 1
    finally:
        if store is not None:
            store.close()


if __name__ == "__main__":
    raise SystemExit(main())
