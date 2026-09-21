#!/usr/bin/env python3
"""Preview explicit current chunks; apply only an identical private reviewed plan."""
import argparse
import json
import math
import os
from pathlib import Path
import sys
import time

RECALL = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(RECALL), str(RECALL / 'server')]
from recall_server.archive_runtime import build_evidence_archive_store
from recall_server.chunk_retirement import (
    ChunkRetirementError, public_report, read_private_plan,
    retire_current_chunks, write_private_plan,
)
from recall_server.db import BrainStore


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tenant-id', required=True)
    parser.add_argument('--source-id', required=True)
    parser.add_argument('--document-id', required=True, action='append', help='Explicit document ID; repeat at most eight times')
    parser.add_argument('--plan-file', required=True, type=Path, help='Private 0600 plan: create on dry-run, reverify on apply')
    parser.add_argument('--apply', action='store_true', help='Apply the reviewed operation; default performs no writes')
    parser.add_argument('--restore', action='store_true', help='Restore exact PG chunk bodies; restore ALL retired targets before disabling archive reads')
    parser.add_argument('--timeout-seconds', type=float, default=20)
    args = parser.parse_args(argv)
    store = None
    try:
        if not math.isfinite(args.timeout_seconds) or not 0 < args.timeout_seconds <= 120:
            raise ChunkRetirementError('chunk_retirement_deadline_invalid')
        deadline_at = time.monotonic() + args.timeout_seconds
        plan = read_private_plan(args.plan_file) if args.apply else None
        store = BrainStore(os.environ['RECALL_DATABASE_URL'])
        archive = build_evidence_archive_store(deadline_reads=True)
        result = retire_current_chunks(store, archive, tenant_id=args.tenant_id, source_id=args.source_id,
            document_ids=tuple(args.document_id), apply=args.apply, reviewed_plan=plan, deadline_at=deadline_at, restore=args.restore)
        if not args.apply:
            write_private_plan(args.plan_file, result['plan'])
        print(json.dumps(public_report(result), sort_keys=True))
        return 0
    except ChunkRetirementError as error:
        print(json.dumps({'error': error.error_code}), file=sys.stderr)
        return 1
    except Exception:
        print(json.dumps({'error': 'chunk_retirement_unavailable'}), file=sys.stderr)
        return 1
    finally:
        if store is not None:
            store.close()


if __name__ == '__main__':
    raise SystemExit(main())
