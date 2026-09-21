#!/usr/bin/env python3
"""Prove one exact parent once, then retire bounded batches with resumable metadata.

Enable is a separate explicit scope action. Restore all retired current bodies
before disabling archive reads; restoration also disables this parent job.
"""
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
from recall_server.chunk_retirement import (ChunkRetirementError, ParentRetirementLimits,
    read_private_plan, retire_parent_chunks, set_parent_retirement_enabled, write_private_plan)
from recall_server.db import BrainStore


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=('plan', 'apply', 'enable', 'disable'))
    parser.add_argument('--tenant-id', required=True)
    parser.add_argument('--source-id', required=True)
    parser.add_argument('--native-parent-id', required=True)
    parser.add_argument('--plan-file', type=Path, help='Private 0600 plan: new file for plan; reviewed file for apply')
    parser.add_argument('--batch-documents', type=int, default=64)
    parser.add_argument('--max-documents', type=int, default=2_000_000)
    parser.add_argument('--max-chunks', type=int, default=8_000_000)
    parser.add_argument('--hash-bytes', type=int, default=8 * 1024**2)
    parser.add_argument('--max-batches', type=int, default=1000)
    parser.add_argument('--max-clear-bytes', type=int, default=1024**3)
    parser.add_argument('--max-archive-bytes', type=int, default=8 * 1024**3)
    parser.add_argument('--max-spool-bytes', type=int, default=1024**3)
    parser.add_argument('--timeout-seconds', type=float, default=300)
    args = parser.parse_args(argv)
    if args.operation in {'plan', 'apply'} and args.plan_file is None:
        parser.error('--plan-file is required for plan and apply')
    store = None
    try:
        if not math.isfinite(args.timeout_seconds) or not 0 < args.timeout_seconds <= 3600:
            raise ChunkRetirementError('parent_retirement_deadline_invalid')
        deadline_at = time.monotonic() + args.timeout_seconds
        limits = ParentRetirementLimits(batch_documents=args.batch_documents, hash_bytes=args.hash_bytes,
            max_documents=args.max_documents, max_chunks=args.max_chunks,
            max_batches=args.max_batches, max_clear_bytes=args.max_clear_bytes,
            max_archive_bytes=args.max_archive_bytes, max_spool_bytes=args.max_spool_bytes)
        scope = dict(tenant_id=args.tenant_id, source_id=args.source_id, native_parent_id=args.native_parent_id)
        plan = read_private_plan(args.plan_file) if args.operation == 'apply' else None
        store = BrainStore(os.environ['RECALL_DATABASE_URL'])
        if args.operation in {'enable', 'disable'}:
            set_parent_retirement_enabled(store, **scope, enabled=args.operation == 'enable', deadline_at=deadline_at)
            print(json.dumps(dict(status=args.operation + 'd', parents=1)))
            return 0
        archive = build_evidence_archive_store(deadline_reads=True)
        result = retire_parent_chunks(store, archive, **scope, limits=limits, deadline_at=deadline_at,
                                      apply=args.operation == 'apply', reviewed_plan=plan)
        if args.operation == 'plan':
            write_private_plan(args.plan_file, result['plan'])
        public_fields = {'status', 'current_documents', 'current_chunks', 'eligible_documents', 'eligible_utf8_bytes',
            'excluded', 'archive_gets', 'archive_bytes', 'cleared_documents', 'cleared_chunks',
            'cleared_utf8_bytes', 'hashed_utf8_bytes', 'hash_ms', 'sql_ms', 'batches', 'complete'}
        public = {key: value for key, value in result.items() if key in public_fields}
        public['proof_sha256'] = result['plan']['proof_sha256']
        print(json.dumps(public, sort_keys=True))
        return 0
    except ChunkRetirementError as error:
        print(json.dumps(dict(error=error.error_code, committed=getattr(error, 'committed', {}))), file=sys.stderr)
        return 1
    except Exception:
        print(json.dumps(dict(error='parent_retirement_unavailable')), file=sys.stderr)
        return 1
    finally:
        if store is not None:
            store.close()


if __name__ == '__main__':
    raise SystemExit(main())
