#!/usr/bin/env python3
"""Prove bounded locator coverage; optionally fill only verified NULL positions."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import time

SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVER.parent))
sys.path.insert(0, str(SERVER))
from recall_server.archive_runtime import build_evidence_archive_store
from recall_server.db import BrainStore, SearchDeadlineExceeded
from recall_server.locator_backfill_plan import LocatorPlanError, PlanLimits, apply_parent, plan_parent, select_parents


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true', help='rerun proof and fill only verified NULL positions')
    parser.add_argument('--tenant', required=True)
    parser.add_argument('--source')
    parser.add_argument('--limit', type=int, default=1, help='parent page size, 1..100')
    parser.add_argument('--max-bytes', type=int, default=256 * 1024**2, help='total archive bytes, at most 8 GiB')
    parser.add_argument('--seconds', type=int, default=60, help='whole run budget, 1..3600 seconds')
    parser.add_argument('--resume', type=Path, help='prior private report; resume its next_cursor')
    parser.add_argument('--output', required=True, type=Path, help='new private report file (0600, never overwritten)')
    args = parser.parse_args()
    if not 1 <= args.seconds <= 3600 or not 1 <= args.limit <= 100:
        parser.error('seconds must be 1..3600; limit must be 1..100')
    try:
        PlanLimits(max_bytes=args.max_bytes)
        after = None
        if args.resume:
            previous = json.loads(args.resume.read_text())
            if previous['tenant'] != args.tenant or previous['source'] != args.source:
                parser.error('resume scope differs from requested scope')
            if args.apply and previous['mode'] != 'apply_verified_positions':
                parser.error('apply resume requires a prior apply report; start a fresh apply for a dry-run scope')
            if previous['next_cursor'] is None:
                parser.error('prior report has no continuation')
            after = tuple(previous['next_cursor'])
        dsn = os.environ.get('RECALL_DATABASE_URL')
        if not dsn:
            parser.error('RECALL_DATABASE_URL is required')
        # O_EXCL prevents following an existing symlink or overwriting a report.
        descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except (ValueError, KeyError, TypeError, OSError):
        parser.error('invalid limits/resume or output file unavailable')
    report = dict(mode='apply_verified_positions' if args.apply else 'dry_run_only',
                  tenant=args.tenant, source=args.source, parents=[], next_cursor=list(after or ('', '')),
                  archive_gets=0, archive_bytes=0, proposed_documents=0, applied_documents=0, errors=0, stopped=None)
    store = None
    try:
        deadline = time.monotonic() + args.seconds
        store = BrainStore(dsn)
        archive = build_evidence_archive_store(deadline_reads=True)
        parents, more = select_parents(store, tenant_id=args.tenant, source_id=args.source,
                                      after=after, limit=args.limit, deadline_at=deadline)
        for parent in parents:
            if report['proposed_documents'] >= 50_000:
                report['stopped'] = 'proposal_count_budget_exhausted'
                break
            remaining = args.max_bytes - report['archive_bytes']
            if remaining < 1:
                report['stopped'] = 'archive_budget_exhausted'
                break
            entry = dict(parent)
            try:
                operation = apply_parent if args.apply else plan_parent
                entry.update(operation(store, archive, tenant_id=args.tenant,
                    source_id=parent['source_id'], native_parent_id=parent['native_parent_id'],
                    limits=PlanLimits(max_bytes=remaining), deadline_at=deadline))
                report['proposed_documents'] += len(entry['changes'])
                report['applied_documents'] += entry.get('applied_documents', 0)
            except SearchDeadlineExceeded as error:
                report['archive_gets'] += getattr(error, 'archive_gets', 0)
                report['archive_bytes'] += getattr(error, 'archive_bytes', 0)
                raise
            except LocatorPlanError as error:
                entry.update(status='failed', error=str(error), archive_gets=getattr(error, 'archive_gets', 0),
                             archive_bytes=getattr(error, 'archive_bytes', 0),
                             expected_archive_gets=getattr(error, 'expected_archive_gets', None),
                             expected_archive_bytes=getattr(error, 'expected_archive_bytes', None))
                report['errors'] += 1
            report['archive_gets'] += entry['archive_gets']
            report['archive_bytes'] += entry['archive_bytes']
            report['parents'].append(entry)
            if args.apply and entry['status'] == 'failed':
                report['stopped'] = entry['error']
                break
            report['next_cursor'] = [parent['source_id'], parent['native_parent_id']]
        else:
            if not more:
                report['next_cursor'] = None
    except SearchDeadlineExceeded:
        report['stopped'] = 'deadline_exceeded'
    except KeyboardInterrupt:
        report['stopped'] = 'interrupted'
    except Exception:
        report['stopped'] = 'planner_unavailable'
    finally:
        if store is not None:
            try:
                store.close()
            except Exception:
                report['stopped'] = report['stopped'] or 'store_close_failed'
        with os.fdopen(descriptor, 'w') as output:
            json.dump(report, output, sort_keys=True)
            output.write('\n')
    print(json.dumps({key: report[key] for key in (
        'mode', 'archive_gets', 'archive_bytes', 'proposed_documents', 'applied_documents', 'errors', 'stopped')}, sort_keys=True))
    return 1 if report['errors'] or report['stopped'] else 0


if __name__ == '__main__':
    raise SystemExit(main())
