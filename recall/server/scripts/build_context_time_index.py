#!/usr/bin/env python3
"""Inspect or explicitly build the optional session-context timestamp index."""
import argparse
import json
import os
from pathlib import Path
import sys

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
from recall_server.context_time_index import ensure_context_time_index  # noqa:E402


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true', help='attempt one concurrent build; never replace or retry')
    parser.add_argument('--timeout-seconds', type=int, default=900, help='build statement budget,1..1800(default900)')
    args = parser.parse_args(argv)
    if not 1 <= args.timeout_seconds <= 1800:
        parser.error('timeout-seconds must be1..1800')
    dsn = os.environ.get('RECALL_DATABASE_URL')
    if not dsn:
        parser.error('RECALL_DATABASE_URL is required')
    result = ensure_context_time_index(dsn, apply=args.apply, timeout_seconds=args.timeout_seconds)
    print(json.dumps(result, sort_keys=True))
    return int(result['status'] not in ('ready', 'absent') or args.apply and result['status'] != 'ready')


if __name__ == '__main__':
    raise SystemExit(main())
