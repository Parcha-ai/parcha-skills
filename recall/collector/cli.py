from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from .collector import Collector
from client.mac import (
    CanonicalArchiveClient,
    CanonicalBrainWriter,
    load_file_token,
)
from privacy.policy import AgenticJudge, PrivacyPolicy, load_scoped_virtual_key


def token_from_file(path: str) -> str:
    return load_file_token(Path(path))


def main() -> None:
    parser = argparse.ArgumentParser(description="Acknowledgement-gated Recall history collector")
    parser.add_argument("command", choices=("scan", "flush", "run", "watch", "doctor", "locate", "recover"))
    parser.add_argument("--harness", choices=("claude", "codex"), required=True)
    parser.add_argument("--root", required=True)
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--spool", required=True)
    parser.add_argument("--endpoint", default=os.environ.get("RECALL_ENDPOINT", ""))
    parser.add_argument("--token-file", default=os.environ.get("RECALL_COLLECTOR_TOKEN_FILE", ""))
    parser.add_argument("--principal-id", default="owner")
    parser.add_argument("--visibility", choices=("private", "shared"), default="private")
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.environ.get("RECALL_INTERVAL_SECONDS", "30")),
    )
    parser.add_argument(
        "--max-scan-records",
        type=int,
        default=int(os.environ.get("RECALL_MAX_SCAN_RECORDS", "1000")),
    )
    parser.add_argument(
        "--max-scan-seconds",
        type=float,
        default=float(os.environ.get("RECALL_MAX_SCAN_SECONDS", "20")),
    )
    parser.add_argument(
        "--archive-workers",
        type=int,
        default=int(os.environ.get("RECALL_ARCHIVE_WORKERS", "2")),
    )
    parser.add_argument(
        "--bulk-manifest-archive",
        action="store_true",
        default=os.environ.get("RECALL_BULK_MANIFEST_ARCHIVE") == "1",
    )
    parser.add_argument(
        "--bulk-bundle-records",
        type=int,
        default=int(os.environ.get("RECALL_BULK_BUNDLE_RECORDS", "500")),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=int(os.environ.get("RECALL_BATCH_SIZE", "500")),
    )
    parser.add_argument("--shard-count", type=int, default=1)
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--receipt")
    parser.add_argument("--privacy-mode", choices=("off", "scrub", "drop"), default=os.environ.get("RECALL_PRIVACY_MODE", "off"))
    parser.add_argument("--privacy-judge-base-url", default=os.environ.get("RECALL_PRIVACY_JUDGE_BASE_URL"))
    parser.add_argument("--privacy-judge-key-file", default=os.environ.get("RECALL_PRIVACY_JUDGE_KEY_FILE"))
    parser.add_argument("--privacy-judge-model", default=os.environ.get("RECALL_PRIVACY_JUDGE_MODEL"))
    parser.add_argument("--privacy-judge-failure", choices=("drop", "ignore"), default="drop")
    args = parser.parse_args()
    if args.command in {"flush", "run", "watch"} and (not args.endpoint or not args.token_file):
        parser.error("--endpoint and --token-file are required for network commands")
    judge_values = (args.privacy_judge_base_url, args.privacy_judge_key_file, args.privacy_judge_model)
    if any(judge_values) and not all(judge_values):
        parser.error("privacy judge requires base URL, private virtual-key file, and model")
    judge = AgenticJudge(
        base_url=args.privacy_judge_base_url,
        virtual_key=load_scoped_virtual_key(Path(args.privacy_judge_key_file)),
        model=args.privacy_judge_model,
    ) if all(judge_values) else None
    privacy = PrivacyPolicy(mode=args.privacy_mode, judge=judge, judge_failure=args.privacy_judge_failure)
    token = token_from_file(args.token_file) if args.token_file else "unused"
    canonical = None
    if os.environ.get("RECALL_CANONICAL_V2_ENABLED") == "1":
        tenant_id = os.environ.get("RECALL_TENANT_ID")
        principal_id = os.environ.get("RECALL_PRINCIPAL_ID")
        if (
            not tenant_id
            or not principal_id
            or principal_id != args.principal_id
            or args.visibility != "private"
        ):
            parser.error("canonical collector identity is incomplete")
        common = {
            "endpoint": args.endpoint,
            "token": token,
            "source_id": args.source_id,
            "tenant_id": tenant_id,
            "principal_id": principal_id,
        }
        canonical = (
            CanonicalBrainWriter(**common),
            CanonicalArchiveClient(**common),
            tenant_id,
        )
    collector = Collector(
        root=Path(args.root), harness=args.harness, source_id=args.source_id,
        spool_path=Path(args.spool), endpoint=args.endpoint or "http://127.0.0.1:1",
        token=token,
        principal_id=args.principal_id,
        visibility=args.visibility,
        privacy=privacy,
        brain_writer=canonical[0] if canonical else None,
        archive=canonical[1] if canonical else None,
        tenant_id=canonical[2] if canonical else None,
        archive_workers=args.archive_workers,
        max_scan_records=args.max_scan_records,
        max_scan_seconds=args.max_scan_seconds,
        bulk_manifest_archive=args.bulk_manifest_archive,
        bulk_bundle_records=args.bulk_bundle_records,
        batch_size=args.batch_size,
    )
    if args.shard_count < 1 or not 0 <= args.shard_index < args.shard_count:
        parser.error("shard index must be within shard count")
    collector.shard_count = args.shard_count
    collector.shard_index = args.shard_index
    try:
        if args.command == "scan":
            print(json.dumps(collector.scan(), sort_keys=True))
        elif args.command == "flush":
            print(json.dumps(collector.flush(), sort_keys=True))
        elif args.command == "run":
            print(json.dumps({
                "scan": collector.scan(), "flush": collector.flush(),
                "doctor": collector.doctor(include_dead_letters=False),
            }, sort_keys=True))
        elif args.command == "doctor":
            print(json.dumps(collector.doctor(), sort_keys=True))
        elif args.command == "recover":
            print(json.dumps(collector.recover_dead_payloads(), sort_keys=True))
        elif args.command == "locate":
            if not args.receipt:
                parser.error("--receipt is required for locate")
            print(json.dumps(collector.locate_receipt(args.receipt), sort_keys=True))
        else:
            while True:
                print(json.dumps({"scan": collector.scan(), "flush": collector.flush(), "doctor": collector.doctor(include_dead_letters=False)}, sort_keys=True), flush=True)
                time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        collector.close()


if __name__ == "__main__":
    main()
