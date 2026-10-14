#!/usr/bin/env python3
"""Packaged-runtime C6D proof for offline Claude and Codex recovery."""
from __future__ import annotations

import argparse
import ctypes
import ctypes.util
import json
import shutil
import sys
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

from client.mac import BrainClient, MemoryClient, load_keychain_token, store_keychain_token
from collector.collector import Collector


ITEM_NOT_FOUND = -25300


def delete_keychain_token(service: str, account: str) -> int:
    security = ctypes.CDLL(ctypes.util.find_library("Security"))
    security.SecKeychainFindGenericPassword.restype = ctypes.c_int32
    security.SecKeychainItemDelete.restype = ctypes.c_int32
    service_bytes, account_bytes = service.encode(), account.encode()
    item = ctypes.c_void_p()
    status = security.SecKeychainFindGenericPassword(
        None,
        len(service_bytes), ctypes.c_char_p(service_bytes),
        len(account_bytes), ctypes.c_char_p(account_bytes),
        None, None, ctypes.byref(item),
    )
    if status == 0:
        try:
            status = security.SecKeychainItemDelete(item)
        finally:
            core = ctypes.CDLL(ctypes.util.find_library("CoreFoundation"))
            core.CFRelease.argtypes = [ctypes.c_void_p]
            core.CFRelease(item)
    return status


def fixture(harness: str, marker: str, timestamp: str) -> dict:
    if harness == "claude":
        return {"type": "user", "timestamp": timestamp, "message": {"content": marker}}
    return {
        "type": "response_item",
        "timestamp": timestamp,
        "payload": {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": marker}],
        },
    }


def rank_for(result: dict, receipt: str) -> int:
    event_receipt = receipt.split("#", 1)[0]
    for rank, item in enumerate(result["results"], 1):
        if item["receipt"].split("#", 1)[0] == event_receipt:
            return rank
    raise AssertionError("collector receipt was not returned by source-scoped search")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--keychain-service", required=True)
    parser.add_argument("--claude-source", required=True)
    parser.add_argument("--codex-source", required=True)
    parser.add_argument("--nonce", required=True)
    parser.add_argument("--workspace", type=Path, required=True)
    args = parser.parse_args()

    credentials = json.load(sys.stdin)
    if not isinstance(credentials, list) or len(credentials) != 2:
        raise ValueError("stdin must contain Claude and Codex credential strings")
    if not all(isinstance(value, str) and value for value in credentials):
        raise ValueError("credentials must be non-empty strings")

    sources = {"claude": args.claude_source, "codex": args.codex_source}
    tokens = dict(zip(("claude", "codex"), credentials, strict=True))
    root = args.workspace / "synthetic-collectors"
    receipts: dict[str, str] = {}
    tombstoned: set[str] = set()
    result = None
    try:
        timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        for harness in ("claude", "codex"):
            store_keychain_token(args.keychain_service, sources[harness], tokens[harness])
            assert load_keychain_token(args.keychain_service, sources[harness]) == tokens[harness]
            harness_root = root / harness
            harness_root.mkdir(parents=True)
            path = harness_root / ("rollout-synthetic.jsonl" if harness == "codex" else "synthetic.jsonl")
            marker = f"c6d-{harness}-offline-{args.nonce}"
            path.write_text(json.dumps(fixture(harness, marker, timestamp)) + "\n")
            spool = root / f"{harness}.db"

            offline = Collector(
                root=harness_root,
                harness=harness,
                source_id=sources[harness],
                spool_path=spool,
                endpoint="http://127.0.0.1:1",
                token=tokens[harness],
                principal_id="owner",
                visibility="private",
            )
            try:
                scan = offline.scan()
                assert scan["records_queued"] == 1
                assert offline.doctor()["committed_files"] == 0
                failed = offline.flush()
                assert failed["acked"] == 0 and failed["errors"] > 0
                assert offline.doctor()["pending"] == 1
                assert offline.doctor()["committed_files"] == 0
            finally:
                offline.close()

            online = Collector(
                root=harness_root,
                harness=harness,
                source_id=sources[harness],
                spool_path=spool,
                endpoint=args.endpoint,
                token=tokens[harness],
                principal_id="owner",
                visibility="private",
            )
            try:
                recovered = online.flush()
                assert recovered["acked"] == 1
                doctor = online.doctor()
                assert doctor["pending"] == 0
                assert doctor["acked"] == 1
                assert doctor["committed_files"] == 1
                receipt = online.db.execute(
                    "SELECT receipt FROM outbox WHERE state='acked'"
                ).fetchone()["receipt"]
                receipts[harness] = receipt
                replay = online.flush()
                assert replay["acked"] == 0 and replay["batches"] == 0
                online.scan()
                assert online.flush()["acked"] == 0

                client = BrainClient(
                    endpoint=args.endpoint,
                    token=tokens[harness],
                    source_id=sources[harness],
                    principal_id="owner",
                    visibility="private",
                )
                central = client.doctor()
                assert central["source_events"] == 1 and central["live_items"] == 1
                assert rank_for(client.search(marker, limit=5), receipt) == 1
                resolved = client.resolve(receipt)
                assert marker in json.dumps(resolved)

                path.unlink()
                deletion_scan = online.scan()
                assert deletion_scan["tombstones_queued"] == 0
                assert online.flush()["acked"] == 0
                central = client.doctor()
                assert central["source_events"] == 1 and central["live_items"] == 1
                assert rank_for(client.search(marker, limit=5), receipt) == 1
                assert marker in json.dumps(client.resolve(receipt))
                # Explicit owner deletion remains supported after local cleanup.
                deletion = MemoryClient(
                    endpoint=args.endpoint, token=tokens[harness],
                    source_id=sources[harness], principal_id="owner", visibility="private",
                ).delete(receipt)
                assert deletion["acknowledgement"]["status"] == "committed"
                tombstoned.add(harness)
                central = client.doctor()
                assert central["source_events"] == 2 and central["live_items"] == 0
                assert client.search(marker, limit=5)["results"] == []
                try:
                    client.resolve(receipt)
                except urllib.error.HTTPError as error:
                    assert error.code == 404
                else:
                    raise AssertionError("explicit deletion did not hide old receipt")
            finally:
                online.close()

        result = {
            "status": "pass",
            "summary": {
                "claude_offline_queued": 1,
                "codex_offline_queued": 1,
                "offline_committed_files": 0,
                "claude_recovered_exactly_once": True,
                "codex_recovered_exactly_once": True,
                "claude_rank": 1,
                "codex_rank": 1,
                "receipts_exact": 2,
                "replay_added_events": 0,
                "tombstones": 2,
                "local_cleanup_retained_records": 2,
                "live_items_after_rollback": 0,
                "credential_bytes_rendered": False,
            },
        }
    finally:
        for harness, receipt in receipts.items():
            if harness not in tombstoned:
                try:
                    MemoryClient(
                        endpoint=args.endpoint,
                        token=tokens[harness],
                        source_id=sources[harness],
                        principal_id="owner",
                        visibility="private",
                    ).delete(receipt)
                except Exception:
                    pass
        statuses = [
            delete_keychain_token(args.keychain_service, sources[harness])
            for harness in ("claude", "codex")
        ]
        if any(status not in {0, ITEM_NOT_FOUND} for status in statuses):
            raise RuntimeError(f"Keychain cleanup failed with OSStatus values {statuses}")
        shutil.rmtree(root, ignore_errors=True)

    for harness in ("claude", "codex"):
        try:
            load_keychain_token(args.keychain_service, sources[harness])
        except RuntimeError as error:
            if f"OSStatus {ITEM_NOT_FOUND}" not in str(error):
                raise
        else:
            raise AssertionError("Keychain credential residue remains")
    assert not root.exists()
    assert result is not None
    result["summary"]["keychain_residue"] = 0
    result["summary"]["spool_and_fixture_residue"] = 0
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
