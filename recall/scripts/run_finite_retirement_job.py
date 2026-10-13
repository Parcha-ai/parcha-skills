#!/usr/bin/env python3
# ruff: noqa: E402  # Direct scripts add the repository modules to sys.path.
"""Host journal + pinned Render job, using only the existing Render operator key.

Capture one durable metadata page or enroll one sealed quantum. No direct
PostgreSQL network access from the host; no journal on ephemeral Render storage.
The caller refreshes the serving/provider checkpoint before each invocation.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys

RECALL = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(RECALL), str(RECALL / "server")]
from recall_server.retirement_campaign import Campaign, _write, seal_inventory
from recall_server.retirement_jobs import RenderJob, RemoteEnrollmentOwner, package_body


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("capture", "enroll", "status"))
    parser.add_argument("--config", type=Path)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--inventory-sha256")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--tenant-id")
    parser.add_argument("--principal-id")
    parser.add_argument("--source-id")
    parser.add_argument("--after-page", type=Path)
    parser.add_argument("--after-page-sha256")
    args = parser.parse_args(argv)
    if not 1 <= args.limit <= (100 if args.operation == "capture" else 25):
        parser.error(
            "capture accepts 1–100 considered parents; enrollment accepts 1–25"
        )
    if args.operation == "enroll" and not (args.inventory_sha256 and args.apply):
        parser.error("enroll requires --apply and exact --inventory-sha256")
    if args.operation == "capture" and (
        args.apply or not (args.tenant_id and args.principal_id and args.source_id)
    ):
        parser.error(
            "capture requires explicit tenant/principal/source and cannot apply"
        )
    if args.operation != "status" and args.config is None:
        parser.error("capture/enroll require --config")
    if args.operation == "status":
        if not args.inventory_sha256 or args.apply:
            parser.error("status requires --inventory-sha256 and cannot apply")
        inventory = json.loads(args.inventory.read_bytes())
        with Campaign(args.state_dir, inventory, args.inventory_sha256) as campaign:
            print(json.dumps(campaign.report(), sort_keys=True))
        return 0
    os.umask(0o077)
    config = json.loads(args.config.read_bytes())
    if args.operation == "capture":
        after = None
        predecessor = None
        if args.after_page:
            raw = args.after_page.read_bytes()
            if hashlib.sha256(raw).hexdigest() != args.after_page_sha256:
                raise ValueError("prior_page_digest")
            previous = json.loads(raw)
            if (
                previous["scope"]
                != dict(
                    tenant_id=args.tenant_id,
                    principal_id=args.principal_id,
                    source_ids=[args.source_id],
                )
                or not previous["more"]
            ):
                raise ValueError("prior_page_scope_or_exhausted")
            after = previous["next_cursor"]
            predecessor = args.after_page_sha256
        payload = dict(
            operation="capture",
            tenant=args.tenant_id,
            principal=args.principal_id,
            source=args.source_id,
            limit=args.limit,
            after=after,
        )
        body, pin = package_body(config, payload)
        args.state_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
        _write(
            args.state_dir / "intent.json",
            dict(payload=payload, pin=pin, predecessor_sha256=predecessor),
        )
        counter = 0

        def emit(name, value):
            nonlocal counter
            _write(
                args.state_dir / f"event-{counter:05d}.json",
                dict(name=name, value=value),
            )
            counter += 1

        page = RenderJob(config).run(body, pin, emit)
        _write(args.state_dir / "page.json", page)
        if page["plans"]:
            inventory = seal_inventory(page["plans"])
            _write(args.inventory, inventory)
        else:
            inventory = None
        print(
            json.dumps(
                dict(
                    considered=len(page["considered"]),
                    excluded=len(page["excluded"]),
                    more=page["more"],
                    next_cursor=page["next_cursor"],
                    page_sha256=hashlib.sha256(
                        (args.state_dir / "page.json").read_bytes()
                    ).hexdigest(),
                    inventory_sha256=inventory["sha256"] if inventory else None,
                )
            )
        )
    else:
        inventory = json.loads(args.inventory.read_bytes())
        with Campaign(args.state_dir, inventory, args.inventory_sha256) as campaign:
            campaign.quantum(RemoteEnrollmentOwner(config), limit=args.limit)
            report = campaign.report()
            print(json.dumps(report, sort_keys=True))
            return 2 if report["reconciliation_required"] else 0
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        # Unclassified provider exceptions can contain credentials or job bodies.
        print(
            json.dumps(
                dict(
                    error="retirement_job_requires_inspection",
                    error_class=type(error).__name__,
                )
            ),
            file=sys.stderr,
        )
        raise SystemExit(1)
