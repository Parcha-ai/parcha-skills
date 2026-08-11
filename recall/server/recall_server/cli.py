from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
import re
import sys
from pathlib import Path

from . import SCHEMA_VERSION
from .app import serve, serve_unix
from .archive import ArchiveError
from .archive_runtime import (
    build_archive_store,
    build_evidence_archive_store,
    probe_archive,
)
from .canonical_retrieval import CanonicalRetrieval
from .capabilities import CapabilityError, probe_database
from .control import ControlPlane, SecretBox
from .db import BrainStore
from .deployment import DeploymentManifestError, load_manifest, preview
from .embedding_worker import run_canonical_embedding_worker
from .evidence_projection import (
    CanonicalEvidenceProjector,
    EvidenceProjectionStore,
)
from .evidence_worker import (
    run_canonical_evidence_worker,
    run_logical_evidence_worker,
)
from .logical_evidence import LogicalEvidenceProjectionStore
from .logical_evidence_projection import CanonicalLogicalEvidenceProjector
from .passage_index import CanonicalPassageProjector
from .passage_projection import PassagePolicy
from .parquet_scan import CanonicalParquetScanProjector
from .passage_worker import run_passage_worker
from .projection_worker import run_projection_worker
from .federation import QUALITY_SCORES, SOURCE_FAMILIES
from .live_providers import (
    LiveProviderError,
    build_live_adapters,
)
from .managed_apply import (
    ApprovalError,
    approval_status,
    load_approvals,
    reconcile_infrastructure,
)
from .managed_worker import run_managed_worker
from .mcp_conformance import (
    ConformanceError,
    McpConformanceConfig,
    run_conformance,
)
from .semantic import SemanticRuntime


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"), format="%(levelname)s %(message)s"
    )
    ap = argparse.ArgumentParser(prog="recall-server")
    ap.add_argument("--dsn", default=os.environ.get("RECALL_DATABASE_URL"))
    sub = ap.add_subparsers(dest="command", required=True)
    sub.add_parser("migrate")
    sub.add_parser("archive-check")
    sub.add_parser("evidence-archive-check")
    publish_duckdb = sub.add_parser("publish-archil-duckdb")
    publish_duckdb.add_argument("--path", type=Path, required=True)
    publish_duckdb.add_argument("--version", required=True)
    publish_duckdb.add_argument("--sha256", required=True)
    capability = sub.add_parser("capability-check")
    capability.add_argument(
        "--profile", choices=("production", "local-fixture"), default="production"
    )
    deployment = sub.add_parser("deployment-preview")
    deployment.add_argument("--manifest", type=Path, required=True)
    approval = sub.add_parser("deployment-approval-check")
    approval.add_argument("--manifest", type=Path, required=True)
    approval.add_argument("--approvals", type=Path, required=True)
    apply = sub.add_parser("deployment-apply")
    apply.add_argument("--manifest", type=Path, required=True)
    apply.add_argument("--approvals", type=Path, required=True)
    apply.add_argument("--planetscale-organization", required=True)
    apply.add_argument("--database-name", required=True)
    apply.add_argument("--render-owner-id", required=True)
    apply.add_argument("--core-name", required=True)
    apply.add_argument("--gateway-name", required=True)
    apply.add_argument("--tailnet-hostname", required=True)
    apply.add_argument("--tailnet-tag", required=True)
    sub.add_parser("rebuild")
    backfill_entities = sub.add_parser("backfill-entities")
    backfill_entities.add_argument("--batch-size", type=int, default=5000)
    backfill_entities.add_argument("--max-batches", type=int)
    backfill_redaction = sub.add_parser("backfill-redaction")
    backfill_redaction.add_argument("--batch-size", type=int, default=5000)
    backfill_redaction.add_argument("--max-batches", type=int)
    backfill_redaction.add_argument("--workers", type=int, default=1)
    backfill_cowork = sub.add_parser("backfill-cowork-sessions")
    backfill_cowork.add_argument("--batch-size", type=int, default=5000)
    backfill_cowork.add_argument("--max-batches", type=int)
    backfill_embeddings = sub.add_parser("backfill-embeddings")
    backfill_embeddings.add_argument("--batch-size", type=int, default=128)
    backfill_embeddings.add_argument("--max-batches", type=int)
    backfill_embeddings.add_argument("--source-id")
    backfill_embeddings.add_argument("--surface")
    backfill_turn_embeddings = sub.add_parser("backfill-turn-embeddings")
    backfill_turn_embeddings.add_argument("--batch-size", type=int, default=128)
    backfill_turn_embeddings.add_argument("--max-batches", type=int)
    backfill_turn_embeddings.add_argument("--source-id")
    canonical_embeddings = sub.add_parser("backfill-canonical-embeddings")
    canonical_embeddings.add_argument("--tenant")
    canonical_embeddings.add_argument("--batch-size", type=int, default=100)
    canonical_embeddings.add_argument("--max-batches", type=int, default=10)
    canonical_embedding_worker = sub.add_parser("canonical-embedding-worker")
    canonical_embedding_worker.add_argument("--tenant")
    canonical_embedding_worker.add_argument("--batch-size", type=int, default=128)
    canonical_embedding_worker.add_argument(
        "--max-batches-per-cycle", type=int, default=10
    )
    canonical_embedding_worker.add_argument("--parallel-tenants", type=int, default=1)
    canonical_embedding_worker.add_argument("--interval-seconds", type=float, default=5)
    canonical_embedding_worker.add_argument("--once", action="store_true")
    canonical_evidence = sub.add_parser("backfill-canonical-evidence")
    canonical_evidence.add_argument("--tenant", required=True)
    canonical_evidence.add_argument("--batch-size", type=int, default=100)
    canonical_evidence.add_argument("--max-batches", type=int, default=10)
    canonical_evidence_worker = sub.add_parser("canonical-evidence-worker")
    canonical_evidence_worker.add_argument("--tenant", required=True)
    canonical_evidence_worker.add_argument("--batch-size", type=int, default=100)
    canonical_evidence_worker.add_argument(
        "--max-batches-per-cycle", type=int, default=10
    )
    canonical_evidence_worker.add_argument("--interval-seconds", type=float, default=5)
    canonical_evidence_worker.add_argument("--once", action="store_true")
    logical_evidence = sub.add_parser("backfill-logical-evidence")
    logical_evidence.add_argument("--tenant", required=True)
    logical_evidence.add_argument(
        "--source",
        help="queue only this exact source within the tenant",
    )
    logical_evidence.add_argument("--batch-size", type=int, default=25)
    logical_evidence.add_argument("--max-batches", type=int, default=10)
    logical_evidence.add_argument("--upload-concurrency", type=int, default=2)
    logical_evidence.add_argument(
        "--cursor-fetch-rows",
        type=int,
        default=10_000,
    )
    logical_evidence.add_argument(
        "--rebuild-existing",
        action="store_true",
        help="queue current logical documents even when a projection already exists",
    )
    logical_evidence_worker = sub.add_parser("logical-evidence-worker")
    logical_evidence_worker.add_argument("--tenant", required=True)
    logical_evidence_worker.add_argument("--batch-size", type=int, default=25)
    logical_evidence_worker.add_argument(
        "--max-batches-per-cycle", type=int, default=10
    )
    logical_evidence_worker.add_argument("--upload-concurrency", type=int, default=2)
    logical_evidence_worker.add_argument(
        "--cursor-fetch-rows",
        type=int,
        default=10_000,
    )
    logical_evidence_worker.add_argument("--interval-seconds", type=float, default=5)
    logical_evidence_worker.add_argument("--once", action="store_true")
    passage_backfill = sub.add_parser("backfill-lossless-passages")
    passage_backfill.add_argument("--tenant", required=True)
    passage_backfill.add_argument("--target-tokens", type=int, default=1024)
    passage_backfill.add_argument("--overlap-tokens", type=int, default=128)
    passage_backfill.add_argument("--batch-size", type=int, default=100)
    passage_backfill.add_argument("--max-batches", type=int, default=10)
    passage_backfill.add_argument("--concurrency", type=int, default=4)
    passage_worker = sub.add_parser("lossless-passage-worker")
    passage_worker.add_argument("--tenant", required=True)
    passage_worker.add_argument("--target-tokens", type=int, default=1024)
    passage_worker.add_argument("--overlap-tokens", type=int, default=128)
    passage_worker.add_argument(
        "--projection-batch-size",
        type=int,
        default=100,
    )
    passage_worker.add_argument(
        "--embedding-batch-size",
        type=int,
        default=128,
    )
    passage_worker.add_argument(
        "--max-batches-per-cycle",
        type=int,
        default=10,
    )
    passage_worker.add_argument("--concurrency", type=int, default=4)
    passage_worker.add_argument(
        "--interval-seconds",
        type=float,
        default=5,
    )
    passage_worker.add_argument("--once", action="store_true")
    parquet_backfill = sub.add_parser("backfill-parquet-scan")
    parquet_backfill.add_argument("--tenant", required=True)
    parquet_backfill.add_argument("--source")
    parquet_backfill.add_argument("--batch-size", type=int, default=4)
    parquet_backfill.add_argument("--max-batches", type=int, default=10)
    projection_worker = sub.add_parser("projection-worker")
    projection_worker.add_argument("--tenant", required=True)
    projection_worker.add_argument("--target-tokens", type=int, default=1024)
    projection_worker.add_argument("--overlap-tokens", type=int, default=128)
    projection_worker.add_argument("--logical-batch-size", type=int, default=25)
    projection_worker.add_argument("--passage-batch-size", type=int, default=100)
    projection_worker.add_argument("--embedding-batch-size", type=int, default=128)
    projection_worker.add_argument("--max-batches-per-cycle", type=int, default=10)
    projection_worker.add_argument("--upload-concurrency", type=int, default=2)
    projection_worker.add_argument("--passage-concurrency", type=int, default=4)
    projection_worker.add_argument("--cursor-fetch-rows", type=int, default=10_000)
    projection_worker.add_argument("--interval-seconds", type=float, default=5)
    projection_worker.add_argument("--once", action="store_true")
    sub.add_parser("export")
    conformance = sub.add_parser("mcp-conformance")
    conformance.add_argument("--config", type=Path, required=True)
    create_token = sub.add_parser("token-create")
    create_token.add_argument("name")
    create_token.add_argument("--source")
    create_token.add_argument(
        "--tenant",
        help="bind a canonical v2 write credential to exactly one tenant",
    )
    create_token.add_argument(
        "--principal",
        help="read every source granted to this principal; writes stay source-bound",
    )
    create_token.add_argument(
        "--capture-origin",
        help="host-bound origin for deliberate MCP capture tools",
    )
    create_token.add_argument(
        "--webhook-privacy-mode",
        choices=("scrub", "drop"),
        help="server-owned privacy mode for a source-scoped webhook capability",
    )
    create_token.add_argument("--scopes", default="read,write")
    create_token.add_argument(
        "--output",
        required=True,
        help="write the one-time plaintext credential to a new mode-0600 file",
    )
    revoke_token = sub.add_parser("token-revoke")
    revoke_token.add_argument("name")
    brain = sub.add_parser("brain-provision")
    brain.add_argument("--organization", required=True)
    brain.add_argument("--kind", choices=("personal", "company"), required=True)
    brain.add_argument("--display-name", required=True)
    brain.add_argument("--tenant", required=True)
    brain.add_argument("--slug", required=True)
    brain.add_argument("--owner-principal", required=True)
    create_mcp_token = sub.add_parser("mcp-token-create")
    create_mcp_token.add_argument("name")
    create_mcp_token.add_argument("--tenant", required=True)
    create_mcp_token.add_argument("--principal", required=True)
    create_mcp_token.add_argument(
        "--principal-kind", choices=("human", "workload"), required=True
    )
    create_mcp_token.add_argument("--scopes", default="read")
    create_mcp_token.add_argument("--expires-in-days", type=int, default=30)
    create_mcp_token.add_argument("--output", required=True)
    revoke_mcp_token = sub.add_parser("mcp-token-revoke")
    revoke_mcp_token.add_argument("name")
    create_admin_token = sub.add_parser("admin-token-create")
    create_admin_token.add_argument("name")
    create_admin_token.add_argument("--principal", required=True)
    create_admin_token.add_argument("--expires-in-days", type=int, default=30)
    create_admin_token.add_argument("--output", required=True)
    revoke_admin_token = sub.add_parser("admin-token-revoke")
    revoke_admin_token.add_argument("name")
    managed_worker = sub.add_parser("managed-worker")
    managed_worker.add_argument(
        "--state-root",
        type=Path,
        default=Path("/var/lib/recall/worker"),
    )
    managed_worker.add_argument("--once", action="store_true")
    managed_worker.add_argument(
        "--interval-seconds",
        type=int,
        default=60,
    )
    source_profile = sub.add_parser("source-profile-set")
    source_profile.add_argument("source_id")
    source_profile.add_argument(
        "--family", choices=sorted(SOURCE_FAMILIES), required=True
    )
    source_profile.add_argument(
        "--quality", choices=sorted(QUALITY_SCORES), required=True
    )
    source_profile.add_argument("--freshness-half-life-days", type=int, required=True)
    source_alias = sub.add_parser("source-alias-set")
    source_alias.add_argument("alias")
    source_alias.add_argument("source_id")
    sub.add_parser("federation-scoreboard")
    server = sub.add_parser("serve")
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=8788)
    server.add_argument("--unix-socket")
    server.add_argument("--require-auth", action="store_true")
    server.add_argument(
        "--capability-profile",
        choices=("production", "local-fixture"),
    )
    args = ap.parse_args()
    if args.command == "deployment-preview":
        try:
            print(json.dumps(preview(load_manifest(args.manifest)), sort_keys=True))
        except DeploymentManifestError:
            print(
                json.dumps({"status": "rejected", "code": "manifest_invalid"}),
                file=sys.stderr,
            )
            raise SystemExit(2) from None
        return
    if args.command == "deployment-approval-check":
        try:
            manifest = load_manifest(args.manifest)
            plan_sha256 = preview(manifest)["plan_sha256"]
            print(
                json.dumps(
                    approval_status(
                        load_approvals(args.approvals, plan_sha256),
                    ),
                    sort_keys=True,
                )
            )
        except (ApprovalError, DeploymentManifestError) as error:
            code = (
                error.code if isinstance(error, ApprovalError) else "manifest_invalid"
            )
            print(json.dumps({"status": "rejected", "code": code}), file=sys.stderr)
            raise SystemExit(2) from None
        return
    if args.command == "deployment-apply":
        try:
            manifest = load_manifest(args.manifest)
            approvals = load_approvals(args.approvals, preview(manifest)["plan_sha256"])
            pending = approval_status(approvals)["pending_gates"]
            if any(gate != "writer-cutover" for gate in pending):
                raise ApprovalError("infrastructure_approval_required")
            adapters = build_live_adapters(
                planetscale_organization=args.planetscale_organization,
                database_name=args.database_name,
                render_owner_id=args.render_owner_id,
                core_name=args.core_name,
                gateway_name=args.gateway_name,
                tailnet_hostname=args.tailnet_hostname,
                tailnet_tag=args.tailnet_tag,
            )
            print(
                json.dumps(
                    reconcile_infrastructure(manifest, approvals, adapters),
                    sort_keys=True,
                )
            )
        except (
            ApprovalError,
            DeploymentManifestError,
            LiveProviderError,
        ) as error:
            code = (
                error.code
                if isinstance(error, (ApprovalError, LiveProviderError))
                else "manifest_invalid"
            )
            print(
                json.dumps({"status": "rejected", "code": code}),
                file=sys.stderr,
            )
            raise SystemExit(2) from None
        return
    if args.command == "mcp-conformance":
        try:
            report = run_conformance(McpConformanceConfig.load(args.config))
            print(json.dumps(report, sort_keys=True))
        except ConformanceError:
            print(
                json.dumps({"status": "rejected", "code": "mcp_conformance_failed"}),
                file=sys.stderr,
            )
            raise SystemExit(2) from None
        return
    if args.command == "archive-check":
        try:
            print(json.dumps(probe_archive(build_archive_store()), sort_keys=True))
        except (ArchiveError, ValueError):
            print(
                json.dumps(
                    {"status": "rejected", "code": "archive_check_failed"},
                    sort_keys=True,
                ),
                file=sys.stderr,
            )
            raise SystemExit(2) from None
        return
    if args.command == "evidence-archive-check":
        try:
            print(
                json.dumps(
                    probe_archive(build_evidence_archive_store()),
                    sort_keys=True,
                )
            )
        except (ArchiveError, ValueError):
            print(
                json.dumps(
                    {"status": "rejected", "code": "evidence_archive_check_failed"},
                    sort_keys=True,
                ),
                file=sys.stderr,
            )
            raise SystemExit(2) from None
        return
    if args.command == "publish-archil-duckdb":
        if (
            not args.path.is_file()
            or re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", args.version) is None
        ):
            raise ValueError("DuckDB tool artifact is invalid")
        payload = args.path.read_bytes()
        digest = hashlib.sha256(payload).hexdigest()
        if (
            not 1_000_000 <= len(payload) <= 100_000_000
            or digest != args.sha256
        ):
            raise ValueError("DuckDB tool artifact is invalid")
        reference = build_evidence_archive_store().put_raw(
            tenant_id="tenant:system:tools",
            source_id="source:system:tools",
            # Archil serverless execution currently runs on aarch64. Keep the
            # architecture in the immutable source identity so an x86 build
            # cannot silently replace the executable used by recall_scan.
            native_id=f"archil-duckdb:{args.version}:linux-arm64",
            payload=payload,
            media_type="application/vnd.duckdb.cli",
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        print(json.dumps({
            "status": "published",
            "version": args.version,
            "object_key": reference["object_key"],
            "content_sha256": reference["content_sha256"],
            "size_bytes": reference["size_bytes"],
        }, sort_keys=True))
        return
    if not args.dsn:
        ap.error("--dsn or RECALL_DATABASE_URL is required")
    if args.command == "capability-check":
        try:
            print(json.dumps(probe_database(args.dsn, args.profile), sort_keys=True))
        except CapabilityError as error:
            print(
                json.dumps({"status": "rejected", "code": error.code}), file=sys.stderr
            )
            raise SystemExit(2) from None
        return
    if args.command == "serve" and args.capability_profile:
        try:
            probe_database(args.dsn, args.capability_profile)
        except CapabilityError as error:
            print(
                json.dumps({"status": "rejected", "code": error.code}), file=sys.stderr
            )
            raise SystemExit(2) from None
    pool_max_size = None
    if args.command in {
        "backfill-lossless-passages",
        "lossless-passage-worker",
    }:
        pool_max_size = max(8, args.concurrency)
    elif args.command == "projection-worker":
        pool_max_size = max(
            8,
            args.passage_concurrency,
            args.upload_concurrency,
        )
    store = BrainStore(
        args.dsn,
        semantic_runtime=SemanticRuntime.from_env(),
        pool_max_size=pool_max_size,
    )
    if args.command == "migrate":
        store.migrate()
        print(json.dumps({"status": "ok", "schema_version": SCHEMA_VERSION}))
    elif args.command == "rebuild":
        print(json.dumps(store.rebuild(), sort_keys=True))
    elif args.command == "managed-worker":
        print(
            json.dumps(
                run_managed_worker(
                    store,
                    state_root=args.state_root,
                    once=args.once,
                    interval_seconds=args.interval_seconds,
                ),
                sort_keys=True,
            )
        )
    elif args.command == "backfill-entities":
        print(
            json.dumps(
                store.backfill_entities(args.batch_size, args.max_batches),
                sort_keys=True,
            )
        )
    elif args.command == "backfill-redaction":
        print(
            json.dumps(
                store.backfill_redaction(
                    args.batch_size,
                    args.max_batches,
                    args.workers,
                ),
                sort_keys=True,
            )
        )
    elif args.command == "backfill-cowork-sessions":
        print(
            json.dumps(
                store.backfill_cowork_sessions(
                    args.batch_size,
                    args.max_batches,
                ),
                sort_keys=True,
            )
        )
    elif args.command == "backfill-embeddings":
        print(
            json.dumps(
                store.embed_pending(
                    args.batch_size,
                    args.max_batches,
                    args.source_id,
                    args.surface,
                ),
                sort_keys=True,
            )
        )
    elif args.command == "backfill-turn-embeddings":
        print(
            json.dumps(
                store.embed_pending_turns(
                    args.batch_size,
                    args.max_batches,
                    args.source_id,
                ),
                sort_keys=True,
            )
        )
    elif args.command == "backfill-canonical-embeddings":
        print(
            json.dumps(
                CanonicalRetrieval(store).embed_pending(
                    tenant_id=args.tenant,
                    batch_size=args.batch_size,
                    max_batches=args.max_batches,
                ),
                sort_keys=True,
            )
        )
    elif args.command == "canonical-embedding-worker":
        print(
            json.dumps(
                run_canonical_embedding_worker(
                    CanonicalRetrieval(store),
                    tenant_id=args.tenant,
                    parallel_tenants=args.parallel_tenants,
                    batch_size=args.batch_size,
                    max_batches_per_cycle=args.max_batches_per_cycle,
                    interval_seconds=args.interval_seconds,
                    once=args.once,
                ),
                sort_keys=True,
            )
        )
    elif args.command in {
        "backfill-canonical-evidence",
        "canonical-evidence-worker",
    }:
        projector = CanonicalEvidenceProjector(
            store,
            EvidenceProjectionStore(build_evidence_archive_store()),
            bound_tenant_id=args.tenant,
        )
        if args.command == "backfill-canonical-evidence":
            result = projector.project_pending(
                tenant_id=args.tenant,
                batch_size=args.batch_size,
                max_batches=args.max_batches,
            )
        else:
            result = run_canonical_evidence_worker(
                projector,
                tenant_id=args.tenant,
                batch_size=args.batch_size,
                max_batches_per_cycle=args.max_batches_per_cycle,
                interval_seconds=args.interval_seconds,
                once=args.once,
            )
        print(json.dumps(result, sort_keys=True))
    elif args.command in {
        "backfill-logical-evidence",
        "logical-evidence-worker",
    }:
        projector = CanonicalLogicalEvidenceProjector(
            store,
            LogicalEvidenceProjectionStore(
                build_evidence_archive_store(),
                part_upload_concurrency=min(4, args.upload_concurrency),
            ),
            bound_tenant_id=args.tenant,
            raw_archive=build_archive_store(),
            cursor_fetch_rows=args.cursor_fetch_rows,
        )
        if args.command == "backfill-logical-evidence":
            seeded = projector.seed_backfill(
                tenant_id=args.tenant,
                source_id=args.source,
                include_existing=args.rebuild_existing,
            )
            result = projector.project_pending(
                tenant_id=args.tenant,
                batch_size=args.batch_size,
                max_batches=args.max_batches,
                upload_concurrency=args.upload_concurrency,
            )
            result = {"seeded": seeded, **result}
        else:
            result = run_logical_evidence_worker(
                projector,
                tenant_id=args.tenant,
                batch_size=args.batch_size,
                max_batches_per_cycle=args.max_batches_per_cycle,
                upload_concurrency=args.upload_concurrency,
                interval_seconds=args.interval_seconds,
                once=args.once,
            )
        print(json.dumps(result, sort_keys=True))
    elif args.command in {
        "backfill-lossless-passages",
        "lossless-passage-worker",
    }:
        projector = CanonicalPassageProjector(
            store,
            LogicalEvidenceProjectionStore(
                build_evidence_archive_store(),
            ),
            policy=PassagePolicy(
                target_tokens=args.target_tokens,
                overlap_tokens=args.overlap_tokens,
            ),
            bound_tenant_id=args.tenant,
        )
        if args.command == "backfill-lossless-passages":
            seeded = projector.seed_backfill(tenant_id=args.tenant)
            result = projector.project_pending(
                tenant_id=args.tenant,
                batch_size=args.batch_size,
                max_batches=args.max_batches,
                concurrency=args.concurrency,
            )
            result = {"seeded": seeded, **result}
        else:
            result = run_passage_worker(
                projector,
                tenant_id=args.tenant,
                projection_batch_size=args.projection_batch_size,
                embedding_batch_size=args.embedding_batch_size,
                max_batches_per_cycle=args.max_batches_per_cycle,
                concurrency=args.concurrency,
                interval_seconds=args.interval_seconds,
                once=args.once,
            )
        print(json.dumps(result, sort_keys=True))
    elif args.command == "backfill-parquet-scan":
        projector = CanonicalParquetScanProjector(
            store,
            LogicalEvidenceProjectionStore(build_evidence_archive_store()),
        )
        seeded = projector.seed_backfill(
            tenant_id=args.tenant,
            source_id=args.source,
        )
        print(
            json.dumps(
                {
                    "seeded": seeded,
                    **projector.project_pending(
                        tenant_id=args.tenant,
                        batch_size=args.batch_size,
                        max_batches=args.max_batches,
                    ),
                },
                sort_keys=True,
            )
        )
    elif args.command == "projection-worker":
        logical = CanonicalLogicalEvidenceProjector(
            store,
            LogicalEvidenceProjectionStore(
                build_evidence_archive_store(),
                part_upload_concurrency=min(4, args.upload_concurrency),
            ),
            bound_tenant_id=args.tenant,
            raw_archive=build_archive_store(),
            cursor_fetch_rows=args.cursor_fetch_rows,
        )
        passages = CanonicalPassageProjector(
            store,
            LogicalEvidenceProjectionStore(build_evidence_archive_store()),
            policy=PassagePolicy(
                target_tokens=args.target_tokens,
                overlap_tokens=args.overlap_tokens,
            ),
            bound_tenant_id=args.tenant,
        )
        scan = CanonicalParquetScanProjector(
            store,
            LogicalEvidenceProjectionStore(build_evidence_archive_store()),
        )
        print(
            json.dumps(
                run_projection_worker(
                    logical,
                    passages,
                    scan,
                    tenant_id=args.tenant,
                    logical_batch_size=args.logical_batch_size,
                    passage_batch_size=args.passage_batch_size,
                    embedding_batch_size=args.embedding_batch_size,
                    max_batches_per_cycle=args.max_batches_per_cycle,
                    upload_concurrency=args.upload_concurrency,
                    passage_concurrency=args.passage_concurrency,
                    interval_seconds=args.interval_seconds,
                    once=args.once,
                ),
                sort_keys=True,
            )
        )
    elif args.command == "export":
        for envelope in store.export_raw():
            print(json.dumps(envelope, sort_keys=True))
    elif args.command == "token-create":
        credential = store.create_collector_token(
            args.name,
            args.source,
            [scope.strip() for scope in args.scopes.split(",") if scope.strip()],
            tenant_id=args.tenant,
            principal_id=args.principal,
            capture_origin=args.capture_origin,
            webhook_privacy_mode=args.webhook_privacy_mode,
        )
        payload = (json.dumps(credential, sort_keys=True) + "\n").encode()
        descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as output:
            output.write(payload)
        print(
            json.dumps(
                {key: value for key, value in credential.items() if key != "token"},
                sort_keys=True,
            )
        )
    elif args.command == "token-revoke":
        print(json.dumps({"revoked": store.revoke_collector_token(args.name)}))
    elif args.command == "brain-provision":
        print(
            json.dumps(
                store.provision_brain(
                    organization_id=args.organization,
                    organization_kind=args.kind,
                    display_name=args.display_name,
                    tenant_id=args.tenant,
                    brain_kind=args.kind,
                    slug=args.slug,
                    owner_principal_id=args.owner_principal,
                ),
                sort_keys=True,
            )
        )
    elif args.command == "mcp-token-create":
        credential = store.create_mcp_token(
            args.name,
            tenant_id=args.tenant,
            principal_id=args.principal,
            principal_kind=args.principal_kind,
            scopes=[scope.strip() for scope in args.scopes.split(",") if scope.strip()],
            expires_in_days=args.expires_in_days,
        )
        payload = (json.dumps(credential, sort_keys=True) + "\n").encode()
        descriptor = os.open(
            args.output,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        with os.fdopen(descriptor, "wb") as output:
            output.write(payload)
        print(
            json.dumps(
                {key: value for key, value in credential.items() if key != "token"},
                sort_keys=True,
            )
        )
    elif args.command == "mcp-token-revoke":
        print(json.dumps({"revoked": store.revoke_mcp_token(args.name)}))
    elif args.command == "admin-token-create":
        credential = ControlPlane(store, SecretBox.from_env(), {}).create_admin_token(
            args.name,
            principal_id=args.principal,
            expires_in_days=args.expires_in_days,
        )
        payload = (json.dumps(credential, sort_keys=True) + "\n").encode()
        descriptor = os.open(
            args.output,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        with os.fdopen(descriptor, "wb") as output:
            output.write(payload)
        print(
            json.dumps(
                {key: value for key, value in credential.items() if key != "token"},
                sort_keys=True,
            )
        )
    elif args.command == "admin-token-revoke":
        print(
            json.dumps(
                {
                    "revoked": ControlPlane(
                        store, SecretBox.from_env(), {}
                    ).revoke_admin_token(args.name)
                }
            )
        )
    elif args.command == "source-profile-set":
        print(
            json.dumps(
                store.set_source_profile(
                    {
                        "source_id": args.source_id,
                        "family": args.family,
                        "quality": args.quality,
                        "freshness_half_life_days": args.freshness_half_life_days,
                    }
                ),
                sort_keys=True,
            )
        )
    elif args.command == "source-alias-set":
        print(
            json.dumps(
                store.set_source_alias(args.alias, args.source_id), sort_keys=True
            )
        )
    elif args.command == "federation-scoreboard":
        print(json.dumps(store.federation_scoreboard(), sort_keys=True))
    else:
        if args.require_auth:
            os.environ["RECALL_AUTH_REQUIRED"] = "1"
        if args.unix_socket:
            serve_unix(args.dsn, args.unix_socket)
        else:
            serve(args.dsn, args.host, args.port)


if __name__ == "__main__":
    main()
