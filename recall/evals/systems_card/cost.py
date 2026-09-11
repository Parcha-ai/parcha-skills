"""Cost: managed database spend (optional) and where the stored bytes are."""
from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from .mcp_client import McpClientError, _private_json
from .model import Gate, ProbeResult
from .probes import ProbeContext

PLANETSCALE_API = "https://api.planetscale.com/v1"


def _get(url: str, headers: dict[str, str], timeout: float = 20.0) -> dict[str, Any]:
    request = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310 - fixed https host
        return json.loads(response.read(1_000_000))


class PlanetScaleCostProbe:
    name = "cost.planetscale"
    dimension = "cost"

    def run(self, context: ProbeContext) -> ProbeResult:
        result = ProbeResult(name=self.name, dimension=self.dimension, status="ok")
        org = context.options.get("planetscale_org") or os.environ.get("RECALL_PLANETSCALE_ORG")
        database = context.options.get("planetscale_database") or os.environ.get("RECALL_PLANETSCALE_DATABASE")
        account = os.environ.get("PLANETSCALE_SERVICE_ACCOUNT_ID")
        token = os.environ.get("PLANETSCALE_SERVICE_TOKEN")
        if not (org and database and account and token):
            result.status = "skipped"
            result.notes.append("PlanetScale credentials/org/database not configured; cost not measured")
            return result
        headers = {"Authorization": f"{account}:{token}", "Accept": "application/json"}
        getter = context.options.get("_planetscale_get") or _get
        branch = getter(f"{PLANETSCALE_API}/organizations/{org}/databases/{database}/branches/main", headers)
        org_info = getter(f"{PLANETSCALE_API}/organizations/{org}", headers)
        invoices = getter(f"{PLANETSCALE_API}/organizations/{org}/invoices", headers).get("data", [])
        current = invoices[0] if invoices else {}
        previous = invoices[1] if len(invoices) > 1 else {}
        gib = 1024 ** 3
        result.metrics = {
            "cluster": branch.get("cluster_display_name"),
            "storage_iops": branch.get("storage_iops"),
            "storage_throughput_mibs": branch.get("storage_throughput_mibs"),
            "storage_min_gib": round((branch.get("minimum_storage_bytes") or 0) / gib, 1),
            "storage_max_gib": round((branch.get("maximum_storage_bytes") or 0) / gib, 1),
            "storage_autoscaling": branch.get("storage_autoscaling"),
            "budget_alerts_enabled": bool(org_info.get("invoice_budget_alerts")),
            "budget_amount_usd": float(org_info.get("invoice_budget_amount") or 0.0),
            "invoice_mtd_usd": float(current.get("total") or 0.0),
            "invoice_period_start": current.get("billing_period_start"),
            "invoice_previous_usd": float(previous.get("total") or 0.0),
        }
        result.samples = len(invoices)
        result.gates = [
            Gate("storage_iops", "<=", 3000.0).evaluate(float(branch.get("storage_iops") or 0)),
            Gate("storage_throughput_mibs", "<=", 125.0).evaluate(float(branch.get("storage_throughput_mibs") or 0)),
            Gate("budget_alerts_enabled", "==", 1.0).evaluate(1.0 if result.metrics["budget_alerts_enabled"] else 0.0),
        ]
        if any(g.passed is False for g in result.gates):
            result.status = "degraded"
        return result


# ---------------------------------------------------------------------------
# Storage: where the bytes are (Postgres, managed-DB bounds, evidence bucket).
# ---------------------------------------------------------------------------

GIB = 1024 ** 3
STORAGE_TABLES = 12
S3_PAGE_CAP = 200
S3_PAGE_SIZE = 1000
EVIDENCE_OBJECT_PREFIX = "objects/"
PROMETHEUS_LINE = re.compile(r"^([A-Za-z_:][A-Za-z0-9_:]*)(?:\{([^}]*)\})?\s+(\S+)\s*$")
PROMETHEUS_LABEL = re.compile(r'([A-Za-z_][A-Za-z0-9_]*)="((?:[^"\\]|\\.)*)"')
USED_BYTES_KEY = re.compile(r"(used|usage|current).*bytes|bytes.*(used|usage|current)")
AWS_AMBIENT_CREDENTIAL_KEYS = (
    "AWS_ACCESS_KEY_ID",
    "AWS_PROFILE",
    "AWS_ROLE_ARN",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
)


def _get_text(url: str, headers: dict[str, str], timeout: float = 20.0) -> tuple[int, str]:
    request = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310 - https only
            return response.status, response.read(4_000_000).decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return exc.code, ""


def parse_prometheus(text: str) -> list[tuple[str, dict[str, str], float]]:
    """Parse Prometheus text exposition into (name, labels, value); comments and junk are skipped."""
    samples: list[tuple[str, dict[str, str], float]] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = PROMETHEUS_LINE.match(line)
        if not match:
            continue
        name, label_text, value_text = match.groups()
        try:
            value = float(value_text)
        except ValueError:
            continue
        labels = dict(PROMETHEUS_LABEL.findall(label_text or ""))
        samples.append((name, labels, value))
    return samples


class StorageProbe:
    """Where the bytes are. Every source is optional and skipped with a note when unconfigured."""

    name = "cost.storage"
    dimension = "cost"
    postgres_gate_gib = 40.0  # H1 target; the plan lowers it to 10 after H3

    def run(self, context: ProbeContext) -> ProbeResult:
        result = ProbeResult(name=self.name, dimension=self.dimension, status="ok")
        metrics: dict[str, Any] = {
            "postgres_available": False,
            "planetscale_available": False,
            "s3_available": False,
        }
        self._postgres(context, result, metrics)
        self._planetscale(context, result, metrics)
        self._s3(context, result, metrics)
        result.metrics = metrics
        if not any(metrics[key] for key in ("postgres_available", "planetscale_available", "s3_available")):
            result.status = "skipped"
            return result
        result.gates = [
            Gate("postgres_database_gib", "<=", self.postgres_gate_gib, note="H1 target; H3 lowers it to 10").evaluate(
                metrics.get("postgres_database_gib")
            ),
        ]
        if any(g.passed is False for g in result.gates):
            result.status = "degraded"
        return result

    # -- (b) brain /metrics: Postgres breakdown -------------------------------

    def _postgres(self, context: ProbeContext, result: ProbeResult, metrics: dict[str, Any]) -> None:
        token_path = context.options.get("metrics_token_file") or os.environ.get("RECALL_METRICS_TOKEN_FILE")
        if not token_path:
            result.notes.append("metrics token not configured (--metrics-token-file / RECALL_METRICS_TOKEN_FILE); postgres storage not measured")
            return
        try:
            token = _private_json(Path(token_path).expanduser()).get("token")
        except (OSError, ValueError, McpClientError) as exc:
            result.notes.append(f"metrics token file unusable ({type(exc).__name__}); postgres storage not measured")
            return
        if not isinstance(token, str) or not token:
            result.notes.append("metrics token file has no token; postgres storage not measured")
            return
        origin = context.base_url.rsplit("/mcp", 1)[0]
        getter = context.options.get("_metrics_get") or _get_text
        headers = {"Authorization": f"Bearer {token}", "Accept": "text/plain"}
        try:
            status, text = getter(f"{origin}/metrics", headers)
        except Exception as exc:  # transport failure is a finding, not a crash
            result.notes.append(f"/metrics unreachable ({type(exc).__name__})")
            return
        if status != 200:
            result.notes.append(f"/metrics returned http:{status}")
            return
        table_bytes: dict[str, float] = {}
        database_bytes: float | None = None
        for name, labels, value in parse_prometheus(text):
            if name == "recall_database_bytes":
                database_bytes = value
            elif name == "recall_table_bytes" and labels.get("table"):
                table_bytes[labels["table"]] = value
            elif name in ("recall_source_events", "recall_embedded_items"):
                metrics[name.removeprefix("recall_")] = int(value)
            elif name.startswith("recall_storage_"):
                metrics[name.removeprefix("recall_")] = value
        metrics["postgres_available"] = True
        result.samples += 1
        if database_bytes is None and not table_bytes:
            result.notes.append("/metrics has no storage breakdown yet (brain predates recall_database_bytes)")
            return
        if database_bytes is not None:
            metrics["postgres_database_gib"] = round(database_bytes / GIB, 2)
        ranked = sorted(table_bytes.items(), key=lambda item: (-item[1], item[0]))[:STORAGE_TABLES]
        metrics["postgres_table_gib"] = {table: round(size / GIB, 2) for table, size in ranked}
        metrics["postgres_largest_table"] = ranked[0][0] if ranked else None
        metrics["postgres_tables_reported"] = len(ranked)

    # -- (a) PlanetScale branch: managed storage bounds -----------------------

    def _planetscale(self, context: ProbeContext, result: ProbeResult, metrics: dict[str, Any]) -> None:
        org = context.options.get("planetscale_org") or os.environ.get("RECALL_PLANETSCALE_ORG")
        database = context.options.get("planetscale_database") or os.environ.get("RECALL_PLANETSCALE_DATABASE")
        account = os.environ.get("PLANETSCALE_SERVICE_ACCOUNT_ID")
        token = os.environ.get("PLANETSCALE_SERVICE_TOKEN")
        if not (org and database and account and token):
            result.notes.append("PlanetScale credentials/org/database not configured; managed storage bounds not measured")
            return
        headers = {"Authorization": f"{account}:{token}", "Accept": "application/json"}
        getter = context.options.get("_planetscale_get") or _get
        try:
            branch = getter(f"{PLANETSCALE_API}/organizations/{org}/databases/{database}/branches/main", headers)
        except Exception as exc:
            result.notes.append(f"PlanetScale branch lookup failed ({type(exc).__name__})")
            return
        if not isinstance(branch, dict):
            result.notes.append("PlanetScale branch response is not an object")
            return
        metrics["planetscale_available"] = True
        result.samples += 1
        metrics["db_cluster"] = branch.get("cluster_display_name")
        metrics["db_storage_min_gib"] = round((branch.get("minimum_storage_bytes") or 0) / GIB, 1)
        metrics["db_storage_max_gib"] = round((branch.get("maximum_storage_bytes") or 0) / GIB, 1)
        # The API publishes no per-table sizes; a used-bytes field is reported only when present.
        used_key = next(
            (key for key in sorted(branch) if USED_BYTES_KEY.search(key) and isinstance(branch.get(key), (int, float))),
            None,
        )
        if used_key:
            metrics["db_storage_used_gib"] = round(float(branch[used_key]) / GIB, 1)
            metrics["db_storage_used_field"] = used_key
        else:
            result.notes.append("PlanetScale branch exposes no used-bytes field; only min/max bounds reported")

    # -- (c) evidence bucket: object bytes under the archive prefix -----------

    def _s3(self, context: ProbeContext, result: ProbeResult, metrics: dict[str, Any]) -> None:
        env = os.environ
        bucket = env.get("RECALL_EVIDENCE_ARCHIVE_BUCKET")
        if not bucket:
            result.notes.append("RECALL_EVIDENCE_ARCHIVE_BUCKET not set; evidence bytes not measured")
            return
        factory = context.options.get("_s3_client_factory")
        if factory is None:
            try:
                import boto3  # type: ignore[import-not-found]
            except ImportError:
                result.notes.append("boto3 not importable; evidence bytes not measured")
                return
            factory = boto3.client
        client_kwargs: dict[str, Any] = {"service_name": "s3"}
        if env.get("RECALL_EVIDENCE_ARCHIVE_ENDPOINT_URL"):
            client_kwargs["endpoint_url"] = env["RECALL_EVIDENCE_ARCHIVE_ENDPOINT_URL"]
        if env.get("RECALL_EVIDENCE_ARCHIVE_REGION"):
            client_kwargs["region_name"] = env["RECALL_EVIDENCE_ARCHIVE_REGION"]
        access = env.get("RECALL_EVIDENCE_ARCHIVE_ACCESS_KEY_ID")
        secret = env.get("RECALL_EVIDENCE_ARCHIVE_SECRET_ACCESS_KEY")
        if access and secret:
            client_kwargs["aws_access_key_id"] = access
            client_kwargs["aws_secret_access_key"] = secret
        elif not any(env.get(key) for key in AWS_AMBIENT_CREDENTIAL_KEYS):
            result.notes.append("no evidence-archive or AWS credentials in the environment; evidence bytes not measured")
            return
        prefix = env.get("RECALL_EVIDENCE_ARCHIVE_PREFIX", EVIDENCE_OBJECT_PREFIX)
        page_cap = int(context.options.get("s3_page_cap") or S3_PAGE_CAP)
        total_bytes = objects = pages = 0
        complete = True
        try:
            client = factory(**client_kwargs)
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": S3_PAGE_SIZE}):
                pages += 1
                for obj in page.get("Contents") or []:
                    total_bytes += int(obj.get("Size") or 0)
                    objects += 1
                if pages >= page_cap:
                    complete = not page.get("IsTruncated", False)
                    break
        except Exception as exc:  # credentials, network, permissions: content-free note
            result.notes.append(f"evidence bucket listing failed ({type(exc).__name__})")
            return
        metrics["s3_available"] = True
        result.samples += 1
        metrics["s3_bytes_sampled"] = total_bytes
        metrics["s3_objects_sampled"] = objects
        metrics["s3_pages_listed"] = pages
        metrics["s3_listing_complete"] = complete
        metrics["s3_evidence_gib"] = round(total_bytes / GIB, 2)
        if not complete:
            result.notes.append(f"evidence bucket listing capped at {page_cap} pages; s3_evidence_gib is a lower bound")
