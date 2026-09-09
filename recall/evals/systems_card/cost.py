"""Cost: managed database configuration and month-to-date spend (optional)."""
from __future__ import annotations

import json
import os
import urllib.request
from typing import Any

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
