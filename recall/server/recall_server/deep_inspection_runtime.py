from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

from .deep_inspection import ArchilDeepInspector, LocalDeepInspector
from .evidence_projection import EvidenceProjectionStore


def build_deep_inspector(
    projection: EvidenceProjectionStore,
    environment: Mapping[str, str] | None = None,
    *,
    transport: Any = None,
) -> Any:
    values = os.environ if environment is None else environment
    provider = values.get("RECALL_DEEP_INSPECTOR", "off").strip()
    if provider == "off":
        return None
    if provider == "local":
        return LocalDeepInspector(projection)
    if provider != "archil":
        raise ValueError("deep inspector provider is unsupported")
    required = {}
    for name in ("ARCHIL_API_KEY", "RECALL_ARCHIL_DISK_ID", "RECALL_ARCHIL_REGION"):
        value = values.get(name)
        if not isinstance(value, str) or not value.strip():
            raise ValueError("deep inspector configuration is incomplete")
        required[name] = value.strip()
    return ArchilDeepInspector(
        api_key=required["ARCHIL_API_KEY"],
        disk_id=required["RECALL_ARCHIL_DISK_ID"],
        region=required["RECALL_ARCHIL_REGION"],
        transport=transport,
    )
