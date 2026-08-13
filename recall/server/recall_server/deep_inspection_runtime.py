from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

from .deep_inspection import AgentExecObject, ArchilDeepInspector, LocalDeepInspector
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
    tool_key_value = values.get("RECALL_ARCHIL_DUCKDB_OBJECT_KEY", "")
    tool_sha256_value = values.get("RECALL_ARCHIL_DUCKDB_SHA256", "")
    if not isinstance(tool_key_value, str) or not isinstance(
        tool_sha256_value, str
    ):
        raise ValueError("DuckDB tool configuration is incomplete")
    tool_key = tool_key_value.strip()
    tool_sha256 = tool_sha256_value.strip()
    if bool(tool_key) != bool(tool_sha256):
        raise ValueError("DuckDB tool configuration is incomplete")
    return ArchilDeepInspector(
        api_key=required["ARCHIL_API_KEY"],
        disk_id=required["RECALL_ARCHIL_DISK_ID"],
        region=required["RECALL_ARCHIL_REGION"],
        duckdb_tool=(
            AgentExecObject(
                object_key=tool_key,
                content_sha256=tool_sha256,
            )
            if tool_key
            else None
        ),
        transport=transport,
    )
