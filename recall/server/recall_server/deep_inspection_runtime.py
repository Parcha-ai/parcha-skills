from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

from .deep_inspection import AgentExecObject, ArchilDeepInspector, LocalDeepInspector
from .evidence_projection import EvidenceProjectionStore


ARCHIL_SETTINGS = (
    "ARCHIL_API_KEY",
    "RECALL_ARCHIL_DISK_ID",
    "RECALL_ARCHIL_REGION",
    "RECALL_ARCHIL_DUCKDB_OBJECT_KEY",
    "RECALL_ARCHIL_DUCKDB_SHA256",
    "RECALL_ARCHIL_DUCKDB_X86_64_OBJECT_KEY",
    "RECALL_ARCHIL_DUCKDB_X86_64_SHA256",
)

# Env prefix -> published tool architecture. The unsuffixed pair is the
# original arm64 build; the sandbox picks whichever matches its own machine.
DUCKDB_TOOL_ENV = (
    ("RECALL_ARCHIL_DUCKDB", "linux-arm64"),
    ("RECALL_ARCHIL_DUCKDB_X86_64", "linux-x86_64"),
)


def _enabled(values: Mapping[str, str], name: str) -> bool:
    value = values.get(name, "0").strip()
    if value not in {"0", "1"}:
        raise ValueError(f"{name} must be 0 or 1")
    return value == "1"


def build_deep_inspector(
    projection: EvidenceProjectionStore,
    environment: Mapping[str, str] | None = None,
    *,
    transport: Any = None,
) -> Any:
    values = os.environ if environment is None else environment
    provider = values.get("RECALL_DEEP_INSPECTOR", "off").strip()
    required = _enabled(values, "RECALL_DEEP_INSPECTION_REQUIRED")
    if provider == "off":
        # Treat leftover provider configuration as deployment drift instead of
        # silently serving code-mode tools backed by no runtime.
        if required or any(values.get(name, "").strip() for name in ARCHIL_SETTINGS):
            raise ValueError("deep inspector configuration is incomplete")
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
    tools: dict[str, AgentExecObject] = {}
    for prefix, arch in DUCKDB_TOOL_ENV:
        tool_key_value = values.get(prefix + "_OBJECT_KEY", "")
        tool_sha256_value = values.get(prefix + "_SHA256", "")
        if not isinstance(tool_key_value, str) or not isinstance(
            tool_sha256_value, str
        ):
            raise ValueError("DuckDB tool configuration is incomplete")
        tool_key = tool_key_value.strip()
        tool_sha256 = tool_sha256_value.strip()
        if bool(tool_key) != bool(tool_sha256):
            raise ValueError("DuckDB tool configuration is incomplete")
        if tool_key:
            tools[arch] = AgentExecObject(
                object_key=tool_key,
                content_sha256=tool_sha256,
            )
    return ArchilDeepInspector(
        api_key=required["ARCHIL_API_KEY"],
        disk_id=required["RECALL_ARCHIL_DISK_ID"],
        region=required["RECALL_ARCHIL_REGION"],
        duckdb_tools=tools or None,
        transport=transport,
    )
