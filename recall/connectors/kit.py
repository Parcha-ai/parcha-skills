"""Versioned public connector-kit surface and closed out-of-process page wire."""

from __future__ import annotations

import json
from typing import Any

from connectors.registry import (
    ConnectorAuth,
    ConnectorDefinitionV3,
    ConnectorPlacement,
    ConnectorPolicy,
    ConnectorRegistryError,
    ConnectorSync,
)
from connectors.sdk import (
    ConnectorContractError,
    ConnectorPage,
    ConnectorRateLimited,
    ConnectorRecord,
    ConnectorRecordV2,
    ConnectorRunError,
    ConnectorRunner,
    ConnectorUpstreamError,
    PullConnector,
)
from connectors.remote_api import BoundedJsonRail, RemoteApiError, RemoteOperation
from connectors.managed_auth import (
    ManagedAuthConnector,
    ManagedPage,
    ManagedProjection,
    ManagedRecord,
    ManagedTransport,
    ManagedWakeup,
    verify_nango_wakeup,
)
from connectors.source_plugin import (
    SOURCE_PLUGIN_API_VERSION,
    SourcePlugin,
    SourcePluginManifest,
    WireSourceConnector,
    pull_source_plugin_wire,
    run_source_plugin_conformance,
    validate_source_plugin,
)


CONNECTOR_KIT_API_VERSION = "recall.connector-kit.v1"
CONNECTOR_PAGE_WIRE_VERSION = "recall.connector-page.v1"
MAX_PAGE_WIRE_BYTES = 8_500_000
PAGE_WIRE_FIELDS = {"api_version", "records", "next_cursor", "has_more"}

def encode_page_wire(page: ConnectorPage) -> bytes:
    if not isinstance(page, ConnectorPage):
        raise ConnectorContractError("wire page must be ConnectorPage")
    value = {
        "api_version": CONNECTOR_PAGE_WIRE_VERSION,
        "records": [record.to_mapping() for record in page.records],
        "next_cursor": page.next_cursor,
        "has_more": page.has_more,
    }
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode()
    except (TypeError, ValueError) as error:
        raise ConnectorContractError("wire page must be finite JSON") from error
    if len(encoded) > MAX_PAGE_WIRE_BYTES:
        raise ConnectorContractError("wire page exceeds maximum byte count")
    return encoded


def decode_page_wire(payload: bytes) -> ConnectorPage:
    if not isinstance(payload, bytes) or not payload or len(payload) > MAX_PAGE_WIRE_BYTES:
        raise ConnectorContractError("wire payload is invalid")
    try:
        value: Any = json.loads(
            payload,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise ConnectorContractError("wire payload is invalid JSON") from error
    if not isinstance(value, dict) or set(value) != PAGE_WIRE_FIELDS:
        raise ConnectorContractError("wire page must be a closed object")
    if value["api_version"] != CONNECTOR_PAGE_WIRE_VERSION:
        raise ConnectorContractError("wire page api_version is unsupported")
    records = value["records"]
    if not isinstance(records, list):
        raise ConnectorContractError("wire records must be a list")
    decoded = []
    for record in records:
        if not isinstance(record, dict):
            raise ConnectorContractError("wire record must be an object")
        schema_version = record.get("schema_version")
        if schema_version == 1:
            decoded.append(ConnectorRecord.from_mapping(record))
        elif schema_version == 2:
            decoded.append(ConnectorRecordV2.from_mapping(record))
        else:
            raise ConnectorContractError("wire record schema_version is unsupported")
    return ConnectorPage(
        records=tuple(decoded),
        next_cursor=value["next_cursor"],
        has_more=value["has_more"],
    )


__all__ = [
    "CONNECTOR_KIT_API_VERSION",
    "CONNECTOR_PAGE_WIRE_VERSION",
    "ConnectorAuth",
    "ConnectorContractError",
    "ConnectorDefinitionV3",
    "ConnectorPage",
    "ConnectorPlacement",
    "ConnectorPolicy",
    "ConnectorRateLimited",
    "ConnectorRecord",
    "ConnectorRecordV2",
    "ConnectorRegistryError",
    "ConnectorRunError",
    "ConnectorRunner",
    "ConnectorSync",
    "ConnectorUpstreamError",
    "PullConnector",
    "BoundedJsonRail",
    "RemoteApiError",
    "RemoteOperation",
    "ManagedAuthConnector",
    "ManagedPage",
    "ManagedProjection",
    "ManagedRecord",
    "ManagedTransport",
    "ManagedWakeup",
    "verify_nango_wakeup",
    "decode_page_wire",
    "encode_page_wire",
    "SOURCE_PLUGIN_API_VERSION",
    "SourcePlugin",
    "SourcePluginManifest",
    "WireSourceConnector",
    "pull_source_plugin_wire",
    "run_source_plugin_conformance",
    "validate_source_plugin",
]
