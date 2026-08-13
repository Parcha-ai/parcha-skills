"""Public, out-of-process source-plugin contract for Recall.

Plugins own provider credentials and acquisition.  Recall accepts only a closed
manifest and the existing connector-page wire, so installing a plugin never
means importing untrusted provider code into the Brain process.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from connectors.registry import ConnectorDefinitionV3
from connectors.sdk import ConnectorContractError, ConnectorPage, SOURCE_ID


SOURCE_PLUGIN_API_VERSION = "recall.source-plugin.v1"
SOURCE_PLUGIN_OPERATIONS = frozenset({"backfill", "event", "reconcile"})


@dataclass(frozen=True)
class SourcePluginManifest:
    """Portable metadata exchanged before any source page is accepted."""

    definition: ConnectorDefinitionV3
    operations: tuple[str, ...]
    api_version: str = SOURCE_PLUGIN_API_VERSION

    def __post_init__(self) -> None:
        if self.api_version != SOURCE_PLUGIN_API_VERSION:
            raise ConnectorContractError("source plugin api_version is unsupported")
        if (
            not isinstance(self.definition, ConnectorDefinitionV3)
            or not isinstance(self.operations, tuple)
            or not self.operations
            or self.operations != tuple(sorted(set(self.operations)))
            or any(item not in SOURCE_PLUGIN_OPERATIONS for item in self.operations)
        ):
            raise ConnectorContractError("source plugin manifest is invalid")

    def to_public(self) -> dict[str, object]:
        return {
            "api_version": self.api_version,
            "definition": self.definition.to_public(),
            "operations": list(self.operations),
            "execution": "out_of_process",
            "page_wire": "recall.connector-page.v1",
        }

    @classmethod
    def from_mapping(cls, value: object) -> "SourcePluginManifest":
        expected = {
            "api_version", "definition", "operations", "execution", "page_wire",
        }
        if not isinstance(value, dict) or set(value) != expected:
            raise ConnectorContractError("source plugin manifest must be a closed object")
        if (
            value["execution"] != "out_of_process"
            or value["page_wire"] != "recall.connector-page.v1"
            or not isinstance(value["operations"], list)
        ):
            raise ConnectorContractError("source plugin manifest is invalid")
        try:
            definition = ConnectorDefinitionV3.from_mapping(value["definition"])
        except (TypeError, ValueError) as error:
            raise ConnectorContractError("source plugin definition is invalid") from error
        return cls(
            api_version=value["api_version"],
            definition=definition,
            operations=tuple(value["operations"]),
        )


class SourcePlugin(Protocol):
    """Contributor surface: one manifest plus the normal pull connector API."""

    manifest: SourcePluginManifest
    connector_id: str
    source_id: str

    def pull(self, cursor: str | None) -> ConnectorPage: ...


class SourcePluginFixtureFactory(Protocol):
    manifest: SourcePluginManifest
    source_id: str

    def build(self, scenario: str) -> SourcePlugin: ...


def validate_source_plugin(plugin: SourcePlugin) -> None:
    manifest = getattr(plugin, "manifest", None)
    if not isinstance(manifest, SourcePluginManifest):
        raise ConnectorContractError("source plugin manifest is unavailable")
    if (
        getattr(plugin, "connector_id", None) != manifest.definition.connector_id
        or not isinstance(getattr(plugin, "source_id", None), str)
        or SOURCE_ID.fullmatch(plugin.source_id) is None
    ):
        raise ConnectorContractError("source plugin identity mismatch")


def pull_source_plugin_wire(plugin: SourcePlugin, cursor: str | None) -> bytes:
    """Run one contributor-owned page and force a closed-wire round trip."""

    validate_source_plugin(plugin)
    page = plugin.pull(cursor)
    from connectors.kit import encode_page_wire
    return encode_page_wire(page)


class WireSourceConnector:
    """Host adapter for a plugin process that returns connector-page bytes."""

    def __init__(self, *, manifest: SourcePluginManifest, source_id: str, transport):
        if (
            not callable(transport) or not isinstance(source_id, str)
            or SOURCE_ID.fullmatch(source_id) is None
        ):
            raise ConnectorContractError("wire source plugin is invalid")
        self.manifest = manifest
        self.connector_id = manifest.definition.connector_id
        self.source_id = source_id
        self._transport = transport

    def pull(self, cursor: str | None) -> ConnectorPage:
        from connectors.kit import decode_page_wire
        payload = self._transport(cursor)
        return decode_page_wire(payload)


def run_source_plugin_conformance(factory: SourcePluginFixtureFactory):
    """Run the standard ACK/replay/privacy matrix against a plugin publisher."""

    from connectors.conformance import run_connector_conformance

    class Adapter:
        manifest = factory.manifest.definition
        source_id = factory.source_id

        @staticmethod
        def build(scenario: str):
            plugin = factory.build(scenario)
            validate_source_plugin(plugin)
            return plugin

    return run_connector_conformance(Adapter())


__all__ = [
    "SOURCE_PLUGIN_API_VERSION",
    "SourcePlugin",
    "SourcePluginManifest",
    "WireSourceConnector",
    "pull_source_plugin_wire",
    "run_source_plugin_conformance",
    "validate_source_plugin",
]
