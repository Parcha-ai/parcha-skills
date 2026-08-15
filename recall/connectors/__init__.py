"""Versioned connector API with dependency-isolated, lazy public exports."""

from __future__ import annotations

from importlib import import_module
from typing import Any


_EXPORTS = {
    "ConnectorContractError": ("sdk", "ConnectorContractError"),
    "ConnectorPage": ("sdk", "ConnectorPage"),
    "ConnectorRateLimited": ("sdk", "ConnectorRateLimited"),
    "ConnectorRecord": ("sdk", "ConnectorRecord"),
    "ConnectorRunError": ("sdk", "ConnectorRunError"),
    "ConnectorRunner": ("sdk", "ConnectorRunner"),
    "seed_acknowledged_records": ("sdk", "seed_acknowledged_records"),
    "ExportInboxConnector": ("export_inbox", "ExportInboxConnector"),
    "ExportInboxError": ("export_inbox", "ExportInboxError"),
    "ConnectorDefinition": ("registry", "ConnectorDefinition"),
    "ConnectorDefinitionV3": ("registry", "ConnectorDefinitionV3"),
    "ConnectorPlacement": ("registry", "ConnectorPlacement"),
    "ConnectorAuth": ("registry", "ConnectorAuth"),
    "ConnectorSync": ("registry", "ConnectorSync"),
    "ConnectorPolicy": ("registry", "ConnectorPolicy"),
    "ConnectorRegistryError": ("registry", "ConnectorRegistryError"),
    "REGISTRY": ("registry", "REGISTRY"),
    "CONNECTOR_KIT_API_VERSION": ("kit", "CONNECTOR_KIT_API_VERSION"),
    "CONNECTOR_PAGE_WIRE_VERSION": ("kit", "CONNECTOR_PAGE_WIRE_VERSION"),
    "encode_page_wire": ("kit", "encode_page_wire"),
    "decode_page_wire": ("kit", "decode_page_wire"),
    "CONNECTOR_CONFORMANCE_CELLS": ("conformance", "CONNECTOR_CONFORMANCE_CELLS"),
    "CONNECTOR_CONFORMANCE_VERSION": ("conformance", "CONNECTOR_CONFORMANCE_VERSION"),
    "ConformanceReport": ("conformance", "ConformanceReport"),
    "render_conformance_report": ("conformance", "render_conformance_report"),
    "run_connector_conformance": ("conformance", "run_connector_conformance"),
    "BoundedJsonRail": ("remote_api", "BoundedJsonRail"),
    "RemoteApiError": ("remote_api", "RemoteApiError"),
    "RemoteOperation": ("remote_api", "RemoteOperation"),
    "GmailConnector": ("google_workspace", "GmailConnector"),
    "GoogleCalendarConnector": ("google_workspace", "GoogleCalendarConnector"),
    "GoogleContactsConnector": ("google_workspace", "GoogleContactsConnector"),
    "GoogleDriveConnector": ("google_workspace", "GoogleDriveConnector"),
    "ComposioWorkspaceRail": (
        "composio_workspace_rail",
        "ComposioWorkspaceRail",
    ),
    "toolkit_for_connector": (
        "composio_workspace_rail",
        "toolkit_for_connector",
    ),
    "GitHubActivityConnector": ("work_apis", "GitHubActivityConnector"),
    "LinearActivityConnector": ("work_apis", "LinearActivityConnector"),
    "NotionWorkspaceConnector": ("work_apis", "NotionWorkspaceConnector"),
    "SlackMessagesConnector": ("work_apis", "SlackMessagesConnector"),
    "SlackPublicHistoryRail": ("work_apis", "SlackPublicHistoryRail"),
    "github_rail": ("work_apis", "github_rail"),
    "linear_rail": ("work_apis", "linear_rail"),
    "notion_rail": ("work_apis", "notion_rail"),
    "slack_rail": ("work_apis", "slack_rail"),
    "slack_public_history_rail": ("work_apis", "slack_public_history_rail"),
    "x_rail": ("work_apis", "x_rail"),
    "XActivityConnector": ("x_activity", "XActivityConnector"),
    "ConnectorSupervisor": ("supervisor", "ConnectorSupervisor"),
    "ScheduleDefinition": ("supervisor", "ScheduleDefinition"),
    "ScheduledJob": ("supervisor", "ScheduledJob"),
    "SupervisorContractError": ("supervisor", "SupervisorContractError"),
    "SupervisorStore": ("supervisor", "SupervisorStore"),
    "ConnectorHostConfig": ("host", "ConnectorHostConfig"),
    "ConnectorHostError": ("host", "ConnectorHostError"),
    "build_host": ("host", "build_host"),
    "load_host_config": ("host", "load_host_config"),
}

__all__ = sorted(_EXPORTS)  # noqa: PLE0605 - derived from the closed export map above


def __getattr__(name: str) -> Any:
    try:
        module_name, attribute = _EXPORTS[name]
    except KeyError:
        raise AttributeError(name) from None
    value = getattr(import_module(f"{__name__}.{module_name}"), attribute)
    globals()[name] = value
    return value
