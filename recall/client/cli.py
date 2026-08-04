from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from client.mac import (
    BrainClient,
    CanonicalArchiveClient,
    CanonicalBrainWriter,
    ExportImporter,
    MemoryClient,
    dry_run_manifest,
    load_file_token,
    load_keychain_token,
    store_keychain_token,
)
from collector.collector import Collector
from client.capture import CaptureClient, ORIGIN
from client.mcp import McpServer, serve as serve_mcp
from client.macos_utility import (
    SOURCE_SPECS,
    MacUtilityError,
    apply_route,
    disable_source,
    mac_status,
    pause_source,
    reset_local_source,
    resume_source,
    revoke_source,
    route_info,
    support_report,
)
from client.local_surfaces import (
    LocalSurfaceError,
    mac_claude_surface_preview,
    mac_local_surface_preview,
)
from connectors.cowork_local import CoworkLocalConnector
from connectors.export_inbox import ExportInboxConnector
from connectors.feeds import FeedConnector
from connectors.grep_ai import GrepAIConnector, load_private_api_key, validate_api_key
from connectors.imessage import IMessageConnector
from connectors.local_activity import (
    AppleNotesConnector,
    BrowserActivityConnector,
    HermesSessionConnector,
)
from connectors.local_files import SelectedTextConnector
from connectors.portable_archives import (
    NotionArchiveConnector,
    SlackArchiveConnector,
    XArchiveConnector,
)
from connectors.portable_pim import (
    CalendarImportConnector,
    ContactImportConnector,
    MailImportConnector,
)
from connectors.whatsapp_export import WhatsAppExportConnector
from connectors.activation import (
    ActivationError,
    ActivationStore,
    activation_catalog,
    load_activation_config,
    preview_activation_config,
)
from connectors.registry import (
    REGISTRY,
    ConnectorRegistryError,
    aggregate_status,
    catalog as integration_catalog,
    preview as registry_preview,
    validate_policy,
)
from connectors.selected_jsonl import SelectedJsonlConnector
from connectors.sdk import ConnectorContractError, ConnectorRunner, seed_acknowledged_records
from connectors.supervisor import (
    SupervisorContractError,
    aggregate_supervisor_status,
    preview_supervisor_policy,
)
from connectors.host import (
    ConnectorHostError,
    load_host_config,
    preview_host_config,
    run_host_daemon,
    run_host_once,
    validate_reserved_export_inbox,
)
from connectors.remote_worker import (
    load_remote_worker_config,
    preview_remote_worker_config,
    run_remote_worker_daemon,
    run_remote_worker_once,
)
from privacy.policy import AgenticJudge, PrivacyPolicy, load_scoped_virtual_key


def _token(args) -> str:
    if args.token_file:
        return load_file_token(Path(args.token_file))
    return load_keychain_token(args.keychain_service, args.keychain_account)


def _connector_runner(
    *,
    connector,
    common: dict,
    spool_path: Path,
    privacy: PrivacyPolicy,
) -> ConnectorRunner:
    canonical = _canonical_clients(common)
    if canonical is None:
        return ConnectorRunner(
            connector=connector,
            brain=BrainClient(**common),
            spool_path=spool_path,
            privacy=privacy,
        )
    brain, archive, tenant_id, principal_id = canonical
    return ConnectorRunner(
        connector=connector,
        brain=brain,
        archive=archive,
        tenant_id=tenant_id,
        principal_id=principal_id,
        spool_path=spool_path,
        privacy=privacy,
    )


def _canonical_clients(
    common: dict,
) -> tuple[CanonicalBrainWriter, CanonicalArchiveClient, str, str] | None:
    if os.environ.get("RECALL_CANONICAL_V2_ENABLED") != "1":
        return None
    tenant_id = os.environ.get("RECALL_TENANT_ID")
    principal_id = os.environ.get("RECALL_PRINCIPAL_ID")
    if (
        not tenant_id
        or not principal_id
        or principal_id != common["principal_id"]
        or common.get("visibility") != "private"
    ):
        raise ValueError("canonical connector identity is incomplete")
    canonical = {
        "endpoint": common["endpoint"],
        "token": common["token"],
        "source_id": common["source_id"],
        "tenant_id": tenant_id,
        "principal_id": principal_id,
    }
    return (
        CanonicalBrainWriter(**canonical),
        CanonicalArchiveClient(**canonical),
        tenant_id,
        principal_id,
    )


def _auth(parser: argparse.ArgumentParser) -> None:
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--token-file")
    group.add_argument("--keychain-service")
    parser.add_argument("--keychain-account")


def _connection(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--principal-id", default="owner")
    parser.add_argument("--visibility", choices=("private", "shared"), required=True)
    _auth(parser)


def _mcp_connection(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--principal-id", default="owner")
    parser.add_argument("--visibility", choices=("private", "shared"), default="private")
    _auth(parser)


def _capture_origin(value: str) -> str:
    if not ORIGIN.fullmatch(value):
        raise argparse.ArgumentTypeError("capture origin is invalid")
    return value


def _privacy(parser: argparse.ArgumentParser, *, choices=("off", "scrub", "drop"), default=None) -> None:
    parser.add_argument("--privacy-mode", choices=choices, default=default or os.environ.get("RECALL_PRIVACY_MODE", "off"))
    parser.add_argument("--privacy-judge-base-url", default=os.environ.get("RECALL_PRIVACY_JUDGE_BASE_URL"))
    parser.add_argument("--privacy-judge-key-file", default=os.environ.get("RECALL_PRIVACY_JUDGE_KEY_FILE"))
    parser.add_argument("--privacy-judge-model", default=os.environ.get("RECALL_PRIVACY_JUDGE_MODEL"))
    parser.add_argument("--privacy-judge-failure", choices=("drop", "ignore"), default="drop")


def _export_inbox(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--inbox", required=True)
    parser.add_argument("--catalog", required=True)


def _private_connection(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--principal-id", default="owner")
    parser.set_defaults(visibility="private")
    _auth(parser)


def _grep_ai(parser: argparse.ArgumentParser) -> None:
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--grep-api-key-file")
    group.add_argument("--grep-keychain-service")
    parser.add_argument("--grep-keychain-account")
    parser.add_argument("--spool", required=True)
    parser.add_argument("--max-pages", type=int, default=100)
    parser.add_argument("--page-size", type=int, default=10)


def _privacy_policy(args) -> PrivacyPolicy:
    values = (args.privacy_judge_base_url, args.privacy_judge_key_file, args.privacy_judge_model)
    if any(values) and not all(values):
        raise SystemExit("privacy judge requires base URL, private virtual-key file, and model")
    judge = AgenticJudge(
        base_url=args.privacy_judge_base_url,
        virtual_key=load_scoped_virtual_key(Path(args.privacy_judge_key_file)),
        model=args.privacy_judge_model,
    ) if all(values) else None
    return PrivacyPolicy(mode=args.privacy_mode, judge=judge, judge_failure=args.privacy_judge_failure)


def _registry_policy(connector_id: str, *, visibility: str, privacy_mode: str,
                     authorities: set[str]) -> None:
    try:
        validate_policy(
            connector_id, visibility=visibility,
            privacy_mode=privacy_mode, authorities=authorities,
        )
    except ConnectorRegistryError as error:
        raise SystemExit(str(error)) from None


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description="Consent-first Recall Brain client")
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser("integration-catalog")
    commands.add_parser("integration-activation-catalog")
    activation_preview = commands.add_parser("integration-activation-preview")
    activation_preview.add_argument("--config", required=True)
    activation_configure = commands.add_parser("integration-activation-configure")
    activation_configure.add_argument("--config", required=True)
    activation_configure.add_argument("--state", required=True)
    activation_transition = commands.add_parser("integration-activation-transition")
    activation_transition.add_argument("--state", required=True)
    activation_transition.add_argument(
        "--connector-id",
        choices=tuple(item.connector_id for item in REGISTRY),
        required=True,
    )
    activation_transition.add_argument(
        "--action",
        choices=("enable", "pause", "resume", "revoke", "uninstall"),
        required=True,
    )
    activation_status = commands.add_parser("integration-activation-status")
    activation_status.add_argument("--state", required=True)
    activation_status.add_argument(
        "--connector-id",
        choices=tuple(item.connector_id for item in REGISTRY),
        required=True,
    )
    commands.add_parser("connector-registry-preview")
    registry_status = commands.add_parser("connector-registry-status")
    registry_status.add_argument("--connector-id", choices=tuple(item.connector_id for item in REGISTRY), required=True)
    registry_status.add_argument("--enabled", action="store_true")
    registry_status.add_argument("--privacy-mode", choices=("off", "scrub", "drop"), required=True)
    registry_status.add_argument("--authority", choices=("brain", "source"), action="append", default=[])
    registry_status.add_argument("--spool")
    seed_acknowledged = commands.add_parser("connector-spool-seed-acknowledged")
    seed_acknowledged.add_argument("--spool", required=True)
    seed_acknowledged.add_argument("--input", required=True)
    commands.add_parser("connector-supervisor-preview")
    supervisor_status = commands.add_parser("connector-supervisor-status")
    supervisor_status.add_argument("--state", required=True)
    supervisor_status.add_argument("--now", type=float)
    host_preview = commands.add_parser("connector-supervisor-config-preview")
    host_preview.add_argument("--config", required=True)
    host_preview.add_argument("--reserved-export-inbox")
    host_run = commands.add_parser("connector-supervisor-run")
    host_run.add_argument("--config", required=True)
    host_run.add_argument("--state", required=True)
    host_run.add_argument("--once", action="store_true")
    remote_preview = commands.add_parser("remote-worker-config-preview")
    remote_preview.add_argument("--config", required=True)
    remote_run = commands.add_parser("remote-worker-run")
    remote_run.add_argument("--config", required=True)
    remote_run.add_argument("--state", required=True)
    remote_run.add_argument("--once", action="store_true")
    dry = commands.add_parser("dry-run")
    dry.add_argument("--visibility", choices=("private", "shared"), required=True)
    dry.add_argument("--claude-root")
    dry.add_argument("--codex-root")
    local_surface = commands.add_parser("mac-local-surface-preview")
    local_surface.add_argument("--app", default="/Applications/ChatGPT.app")
    local_surface.add_argument("--codex-root", default="~/.codex/sessions")
    claude_surface = commands.add_parser("mac-claude-surface-preview")
    claude_surface.add_argument("--app", default="/Applications/Claude.app")
    claude_surface.add_argument(
        "--support-root", default="~/Library/Application Support/Claude"
    )

    keychain = commands.add_parser("keychain-store")
    keychain.add_argument("--service", required=True)
    keychain.add_argument("--account", required=True)

    collect = commands.add_parser("collect")
    _connection(collect)
    _privacy(collect)
    collect.add_argument("--harness", choices=("claude", "codex"), required=True)
    collect.add_argument("--root", required=True)
    collect.add_argument("--spool", required=True)
    collect.add_argument("--max-scan-records", type=int, default=1000)
    collect.add_argument("--max-scan-seconds", type=float, default=20.0)

    cowork = commands.add_parser("cowork-local-sync")
    _private_connection(cowork)
    _privacy(cowork, choices=("scrub", "drop"), default="scrub")
    cowork.add_argument("--root", required=True)
    cowork.add_argument("--spool", required=True)

    imessage = commands.add_parser("imessage-sync")
    _private_connection(imessage)
    _privacy(imessage, choices=("scrub", "drop"), default="scrub")
    imessage.add_argument("--database", required=True)
    imessage.add_argument("--spool", required=True)
    imessage.add_argument("--page-size", type=int, default=100)
    imessage.add_argument("--chat-id", action="append", default=[])
    imessage.add_argument("--date-min")
    imessage.add_argument("--date-max")

    whatsapp = commands.add_parser("whatsapp-export-sync")
    _private_connection(whatsapp)
    _privacy(whatsapp, choices=("scrub", "drop"), default="scrub")
    whatsapp.add_argument("--export", required=True)
    whatsapp.add_argument("--conversation-id", required=True)
    whatsapp.add_argument("--owner-name", action="append", default=[])
    whatsapp.add_argument("--date-order", choices=("dmy", "mdy"), required=True)
    whatsapp.add_argument("--timezone", required=True)
    whatsapp.add_argument("--spool", required=True)
    whatsapp.add_argument("--page-size", type=int, default=500)

    selected_text = commands.add_parser("selected-text-sync")
    _private_connection(selected_text)
    _privacy(selected_text, choices=("scrub", "drop"), default="scrub")
    selected_text.add_argument("--root", required=True)
    selected_text.add_argument(
        "--extension",
        choices=(".md", ".markdown", ".txt"),
        action="append",
    )
    selected_text.add_argument("--max-depth", type=int, default=8)
    selected_text.add_argument("--spool", required=True)
    selected_text.add_argument("--page-size", type=int, default=500)

    browser = commands.add_parser("browser-sync")
    _private_connection(browser)
    _privacy(browser, choices=("scrub", "drop"), default="scrub")
    browser.add_argument("--browser", choices=("chrome", "safari"), required=True)
    browser.add_argument("--history")
    browser.add_argument("--bookmarks")
    browser.add_argument("--date-min")
    browser.add_argument("--date-max")
    browser.add_argument("--spool", required=True)
    browser.add_argument("--page-size", type=int, default=500)

    notes = commands.add_parser("apple-notes-sync")
    _private_connection(notes)
    _privacy(notes, choices=("scrub", "drop"), default="scrub")
    notes.add_argument("--database", required=True)
    notes.add_argument("--date-min")
    notes.add_argument("--date-max")
    notes.add_argument("--spool", required=True)
    notes.add_argument("--page-size", type=int, default=500)

    hermes = commands.add_parser("hermes-session-sync")
    _private_connection(hermes)
    _privacy(hermes, choices=("scrub", "drop"), default="scrub")
    hermes.add_argument("--database", required=True)
    hermes.add_argument("--source", action="append", required=True)
    hermes.add_argument(
        "--role", action="append", choices=("assistant", "user"), default=[]
    )
    hermes.add_argument("--date-min")
    hermes.add_argument("--date-max")
    hermes.add_argument("--spool", required=True)
    hermes.add_argument("--page-size", type=int, default=500)

    for command in (
        "mail-import-sync", "calendar-import-sync", "contact-import-sync",
        "slack-archive-sync", "notion-archive-sync", "x-archive-sync",
    ):
        portable = commands.add_parser(command)
        _private_connection(portable)
        _privacy(portable, choices=("scrub", "drop"), default="scrub")
        portable.add_argument("--input", required=True)
        portable.add_argument("--archive-id", required=True)
        portable.add_argument("--owner-identifier", action="append", default=[])
        portable.add_argument("--remove-native-id", action="append", default=[])
        portable.add_argument("--spool", required=True)
        portable.add_argument("--page-size", type=int, default=500)

    feed = commands.add_parser("feed-sync")
    _private_connection(feed)
    _privacy(feed, choices=("scrub", "drop"), default="scrub")
    feed.add_argument("--url", required=True)
    feed.add_argument("--feed-id", required=True)
    feed.add_argument("--spool", required=True)

    selected_jsonl = commands.add_parser("jsonl-import-sync")
    _private_connection(selected_jsonl)
    _privacy(selected_jsonl, choices=("scrub", "drop"), default="scrub")
    selected_jsonl.add_argument("--root", required=True)
    selected_jsonl.add_argument("--max-depth", type=int, default=8)
    selected_jsonl.add_argument("--remove-native-id", action="append", default=[])
    selected_jsonl.add_argument("--spool", required=True)
    selected_jsonl.add_argument("--page-size", type=int, default=500)

    mac_status_parser = commands.add_parser("mac-status")
    mac_status_parser.add_argument(
        "--prefix", default=str(Path.home() / "Library" / "Application Support" / "RecallBrain")
    )
    mac_status_parser.add_argument(
        "--launch-agents", default=str(Path.home() / "Library" / "LaunchAgents")
    )
    mac_status_parser.add_argument("--now", type=float)

    mac_disable = commands.add_parser("mac-disable")
    mac_disable.add_argument("--source", choices=tuple(SOURCE_SPECS), required=True)
    mac_disable.add_argument(
        "--launch-agents", default=str(Path.home() / "Library" / "LaunchAgents")
    )
    mac_disable.add_argument("--no-load", action="store_true")

    for command in ("mac-pause", "mac-resume"):
        lifecycle = commands.add_parser(command)
        lifecycle.add_argument("--source", choices=tuple(SOURCE_SPECS), required=True)
        lifecycle.add_argument(
            "--prefix",
            default=str(
                Path.home() / "Library" / "Application Support" / "RecallBrain"
            ),
        )
        lifecycle.add_argument(
            "--launch-agents",
            default=str(Path.home() / "Library" / "LaunchAgents"),
        )
        lifecycle.add_argument("--no-load", action="store_true")

    mac_route = commands.add_parser("mac-route-info")
    mac_route.add_argument("--source", choices=tuple(SOURCE_SPECS), required=True)
    mac_route.add_argument(
        "--launch-agents", default=str(Path.home() / "Library" / "LaunchAgents")
    )
    mac_route_apply = commands.add_parser("mac-route-apply")
    mac_route_apply.add_argument(
        "--source", choices=tuple(SOURCE_SPECS), required=True
    )
    mac_route_apply.add_argument("--tenant-id", required=True)
    mac_route_apply.add_argument("--principal-id", required=True)
    mac_route_apply.add_argument(
        "--launch-agents", default=str(Path.home() / "Library" / "LaunchAgents")
    )

    mac_revoke = commands.add_parser("mac-revoke")
    mac_revoke.add_argument("--source", choices=tuple(SOURCE_SPECS), required=True)
    mac_revoke.add_argument(
        "--launch-agents", default=str(Path.home() / "Library" / "LaunchAgents")
    )
    mac_revoke.add_argument("--no-load", action="store_true")

    mac_reset = commands.add_parser("mac-reset-local")
    mac_reset.add_argument("--source", choices=tuple(SOURCE_SPECS), required=True)
    mac_reset.add_argument("--confirm-source", required=True)
    mac_reset.add_argument(
        "--prefix", default=str(Path.home() / "Library" / "Application Support" / "RecallBrain")
    )
    mac_reset.add_argument(
        "--launch-agents", default=str(Path.home() / "Library" / "LaunchAgents")
    )
    mac_reset.add_argument("--no-load", action="store_true")

    mac_support = commands.add_parser("mac-support")
    mac_support.add_argument(
        "--prefix", default=str(Path.home() / "Library" / "Application Support" / "RecallBrain")
    )
    mac_support.add_argument(
        "--launch-agents", default=str(Path.home() / "Library" / "LaunchAgents")
    )
    mac_support.add_argument("--now", type=float)

    export = commands.add_parser("export")
    _connection(export)
    _privacy(export)
    export.add_argument("--dry-run", action="store_true")
    export.add_argument("inputs", nargs="+")

    put = commands.add_parser("put")
    _connection(put)
    _privacy(put)
    put.add_argument("--text")
    put.add_argument("--provenance-uri", default="manual://recall_put")

    preview = commands.add_parser("privacy-preview")
    _privacy(preview)

    inbox_dry = commands.add_parser("export-inbox-dry-run")
    _export_inbox(inbox_dry)
    inbox_dry.add_argument("--privacy-mode", choices=("off", "scrub", "drop"), default=os.environ.get("RECALL_PRIVACY_MODE", "off"))

    inbox_list = commands.add_parser("export-inbox-list")
    _export_inbox(inbox_list)

    inbox_remove = commands.add_parser("export-inbox-remove")
    _export_inbox(inbox_remove)
    inbox_remove.add_argument("export_id")

    inbox_sync = commands.add_parser("export-inbox-sync")
    _connection(inbox_sync)
    _privacy(inbox_sync)
    _export_inbox(inbox_sync)
    inbox_sync.add_argument("--spool", required=True)

    grep_preview = commands.add_parser("grep-ai-config-preview")
    _private_connection(grep_preview)
    _privacy(grep_preview, choices=("scrub", "drop"), default="drop")
    _grep_ai(grep_preview)
    grep_preview.add_argument("--executable", default="recall-brain")

    grep_sync = commands.add_parser("grep-ai-sync")
    _private_connection(grep_sync)
    _privacy(grep_sync, choices=("scrub", "drop"), default="drop")
    _grep_ai(grep_sync)

    mcp_preview = commands.add_parser("mcp-config-preview")
    _mcp_connection(mcp_preview)
    _privacy(mcp_preview)
    mcp_preview.add_argument("--capture-origin", required=True, type=_capture_origin)
    mcp_preview.add_argument("--executable", default="recall-brain")

    mcp_serve = commands.add_parser("mcp-serve")
    _mcp_connection(mcp_serve)
    _privacy(mcp_serve)
    mcp_serve.add_argument("--capture-origin", required=True, type=_capture_origin)

    delete = commands.add_parser("delete")
    _connection(delete)
    delete.add_argument("receipt")

    search = commands.add_parser("search")
    _connection(search)
    search.add_argument("query")
    search.add_argument("--limit", type=int, default=10)
    search.add_argument("--route-source-id")
    search.add_argument("--source-family", choices=(
        "coding_history", "deliberate_capture", "user_export", "third_party_research",
        "communications", "schedule", "contacts", "social", "documents",
        "work_activity", "local_activity", "personal_media",
    ))
    search.add_argument("--source-alias")

    show = commands.add_parser("show")
    _connection(show)
    show.add_argument("receipt")

    doctor = commands.add_parser("doctor")
    _connection(doctor)
    return root


def main() -> None:
    args = parser().parse_args()
    if args.command == "mac-status":
        try:
            result = mac_status(
                prefix=Path(args.prefix), launch_agents=Path(args.launch_agents),
                now=time.time() if args.now is None else args.now,
            )
        except MacUtilityError as error:
            raise SystemExit(str(error)) from None
        print(json.dumps(result, sort_keys=True))
        return
    if args.command == "mac-disable":
        try:
            result = disable_source(
                args.source, launch_agents=Path(args.launch_agents), no_load=args.no_load,
            )
        except MacUtilityError as error:
            raise SystemExit(str(error)) from None
        print(json.dumps(result, sort_keys=True))
        return
    if args.command in {"mac-pause", "mac-resume"}:
        action = pause_source if args.command == "mac-pause" else resume_source
        try:
            result = action(
                args.source,
                prefix=Path(args.prefix),
                launch_agents=Path(args.launch_agents),
                no_load=args.no_load,
            )
        except MacUtilityError as error:
            raise SystemExit(str(error)) from None
        print(json.dumps(result, sort_keys=True))
        return
    if args.command == "mac-route-info":
        try:
            result = route_info(
                args.source, launch_agents=Path(args.launch_agents)
            )
        except MacUtilityError as error:
            raise SystemExit(str(error)) from None
        print(json.dumps(result, sort_keys=True))
        return
    if args.command == "mac-route-apply":
        try:
            result = apply_route(
                args.source,
                launch_agents=Path(args.launch_agents),
                tenant_id=args.tenant_id,
                principal_id=args.principal_id,
            )
        except MacUtilityError as error:
            raise SystemExit(str(error)) from None
        print(json.dumps(result, sort_keys=True))
        return
    if args.command == "mac-revoke":
        try:
            result = revoke_source(
                args.source, launch_agents=Path(args.launch_agents), no_load=args.no_load,
            )
        except MacUtilityError as error:
            raise SystemExit(str(error)) from None
        print(json.dumps(result, sort_keys=True))
        return
    if args.command == "mac-reset-local":
        try:
            result = reset_local_source(
                args.source, prefix=Path(args.prefix),
                launch_agents=Path(args.launch_agents),
                confirmation=args.confirm_source, no_load=args.no_load,
            )
        except MacUtilityError as error:
            raise SystemExit(str(error)) from None
        print(json.dumps(result, sort_keys=True))
        return
    if args.command == "mac-support":
        try:
            result = support_report(
                prefix=Path(args.prefix), launch_agents=Path(args.launch_agents),
                now=time.time() if args.now is None else args.now,
            )
        except MacUtilityError as error:
            raise SystemExit(str(error)) from None
        print(json.dumps(result, sort_keys=True))
        return
    if args.command == "connector-registry-preview":
        print(json.dumps(registry_preview(), sort_keys=True))
        return
    if args.command == "integration-catalog":
        print(json.dumps(integration_catalog(), sort_keys=True))
        return
    if args.command == "integration-activation-catalog":
        print(json.dumps(activation_catalog(), sort_keys=True))
        return
    if args.command == "integration-activation-preview":
        try:
            result = preview_activation_config(
                load_activation_config(Path(args.config))
            )
        except ActivationError as error:
            raise SystemExit(str(error)) from None
        print(json.dumps(result, sort_keys=True))
        return
    if args.command == "integration-activation-configure":
        store = None
        try:
            config = load_activation_config(Path(args.config))
            store = ActivationStore(Path(args.state))
            result = store.configure(config)
        except ActivationError as error:
            raise SystemExit(str(error)) from None
        finally:
            if store is not None:
                store.close()
        print(json.dumps(result, sort_keys=True))
        return
    if args.command in {
        "integration-activation-transition",
        "integration-activation-status",
    }:
        store = None
        try:
            store = ActivationStore(
                Path(args.state),
                create=False,
                read_only=args.command == "integration-activation-status",
            )
            if args.command == "integration-activation-transition":
                result = store.transition(args.connector_id, args.action)
            else:
                result = store.status(args.connector_id)
        except ActivationError as error:
            raise SystemExit(str(error)) from None
        finally:
            if store is not None:
                store.close()
        print(json.dumps(result, sort_keys=True))
        return
    if args.command == "connector-registry-status":
        if len(args.authority) != len(set(args.authority)):
            raise SystemExit("duplicate_authority_slots")
        try:
            result = aggregate_status(
                args.connector_id, args.enabled, args.privacy_mode,
                set(args.authority), Path(args.spool) if args.spool else None,
            )
        except ConnectorRegistryError as error:
            raise SystemExit(str(error)) from None
        print(json.dumps(result, sort_keys=True))
        return
    if args.command == "connector-spool-seed-acknowledged":
        try:
            result = seed_acknowledged_records(
                spool_path=Path(args.spool), seed_path=Path(args.input),
            )
        except ConnectorContractError as error:
            raise SystemExit(str(error)) from None
        print(json.dumps(result, sort_keys=True))
        return
    if args.command == "connector-supervisor-preview":
        print(json.dumps(preview_supervisor_policy(), sort_keys=True))
        return
    if args.command == "connector-supervisor-status":
        try:
            result = aggregate_supervisor_status(
                Path(args.state), now=time.time() if args.now is None else args.now,
            )
        except SupervisorContractError as error:
            raise SystemExit(str(error)) from None
        print(json.dumps(result, sort_keys=True))
        return
    if args.command == "connector-supervisor-config-preview":
        try:
            config = load_host_config(Path(args.config))
            if args.reserved_export_inbox:
                validate_reserved_export_inbox(config, Path(args.reserved_export_inbox))
            result = preview_host_config(config)
        except ConnectorHostError as error:
            raise SystemExit(str(error)) from None
        print(json.dumps(result, sort_keys=True))
        return
    if args.command == "connector-supervisor-run":
        try:
            if args.once:
                result = run_host_once(Path(args.config), Path(args.state))
            else:
                result = run_host_daemon(Path(args.config), Path(args.state))
        except ConnectorHostError as error:
            raise SystemExit(str(error)) from None
        print(json.dumps(result, sort_keys=True))
        return
    if args.command == "remote-worker-config-preview":
        try:
            result = preview_remote_worker_config(
                load_remote_worker_config(Path(args.config))
            )
        except ConnectorHostError as error:
            raise SystemExit(str(error)) from None
        print(json.dumps(result, sort_keys=True))
        return
    if args.command == "remote-worker-run":
        try:
            if args.once:
                result = run_remote_worker_once(Path(args.config), Path(args.state))
            else:
                result = run_remote_worker_daemon(Path(args.config), Path(args.state))
        except ConnectorHostError as error:
            raise SystemExit(str(error)) from None
        print(json.dumps(result, sort_keys=True))
        return
    if args.command == "dry-run":
        selections = []
        if args.claude_root:
            selections.append({"harness": "claude", "root": args.claude_root})
        if args.codex_root:
            selections.append({"harness": "codex", "root": args.codex_root})
        print(json.dumps(dry_run_manifest(selections=selections, visibility=args.visibility), sort_keys=True))
        return
    if args.command == "mac-local-surface-preview":
        try:
            result = mac_local_surface_preview(
                app=Path(args.app), codex_root=Path(args.codex_root),
            )
        except LocalSurfaceError as error:
            raise SystemExit(str(error)) from None
        print(json.dumps(result, sort_keys=True))
        return
    if args.command == "mac-claude-surface-preview":
        try:
            result = mac_claude_surface_preview(
                app=Path(args.app), support_root=Path(args.support_root),
            )
        except LocalSurfaceError as error:
            raise SystemExit(str(error)) from None
        print(json.dumps(result, sort_keys=True))
        return
    if args.command == "keychain-store":
        store_keychain_token(args.service, args.account, sys.stdin.read().rstrip("\r\n"))
        print(json.dumps({"status": "stored", "service": args.service, "account": args.account}, sort_keys=True))
        return
    if args.command == "privacy-preview":
        raw = sys.stdin.read()
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            value = raw
        print(json.dumps(_privacy_policy(args).apply(value).receipt(), sort_keys=True))
        return
    if args.command in {"export-inbox-dry-run", "export-inbox-list", "export-inbox-remove"}:
        connector = ExportInboxConnector(
            inbox=Path(args.inbox), catalog_path=Path(args.catalog),
            source_id="chatgpt:export:local",
            privacy_mode=getattr(args, "privacy_mode", "off"),
        )
        try:
            if args.command == "export-inbox-dry-run":
                result = connector.dry_run()
            elif args.command == "export-inbox-list":
                result = {"schema_version": 1, "exports": connector.exports()}
            else:
                result = connector.queue_remove(args.export_id)
        finally:
            connector.close()
        print(json.dumps(result, sort_keys=True))
        return
    if args.command == "mcp-config-preview":
        _registry_policy(
            "recall.capture", visibility=args.visibility,
            privacy_mode=args.privacy_mode, authorities={"brain"},
        )
        if args.keychain_service and not args.keychain_account:
            raise SystemExit("--keychain-account is required with --keychain-service")
        auth = ["--token-file", args.token_file] if args.token_file else [
            "--keychain-service", args.keychain_service,
            "--keychain-account", args.keychain_account,
        ]
        command_args = [
            "mcp-serve", "--endpoint", args.endpoint,
            "--source-id", args.source_id, "--principal-id", args.principal_id,
            "--capture-origin", args.capture_origin,
            "--visibility", args.visibility, *auth,
            "--privacy-mode", args.privacy_mode,
            "--privacy-judge-failure", args.privacy_judge_failure,
        ]
        if args.privacy_judge_base_url:
            command_args.extend([
                "--privacy-judge-base-url", args.privacy_judge_base_url,
                "--privacy-judge-key-file", args.privacy_judge_key_file,
                "--privacy-judge-model", args.privacy_judge_model,
            ])
        result = {
            "schema_version": 1, "mode": "mcp-config-preview",
            "network_requests": 0, "writes": 0,
            "mcpServers": {"recall": {"command": args.executable, "args": command_args}},
        }
        print(json.dumps(result, sort_keys=True))
        return
    if args.command == "grep-ai-config-preview":
        _registry_policy(
            "grep.ai", visibility="private",
            privacy_mode=args.privacy_mode, authorities={"brain", "source"},
        )
        if args.keychain_service and not args.keychain_account:
            raise SystemExit("--keychain-account is required with --keychain-service")
        if args.grep_keychain_service and not args.grep_keychain_account:
            raise SystemExit("--grep-keychain-account is required with --grep-keychain-service")
        auth = ["--token-file", args.token_file] if args.token_file else [
            "--keychain-service", args.keychain_service,
            "--keychain-account", args.keychain_account,
        ]
        grep_auth = ["--grep-api-key-file", args.grep_api_key_file] if args.grep_api_key_file else [
            "--grep-keychain-service", args.grep_keychain_service,
            "--grep-keychain-account", args.grep_keychain_account,
        ]
        command_args = [
            "grep-ai-sync", "--endpoint", args.endpoint,
            "--source-id", args.source_id, "--principal-id", args.principal_id,
            *auth, *grep_auth,
            "--spool", args.spool, "--max-pages", str(args.max_pages),
            "--page-size", str(args.page_size),
            "--privacy-mode", args.privacy_mode,
            "--privacy-judge-failure", args.privacy_judge_failure,
        ]
        if args.privacy_judge_base_url:
            command_args.extend([
                "--privacy-judge-base-url", args.privacy_judge_base_url,
                "--privacy-judge-key-file", args.privacy_judge_key_file,
                "--privacy-judge-model", args.privacy_judge_model,
            ])
        print(json.dumps({
            "schema_version": 1, "mode": "grep-ai-config-preview",
            "network_requests": 0, "writes": 0, "visibility": "private",
            "command": args.executable, "args": command_args,
        }, sort_keys=True))
        return

    if args.keychain_service and not args.keychain_account:
        raise SystemExit("--keychain-account is required with --keychain-service")
    if getattr(args, "grep_keychain_service", None) and not args.grep_keychain_account:
        raise SystemExit("--grep-keychain-account is required with --grep-keychain-service")
    if args.command == "mcp-serve":
        _registry_policy(
            "recall.capture", visibility=args.visibility,
            privacy_mode=args.privacy_mode, authorities={"brain"},
        )
    elif args.command == "export-inbox-sync":
        _registry_policy(
            "openai.export-inbox", visibility=args.visibility,
            privacy_mode=args.privacy_mode, authorities={"brain"},
        )
    elif args.command == "grep-ai-sync":
        _registry_policy(
            "grep.ai", visibility="private",
            privacy_mode=args.privacy_mode, authorities={"brain", "source"},
        )
    elif args.command == "imessage-sync":
        _registry_policy(
            "apple.imessage", visibility="private",
            privacy_mode=args.privacy_mode, authorities={"brain", "source"},
        )
    elif args.command == "whatsapp-export-sync":
        _registry_policy(
            "whatsapp.export", visibility="private",
            privacy_mode=args.privacy_mode, authorities={"brain", "source"},
        )
    elif args.command == "selected-text-sync":
        _registry_policy(
            "local.selected-text", visibility="private",
            privacy_mode=args.privacy_mode, authorities={"brain", "source"},
        )
    elif args.command == "browser-sync":
        _registry_policy(
            "apple.safari" if args.browser == "safari" else "google.chrome",
            visibility="private",
            privacy_mode=args.privacy_mode, authorities={"brain", "source"},
        )
    elif args.command == "apple-notes-sync":
        _registry_policy(
            "apple.notes", visibility="private",
            privacy_mode=args.privacy_mode, authorities={"brain", "source"},
        )
    elif args.command == "hermes-session-sync":
        _registry_policy(
            "hermes.sessions", visibility="private",
            privacy_mode=args.privacy_mode, authorities={"brain", "source"},
        )
    elif args.command in {
        "mail-import-sync", "calendar-import-sync", "contact-import-sync",
        "slack-archive-sync", "notion-archive-sync", "x-archive-sync",
    }:
        _registry_policy(
            {
                "mail-import-sync": "portable.mail",
                "calendar-import-sync": "portable.calendar",
                "contact-import-sync": "portable.contacts",
                "slack-archive-sync": "portable.slack",
                "notion-archive-sync": "portable.notion",
                "x-archive-sync": "portable.x",
            }[args.command],
            visibility="private",
            privacy_mode=args.privacy_mode,
            authorities={"brain", "source"},
        )
    elif args.command == "feed-sync":
        _registry_policy(
            "portable.feed", visibility="private",
            privacy_mode=args.privacy_mode, authorities={"brain", "source"},
        )
    elif args.command == "jsonl-import-sync":
        _registry_policy(
            "portable.jsonl", visibility="private",
            privacy_mode=args.privacy_mode, authorities={"brain", "source"},
        )
    token = _token(args)
    common = {
        "endpoint": args.endpoint,
        "token": token,
        "source_id": args.source_id,
        "principal_id": args.principal_id,
        "visibility": args.visibility,
    }
    privacy = _privacy_policy(args) if args.command in {
        "collect", "cowork-local-sync", "export", "put", "export-inbox-sync",
        "mcp-serve", "grep-ai-sync", "imessage-sync", "whatsapp-export-sync",
        "selected-text-sync", "browser-sync", "apple-notes-sync",
        "hermes-session-sync", "mail-import-sync", "calendar-import-sync",
        "contact-import-sync", "slack-archive-sync", "notion-archive-sync",
        "x-archive-sync", "feed-sync", "jsonl-import-sync",
    } else PrivacyPolicy(mode="off")
    if args.command == "mcp-serve":
        backend = CaptureClient(**common, privacy=privacy)
        serve_mcp(
            McpServer(backend, capture_origin=args.capture_origin),
            sys.stdin, sys.stdout, sys.stderr,
        )
        return
    if args.command == "collect":
        canonical = _canonical_clients(common)
        collector = Collector(
            root=Path(args.root), harness=args.harness, source_id=args.source_id,
            spool_path=Path(args.spool), endpoint=args.endpoint, token=token,
            principal_id=args.principal_id, visibility=args.visibility,
            privacy=privacy,
            brain_writer=canonical[0] if canonical else None,
            archive=canonical[1] if canonical else None,
            tenant_id=canonical[2] if canonical else None,
            # Canonical collectors already address individual records through
            # manifest members. Bundle their private archive writes so a
            # history backfill does not perform one object request per record.
            bulk_manifest_archive=canonical is not None,
            max_scan_records=args.max_scan_records,
            max_scan_seconds=args.max_scan_seconds,
        )
        try:
            result = {
                "scan": collector.scan(),
                "flush": collector.flush(),
                "doctor": collector.doctor(include_dead_letters=False),
            }
        finally:
            collector.close()
    elif args.command == "cowork-local-sync":
        connector = CoworkLocalConnector(
            root=Path(args.root), source_id=args.source_id,
        )
        runner = _connector_runner(
            connector=connector, common=common,
            spool_path=Path(args.spool), privacy=privacy,
        )
        try:
            result = {"sync": runner.run_once(), "doctor": runner.doctor()}
        finally:
            runner.close()
    elif args.command == "imessage-sync":
        connector = IMessageConnector(
            database=Path(args.database),
            source_id=args.source_id,
            page_size=args.page_size,
            chat_ids=tuple(args.chat_id),
            date_min=args.date_min,
            date_max=args.date_max,
        )
        runner = _connector_runner(
            connector=connector, common=common,
            spool_path=Path(args.spool), privacy=privacy,
        )
        try:
            result = {"sync": runner.run_once(), "doctor": runner.doctor()}
        finally:
            runner.close()
    elif args.command == "whatsapp-export-sync":
        connector = WhatsAppExportConnector(
            export=Path(args.export),
            source_id=args.source_id,
            conversation_id=args.conversation_id,
            owner_names=tuple(args.owner_name),
            date_order=args.date_order,
            timezone_name=args.timezone,
            page_size=args.page_size,
        )
        runner = _connector_runner(
            connector=connector, common=common,
            spool_path=Path(args.spool), privacy=privacy,
        )
        try:
            result = {"sync": runner.run_once(), "doctor": runner.doctor()}
        finally:
            runner.close()
    elif args.command == "selected-text-sync":
        connector = SelectedTextConnector(
            root=Path(args.root),
            source_id=args.source_id,
            extensions=tuple(sorted(args.extension or (".md", ".markdown", ".txt"))),
            max_depth=args.max_depth,
            page_size=args.page_size,
        )
        runner = _connector_runner(
            connector=connector, common=common,
            spool_path=Path(args.spool), privacy=privacy,
        )
        try:
            result = {"sync": runner.run_once(), "doctor": runner.doctor()}
        finally:
            runner.close()
    elif args.command == "browser-sync":
        connector = BrowserActivityConnector(
            browser=args.browser,
            history=Path(args.history) if args.history else None,
            bookmarks=Path(args.bookmarks) if args.bookmarks else None,
            source_id=args.source_id,
            date_min=args.date_min,
            date_max=args.date_max,
            page_size=args.page_size,
        )
        runner = _connector_runner(
            connector=connector, common=common,
            spool_path=Path(args.spool), privacy=privacy,
        )
        try:
            result = {"sync": runner.run_once(), "doctor": runner.doctor()}
        finally:
            runner.close()
    elif args.command == "apple-notes-sync":
        connector = AppleNotesConnector(
            database=Path(args.database),
            source_id=args.source_id,
            date_min=args.date_min,
            date_max=args.date_max,
            page_size=args.page_size,
        )
        runner = _connector_runner(
            connector=connector, common=common,
            spool_path=Path(args.spool), privacy=privacy,
        )
        try:
            result = {"sync": runner.run_once(), "doctor": runner.doctor()}
        finally:
            runner.close()
    elif args.command == "hermes-session-sync":
        connector = HermesSessionConnector(
            database=Path(args.database),
            source_id=args.source_id,
            sources=tuple(sorted(args.source)),
            roles=tuple(sorted(args.role or ("assistant", "user"))),
            date_min=args.date_min,
            date_max=args.date_max,
            page_size=args.page_size,
        )
        runner = _connector_runner(
            connector=connector, common=common,
            spool_path=Path(args.spool), privacy=privacy,
        )
        try:
            result = {"sync": runner.run_once(), "doctor": runner.doctor()}
        finally:
            runner.close()
    elif args.command in {
        "mail-import-sync", "calendar-import-sync", "contact-import-sync",
        "slack-archive-sync", "notion-archive-sync", "x-archive-sync",
    }:
        connector_type = {
            "mail-import-sync": MailImportConnector,
            "calendar-import-sync": CalendarImportConnector,
            "contact-import-sync": ContactImportConnector,
            "slack-archive-sync": SlackArchiveConnector,
            "notion-archive-sync": NotionArchiveConnector,
            "x-archive-sync": XArchiveConnector,
        }[args.command]
        connector = connector_type(
            path=Path(args.input),
            source_id=args.source_id,
            archive_id=args.archive_id,
            owner_identifiers=tuple(sorted(
                value.lower() for value in args.owner_identifier
            )),
            removed_native_ids=tuple(sorted(args.remove_native_id)),
            page_size=args.page_size,
        )
        runner = _connector_runner(
            connector=connector, common=common,
            spool_path=Path(args.spool), privacy=privacy,
        )
        try:
            result = {"sync": runner.run_once(), "doctor": runner.doctor()}
        finally:
            runner.close()
    elif args.command == "feed-sync":
        runner = _connector_runner(
            connector=FeedConnector(
                url=args.url, feed_id=args.feed_id, source_id=args.source_id,
            ),
            common=common,
            spool_path=Path(args.spool),
            privacy=privacy,
        )
        try:
            result = {"sync": runner.run_once(), "doctor": runner.doctor()}
        finally:
            runner.close()
    elif args.command == "jsonl-import-sync":
        runner = _connector_runner(
            connector=SelectedJsonlConnector(
                root=Path(args.root),
                source_id=args.source_id,
                removed_native_ids=tuple(sorted(args.remove_native_id)),
                max_depth=args.max_depth,
                page_size=args.page_size,
            ),
            common=common,
            spool_path=Path(args.spool),
            privacy=privacy,
        )
        try:
            result = {"sync": runner.run_once(), "doctor": runner.doctor()}
        finally:
            runner.close()
    elif args.command == "export":
        importer = ExportImporter(source_id=args.source_id, principal_id=args.principal_id, visibility=args.visibility, privacy=privacy)
        inventory = importer.inventory([Path(value) for value in args.inputs])
        if args.dry_run:
            result = {**inventory, "records": len(inventory["records"])}
        else:
            result = importer.import_with(BrainClient(**common, privacy=privacy), [Path(value) for value in args.inputs])
    elif args.command == "export-inbox-sync":
        connector = ExportInboxConnector(
            inbox=Path(args.inbox), catalog_path=Path(args.catalog),
            source_id=args.source_id, privacy_mode=args.privacy_mode,
        )
        runner = _connector_runner(
            connector=connector, common=common,
            spool_path=Path(args.spool), privacy=privacy,
        )
        try:
            result = {
                "sync": runner.run_once(),
                "doctor": runner.doctor(),
                "exports": len(connector.exports()),
            }
        finally:
            runner.close()
            connector.close()
    elif args.command == "grep-ai-sync":
        grep_key = load_private_api_key(Path(args.grep_api_key_file)) if args.grep_api_key_file else validate_api_key(
            load_keychain_token(args.grep_keychain_service, args.grep_keychain_account)
        )
        connector = GrepAIConnector(
            api_key=grep_key,
            source_id=args.source_id, max_pages=args.max_pages, page_size=args.page_size,
        )
        runner = _connector_runner(
            connector=connector, common=common,
            spool_path=Path(args.spool), privacy=privacy,
        )
        try:
            result = {"sync": runner.run_once(), "doctor": runner.doctor()}
        finally:
            runner.close()
    elif args.command == "put":
        text = args.text if args.text is not None else sys.stdin.read()
        result = MemoryClient(**common, privacy=privacy).put(text, provenance={"uri": args.provenance_uri})
    elif args.command == "delete":
        result = MemoryClient(**common).delete(args.receipt)
    elif args.command == "search":
        filters = {
            key: value for key, value in {
                "source_id": args.route_source_id,
                "source_family": args.source_family,
                "source_alias": args.source_alias,
            }.items() if value is not None
        }
        result = BrainClient(**common).search(args.query, limit=args.limit, filters=filters)
    elif args.command == "show":
        result = BrainClient(**common).resolve(args.receipt)
    else:
        result = BrainClient(**common).doctor()
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
