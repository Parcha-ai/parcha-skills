"""Closed, content-free inventory and health policy for Recall input surfaces."""

from __future__ import annotations

from dataclasses import dataclass
import re
import sqlite3
from pathlib import Path
from typing import Any, Mapping

from privacy.policy import PrivacyPolicy
from connectors.record_contract import TYPED_RECORD_FIELDS


IDENTITY = re.compile(r"[a-z][a-z0-9_.-]{2,63}\Z")
COMMAND = re.compile(r"[a-z][a-z0-9-]{2,63}\Z")
MODES = {"pull", "push"}
AUTHORITIES = {"brain", "source"}
VISIBILITIES = {"private", "shared"}
PRIVACY_MODES = {"off", "scrub", "drop"}
CHECKPOINTS = {"none", "ack_cursor"}
DELETIONS = {"explicit_receipt", "explicit_export_reference"}
RUNTIME_ERROR_CODES = {
    "archive_identity_forgotten", "archive_invalid_reference", "archive_unavailable",
    "brain_invalid_acknowledgement", "brain_unauthorized", "brain_unavailable",
    "connector_disabled", "connector_invalid_page", "connector_rate_limited",
    "connector_spool_error", "connector_unavailable",
    "connector_authority_forbidden", "connector_authority_revoked",
    "connector_schema_drift", "connector_upstream_error",
    "grep_ai_conflict", "grep_ai_forbidden", "grep_ai_idempotency_key_in_flight",
    "grep_ai_idempotency_key_reuse_mismatch", "grep_ai_idempotency_key_ttl_expired",
    "grep_ai_insufficient_credits", "grep_ai_internal_error", "grep_ai_invalid_error",
    "grep_ai_invalid_request", "grep_ai_invalid_transport",
    "grep_ai_job_not_found", "grep_ai_rate_limited", "grep_ai_redirect_rejected",
    "grep_ai_resource_not_found", "grep_ai_unauthenticated", "grep_ai_unknown_error",
    "grep_ai_unavailable", "grep_ai_upstream_unavailable", "grep_ai_validation_error",
}
FIELDS = {
    "schema_version", "connector_id", "command", "mode", "authority_slots",
    "visibility_modes", "privacy_modes", "checkpoint", "deletion",
}
V2_FIELDS = FIELDS | {
    "source_family", "record_kinds", "execution_placement",
    "minimum_external_scopes", "backfill_modes", "edit_semantics",
    "retention_modes", "attachment_capability", "default_privacy_mode",
}
SOURCE_FAMILIES = {
    "coding_history", "deliberate_capture", "user_export", "third_party_research",
    "communications", "schedule", "contacts", "social", "documents",
    "work_activity", "local_activity", "personal_media",
}
PLACEMENTS = {"source_local", "always_on_api", "either"}
PLACEMENTS_V3 = {"source_local", "remote_worker", "either"}
ACQUISITION_MODES = {"poll", "watch", "snapshot", "import", "webhook"}
AUTH_KINDS = {"none", "oauth2", "api_token", "os_permission", "selected_export"}
BACKFILL_MODES = {"full", "incremental", "export"}
EDIT_SEMANTICS = {"content_revision", "immutable"}
RETENTION_MODES = {"source_controlled", "bounded", "forever"}
DELETION_SEMANTICS = {"explicit_upstream", "explicit_owner", "none"}
SELECTION_FIELD = re.compile(r"[a-z][a-z0-9_]{1,63}\Z")
SCOPE = re.compile(r"[A-Za-z][A-Za-z0-9+._:/-]{1,255}\Z")
MAX_SELECTION_FIELDS = 32
GOOGLE_READ_SCOPES = {
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/calendar.readonly",
    "https://www.googleapis.com/auth/contacts.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
}


class ConnectorRegistryError(ValueError):
    pass


def _closed_tuple(value: Any, label: str, allowed: set[str]) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or not value:
        raise ConnectorRegistryError(f"invalid_{label}")
    result = tuple(value)
    if any(not isinstance(item, str) or item not in allowed for item in result):
        raise ConnectorRegistryError(f"invalid_{label}")
    if len(result) != len(set(result)):
        raise ConnectorRegistryError(f"duplicate_{label}")
    if result != tuple(sorted(result)):
        raise ConnectorRegistryError(f"noncanonical_{label}")
    return result


def _closed_strings(
    value: Any,
    label: str,
    *,
    pattern: re.Pattern[str],
    allow_empty: bool = False,
    maximum: int = 64,
) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or (not value and not allow_empty):
        raise ConnectorRegistryError(f"invalid_{label}")
    result = tuple(value)
    if len(result) > maximum or any(
        not isinstance(item, str) or not pattern.fullmatch(item) for item in result
    ):
        raise ConnectorRegistryError(f"invalid_{label}")
    if len(result) != len(set(result)):
        raise ConnectorRegistryError(f"duplicate_{label}")
    if result != tuple(sorted(result)):
        raise ConnectorRegistryError(f"noncanonical_{label}")
    return result


@dataclass(frozen=True)
class ConnectorDefinition:
    schema_version: int
    connector_id: str
    command: str
    mode: str
    authority_slots: tuple[str, ...]
    visibility_modes: tuple[str, ...]
    privacy_modes: tuple[str, ...]
    checkpoint: str
    deletion: str

    def __post_init__(self) -> None:
        if not isinstance(self.schema_version, int) or isinstance(self.schema_version, bool) or self.schema_version != 1:
            raise ConnectorRegistryError("invalid_schema_version")
        if not isinstance(self.connector_id, str) or not IDENTITY.fullmatch(self.connector_id):
            raise ConnectorRegistryError("invalid_connector_id")
        if not isinstance(self.command, str) or not COMMAND.fullmatch(self.command):
            raise ConnectorRegistryError("invalid_command")
        if not isinstance(self.mode, str) or self.mode not in MODES:
            raise ConnectorRegistryError("invalid_mode")
        for value, label, allowed in (
            (self.authority_slots, "authority_slots", AUTHORITIES),
            (self.visibility_modes, "visibility_modes", VISIBILITIES),
            (self.privacy_modes, "privacy_modes", PRIVACY_MODES),
        ):
            normalized = _closed_tuple(value, label, allowed)
            object.__setattr__(self, label, normalized)
        if "brain" not in self.authority_slots:
            raise ConnectorRegistryError("brain_authority_required")
        if not isinstance(self.checkpoint, str) or self.checkpoint not in CHECKPOINTS:
            raise ConnectorRegistryError("invalid_checkpoint")
        if not isinstance(self.deletion, str) or self.deletion not in DELETIONS:
            raise ConnectorRegistryError("invalid_deletion")
        if self.mode == "push" and self.checkpoint != "none":
            raise ConnectorRegistryError("invalid_push_checkpoint")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ConnectorDefinition":
        if not isinstance(value, Mapping) or set(value) != FIELDS:
            raise ConnectorRegistryError("invalid_definition_shape")
        return cls(**dict(value))

    def to_public(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "connector_id": self.connector_id,
            "command": self.command,
            "mode": self.mode,
            "authority_slots": list(self.authority_slots),
            "visibility_modes": list(self.visibility_modes),
            "privacy_modes": list(self.privacy_modes),
            "checkpoint": self.checkpoint,
            "deletion": self.deletion,
        }


@dataclass(frozen=True)
class ConnectorDefinitionV2(ConnectorDefinition):
    source_family: str
    record_kinds: tuple[str, ...]
    execution_placement: str
    minimum_external_scopes: tuple[str, ...]
    backfill_modes: tuple[str, ...]
    edit_semantics: str
    retention_modes: tuple[str, ...]
    attachment_capability: bool
    default_privacy_mode: str

    def __post_init__(self) -> None:
        if self.schema_version != 2 or isinstance(self.schema_version, bool):
            raise ConnectorRegistryError("invalid_schema_version")
        if not isinstance(self.connector_id, str) or not IDENTITY.fullmatch(self.connector_id):
            raise ConnectorRegistryError("invalid_connector_id")
        if not isinstance(self.command, str) or not COMMAND.fullmatch(self.command):
            raise ConnectorRegistryError("invalid_command")
        if self.mode != "pull":
            raise ConnectorRegistryError("invalid_mode")
        for value, label, allowed in (
            (self.authority_slots, "authority_slots", AUTHORITIES),
            (self.visibility_modes, "visibility_modes", VISIBILITIES),
            (self.privacy_modes, "privacy_modes", PRIVACY_MODES),
            (self.record_kinds, "record_kinds", set(TYPED_RECORD_FIELDS)),
            (self.minimum_external_scopes, "minimum_external_scopes", GOOGLE_READ_SCOPES),
            (self.backfill_modes, "backfill_modes", BACKFILL_MODES),
            (self.retention_modes, "retention_modes", RETENTION_MODES),
        ):
            normalized = _closed_tuple(value, label, allowed)
            object.__setattr__(self, label, normalized)
        if self.authority_slots != ("brain", "source"):
            raise ConnectorRegistryError("authority_slots_mismatch")
        if self.visibility_modes != ("private",):
            raise ConnectorRegistryError("invalid_visibility_modes")
        if self.checkpoint != "ack_cursor":
            raise ConnectorRegistryError("invalid_checkpoint")
        if self.deletion != "explicit_receipt":
            raise ConnectorRegistryError("invalid_deletion")
        if self.source_family not in SOURCE_FAMILIES:
            raise ConnectorRegistryError("invalid_source_family")
        if self.execution_placement not in PLACEMENTS:
            raise ConnectorRegistryError("invalid_execution_placement")
        if self.edit_semantics not in EDIT_SEMANTICS:
            raise ConnectorRegistryError("invalid_edit_semantics")
        if not isinstance(self.attachment_capability, bool):
            raise ConnectorRegistryError("invalid_attachment_capability")
        if self.default_privacy_mode not in self.privacy_modes:
            raise ConnectorRegistryError("invalid_default_privacy_mode")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ConnectorDefinitionV2":
        if not isinstance(value, Mapping) or set(value) != V2_FIELDS:
            raise ConnectorRegistryError("invalid_definition_shape")
        return cls(**dict(value))

    def to_public(self) -> dict[str, Any]:
        return {
            **super().to_public(),
            "source_family": self.source_family,
            "record_kinds": list(self.record_kinds),
            "execution_placement": self.execution_placement,
            "minimum_external_scopes": list(self.minimum_external_scopes),
            "backfill_modes": list(self.backfill_modes),
            "edit_semantics": self.edit_semantics,
            "retention_modes": list(self.retention_modes),
            "attachment_capability": self.attachment_capability,
            "default_privacy_mode": self.default_privacy_mode,
        }


@dataclass(frozen=True)
class ConnectorPlacement:
    execution: str
    acquisition: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.execution not in PLACEMENTS_V3:
            raise ConnectorRegistryError("invalid_execution_placement")
        object.__setattr__(
            self,
            "acquisition",
            _closed_tuple(self.acquisition, "acquisition", ACQUISITION_MODES),
        )
        if (
            self.execution == "source_local" and "webhook" in self.acquisition
        ) or (
            self.execution == "remote_worker"
            and set(self.acquisition) & {"snapshot", "watch"}
        ):
            raise ConnectorRegistryError("invalid_placement_acquisition")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ConnectorPlacement":
        if not isinstance(value, Mapping) or set(value) != {"execution", "acquisition"}:
            raise ConnectorRegistryError("invalid_placement_shape")
        return cls(**dict(value))

    def to_public(self) -> dict[str, Any]:
        return {"execution": self.execution, "acquisition": list(self.acquisition)}


@dataclass(frozen=True)
class ConnectorAuth:
    kind: str
    minimum_scopes: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.kind not in AUTH_KINDS:
            raise ConnectorRegistryError("invalid_auth_kind")
        scopes = _closed_strings(
            self.minimum_scopes,
            "minimum_scopes",
            pattern=SCOPE,
            allow_empty=True,
        )
        object.__setattr__(self, "minimum_scopes", scopes)
        if self.kind in {"oauth2", "api_token", "os_permission"} and not scopes:
            raise ConnectorRegistryError("minimum_scopes_required")
        if self.kind in {"none", "selected_export"} and scopes:
            raise ConnectorRegistryError("minimum_scopes_not_allowed")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ConnectorAuth":
        if not isinstance(value, Mapping) or set(value) != {"kind", "minimum_scopes"}:
            raise ConnectorRegistryError("invalid_auth_shape")
        return cls(**dict(value))

    def to_public(self) -> dict[str, Any]:
        return {"kind": self.kind, "minimum_scopes": list(self.minimum_scopes)}


@dataclass(frozen=True)
class ConnectorSync:
    backfill_modes: tuple[str, ...]
    checkpoint: str
    edit_semantics: str
    deletion_semantics: str
    reconciliation: bool

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "backfill_modes",
            _closed_tuple(self.backfill_modes, "backfill_modes", BACKFILL_MODES),
        )
        if self.checkpoint not in CHECKPOINTS:
            raise ConnectorRegistryError("invalid_checkpoint")
        if self.edit_semantics not in EDIT_SEMANTICS:
            raise ConnectorRegistryError("invalid_edit_semantics")
        if self.deletion_semantics not in DELETION_SEMANTICS:
            raise ConnectorRegistryError("invalid_deletion_semantics")
        if not isinstance(self.reconciliation, bool):
            raise ConnectorRegistryError("invalid_reconciliation")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ConnectorSync":
        fields = {
            "backfill_modes",
            "checkpoint",
            "edit_semantics",
            "deletion_semantics",
            "reconciliation",
        }
        if not isinstance(value, Mapping) or set(value) != fields:
            raise ConnectorRegistryError("invalid_sync_shape")
        return cls(**dict(value))

    def to_public(self) -> dict[str, Any]:
        return {
            "backfill_modes": list(self.backfill_modes),
            "checkpoint": self.checkpoint,
            "edit_semantics": self.edit_semantics,
            "deletion_semantics": self.deletion_semantics,
            "reconciliation": self.reconciliation,
        }


@dataclass(frozen=True)
class ConnectorPolicy:
    visibility_modes: tuple[str, ...]
    privacy_modes: tuple[str, ...]
    default_privacy_mode: str
    retention_modes: tuple[str, ...]
    attachment_capability: bool

    def __post_init__(self) -> None:
        for value, label, allowed in (
            (self.visibility_modes, "visibility_modes", VISIBILITIES),
            (self.privacy_modes, "privacy_modes", PRIVACY_MODES),
            (self.retention_modes, "retention_modes", RETENTION_MODES),
        ):
            object.__setattr__(self, label, _closed_tuple(value, label, allowed))
        if self.visibility_modes != ("private",):
            raise ConnectorRegistryError("invalid_visibility_modes")
        if self.default_privacy_mode not in self.privacy_modes:
            raise ConnectorRegistryError("invalid_default_privacy_mode")
        if not isinstance(self.attachment_capability, bool):
            raise ConnectorRegistryError("invalid_attachment_capability")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ConnectorPolicy":
        fields = {
            "visibility_modes",
            "privacy_modes",
            "default_privacy_mode",
            "retention_modes",
            "attachment_capability",
        }
        if not isinstance(value, Mapping) or set(value) != fields:
            raise ConnectorRegistryError("invalid_policy_shape")
        return cls(**dict(value))

    def to_public(self) -> dict[str, Any]:
        return {
            "visibility_modes": list(self.visibility_modes),
            "privacy_modes": list(self.privacy_modes),
            "default_privacy_mode": self.default_privacy_mode,
            "retention_modes": list(self.retention_modes),
            "attachment_capability": self.attachment_capability,
        }


@dataclass(frozen=True)
class ConnectorDefinitionV3:
    """Composable manifest around the proven pull/page runner boundary."""

    schema_version: int
    connector_id: str
    command: str
    mode: str
    authority_slots: tuple[str, ...]
    source_family: str
    record_kinds: tuple[str, ...]
    placement: ConnectorPlacement
    auth: ConnectorAuth
    sync: ConnectorSync
    policy: ConnectorPolicy
    selection_fields: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.schema_version != 3 or isinstance(self.schema_version, bool):
            raise ConnectorRegistryError("invalid_schema_version")
        if not isinstance(self.connector_id, str) or not IDENTITY.fullmatch(self.connector_id):
            raise ConnectorRegistryError("invalid_connector_id")
        if not isinstance(self.command, str) or not COMMAND.fullmatch(self.command):
            raise ConnectorRegistryError("invalid_command")
        if self.mode not in MODES:
            raise ConnectorRegistryError("invalid_mode")
        authorities = _closed_tuple(self.authority_slots, "authority_slots", AUTHORITIES)
        if authorities != ("brain", "source"):
            raise ConnectorRegistryError("authority_slots_mismatch")
        object.__setattr__(self, "authority_slots", authorities)
        if self.source_family not in SOURCE_FAMILIES:
            raise ConnectorRegistryError("invalid_source_family")
        object.__setattr__(
            self,
            "record_kinds",
            _closed_tuple(self.record_kinds, "record_kinds", set(TYPED_RECORD_FIELDS)),
        )
        if not isinstance(self.placement, ConnectorPlacement):
            raise ConnectorRegistryError("invalid_placement")
        if not isinstance(self.auth, ConnectorAuth):
            raise ConnectorRegistryError("invalid_auth")
        if not isinstance(self.sync, ConnectorSync):
            raise ConnectorRegistryError("invalid_sync")
        if not isinstance(self.policy, ConnectorPolicy):
            raise ConnectorRegistryError("invalid_policy")
        selections = _closed_strings(
            self.selection_fields,
            "selection_fields",
            pattern=SELECTION_FIELD,
            allow_empty=True,
            maximum=MAX_SELECTION_FIELDS,
        )
        object.__setattr__(self, "selection_fields", selections)
        if self.auth.kind == "os_permission" and self.placement.execution != "source_local":
            raise ConnectorRegistryError("os_permission_requires_source_local")
        if self.auth.kind == "selected_export" and "import" not in self.placement.acquisition:
            raise ConnectorRegistryError("selected_export_requires_import")
        if self.mode == "push":
            if (
                self.placement.execution != "remote_worker"
                or self.placement.acquisition != ("webhook",)
                or self.auth.kind != "api_token"
                or self.sync.checkpoint != "none"
                or self.sync.backfill_modes != ("incremental",)
                or self.sync.reconciliation
            ):
                raise ConnectorRegistryError("invalid_push_definition")
        elif self.sync.checkpoint != "ack_cursor":
            raise ConnectorRegistryError("invalid_pull_checkpoint")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ConnectorDefinitionV3":
        fields = {
            "schema_version",
            "connector_id",
            "command",
            "mode",
            "authority_slots",
            "source_family",
            "record_kinds",
            "placement",
            "auth",
            "sync",
            "policy",
            "selection_fields",
        }
        if not isinstance(value, Mapping) or set(value) != fields:
            raise ConnectorRegistryError("invalid_definition_shape")
        payload = dict(value)
        payload["placement"] = ConnectorPlacement.from_mapping(payload["placement"])
        payload["auth"] = ConnectorAuth.from_mapping(payload["auth"])
        payload["sync"] = ConnectorSync.from_mapping(payload["sync"])
        payload["policy"] = ConnectorPolicy.from_mapping(payload["policy"])
        return cls(**payload)

    @property
    def execution_placement(self) -> str:
        return self.placement.execution

    @property
    def acquisition_modes(self) -> tuple[str, ...]:
        return self.placement.acquisition

    @property
    def minimum_external_scopes(self) -> tuple[str, ...]:
        return self.auth.minimum_scopes

    @property
    def backfill_modes(self) -> tuple[str, ...]:
        return self.sync.backfill_modes

    @property
    def checkpoint(self) -> str:
        return self.sync.checkpoint

    @property
    def edit_semantics(self) -> str:
        return self.sync.edit_semantics

    @property
    def visibility_modes(self) -> tuple[str, ...]:
        return self.policy.visibility_modes

    @property
    def privacy_modes(self) -> tuple[str, ...]:
        return self.policy.privacy_modes

    @property
    def default_privacy_mode(self) -> str:
        return self.policy.default_privacy_mode

    @property
    def deletion(self) -> str:
        return "explicit_receipt"

    def to_public(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "connector_id": self.connector_id,
            "command": self.command,
            "mode": self.mode,
            "authority_slots": list(self.authority_slots),
            "source_family": self.source_family,
            "record_kinds": list(self.record_kinds),
            "placement": self.placement.to_public(),
            "auth": self.auth.to_public(),
            "sync": self.sync.to_public(),
            "policy": self.policy.to_public(),
            "selection_fields": list(self.selection_fields),
        }


def _remote_v3(
    *,
    connector_id: str,
    command: str,
    source_family: str,
    record_kinds: list[str],
    acquisition: list[str],
    auth_kind: str,
    scopes: list[str],
    selection_fields: list[str],
    attachment_capability: bool = False,
) -> ConnectorDefinitionV3:
    return ConnectorDefinitionV3.from_mapping({
        "schema_version": 3,
        "connector_id": connector_id,
        "command": command,
        "mode": "pull",
        "authority_slots": ["brain", "source"],
        "source_family": source_family,
        "record_kinds": record_kinds,
        "placement": {
            "execution": "remote_worker",
            "acquisition": acquisition,
        },
        "auth": {
            "kind": auth_kind,
            "minimum_scopes": scopes,
        },
        "sync": {
            "backfill_modes": ["full", "incremental"],
            "checkpoint": "ack_cursor",
            "edit_semantics": "content_revision",
            "deletion_semantics": "explicit_upstream",
            "reconciliation": True,
        },
        "policy": {
            "visibility_modes": ["private"],
            "privacy_modes": ["drop", "scrub"],
            "default_privacy_mode": "scrub",
            "retention_modes": ["source_controlled"],
            "attachment_capability": attachment_capability,
        },
        "selection_fields": selection_fields,
    })


REMOTE_API_REGISTRY = (
    _remote_v3(
        connector_id="google.gmail",
        command="remote-worker-run",
        source_family="communications",
        record_kinds=["communication_message.v1", "document.v1"],
        acquisition=["poll"],
        auth_kind="oauth2",
        scopes=["https://www.googleapis.com/auth/gmail.readonly"],
        attachment_capability=True,
        selection_fields=[
            "include_attachments", "include_spam_trash", "label_ids", "own_addresses", "query",
        ],
    ),
    _remote_v3(
        connector_id="google.calendar",
        command="remote-worker-run",
        source_family="schedule",
        record_kinds=["calendar_event.v1"],
        acquisition=["poll"],
        auth_kind="oauth2",
        scopes=["https://www.googleapis.com/auth/calendar.readonly"],
        selection_fields=["calendar_id", "time_max", "time_min"],
    ),
    _remote_v3(
        connector_id="google.contacts",
        command="remote-worker-run",
        source_family="contacts",
        record_kinds=["contact_identity.v1"],
        acquisition=["poll"],
        auth_kind="oauth2",
        scopes=["https://www.googleapis.com/auth/contacts.readonly"],
        selection_fields=[],
    ),
    _remote_v3(
        connector_id="google.drive",
        command="remote-worker-run",
        source_family="documents",
        record_kinds=["document.v1"],
        acquisition=["poll"],
        auth_kind="oauth2",
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
        selection_fields=["drive_id", "include_document_text", "mime_types"],
    ),
    _remote_v3(
        connector_id="github.activity",
        command="remote-worker-run",
        source_family="work_activity",
        record_kinds=["document.v1"],
        acquisition=["poll"],
        auth_kind="api_token",
        scopes=["contents:read", "issues:read", "metadata:read", "pull_requests:read"],
        selection_fields=["owner", "repository"],
    ),
    _remote_v3(
        connector_id="linear.activity",
        command="remote-worker-run",
        source_family="work_activity",
        record_kinds=["document.v1"],
        acquisition=["poll", "webhook"],
        auth_kind="oauth2",
        scopes=["read"],
        selection_fields=["team_id"],
    ),
    _remote_v3(
        connector_id="slack.messages",
        command="remote-worker-run",
        source_family="communications",
        record_kinds=["communication_message.v1", "contact_identity.v1", "document.v1"],
        acquisition=["poll", "webhook"],
        auth_kind="oauth2",
        attachment_capability=True,
        scopes=[
            "channels:history",
            "channels:join",
            "channels:read",
            "files:read",
            "users:read",
            "users:read.email",
        ],
        selection_fields=["channel_ids", "owner_user_ids", "workspace_id"],
    ),
    _remote_v3(
        connector_id="notion.workspace",
        command="remote-worker-run",
        source_family="documents",
        record_kinds=["document.v1"],
        acquisition=["poll"],
        auth_kind="oauth2",
        scopes=["read_content"],
        selection_fields=[],
    ),
    _remote_v3(
        connector_id="x.activity",
        command="remote-worker-run",
        source_family="social",
        record_kinds=["social_post.v1"],
        acquisition=["poll"],
        auth_kind="oauth2",
        scopes=["bookmark.read", "offline.access", "tweet.read", "users.read"],
        selection_fields=["streams", "user_id"],
    ),
)


def _local_v3(
    *,
    connector_id: str,
    command: str,
    source_family: str,
    record_kinds: list[str],
    scopes: list[str],
    selection_fields: list[str],
    deletion_semantics: str,
    acquisition: list[str] | None = None,
    auth_kind: str = "os_permission",
    backfill_modes: list[str] | None = None,
    reconciliation: bool = True,
) -> ConnectorDefinitionV3:
    return ConnectorDefinitionV3.from_mapping({
        "schema_version": 3,
        "connector_id": connector_id,
        "command": command,
        "mode": "pull",
        "authority_slots": ["brain", "source"],
        "source_family": source_family,
        "record_kinds": record_kinds,
        "placement": {
            "execution": "source_local",
            "acquisition": acquisition or ["snapshot"],
        },
        "auth": {
            "kind": auth_kind,
            "minimum_scopes": scopes,
        },
        "sync": {
            "backfill_modes": backfill_modes or ["full", "incremental"],
            "checkpoint": "ack_cursor",
            "edit_semantics": "content_revision",
            "deletion_semantics": deletion_semantics,
            "reconciliation": reconciliation,
        },
        "policy": {
            "visibility_modes": ["private"],
            "privacy_modes": ["drop", "scrub"],
            "default_privacy_mode": "scrub",
            "retention_modes": ["source_controlled"],
            "attachment_capability": False,
        },
        "selection_fields": selection_fields,
    })


LOCAL_MAC_REGISTRY = (
    _local_v3(
        connector_id="local.claude-code",
        command="collect",
        source_family="coding_history",
        record_kinds=["communication_message.v1"],
        scopes=["macos.user_selected_files"],
        selection_fields=["root"],
        deletion_semantics="explicit_upstream",
        acquisition=["snapshot", "watch"],
    ),
    _local_v3(
        connector_id="local.codex",
        command="collect",
        source_family="coding_history",
        record_kinds=["communication_message.v1"],
        scopes=["macos.user_selected_files"],
        selection_fields=["root"],
        deletion_semantics="explicit_upstream",
        acquisition=["snapshot", "watch"],
    ),
    _local_v3(
        connector_id="local.cowork",
        command="collect",
        source_family="coding_history",
        record_kinds=["communication_message.v1"],
        scopes=["macos.user_selected_files"],
        selection_fields=["root"],
        deletion_semantics="explicit_upstream",
        acquisition=["snapshot", "watch"],
    ),
    _local_v3(
        connector_id="local.chatgpt-export",
        command="export-inbox-sync",
        source_family="user_export",
        record_kinds=["communication_message.v1"],
        scopes=[],
        selection_fields=["root"],
        deletion_semantics="explicit_owner",
        acquisition=["import", "watch"],
        auth_kind="selected_export",
        backfill_modes=["export", "incremental"],
        reconciliation=False,
    ),
    _local_v3(
        connector_id="apple.imessage",
        command="imessage-sync",
        source_family="communications",
        record_kinds=["communication_message.v1"],
        scopes=["macos.full_disk_access"],
        selection_fields=["chat_ids", "date_max", "date_min"],
        deletion_semantics="explicit_upstream",
    ),
    _local_v3(
        connector_id="whatsapp.export",
        command="whatsapp-export-sync",
        source_family="communications",
        record_kinds=["communication_message.v1"],
        scopes=[],
        selection_fields=[
            "conversation_id", "date_order", "owner_names", "timezone",
        ],
        deletion_semantics="none",
        acquisition=["import", "watch"],
        auth_kind="selected_export",
        backfill_modes=["export", "incremental"],
        reconciliation=False,
    ),
    _local_v3(
        connector_id="local.selected-text",
        command="selected-text-sync",
        source_family="documents",
        record_kinds=["document.v1"],
        scopes=["macos.user_selected_files"],
        selection_fields=["extensions", "max_depth", "root"],
        deletion_semantics="none",
        acquisition=["snapshot", "watch"],
    ),
    _local_v3(
        connector_id="apple.safari",
        command="browser-sync",
        source_family="local_activity",
        record_kinds=["document.v1"],
        scopes=["macos.full_disk_access"],
        selection_fields=["bookmarks", "date_max", "date_min", "history"],
        deletion_semantics="none",
        reconciliation=False,
    ),
    _local_v3(
        connector_id="google.chrome",
        command="browser-sync",
        source_family="local_activity",
        record_kinds=["document.v1"],
        scopes=["macos.full_disk_access"],
        selection_fields=["bookmarks", "date_max", "date_min", "history"],
        deletion_semantics="none",
        reconciliation=False,
    ),
    _local_v3(
        connector_id="apple.notes",
        command="apple-notes-sync",
        source_family="documents",
        record_kinds=["document.v1"],
        scopes=["macos.full_disk_access"],
        selection_fields=["date_max", "date_min"],
        deletion_semantics="none",
        reconciliation=False,
    ),
    _local_v3(
        connector_id="hermes.sessions",
        command="hermes-session-sync",
        source_family="coding_history",
        record_kinds=["communication_message.v1"],
        scopes=["macos.user_selected_files"],
        selection_fields=["date_max", "date_min", "roles", "sources"],
        deletion_semantics="explicit_upstream",
    ),
)

PORTABLE_IMPORT_REGISTRY = (
    _local_v3(
        connector_id="portable.mail",
        command="mail-import-sync",
        source_family="communications",
        record_kinds=["communication_message.v1"],
        scopes=[],
        selection_fields=[
            "archive_id", "owner_identifiers", "removed_native_ids",
        ],
        deletion_semantics="explicit_owner",
        acquisition=["import"],
        auth_kind="selected_export",
        backfill_modes=["export"],
        reconciliation=False,
    ),
    _local_v3(
        connector_id="portable.calendar",
        command="calendar-import-sync",
        source_family="schedule",
        record_kinds=["calendar_event.v1"],
        scopes=[],
        selection_fields=[
            "archive_id", "owner_identifiers", "removed_native_ids",
        ],
        deletion_semantics="explicit_owner",
        acquisition=["import"],
        auth_kind="selected_export",
        backfill_modes=["export"],
        reconciliation=False,
    ),
    _local_v3(
        connector_id="portable.contacts",
        command="contact-import-sync",
        source_family="contacts",
        record_kinds=["contact_identity.v1"],
        scopes=[],
        selection_fields=[
            "archive_id", "owner_identifiers", "removed_native_ids",
        ],
        deletion_semantics="explicit_owner",
        acquisition=["import"],
        auth_kind="selected_export",
        backfill_modes=["export"],
        reconciliation=False,
    ),
    _local_v3(
        connector_id="portable.slack",
        command="slack-archive-sync",
        source_family="communications",
        record_kinds=["communication_message.v1", "contact_identity.v1"],
        scopes=[],
        selection_fields=[
            "archive_id", "owner_identifiers", "removed_native_ids",
        ],
        deletion_semantics="explicit_owner",
        acquisition=["import"],
        auth_kind="selected_export",
        backfill_modes=["export"],
        reconciliation=False,
    ),
    _local_v3(
        connector_id="portable.notion",
        command="notion-archive-sync",
        source_family="documents",
        record_kinds=["document.v1"],
        scopes=[],
        selection_fields=[
            "archive_id", "owner_identifiers", "removed_native_ids",
        ],
        deletion_semantics="explicit_owner",
        acquisition=["import"],
        auth_kind="selected_export",
        backfill_modes=["export"],
        reconciliation=False,
    ),
    _local_v3(
        connector_id="portable.x",
        command="x-archive-sync",
        source_family="social",
        record_kinds=["social_post.v1"],
        scopes=[],
        selection_fields=[
            "archive_id", "owner_identifiers", "removed_native_ids",
        ],
        deletion_semantics="explicit_owner",
        acquisition=["import"],
        auth_kind="selected_export",
        backfill_modes=["export"],
        reconciliation=False,
    ),
    ConnectorDefinitionV3.from_mapping({
        "schema_version": 3,
        "connector_id": "portable.feed",
        "command": "feed-sync",
        "mode": "pull",
        "authority_slots": ["brain", "source"],
        "source_family": "documents",
        "record_kinds": ["document.v1"],
        "placement": {
            "execution": "either",
            "acquisition": ["poll"],
        },
        "auth": {
            "kind": "none",
            "minimum_scopes": [],
        },
        "sync": {
            "backfill_modes": ["full", "incremental"],
            "checkpoint": "ack_cursor",
            "edit_semantics": "content_revision",
            "deletion_semantics": "none",
            "reconciliation": False,
        },
        "policy": {
            "visibility_modes": ["private"],
            "privacy_modes": ["drop", "scrub"],
            "default_privacy_mode": "scrub",
            "retention_modes": ["source_controlled"],
            "attachment_capability": False,
        },
        "selection_fields": ["feed_id", "url"],
    }),
    _local_v3(
        connector_id="portable.jsonl",
        command="jsonl-import-sync",
        source_family="documents",
        record_kinds=["document.v1"],
        scopes=[],
        selection_fields=["max_depth", "removed_native_ids", "root"],
        deletion_semantics="explicit_owner",
        acquisition=["import", "snapshot"],
        auth_kind="selected_export",
        backfill_modes=["export"],
        reconciliation=False,
    ),
)


REGISTRY = (
    ConnectorDefinition.from_mapping({
        "schema_version": 1, "connector_id": "recall.capture", "command": "mcp-serve",
        "mode": "push", "authority_slots": ["brain"],
        "visibility_modes": ["private", "shared"], "privacy_modes": ["drop", "off", "scrub"],
        "checkpoint": "none", "deletion": "explicit_receipt",
    }),
    ConnectorDefinitionV3.from_mapping({
        "schema_version": 3,
        "connector_id": "custom.webhook",
        "command": "serve",
        "mode": "push",
        "authority_slots": ["brain", "source"],
        "source_family": "deliberate_capture",
        "record_kinds": sorted(TYPED_RECORD_FIELDS),
        "placement": {
            "execution": "remote_worker",
            "acquisition": ["webhook"],
        },
        "auth": {
            "kind": "api_token",
            "minimum_scopes": ["webhook"],
        },
        "sync": {
            "backfill_modes": ["incremental"],
            "checkpoint": "none",
            "edit_semantics": "content_revision",
            "deletion_semantics": "explicit_upstream",
            "reconciliation": False,
        },
        "policy": {
            "visibility_modes": ["private"],
            "privacy_modes": ["drop", "scrub"],
            "default_privacy_mode": "scrub",
            "retention_modes": ["source_controlled"],
            "attachment_capability": False,
        },
        "selection_fields": [],
    }),
    ConnectorDefinition.from_mapping({
        "schema_version": 1, "connector_id": "openai.export-inbox", "command": "export-inbox-sync",
        "mode": "pull", "authority_slots": ["brain"],
        "visibility_modes": ["private"], "privacy_modes": ["drop", "off", "scrub"],
        "checkpoint": "ack_cursor", "deletion": "explicit_export_reference",
    }),
    ConnectorDefinition.from_mapping({
        "schema_version": 1, "connector_id": "grep.ai", "command": "grep-ai-sync",
        "mode": "pull", "authority_slots": ["brain", "source"],
        "visibility_modes": ["private"], "privacy_modes": ["drop", "scrub"],
        "checkpoint": "ack_cursor", "deletion": "explicit_receipt",
    }),
    *REMOTE_API_REGISTRY,
    *LOCAL_MAC_REGISTRY,
    *PORTABLE_IMPORT_REGISTRY,
)
def _index(items: tuple[Any, ...]) -> dict[str, Any]:
    result = {item.connector_id: item for item in items}
    if len(result) != len(items):
        raise ConnectorRegistryError("duplicate_connector_id")
    return result


_BY_ID = _index(REGISTRY)


def definition(connector_id: str) -> Any:
    try:
        return _BY_ID[connector_id]
    except (KeyError, TypeError):
        raise ConnectorRegistryError("unknown_connector") from None


def validate_policy(connector_id: str, *, visibility: str, privacy_mode: str,
                    authorities: set[str]) -> Any:
    item = definition(connector_id)
    if visibility not in item.visibility_modes:
        raise ConnectorRegistryError("visibility_not_allowed")
    if privacy_mode not in item.privacy_modes:
        raise ConnectorRegistryError("privacy_mode_not_allowed")
    if not isinstance(authorities, set) or authorities != set(item.authority_slots):
        raise ConnectorRegistryError("authority_slots_mismatch")
    return item


def preview() -> dict[str, Any]:
    return {
        "schema_version": 2,
        "mode": "connector-registry-preview",
        "credential_reads": 0,
        "source_reads": 0,
        "network_requests": 0,
        "writes": 0,
        "connectors": [item.to_public() for item in REGISTRY],
    }


def activation_surface(
    item: ConnectorDefinition | ConnectorDefinitionV3,
) -> dict[str, str]:
    """Map a registered connector to its concrete shipped activation surface."""

    if isinstance(item, ConnectorDefinitionV3):
        if item.mode == "push":
            runtime, package, contract, lifecycle = (
                "public_edge",
                "recall-core",
                "webhook-credential-v1",
                "server_credential",
            )
        elif item.execution_placement == "remote_worker":
            runtime, package, contract, lifecycle = (
                "remote_worker",
                "recall-remote-worker",
                "connector-host-v1",
                "supervised_job",
            )
        elif (
            item.execution_placement == "either"
            or item.connector_id.startswith("portable.")
        ):
            runtime, package, contract, lifecycle = (
                "portable_runner",
                "recall-brain",
                "selected-input-v1",
                "explicit_import",
            )
        else:
            runtime, package, contract, lifecycle = (
                "mac_utility",
                "recall-brain-macos",
                "selected-local-source-v1",
                "scheduled_source",
            )
    elif item.mode == "push":
        runtime, package, contract, lifecycle = (
            "mcp_host",
            "recall-brain",
            "mcp-host-profile-v1",
            "host_credential",
        )
    elif item.authority_slots == ("brain",):
        runtime, package, contract, lifecycle = (
            "mac_utility",
            "recall-brain-macos",
            "selected-export-v1",
            "scheduled_source",
        )
    else:
        runtime, package, contract, lifecycle = (
            "connector_supervisor",
            "recall-brain",
            "connector-host-v1",
            "supervised_job",
        )
    return {
        "connector_id": item.connector_id,
        "runtime": runtime,
        "package": package,
        "entrypoint": item.command,
        "config_contract": contract,
        "lifecycle": lifecycle,
        "implementation": "available",
    }


def _catalog_entry(
    item: ConnectorDefinition | ConnectorDefinitionV3,
) -> tuple[str, dict[str, Any]]:
    if isinstance(item, ConnectorDefinitionV3):
        if item.execution_placement == "remote_worker":
            group = "remote"
        elif (
            item.execution_placement == "either"
            or item.auth.kind == "selected_export"
        ):
            group = "portable"
        else:
            group = "local"
        entry = {
            "connector_id": item.connector_id,
            "source_family": item.source_family,
            "execution": item.execution_placement,
            "acquisition": list(item.acquisition_modes),
            "auth": item.auth.kind,
            "record_kinds": list(item.record_kinds),
        }
    elif item.mode == "push":
        group, entry = "deliberate_capture", {
            "connector_id": item.connector_id,
            "source_family": "deliberate_capture",
            "execution": "edge_client",
            "acquisition": ["push"],
            "auth": "none",
            "record_kinds": [],
        }
    elif item.authority_slots == ("brain",):
        group, entry = "portable", {
            "connector_id": item.connector_id,
            "source_family": "user_export",
            "execution": "source_local",
            "acquisition": ["import"],
            "auth": "selected_export",
            "record_kinds": [],
        }
    else:
        group, entry = "remote", {
            "connector_id": item.connector_id,
            "source_family": "third_party_research",
            "execution": "remote_worker",
            "acquisition": ["poll"],
            "auth": "api_token",
            "record_kinds": [],
        }
    entry["activation"] = activation_surface(item)
    return group, entry


def catalog() -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = {
        "deliberate_capture": [],
        "local": [],
        "portable": [],
        "remote": [],
    }
    for item in REGISTRY:
        group, entry = _catalog_entry(item)
        groups[group].append(entry)
    return {
        "schema_version": 2,
        "mode": "integration-catalog",
        "credential_reads": 0,
        "source_reads": 0,
        "network_requests": 0,
        "writes": 0,
        "groups": {
            name: {
                "count": len(entries),
                "connectors": sorted(
                    entries,
                    key=lambda entry: entry["connector_id"],
                ),
            }
            for name, entries in groups.items()
        },
    }


def _base_status(item: ConnectorDefinition, enabled: bool, privacy_mode: str,
                 authorities: set[str]) -> dict[str, Any]:
    if not isinstance(enabled, bool):
        raise ConnectorRegistryError("invalid_enabled")
    if privacy_mode not in item.privacy_modes:
        raise ConnectorRegistryError("privacy_mode_not_allowed")
    if (
        not isinstance(authorities, set)
        or set(authorities) - AUTHORITIES
        or set(authorities) - set(item.authority_slots)
    ):
        raise ConnectorRegistryError("invalid_authority_slots")
    configured = set(item.authority_slots).issubset(authorities)
    return {
        "schema_version": 1,
        "connector_id": item.connector_id,
        "enabled": enabled,
        "configured": configured,
        "authority_present": {
            slot: slot in authorities for slot in item.authority_slots
        },
        "mode": item.mode,
        "privacy_mode": privacy_mode,
        "privacy_policy_version": PrivacyPolicy(mode=privacy_mode).apply({}).policy_version,
        "checkpointed": False,
        "pending": 0,
        "pending_pages": 0,
        "error_code": None,
    }


def aggregate_status(connector_id: str, enabled: bool, privacy_mode: str,
                     authorities: set[str], spool_path: Path | None) -> dict[str, Any]:
    item = definition(connector_id)
    result = _base_status(item, enabled, privacy_mode, authorities)
    if not enabled:
        return {**result, "health": "disabled"}
    if not result["configured"]:
        return {**result, "health": "reference_missing"}
    if item.checkpoint == "none":
        return {**result, "health": "ready"}
    if spool_path is None:
        return {**result, "health": "local_state_unavailable"}
    path = Path(spool_path).expanduser()
    if not path.is_file() or path.is_symlink():
        return {**result, "health": "local_state_unavailable"}
    try:
        uri = path.resolve(strict=True).as_uri() + "?mode=ro&immutable=1"
        connection = sqlite3.connect(uri, uri=True)
        try:
            tables = {row[0] for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )}
            if not {"meta", "pages", "outbox"}.issubset(tables):
                raise ConnectorRegistryError("local_state_invalid")
            metadata = dict(connection.execute(
                "SELECT key,value FROM meta WHERE key IN "
                "('connector_id','source_id','committed_cursor','last_error_code')"
            ))
            pending_pages = connection.execute("SELECT count(*) FROM pages").fetchone()[0]
            pending = connection.execute("SELECT count(*) FROM outbox").fetchone()[0]
        finally:
            connection.close()
    except ConnectorRegistryError:
        raise
    except (OSError, sqlite3.Error):
        return {**result, "health": "local_state_unavailable"}
    error_code = metadata.get("last_error_code")
    if metadata.get("connector_id") != item.connector_id or not metadata.get("source_id"):
        raise ConnectorRegistryError("local_state_identity_mismatch")
    if error_code is not None and error_code not in RUNTIME_ERROR_CODES:
        raise ConnectorRegistryError("local_state_invalid")
    result.update({
        "checkpointed": "committed_cursor" in metadata,
        "pending": int(pending),
        "pending_pages": int(pending_pages),
        "error_code": error_code,
    })
    result["health"] = "degraded" if error_code or pending or pending_pages else "ready"
    return result
