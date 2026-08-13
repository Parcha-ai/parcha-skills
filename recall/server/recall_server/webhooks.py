from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

from privacy.policy import PrivacyPolicy

from .projectors import (
    canonical_json,
    content_sha256,
    validate_envelope,
    validate_typed_connector_content,
)


WEBHOOK_PATH = "/webhooks/v1/events"
WEBHOOK_FIELDS = {
    "schema_version",
    "event_id",
    "parent_id",
    "occurred_at",
    "record",
    "deleted",
}


@dataclass(frozen=True)
class WebhookResult:
    event: dict[str, Any] | None
    idempotency_key: str | None
    privacy: dict[str, Any]


class WebhookError(ValueError):
    pass


def build_webhook_event(body: Any, principal: dict[str, Any]) -> WebhookResult:
    if not isinstance(body, dict):
        raise WebhookError("webhook body must be an object")
    required = WEBHOOK_FIELDS - {"parent_id"}
    if set(body) - WEBHOOK_FIELDS or required - set(body):
        raise WebhookError("webhook body shape is invalid")
    if body["schema_version"] != 1 or isinstance(body["schema_version"], bool):
        raise WebhookError("webhook schema version is invalid")
    if type(body["deleted"]) is not bool:
        raise WebhookError("webhook deleted flag is invalid")
    source_id = principal.get("source_id")
    principal_id = principal.get("principal_id")
    privacy_mode = principal.get("webhook_privacy_mode")
    connector_id = principal.get("connector_id", "custom.webhook")
    if (
        not isinstance(source_id, str)
        or not isinstance(principal_id, str)
        or privacy_mode not in {"scrub", "drop"}
        or connector_id not in {"custom.webhook", "slack.messages"}
    ):
        raise WebhookError("webhook credential is not bound")
    record = body["record"]
    try:
        validate_typed_connector_content(record, deleted=body["deleted"])
    except ValueError as error:
        raise WebhookError("webhook record is invalid") from error
    if body["deleted"]:
        safe_record = {"kind": record["kind"]}
        privacy = PrivacyPolicy(mode="off").apply(safe_record)
        event_kind = "tombstone"
        content = {"target_native_id": body["event_id"]}
    else:
        privacy = PrivacyPolicy(mode=privacy_mode).apply(record)
        if privacy.action == "drop":
            return WebhookResult(
                event=None,
                idempotency_key=None,
                privacy=privacy.receipt(),
            )
        safe_record = privacy.value
        event_kind = "connector_record"
        content = safe_record
    provenance = {
        "connector_id": connector_id,
        "connector_schema_version": 2,
        "uri": (
            "connector://custom-webhook"
            if connector_id == "custom.webhook"
            else "connector://slack/events"
        ),
    }
    event = {
        "schema_version": 1,
        "source_id": source_id,
        "native_id": body["event_id"],
        "native_parent_id": body.get("parent_id"),
        "kind": event_kind,
        "occurred_at": body["occurred_at"],
        "observed_at": body["occurred_at"],
        "principal_id": principal_id,
        "visibility": "private",
        "content_type": "application/json",
        "content": content,
        "provenance": provenance,
        "content_sha256": "",
    }
    event["content_sha256"] = content_sha256(event)
    try:
        validate_envelope(event)
    except ValueError as error:
        raise WebhookError("webhook event is invalid") from error
    idempotency_key = (
        "webhook-v1-" + hashlib.sha256(canonical_json(event)).hexdigest()
    )
    return WebhookResult(
        event=event,
        idempotency_key=idempotency_key,
        privacy=privacy.receipt(),
    )
