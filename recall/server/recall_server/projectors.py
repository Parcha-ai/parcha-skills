from __future__ import annotations

import hashlib
import importlib.util
import json
import re
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

from . import PROJECTOR_VERSION

SOURCE_ID_RE = re.compile(r"[A-Za-z0-9_.:@-]{3,160}\Z")
NATIVE_ID_RE = re.compile(r"[A-Za-z0-9_.:@/=-]{1,512}\Z")
KIND_RE = re.compile(r"[A-Za-z0-9_.:-]{1,64}\Z")
CONTENT_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
ENVELOPE_FIELDS = {
    "schema_version", "source_id", "native_id", "native_parent_id", "kind",
    "occurred_at", "observed_at", "principal_id", "visibility", "content_type",
    "content", "provenance", "content_sha256",
}
OMISSION_CODE_RE = re.compile(r"[a-z][a-z0-9_]{2,63}\Z")

ENVELOPE_VALIDATION_RULES = frozenset({
    "envelope_object", "host_controlled", "required_missing", "unknown_fields",
    "schema_version", "identifier", "kind", "timestamp", "visibility",
    "content_type", "provenance_object", "connector_schema_version",
    "typed_object", "typed_kind", "typed_required_missing", "typed_unknown_fields",
    "typed_invalid_known_field", "typed_fidelity", "digest_shape", "finite_json",
    "digest_mismatch", "tombstone_target",
})

TYPED_FIELD_VALIDATION_RULES = frozenset({
    "typed_required_missing", "typed_invalid_known_field", "typed_fidelity",
})


class StructuredEnvelopeValidationError(ValueError):
    """Original validation message plus content-free invariant metadata."""

    def __init__(self, message: str, *, rule: str, field: str | None = None,
                 kind: str | None = None):
        super().__init__(message)
        self.validation_rule = rule if type(rule) is str and rule in ENVELOPE_VALIDATION_RULES else "unrecognized"
        self.validation_kind = kind if type(kind) is str and kind in TYPED_CONNECTOR_KINDS else "unrecognized"
        allowed = (
            TYPED_RECORD_FIELDS.get(self.validation_kind, {}).get("properties", {})
            if self.validation_rule in TYPED_FIELD_VALIDATION_RULES
            else ENVELOPE_FIELDS
        )
        self.validation_field = field if type(field) is str and field in allowed else "unrecognized"


def _validate_content_fidelity(content: dict[str, Any]) -> None:
    """Enforce the cross-field fidelity rules at the server trust boundary.

    Recall Core is packaged without connector runtime code, so this small
    boundary check intentionally mirrors the SDK rule while the JSON contract
    remains the shared field/type source of truth.
    """
    fidelity = content.get("content_fidelity")
    omissions = content.get("content_omissions")
    valid_omissions = (
        isinstance(omissions, list)
        and bool(omissions)
        and omissions == sorted(set(omissions))
        and all(
            isinstance(item, str) and OMISSION_CODE_RE.fullmatch(item)
            for item in omissions
        )
    )
    if (
        (fidelity == "complete" and omissions is not None)
        or (fidelity == "partial" and not valid_omissions)
        or fidelity not in {"complete", "partial"}
    ):
        raise ValueError("content fidelity is invalid")
    if content.get("format") == "snippet" and (
        fidelity != "partial" or "snippet_fallback" not in omissions
    ):
        raise ValueError("content fidelity is invalid")


def _load_typed_record_fields() -> dict[str, dict[str, Any]]:
    path = Path(__file__).resolve().parents[2] / "contracts" / "connector_v2.json"
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError("connector v2 contract is unavailable") from error
    if not isinstance(value, dict) or value.get("schema_version") != 2:
        raise RuntimeError("connector v2 contract is invalid")
    result = {}
    for kind, schema in value.get("record_kinds", {}).items():
        if (
            not isinstance(kind, str) or not isinstance(schema, dict)
            or set(schema) != {"required", "optional", "properties"}
        ):
            raise RuntimeError("connector v2 contract is invalid")
        required = set(schema.get("required", ()))
        optional = set(schema.get("optional", ()))
        properties = schema.get("properties")
        if (
            "kind" not in required or required & optional
            or not isinstance(properties, dict)
            or required | optional != set(properties)
        ):
            raise RuntimeError("connector v2 contract is invalid")
        result[kind] = {"required": required, "optional": optional, "properties": properties}
    if not result:
        raise RuntimeError("connector v2 contract is invalid")
    return result


TYPED_RECORD_FIELDS = _load_typed_record_fields()
TYPED_CONNECTOR_KINDS = set(TYPED_RECORD_FIELDS)
ENVELOPE_VALIDATION_FIELDS = frozenset(ENVELOPE_FIELDS).union(
    *(schema["properties"] for schema in TYPED_RECORD_FIELDS.values())
)


def _valid_typed_value(value: Any, specification: dict[str, Any]) -> bool:
    value_type = specification.get("type")
    valid = (
        (value_type == "string" and isinstance(value, str))
        or (value_type == "boolean" and type(value) is bool)
        or (value_type == "number" and type(value) in {int, float})
        or (value_type == "object" and isinstance(value, dict))
        or (
            value_type == "string_list" and isinstance(value, list)
            and all(isinstance(item, str) for item in value)
        )
        or (
            value_type == "object_list" and isinstance(value, list)
            and all(isinstance(item, dict) for item in value)
        )
    )
    return (
        valid
        and ("const" not in specification or value == specification["const"])
        and ("enum" not in specification or value in specification["enum"])
        and (
            "pattern" not in specification
            or isinstance(value, str)
            and re.fullmatch(specification["pattern"], value) is not None
        )
    )


def validate_typed_connector_content(
    content: Any,
    *,
    deleted: bool = False,
) -> dict[str, Any]:
    if not isinstance(content, dict):
        raise StructuredEnvelopeValidationError("invalid typed connector record", rule="typed_object", field="content")
    schema = TYPED_RECORD_FIELDS.get(content.get("kind"))
    if schema is None:
        raise StructuredEnvelopeValidationError("invalid typed connector record", rule="typed_kind", field="kind")
    fields = set(content)
    required = {"kind"} if deleted else schema["required"]
    optional = set() if deleted else schema["optional"]
    if required - fields:
        raise StructuredEnvelopeValidationError(
            "invalid typed connector record", rule="typed_required_missing",
            field=sorted(required - fields)[0], kind=content["kind"],
        )
    if fields - required - optional:
        raise StructuredEnvelopeValidationError(
            "invalid typed connector record", rule="typed_unknown_fields",
            kind=content["kind"],
        )
    for field in fields:
        if not _valid_typed_value(content[field], schema["properties"][field]):
            raise StructuredEnvelopeValidationError(
                "invalid typed connector record", rule="typed_invalid_known_field",
                field=field, kind=content["kind"],
            )
    try:
        if not deleted:
            _validate_content_fidelity(content)
    except ValueError:
        raise StructuredEnvelopeValidationError(
            "invalid typed connector record", rule="typed_fidelity",
            field="content_fidelity", kind=content["kind"],
        ) from None
    return content


def redact_text(value: str) -> str:
    safe = legacy_engine().clean_text(value)
    return safe.replace("[redacted-private-key-block]", "[REDACTED-PRIVATE-KEY]").replace(
        "[redacted-secret-line]", "[REDACTED]",
    )


@lru_cache(maxsize=1)
def legacy_engine():
    configured = Path(__file__).resolve().parents[2] / "skills/recall/scripts/recall.py"
    spec = importlib.util.spec_from_file_location("recall_local_engine", configured)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load local recall parser: {configured}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False,
    ).encode()


def advisory_lock_key(source_id: str, native_id: str) -> str:
    # PostgreSQL text cannot contain NUL. Unit Separator is boundary-preserving
    # for the validated source/native identifiers and safe for hashtextextended.
    return source_id + "\x1f" + native_id


def effective_session_id(envelope: dict) -> str:
    """Return explicit parent identity, with a narrow legacy Cowork repair seam."""
    explicit = envelope.get("native_parent_id")
    content = envelope.get("content")
    provenance = envelope.get("provenance") or {}
    legacy = content.get("session_id") if isinstance(content, dict) else None
    if (
        envelope.get("kind") == "connector_record"
        and provenance.get("connector_id") == "anthropic.cowork-local"
        and (explicit is None or explicit == envelope.get("native_id"))
        and isinstance(legacy, str)
        and NATIVE_ID_RE.fullmatch(legacy)
    ):
        return legacy
    return explicit or envelope["native_id"]


def content_sha256(envelope: dict) -> str:
    return hashlib.sha256(canonical_json(envelope["content"])).hexdigest()


def validate_envelope(envelope: dict) -> dict:
    if not isinstance(envelope, dict):
        raise StructuredEnvelopeValidationError("envelope must be an object", rule="envelope_object")
    if set(envelope) & {"source_profile", "source_family", "source_quality", "quality"}:
        raise StructuredEnvelopeValidationError("source profile is host-controlled", rule="host_controlled")
    required = (
        "schema_version", "source_id", "native_id", "kind", "occurred_at",
        "observed_at", "principal_id", "visibility", "content_type", "content",
        "content_sha256",
    )
    missing = [key for key in required if key not in envelope]
    if missing:
        raise StructuredEnvelopeValidationError("missing fields: " + ",".join(missing), rule="required_missing", field=missing[0])
    unknown = set(envelope) - ENVELOPE_FIELDS
    if unknown:
        raise StructuredEnvelopeValidationError("unknown envelope fields", rule="unknown_fields")
    if envelope["schema_version"] != 1 or isinstance(envelope["schema_version"], bool):
        raise StructuredEnvelopeValidationError("unsupported schema_version", rule="schema_version", field="schema_version")
    if not isinstance(envelope["source_id"], str) or not SOURCE_ID_RE.fullmatch(envelope["source_id"]):
        raise StructuredEnvelopeValidationError("invalid source_id", rule="identifier", field="source_id")
    for field in ("native_id", "native_parent_id"):
        value = envelope.get(field)
        if value is not None and (not isinstance(value, str) or not NATIVE_ID_RE.fullmatch(value)):
            raise StructuredEnvelopeValidationError(f"invalid {field}", rule="identifier", field=field)
    if not isinstance(envelope["kind"], str) or not KIND_RE.fullmatch(envelope["kind"]):
        raise StructuredEnvelopeValidationError("invalid kind", rule="kind", field="kind")
    for field in ("occurred_at", "observed_at"):
        value = envelope[field]
        if not isinstance(value, str) or len(value) > 64:
            raise StructuredEnvelopeValidationError(f"invalid {field}", rule="timestamp", field=field)
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise StructuredEnvelopeValidationError(f"invalid {field}", rule="timestamp", field=field) from None
        if parsed.tzinfo is None:
            raise StructuredEnvelopeValidationError(f"invalid {field}", rule="timestamp", field=field)
    principal = envelope["principal_id"]
    if (
        not isinstance(principal, str) or not 1 <= len(principal) <= 160
        or any(ord(character) < 32 for character in principal)
    ):
        raise StructuredEnvelopeValidationError("invalid principal_id", rule="identifier", field="principal_id")
    if envelope["visibility"] not in {"private", "shared"}:
        raise StructuredEnvelopeValidationError("unsupported visibility", rule="visibility", field="visibility")
    if envelope["content_type"] != "application/json":
        raise StructuredEnvelopeValidationError("unsupported content_type", rule="content_type", field="content_type")
    provenance = envelope.get("provenance", {})
    if not isinstance(provenance, dict):
        raise StructuredEnvelopeValidationError("provenance must be an object", rule="provenance_object", field="provenance")
    connector_schema_version = provenance.get("connector_schema_version")
    if connector_schema_version is not None:
        if type(connector_schema_version) is not int or connector_schema_version not in {1, 2}:
            raise StructuredEnvelopeValidationError("unsupported connector schema_version", rule="connector_schema_version", field="provenance")
        if connector_schema_version == 2 and envelope["kind"] != "tombstone":
            content = envelope["content"]
            if envelope["kind"] != "connector_record":
                raise StructuredEnvelopeValidationError("invalid typed connector record", rule="typed_kind", field="kind")
            validate_typed_connector_content(content)
    claimed = envelope["content_sha256"]
    if not isinstance(claimed, str) or not CONTENT_SHA256_RE.fullmatch(claimed):
        raise StructuredEnvelopeValidationError("invalid content_sha256", rule="digest_shape", field="content_sha256")
    try:
        actual = content_sha256(envelope)
        canonical_json(provenance)
    except (TypeError, ValueError):
        raise StructuredEnvelopeValidationError("content and provenance must be finite JSON values", rule="finite_json") from None
    if envelope["content_sha256"] != actual:
        raise StructuredEnvelopeValidationError("content_sha256 mismatch", rule="digest_mismatch", field="content_sha256")
    if envelope["kind"] == "tombstone":
        content = envelope["content"]
        if not isinstance(content, dict) or content.get("target_native_id") != envelope["native_id"]:
            raise StructuredEnvelopeValidationError("tombstone target must match native_id", rule="tombstone_target", field="content")
    return envelope


def event_receipt(source_id: str, native_id: str, revision: int) -> str:
    return f"recall://{source_id}/{native_id}?rev={revision}"


def item_receipt(source_id: str, native_id: str, revision: int, ordinal: int) -> str:
    return f"{event_receipt(source_id, native_id, revision)}#item={ordinal}"


def projected_entities(engine, text: str, extra: list[tuple[str, str]] | None = None) -> list[dict]:
    values = []
    for kind, value in engine.extract_entities(text, extra):
        safe_value = redact_text(str(value))
        values.append({"kind": kind, "value": safe_value, "normalized": safe_value.casefold()})
    return values


def partial_lexical_probes(informative: list[str], *, has_time_filter: bool) -> list[tuple[str, str, int]]:
    """Bounded structural probes for one-token drift; never encode domain answers."""
    if not informative:
        return []
    scored = sorted(
        enumerate(informative),
        key=lambda item: (
            bool(re.search(r"[._/-]", item[1])),
            any(character.isdigit() for character in item[1]),
            len(item[1]),
            -item[0],
        ),
        reverse=True,
    )
    probes: list[tuple[str, str, int]] = []
    if len(scored) >= 2:
        probes.append((" ".join([scored[0][1], scored[1][1]]), "pair", 2))
    compound = [value for value in informative if re.search(r"[._/-]", value)]
    if compound:
        probes.append((max(compound, key=len), "anchor", 2))
    if has_time_filter and not compound:
        probes.append((informative[0], "time-anchor", 1))
    return probes[:3]


def preferred_phrase_probes(phrases: list[str]) -> list[str]:
    """Choose a structural phrase plus one bounded parser fallback."""
    if not phrases:
        return []
    candidates = phrases[1:] or phrases

    def score(value: str) -> tuple[int, int, int]:
        words = re.findall(r"[A-Za-z0-9_./#-]+", value)
        structural = sum(
            1
            for word in words
            if re.search(r"[0-9_./#-]", word)
            or re.search(r"(?:error|exception|timeout|violation|failed)$", word, re.I)
        )
        return structural, len(words), -len(value)

    primary = max(candidates, key=score)
    fallback = candidates[-1]
    return [primary] + ([fallback] if fallback != primary else [])


def preferred_phrase_probe(phrases: list[str]) -> str | None:
    """Compatibility helper returning the strongest bounded phrase probe."""
    probes = preferred_phrase_probes(phrases)
    return probes[0] if probes else None


def phrase_query_spec(probes: list[str]) -> tuple[str, str] | None:
    """Compile bounded phrase alternatives into one indexed PostgreSQL query."""
    if not probes:
        return None
    if len(probes) == 1:
        return probes[0], "phraseto_tsquery"
    quoted = [f'"{probe.replace(chr(34), " ")}"' for probe in probes]
    return " OR ".join(quoted), "websearch_to_tsquery"


def project(envelope: dict, revision: int) -> tuple[list[dict], dict]:
    """Return sanitized items and session metadata for one canonical event."""
    kind = envelope["kind"]
    content = envelope["content"]
    items: list[dict] = []
    metadata: dict[str, Any] = {"projector_version": PROJECTOR_VERSION}
    provenance = envelope.get("provenance", {})
    for key in ("original_path", "cwd", "branch", "slot", "harness", "privacy_policy_version"):
        if provenance.get(key) is not None:
            metadata[key] = redact_text(str(provenance[key]))

    if kind == "tombstone":
        return items, metadata

    record_kind = content.get("kind") if isinstance(content, dict) else None
    if (
        kind == "connector_record"
        and provenance.get("connector_schema_version") == 2
        and record_kind in TYPED_CONNECTOR_KINDS
    ):
        metadata["record_kind"] = record_kind
        metadata["content_fidelity"] = content["content_fidelity"]
        if content.get("content_omissions"):
            metadata["content_omissions"] = list(content["content_omissions"])
        if record_kind == "communication_message.v1":
            parts = [content.get("subject"), content.get("text")]
            role = content.get("direction")
        elif record_kind == "calendar_event.v1":
            parts = [content.get("title"), content.get("description"), content.get("location")]
            role = "event"
        elif record_kind == "contact_identity.v1":
            parts = [
                content.get("display_name"), content.get("identifier"),
                content.get("organization"), content.get("title"),
            ]
            role = content.get("role") or "contact"
        elif record_kind == "social_post.v1":
            parts = [content.get("text")]
            role = content.get("stream_type")
        else:
            parts = [content.get("name"), content.get("text")]
            role = "document"
        cleaned = redact_text("\n".join(str(value) for value in parts if value not in {None, ""}))
        items.append({
            "ordinal": 0,
            "occurred_at": envelope["occurred_at"],
            "role": role,
            "surface": record_kind,
            "text_redacted": cleaned,
            "entities": projected_entities(legacy_engine(), cleaned),
            "receipt": item_receipt(envelope["source_id"], envelope["native_id"], revision, 0),
        })
        return items, metadata

    if kind == "transcript_record":
        harness = envelope.get("provenance", {}).get("harness")
        engine = legacy_engine()
        parser = engine.claude_record if harness == "claude" else engine.codex_record if harness == "codex" else None
        if parser is None:
            raise ValueError("transcript_record requires claude or codex harness")
        parsed, parsed_meta = parser(content)
        metadata.update({key: redact_text(str(value)) for key, value in parsed_meta.items() if value is not None})
        metadata["harness"] = harness
        for ordinal, (timestamp, surface, text, entities) in enumerate(parsed):
            cleaned = engine.clean_text(text)
            items.append({
                "ordinal": ordinal,
                "occurred_at": timestamp,
                "role": surface,
                "surface": surface,
                "text_redacted": cleaned,
                "entities": projected_entities(engine, cleaned, entities),
                "receipt": item_receipt(envelope["source_id"], envelope["native_id"], revision, ordinal),
            })
        return items, metadata

    if isinstance(content, str):
        text = content
        role = None
        surface = kind
    elif isinstance(content, dict):
        text = str(content.get("text") or content.get("content") or json.dumps(content, sort_keys=True))
        role = content.get("role")
        surface = str(content.get("surface") or kind)
    else:
        text = json.dumps(content, sort_keys=True)
        role = None
        surface = kind
    cleaned = redact_text(text)
    items.append({
        "ordinal": 0,
        "occurred_at": envelope["occurred_at"],
        "role": role,
        "surface": surface,
        "text_redacted": cleaned,
        "entities": projected_entities(legacy_engine(), cleaned),
        "receipt": item_receipt(envelope["source_id"], envelope["native_id"], revision, 0),
    })
    return items, metadata
