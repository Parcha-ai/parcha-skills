from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit


MAX_MANIFEST_BYTES = 64 * 1024
IMAGE_RE = re.compile(r"[a-z0-9][a-z0-9._/-]*@sha256:[0-9a-f]{64}\Z")
NAME_RE = re.compile(r"[a-z0-9][a-z0-9-]{2,62}\Z")
MODEL_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}\Z")
SECRET_REF_RE = re.compile(r"secret://[A-Za-z0-9][A-Za-z0-9._/-]{2,127}\Z")
APPROVAL_REF_RE = re.compile(r"approval://[a-z][a-z-]{2,63}\Z")
PENDING_GATES = [
    "provider-billing",
    "provider-region",
    "provider-authorization",
    "tailnet-route",
    "writer-cutover",
]


class DeploymentManifestError(ValueError):
    pass


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise DeploymentManifestError("duplicate manifest key")
        result[key] = value
    return result


def _keys(value: object, expected: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != expected:
        raise DeploymentManifestError(f"invalid {label} fields")
    return value


def _approval(value: object, expected: str) -> None:
    if not isinstance(value, str) or not APPROVAL_REF_RE.fullmatch(value):
        raise DeploymentManifestError("invalid approval reference")
    if value != f"approval://{expected}":
        raise DeploymentManifestError("approval reference does not match gate")


def validate_manifest(value: object) -> dict[str, Any]:
    manifest = _keys(
        value,
        {
            "schema_version",
            "deployment_name",
            "image",
            "database",
            "service",
            "network",
            "authorization",
        },
        "manifest",
    )
    if manifest["schema_version"] != 1 or type(manifest["schema_version"]) is not int:
        raise DeploymentManifestError("unsupported manifest schema")
    if not isinstance(manifest["deployment_name"], str) or not NAME_RE.fullmatch(
        manifest["deployment_name"],
    ):
        raise DeploymentManifestError("invalid deployment name")
    if not isinstance(manifest["image"], str) or not IMAGE_RE.fullmatch(
        manifest["image"]
    ):
        raise DeploymentManifestError("image must be pinned by sha256 digest")

    database = _keys(
        manifest["database"],
        {"adapter", "provider", "url_ref", "tls_mode"},
        "database",
    )
    if database["adapter"] != "postgres" or database["provider"] not in {
        "planetscale",
        "supabase",
        "neon",
        "standard-postgres",
    }:
        raise DeploymentManifestError("unsupported database adapter")
    if not isinstance(database["url_ref"], str) or not SECRET_REF_RE.fullmatch(
        database["url_ref"]
    ):
        raise DeploymentManifestError("database URL must be a secret reference")
    if database["tls_mode"] != "verify-full":
        raise DeploymentManifestError("database TLS must verify server identity")

    service = _keys(
        manifest["service"],
        {
            "adapter",
            "embedding",
            "region_ref",
            "billing_ref",
            "public_ingress",
        },
        "service",
    )
    if (
        service["adapter"] != "render-private-service"
        or service["public_ingress"] is not False
    ):
        raise DeploymentManifestError("service must be private")
    embedding = _keys(
        service["embedding"],
        {
            "protocol",
            "url",
            "approved_url",
            "key_ref",
            "model",
            "revision",
            "dimensions",
            "batch_size",
        },
        "embedding",
    )
    if embedding["protocol"] not in {"voyage", "openai"}:
        raise DeploymentManifestError("unsupported managed embedding protocol")
    for field in ("url", "approved_url"):
        value = embedding[field]
        if not isinstance(value, str):
            raise DeploymentManifestError("invalid managed embedding URL")
        parsed = urlsplit(value)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise DeploymentManifestError("invalid managed embedding URL")
    if embedding["url"].rstrip("/") != embedding["approved_url"].rstrip("/"):
        raise DeploymentManifestError("managed embedding approval mismatch")
    if embedding["key_ref"] != "secret://runtime/RECALL_EMBEDDING_API_KEY":
        raise DeploymentManifestError("invalid managed embedding key reference")
    if not isinstance(embedding["model"], str) or not MODEL_RE.fullmatch(
        embedding["model"]
    ):
        raise DeploymentManifestError("invalid managed embedding model")
    if not isinstance(embedding["revision"], str) or not MODEL_RE.fullmatch(
        embedding["revision"]
    ):
        raise DeploymentManifestError("invalid managed embedding revision")
    if embedding["dimensions"] != 512 or type(embedding["dimensions"]) is not int:
        raise DeploymentManifestError("managed embedding dimensions must be 512")
    if (
        type(embedding["batch_size"]) is not int
        or not 1 <= embedding["batch_size"] <= 512
    ):
        raise DeploymentManifestError("invalid managed embedding batch size")
    _approval(service["region_ref"], "provider-region")
    _approval(service["billing_ref"], "provider-billing")

    network = _keys(
        manifest["network"],
        {"adapter", "gateway_image", "route_ref", "listen_port"},
        "network",
    )
    port = network["listen_port"]
    if (
        network["adapter"] != "tailscale-gateway"
        or type(port) is not int
        or not 1024 <= port <= 65535
        or port == 443
    ):
        raise DeploymentManifestError("unsupported private network profile")
    if not isinstance(network["gateway_image"], str) or not IMAGE_RE.fullmatch(
        network["gateway_image"]
    ):
        raise DeploymentManifestError("gateway image must be pinned by sha256 digest")
    _approval(network["route_ref"], "tailnet-route")

    authorization = _keys(
        manifest["authorization"],
        {"provider_ref", "cutover_ref"},
        "authorization",
    )
    _approval(authorization["provider_ref"], "provider-authorization")
    _approval(authorization["cutover_ref"], "writer-cutover")
    return manifest


def load_manifest(path: Path) -> dict[str, Any]:
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError as error:
        raise DeploymentManifestError("manifest unavailable") from error
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > MAX_MANIFEST_BYTES:
            raise DeploymentManifestError("manifest size is invalid")
        with os.fdopen(descriptor, "rb") as source:
            descriptor = -1
            payload = source.read(MAX_MANIFEST_BYTES + 1)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if not payload or len(payload) > MAX_MANIFEST_BYTES:
        raise DeploymentManifestError("manifest size is invalid")
    try:
        value = json.loads(payload, object_pairs_hook=_object)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise DeploymentManifestError("manifest JSON is invalid") from error
    return validate_manifest(value)


def preview(manifest: dict[str, Any]) -> dict[str, Any]:
    validated = validate_manifest(manifest)
    canonical = json.dumps(
        validated,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode()
    return {
        "schema_version": 1,
        "status": "approval_required",
        "plan_sha256": hashlib.sha256(canonical).hexdigest(),
        "resources": [
            "postgres-database",
            "private-service",
            "tailscale-gateway",
        ],
        "pending_gates": list(PENDING_GATES),
        "runtime_contract": {
            "image": "digest-pinned",
            "database": "standard-postgres",
            "tls": "verify-full",
            "public_ingress": False,
        },
        "mutation_count": 0,
        "network_calls": 0,
        "source_reads": 0,
        "content_free": True,
    }
