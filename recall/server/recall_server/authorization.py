from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass
from datetime import datetime, timezone
import http.client
import json
import os
import re
import ssl
import threading
import time
from typing import Any, Protocol
from urllib.parse import urlsplit

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.exceptions import InvalidSignature


POLICY_VERSION = "recall.authorization.v1"


@dataclass(frozen=True)
class Rule:
    scopes: frozenset[str]
    roles: frozenset[str]
    principal_kinds: frozenset[str]


@dataclass(frozen=True)
class Decision:
    allowed: bool
    reason: str
    policy_version: str = POLICY_VERSION


@dataclass(frozen=True)
class VerifiedExternalIdentity:
    issuer: str
    subject: str
    audience: str
    scopes: tuple[str, ...]
    expires_at: datetime
    email: str | None = None
    email_verified: bool = False

    def valid_for(self, audience: str) -> bool:
        return (
            self.issuer.startswith("https://")
            and bool(self.subject)
            and self.audience == audience
            and bool(self.scopes)
            and self.expires_at.tzinfo is not None
            and self.expires_at > datetime.now(timezone.utc)
        )


class ExternalIdentityVerifier(Protocol):
    """Pluggable OSS/hosted boundary; implementations verify before returning claims."""

    def verify(self, token: str) -> VerifiedExternalIdentity | None: ...


class JwksLoader(Protocol):
    def load(self) -> dict[str, Any]: ...


_KID = re.compile(r"[A-Za-z0-9._:-]{1,128}\Z")
_MAX_TOKEN_BYTES = 16 * 1024
_MAX_JWKS_BYTES = 256 * 1024
_CLOCK_SKEW_SECONDS = 60


def normalize_verified_email(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().casefold()
    if (
        not 3 <= len(normalized) <= 320
        or normalized.count("@") != 1
        or any(character.isspace() or ord(character) < 33 for character in normalized)
    ):
        return None
    local, domain = normalized.rsplit("@", 1)
    if (
        not 1 <= len(local) <= 64
        or not 1 <= len(domain) <= 255
        or "." not in domain
        or domain.startswith(('.', '-'))
        or domain.endswith(('.', '-'))
        or not re.fullmatch(r"[a-z0-9.-]+", domain)
    ):
        return None
    return normalized


def _https_uri(value: str, label: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be an exact HTTPS URI")
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(f"{label} must be an exact HTTPS URI")
    return value.rstrip("/")


def _decode_segment(value: str) -> bytes:
    if not value or len(value) > _MAX_TOKEN_BYTES or not re.fullmatch(
        r"[A-Za-z0-9_-]+", value
    ):
        raise ValueError("JWT segment invalid")
    try:
        return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    except (ValueError, binascii.Error) as error:
        raise ValueError("JWT segment invalid") from error


def _json_segment(value: str) -> dict[str, Any]:
    decoded = _decode_segment(value)
    if len(decoded) > _MAX_TOKEN_BYTES:
        raise ValueError("JWT segment invalid")
    try:
        result = json.loads(decoded)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError("JWT segment invalid") from error
    if not isinstance(result, dict):
        raise ValueError("JWT segment invalid")
    return result


class HttpsJwksCache:
    """Small bounded JWKS loader; configuration is owner-supplied, never request-supplied."""

    def __init__(
        self,
        uri: str,
        *,
        ttl_seconds: int = 300,
        timeout_seconds: float = 5.0,
    ) -> None:
        self.uri = _https_uri(uri, "JWKS URI")
        if not 30 <= ttl_seconds <= 3600 or not 0.5 <= timeout_seconds <= 10:
            raise ValueError("JWKS cache bounds invalid")
        self.ttl_seconds = ttl_seconds
        self.timeout_seconds = timeout_seconds
        self._lock = threading.Lock()
        self._expires_at = 0.0
        self._value: dict[str, Any] | None = None

    def _fetch(self) -> dict[str, Any]:
        parsed = urlsplit(self.uri)
        path = parsed.path or "/"
        context = ssl.create_default_context()
        connection = http.client.HTTPSConnection(
            parsed.hostname,
            parsed.port or 443,
            timeout=self.timeout_seconds,
            context=context,
        )
        try:
            connection.request(
                "GET",
                path,
                headers={"Accept": "application/json", "User-Agent": "recall-core/oidc"},
            )
            response = connection.getresponse()
            content_type = response.getheader("Content-Type", "").split(";", 1)[0]
            length = response.getheader("Content-Length")
            if response.status != 200 or content_type != "application/json":
                raise RuntimeError("JWKS endpoint unavailable")
            if length is not None and (
                not length.isdigit() or int(length) > _MAX_JWKS_BYTES
            ):
                raise RuntimeError("JWKS response invalid")
            payload = response.read(_MAX_JWKS_BYTES + 1)
            if len(payload) > _MAX_JWKS_BYTES:
                raise RuntimeError("JWKS response invalid")
            value = json.loads(payload)
        except (OSError, http.client.HTTPException, json.JSONDecodeError) as error:
            raise RuntimeError("JWKS endpoint unavailable") from error
        finally:
            connection.close()
        if not isinstance(value, dict):
            raise RuntimeError("JWKS response invalid")
        return value

    def load(self) -> dict[str, Any]:
        now = time.monotonic()
        with self._lock:
            if self._value is not None and now < self._expires_at:
                return self._value
            value = self._fetch()
            self._value = value
            self._expires_at = now + self.ttl_seconds
            return value


class OidcJwtVerifier:
    """Provider-neutral OIDC access-token verifier for the MCP resource server."""

    def __init__(
        self,
        *,
        issuer: str,
        audience: str,
        jwks_uri: str,
        additional_issuers: tuple[str, ...] = (),
        provider: str = "oidc",
        jwks_loader: JwksLoader | None = None,
        clock: Any = time.time,
    ) -> None:
        if provider not in {"oidc", "descope"}:
            raise ValueError("OIDC provider invalid")
        self.provider = provider
        self.issuer = _https_uri(issuer, "OIDC issuer")
        if not isinstance(additional_issuers, tuple) or len(additional_issuers) > 8:
            raise ValueError("additional OIDC issuers invalid")
        normalized_additional = tuple(
            _https_uri(value, "additional OIDC issuer")
            for value in additional_issuers
        )
        if len(set(normalized_additional)) != len(normalized_additional):
            raise ValueError("additional OIDC issuers invalid")
        self.accepted_issuers = frozenset((self.issuer, *normalized_additional))
        self.audience = _https_uri(audience, "OIDC audience")
        self.jwks_uri = _https_uri(jwks_uri, "JWKS URI")
        self.jwks_loader = jwks_loader or HttpsJwksCache(self.jwks_uri)
        self.clock = clock

    @classmethod
    def from_env(cls) -> OidcJwtVerifier | None:
        provider = os.environ.get("RECALL_MCP_AUTH_PROVIDER", "").strip().lower()
        if provider in {"", "disabled"}:
            return None
        if provider not in {"oidc", "descope"}:
            raise RuntimeError("MCP auth provider is invalid")
        try:
            additional_issuers = tuple(
                value.strip()
                for value in os.environ.get(
                    "RECALL_OIDC_ADDITIONAL_ISSUERS", ""
                ).split(",")
                if value.strip()
            )
            verifier = cls(
                provider=provider,
                issuer=os.environ.get("RECALL_OIDC_ISSUER", ""),
                audience=os.environ.get("RECALL_MCP_RESOURCE_URI", ""),
                jwks_uri=os.environ.get("RECALL_OIDC_JWKS_URI", ""),
                additional_issuers=additional_issuers,
            )
        except ValueError as error:
            raise RuntimeError("OIDC resource configuration is invalid") from error
        servers = {
            value.strip().rstrip("/")
            for value in os.environ.get("RECALL_AUTHORIZATION_SERVERS", "").split(",")
            if value.strip()
        }
        if verifier.issuer not in servers:
            raise RuntimeError("OIDC issuer must be an authorization server")
        return verifier

    @staticmethod
    def _scopes(payload: dict[str, Any]) -> tuple[str, ...]:
        raw = payload.get("scope")
        if isinstance(raw, str):
            values = raw.split()
        else:
            raw = payload.get("scp")
            values = raw if isinstance(raw, list) else []
        if (
            not values
            or len(values) > 32
            or any(
                not isinstance(value, str)
                or not 1 <= len(value) <= 128
                or not re.fullmatch(r"[A-Za-z0-9._:-]+", value)
                for value in values
            )
        ):
            return ()
        return tuple(sorted(set(values)))

    def verify(self, token: str) -> VerifiedExternalIdentity | None:
        if (
            not isinstance(token, str)
            or not 1 <= len(token.encode()) <= _MAX_TOKEN_BYTES
        ):
            return None
        parts = token.split(".")
        if len(parts) != 3:
            return None
        try:
            header = _json_segment(parts[0])
            payload = _json_segment(parts[1])
            signature = _decode_segment(parts[2])
            kid = header.get("kid")
            if (
                header.get("alg") != "RS256"
                or header.get("crit") is not None
                or not isinstance(kid, str)
                or not _KID.fullmatch(kid)
            ):
                return None
            jwks = self.jwks_loader.load()
            keys = jwks.get("keys")
            if not isinstance(keys, list) or len(keys) > 64:
                return None
            matches = [
                key
                for key in keys
                if isinstance(key, dict)
                and key.get("kid") == kid
                and key.get("kty") == "RSA"
                and key.get("alg", "RS256") == "RS256"
                and key.get("use", "sig") == "sig"
            ]
            if len(matches) != 1:
                return None
            modulus = int.from_bytes(_decode_segment(matches[0].get("n", "")), "big")
            exponent = int.from_bytes(_decode_segment(matches[0].get("e", "")), "big")
            if modulus.bit_length() < 2048 or exponent not in {3, 65537}:
                return None
            key = rsa.RSAPublicNumbers(exponent, modulus).public_key()
            key.verify(
                signature,
                f"{parts[0]}.{parts[1]}".encode("ascii"),
                padding.PKCS1v15(),
                hashes.SHA256(),
            )
        except (ValueError, TypeError, binascii.Error, InvalidSignature):
            return None

        now = float(self.clock())
        subject = payload.get("sub")
        audience = payload.get("aud")
        if isinstance(audience, str):
            audiences = {audience}
        elif isinstance(audience, list) and all(
            isinstance(value, str) for value in audience
        ):
            audiences = set(audience)
        else:
            audiences = set()
        expires_at = payload.get("exp")
        not_before = payload.get("nbf", 0)
        issued_at = payload.get("iat", 0)
        scopes = self._scopes(payload)
        if (
            payload.get("iss") not in self.accepted_issuers
            or not isinstance(subject, str)
            or not 1 <= len(subject) <= 512
            or self.audience not in audiences
            or isinstance(expires_at, bool)
            or not isinstance(expires_at, (int, float))
            or expires_at <= now
            or isinstance(not_before, bool)
            or not isinstance(not_before, (int, float))
            or not_before > now + _CLOCK_SKEW_SECONDS
            or isinstance(issued_at, bool)
            or not isinstance(issued_at, (int, float))
            or issued_at > now + _CLOCK_SKEW_SECONDS
            or "read" not in scopes
        ):
            return None
        email = normalize_verified_email(payload.get("email"))
        return VerifiedExternalIdentity(
            # Provider-specific token issuers are deliberately collapsed onto the
            # configured canonical issuer so one person has one durable binding.
            issuer=self.issuer,
            subject=subject,
            audience=self.audience,
            scopes=scopes,
            expires_at=datetime.fromtimestamp(expires_at, timezone.utc),
            email=email,
            email_verified=payload.get("email_verified") is True,
        )


READ_ROLES = frozenset({"owner", "admin", "member"})
ADMIN_ROLES = frozenset({"owner", "admin"})
REMOTE_PRINCIPALS = frozenset({"human", "workload"})

# This closed matrix is the canonical policy for remote canonical MCP. Missing
# actions, roles, principal kinds, or scopes deny by default.
MCP_POLICY: dict[str, Rule] = {
    "mcp.initialize": Rule(frozenset({"read"}), READ_ROLES, REMOTE_PRINCIPALS),
    "mcp.ping": Rule(frozenset({"read"}), READ_ROLES, REMOTE_PRINCIPALS),
    "mcp.tools.list": Rule(frozenset({"read"}), READ_ROLES, REMOTE_PRINCIPALS),
    "mcp.use_recall": Rule(frozenset({"read"}), READ_ROLES, REMOTE_PRINCIPALS),
    "mcp.recall_agent_start": Rule(frozenset({"read"}), READ_ROLES, REMOTE_PRINCIPALS),
    "mcp.recall_agent_status": Rule(frozenset({"read"}), READ_ROLES, REMOTE_PRINCIPALS),
    "mcp.recall_agent_result": Rule(frozenset({"read"}), READ_ROLES, REMOTE_PRINCIPALS),
    "mcp.recall_agent_cancel": Rule(frozenset({"read"}), READ_ROLES, REMOTE_PRINCIPALS),
    "mcp.recall_search": Rule(frozenset({"read"}), READ_ROLES, REMOTE_PRINCIPALS),
    "mcp.recall_deep_search": Rule(frozenset({"read"}), READ_ROLES, REMOTE_PRINCIPALS),
    "mcp.recall_investigate": Rule(frozenset({"read"}), READ_ROLES, REMOTE_PRINCIPALS),
    "mcp.recall_session_context": Rule(frozenset({"read"}), READ_ROLES, REMOTE_PRINCIPALS),
    "mcp.recall_show": Rule(frozenset({"read"}), READ_ROLES, REMOTE_PRINCIPALS),
    "mcp.recall_related": Rule(frozenset({"read"}), READ_ROLES, REMOTE_PRINCIPALS),
    "mcp.recall_forget": Rule(frozenset({"forget"}), ADMIN_ROLES, REMOTE_PRINCIPALS),
}


def decide(principal: dict[str, Any], action: str, *, tenant_id: str | None = None) -> Decision:
    rule = MCP_POLICY.get(action)
    if rule is None:
        return Decision(False, "action_denied")
    bound_tenant = principal.get("tenant_id")
    if (
        not isinstance(bound_tenant, str)
        or not bound_tenant
        or (tenant_id is not None and tenant_id != bound_tenant)
    ):
        return Decision(False, "tenant_denied")
    if principal.get("principal_kind") not in rule.principal_kinds:
        return Decision(False, "principal_kind_denied")
    if principal.get("role") not in rule.roles:
        return Decision(False, "role_denied")
    scopes = principal.get("scopes")
    if not isinstance(scopes, (list, tuple, set, frozenset)) or not rule.scopes.issubset(scopes):
        return Decision(False, "scope_denied")
    if principal.get("audience") != "recall-mcp":
        return Decision(False, "audience_denied")
    return Decision(True, "allowed")


def allowed_tools(principal: dict[str, Any]) -> frozenset[str]:
    return frozenset(
        tool
        for tool in (
            "use_recall",
            "recall_agent_start",
            "recall_agent_status",
            "recall_agent_result",
            "recall_agent_cancel",
            "recall_search",
            "recall_deep_search",
            "recall_investigate",
            "recall_session_context",
            "recall_show",
            "recall_related",
            "recall_forget",
        )
        if decide(principal, f"mcp.{tool}").allowed
    )
