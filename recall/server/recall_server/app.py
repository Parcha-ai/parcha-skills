from __future__ import annotations

import base64
import binascii
import json
import logging
import os
import re
import socket
import socketserver
import stat
import struct
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit, urlunsplit

from .admin_web import (
    CSRF_COOKIE,
    SESSION_COOKIE,
)
from .admin_web import (
    asset as admin_asset,
)
from .admin_web import (
    cookies as admin_cookies,
)
from .admin_web import (
    session_headers as admin_session_headers,
)
from .archive_runtime import build_archive_store, build_evidence_archive_store
from .actor_attribution import ActorIdentityIndex
from .authorization import ExternalIdentityVerifier, OidcJwtVerifier, decide
from .canonical import (
    CanonicalArchiveGateway,
    CanonicalLifecycleError,
    CanonicalPlane,
)
from .canonical_retrieval import CanonicalRetrieval
from .control import ControlError, ControlPlane
from .db import BrainStore, IdempotencyConflict
from .deep_inspection import DeepInspectionError
from .deep_inspection_runtime import build_deep_inspector
from .evidence_projection import (
    CanonicalEvidenceProjector,
    EvidenceProjectionStore,
)
from .invitation_email import onboarding_page
from .mcp import (
    LATEST_PROTOCOL_VERSION,
    SUPPORTED_PROTOCOL_VERSIONS,
    McpProtocolError,
)
from .mcp import (
    bound_response as bound_mcp_response,
)
from .mcp import (
    dispatch as dispatch_mcp,
)
from .mcp import (
    error_response as mcp_error_response,
)
from .semantic import SemanticRuntime
from .webhooks import WEBHOOK_PATH, WebhookError, build_webhook_event

LOG = logging.getLogger("recall.brainstore")
MAX_BODY_BYTES = 12 * 1024 * 1024
MAX_CANONICAL_EVENTS_BYTES = 8_000_000
MAX_ADMIN_BODY_BYTES = 64 * 1024
ADMIN_INSTALLATION_ACTION = re.compile(
    r"/admin/api/v1/installations/([0-9a-f-]{36})/actions\Z"
)
ADMIN_CONNECTION_REVOKE = re.compile(
    r"/admin/api/v1/connections/([0-9a-f-]{36})/revoke\Z"
)
ADMIN_INVITATION_REVOKE = re.compile(
    r"/admin/api/v1/invitations/([0-9a-f-]{36})/revoke\Z"
)
ADMIN_ACTOR_IDENTITY = re.compile(
    r"/admin/api/v1/actors/(actor_[0-9a-f]{32})/identities\Z"
)
INVITATION_ONBOARDING = re.compile(r"/join/([0-9a-f-]{36})\Z")
INVITATION_LOGIN = re.compile(r"/join/([0-9a-f-]{36})/login\Z")
MCP_BRAIN_PATH = re.compile(
    r"/mcp/brains/([A-Za-z0-9][A-Za-z0-9:._@+-]{1,255})\Z"
)
COUNTERS = {
    "http_requests": 0,
    "http_errors": 0,
    "http_duration_count": 0,
    "http_duration_sum": 0.0,
    "auth_denied": 0,
    "ingest_commits": 0,
    "ingest_replays": 0,
}
COUNTER_LOCK = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    store: BrainStore
    archive_store = None
    evidence_archive_store = None
    evidence_projector = None
    deep_inspector = None
    canonical_plane: CanonicalPlane | None = None
    canonical_retrieval: CanonicalRetrieval | None = None
    control_plane: ControlPlane | None = None
    external_identity_verifier: ExternalIdentityVerifier | None = None

    def log_message(self, fmt: str, *args) -> None:
        LOG.info(
            "http method=%s path=%s status=%s",
            self.command,
            self.path.split("?", 1)[0],
            args[1] if len(args) > 1 else "unknown",
        )

    def send_json(
        self,
        status: int,
        body: object,
        headers: list[tuple[str, str]] | None = None,
    ) -> None:
        if status >= 400:
            with COUNTER_LOCK:
                COUNTERS["http_errors"] += 1
        data = json.dumps(body, default=str, sort_keys=True).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        for name, value in headers or []:
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(data)))
        try:
            self.end_headers()
            self.wfile.write(data)
        except BrokenPipeError:
            # The database work may have completed after a bounded client left.
            # Never turn that ordinary transport event into a content-bearing traceback.
            return

    def send_asset(self, body: bytes, content_type: str) -> None:
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'none'; script-src 'self'; style-src 'self'; "
            "img-src 'self' data:; connect-src 'self'; form-action 'self'; "
            "base-uri 'none'; frame-ancestors 'none'",
        )
        self.end_headers()
        self.wfile.write(body)

    def send_public_html(self, body: bytes) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'none'; style-src 'unsafe-inline'; "
            "base-uri 'none'; frame-ancestors 'none'; form-action 'none'",
        )
        self.end_headers()
        self.wfile.write(body)

    def send_redirect(
        self,
        location: str,
        headers: list[tuple[str, str]] | None = None,
    ) -> None:
        self.send_response(303)
        self.send_header("Location", location)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Referrer-Policy", "no-referrer")
        for name, value in headers or []:
            self.send_header(name, value)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def send_empty(self, status: int, headers: dict[str, str] | None = None) -> None:
        if status >= 400:
            with COUNTER_LOCK:
                COUNTERS["http_errors"] += 1
        self.send_response(status)
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def valid_mcp_origin(self) -> bool:
        origin = self.headers.get("Origin")
        if origin is None:
            return True
        allowed = {
            value.strip()
            for value in os.environ.get("RECALL_MCP_ALLOWED_ORIGINS", "").split(",")
            if value.strip()
        }
        if origin in allowed:
            return True
        self.send_json(
            403,
            mcp_error_response(McpProtocolError(-32000, "invalid origin")),
        )
        return False

    def valid_mcp_accept(self, *, get: bool = False) -> bool:
        accepted = {
            value.split(";", 1)[0].strip().casefold()
            for value in self.headers.get("Accept", "").split(",")
            if value.strip()
        }
        required = (
            {"text/event-stream"} if get else {"application/json", "text/event-stream"}
        )
        if required.issubset(accepted):
            return True
        self.send_json(
            406,
            mcp_error_response(McpProtocolError(-32000, "unsupported accept types")),
        )
        return False

    def valid_mcp_protocol(self) -> bool:
        version = self.headers.get("MCP-Protocol-Version")
        if version is None or version in SUPPORTED_PROTOCOL_VERSIONS:
            return True
        self.send_json(
            400,
            mcp_error_response(
                McpProtocolError(-32600, "unsupported MCP protocol version")
            ),
        )
        return False

    def valid_mcp_routing_headers(
        self,
        body: object,
        protocol_version: str,
    ) -> bool:
        if protocol_version != LATEST_PROTOCOL_VERSION:
            return True
        if not isinstance(body, dict):
            return True
        method = body.get("method")
        params = body.get("params") if isinstance(body.get("params"), dict) else {}
        metadata = params.get("_meta") if isinstance(params.get("_meta"), dict) else {}
        expected_name = None
        if method == "tools/call":
            expected_name = params.get("name")
        elif method in {"tasks/get", "tasks/update", "tasks/cancel"}:
            expected_name = params.get("taskId")
        if (
            not isinstance(method, str)
            or self.headers.get("Mcp-Method") != method
            or metadata.get("io.modelcontextprotocol/protocolVersion")
            != protocol_version
            or (
                expected_name is not None
                and self.headers.get("Mcp-Name") != expected_name
            )
            or (
                expected_name is None
                and self.headers.get("Mcp-Name") is not None
            )
        ):
            self.send_json(
                400,
                mcp_error_response(
                    McpProtocolError(-32020, "MCP routing headers do not match"),
                    body.get("id"),
                ),
            )
            return False
        return True

    def body_length(self, maximum: int) -> int | None:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except (TypeError, ValueError):
            self.send_json(400, {"error": "invalid body size"})
            return None
        if length <= 0 or length > maximum:
            self.send_json(413, {"error": "invalid body size"})
            return None
        return length

    def authenticate(
        self,
        scope: str,
        *,
        requested_tenant: str | None = None,
    ) -> dict | None:
        if os.environ.get("RECALL_AUTH_REQUIRED", "0") != "1":
            return {"kind": "development", "name": "unauthenticated"}
        authorization = self.headers.get("Authorization")
        if authorization is not None:
            if not authorization.startswith("Bearer "):
                return None
            credential = self.store.authenticate_bearer(
                authorization.removeprefix("Bearer ").strip(), scope
            )
            if not credential and self.external_identity_verifier is not None:
                try:
                    request_resource = self.mcp_resource_uri(requested_tenant)
                    claims = self.external_identity_verifier.verify(
                        authorization.removeprefix("Bearer ").strip(),
                        audience=request_resource,
                    )
                except Exception as error:
                    LOG.warning(
                        "external identity verification failed type=%s",
                        type(error).__name__,
                    )
                    return None
                resource = self.mcp_resource_uri()
                if claims is not None and claims.valid_for(resource):
                    credential = self.store.resolve_external_identity(
                        issuer=claims.issuer,
                        subject=claims.subject,
                        scopes=list(claims.scopes),
                        audience=claims.audience,
                        tenant_id=requested_tenant,
                    )
                    if (
                        credential is None
                        and claims.email_verified
                        and claims.email is not None
                        and self.control_plane is not None
                    ):
                        credential = self.store.accept_external_invitation(
                            issuer=claims.issuer,
                            subject=claims.subject,
                            email=claims.email,
                            email_index=self.control_plane.invitation_email_index(
                                claims.email
                            ),
                            scopes=list(claims.scopes),
                            audience=claims.audience,
                            tenant_id=requested_tenant,
                            actor_email_index=self.control_plane.actor_identity_index(
                                claims.email,
                                tenant_id=requested_tenant,
                                connector_id="identity",
                                namespace="email",
                            ),
                        )
            if not credential:
                return None
            credential_kind = credential.get("credential_kind", "collector")
            if credential_kind == "mcp" and (
                credential.get("audience") != "recall-mcp"
                or credential.get("principal_kind") not in {"human", "workload"}
                or credential.get("role") not in {"owner", "admin", "member"}
                or not credential.get("tenant_id")
                or not credential.get("principal_id")
                or (
                    requested_tenant is not None
                    and credential.get("tenant_id") != requested_tenant
                )
            ):
                return None
            if (
                scope == "read"
                and os.environ.get("RECALL_CANONICAL_MCP_ENABLED") == "1"
                and credential_kind != "mcp"
            ):
                return None
            principal = {
                "kind": credential_kind,
                "credential_kind": credential_kind,
                "name": credential["name"],
                "tenant_id": credential.get("tenant_id"),
                "source_id": credential["source_id"],
                "principal_id": credential.get("principal_id"),
                "principal_kind": credential.get("principal_kind"),
                "role": credential.get("role"),
                "audience": credential.get("audience"),
                "capture_origin": credential.get("capture_origin"),
                "webhook_privacy_mode": credential.get("webhook_privacy_mode"),
                "scopes": list(credential.get("scopes", [])),
            }
            if credential.get("principal_id") and scope == "read":
                if credential_kind == "mcp":
                    principal["authorized_sources"] = (
                        self.store.authorized_canonical_source_ids(
                            credential.get("tenant_id"),
                            credential["principal_id"],
                        )
                    )
                else:
                    principal["authorized_sources"] = self.store.authorized_source_ids(
                        credential["principal_id"]
                    )
            return principal
        if (
            os.environ.get("RECALL_TRUST_TAILSCALE_HEADERS", "0") == "1"
            and self.trusted_proxy_peer()
        ):
            login = self.headers.get("Tailscale-User-Login")
            allowed = {
                value.strip().casefold()
                for value in os.environ.get("RECALL_ALLOWED_TAILSCALE_USERS", "").split(
                    ","
                )
                if value.strip()
            }
            if login and login.casefold() in allowed:
                return {"kind": "tailscale-user", "name": login}
        return None

    def trusted_proxy_peer(self) -> bool:
        """Trust identity headers only from root/tailscaled over a Unix socket."""
        if not getattr(self.server, "is_unix_socket", False):
            LOG.warning("tailscale identity rejected transport=tcp")
            return False
        try:
            pid, uid, _gid = struct.unpack(
                "3i",
                self.connection.getsockopt(
                    socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")
                ),
            )
            trusted_uids = {
                int(value.strip())
                for value in os.environ.get("RECALL_TRUSTED_PROXY_UIDS", "0").split(",")
                if value.strip().isdigit()
            }
            trusted = uid in trusted_uids
            LOG.info(
                "tailscale proxy peer trusted=%s uid=%s pid=%s identity_header_present=%s",
                trusted,
                uid,
                pid,
                bool(self.headers.get("Tailscale-User-Login")),
            )
            return trusted
        except (AttributeError, OSError, struct.error):
            LOG.exception("tailscale identity rejected reason=peer_credential_error")
            return False

    def require(
        self,
        scope: str,
        *,
        requested_tenant: str | None = None,
    ) -> dict | None:
        principal = self.authenticate(scope, requested_tenant=requested_tenant)
        if principal:
            return principal
        with COUNTER_LOCK:
            COUNTERS["auth_denied"] += 1
        headers = None
        if (
            urlsplit(self.path).path == "/mcp"
            or MCP_BRAIN_PATH.fullmatch(urlsplit(self.path).path)
        ):
            resource = self.mcp_resource_uri(requested_tenant)
            if resource:
                metadata = self.protected_resource_metadata_uri(resource)
                headers = [
                    (
                        "WWW-Authenticate",
                        f'Bearer resource_metadata="{metadata}", scope="read"',
                    )
                ]
        self.send_json(401, {"error": "unauthorized"}, headers)
        return None

    @staticmethod
    def protected_resource_metadata_uri(resource: str) -> str:
        parsed = urlsplit(resource)
        suffix = parsed.path.rstrip("/")
        path = "/.well-known/oauth-protected-resource" + suffix
        return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))

    @staticmethod
    def mcp_resource_uri(requested_tenant: str | None = None) -> str:
        resource = os.environ.get("RECALL_MCP_RESOURCE_URI", "").rstrip("/")
        if not resource or requested_tenant is None:
            return resource
        return f"{resource}/brains/{requested_tenant}"

    @staticmethod
    def is_protected_resource_metadata_path(path: str, resource: str) -> bool:
        if not resource:
            return False
        prefix = "/.well-known/oauth-protected-resource"
        if path == prefix:
            return True
        if not path.startswith(prefix):
            return False
        resource_path = path[len(prefix) :]
        configured_path = urlsplit(resource).path.rstrip("/")
        return resource_path == configured_path or bool(
            MCP_BRAIN_PATH.fullmatch(resource_path)
        )

    @staticmethod
    def protected_resource_metadata(
        requested_tenant: str | None = None,
    ) -> dict[str, object]:
        resource = Handler.mcp_resource_uri(requested_tenant)
        servers = [
            value.strip().rstrip("/")
            for value in os.environ.get(
                "RECALL_AUTHORIZATION_SERVERS", ""
            ).split(",")
            if value.strip()
        ]
        result: dict[str, object] = {
            "resource": resource,
            "scopes_supported": ["read"],
            "bearer_methods_supported": ["header"],
            "resource_name": "Recall brain",
        }
        if servers:
            result["authorization_servers"] = servers
        return result

    def authorize_mcp(self, principal: dict, action: str) -> bool:
        decision = decide(
            principal,
            action,
            tenant_id=principal.get("tenant_id"),
        )
        audit_action = (
            action
            if re.fullmatch(r"mcp\.[a-z_]{1,64}", action)
            else "mcp.unknown_tool"
        )
        self.store.record_authorization_event(
            principal,
            action=audit_action,
            allowed=decision.allowed,
            reason=decision.reason,
            policy_version=decision.policy_version,
        )
        if not decision.allowed:
            with COUNTER_LOCK:
                COUNTERS["auth_denied"] += 1
        return decision.allowed

    def canonical_authority(
        self,
        principal: dict,
        body: dict,
    ) -> tuple[str, str, str] | None:
        if not isinstance(body, dict):
            self.send_json(400, {"error": "canonical request invalid"})
            return None
        tenant_id = body.get("tenant_id")
        principal_id = body.get("principal_id")
        source_id = body.get("source_id")
        if source_id is None:
            events = body.get("events")
            if (
                isinstance(events, list)
                and events
                and all(isinstance(event, dict) for event in events)
            ):
                sources = {event.get("source_id") for event in events}
                if len(sources) == 1:
                    source_id = sources.pop()
        allowed = principal.get("kind") == "development" or (
            principal.get("kind") == "collector"
            and principal.get("tenant_id") == tenant_id
            and principal.get("principal_id") == principal_id
            and principal.get("source_id") == source_id
        )
        if allowed and all(
            isinstance(value, str) and value
            for value in (tenant_id, principal_id, source_id)
        ):
            return tenant_id, principal_id, source_id
        with COUNTER_LOCK:
            COUNTERS["auth_denied"] += 1
        self.send_json(403, {"error": "canonical authority forbidden"})
        return None

    def metrics(self) -> bytes:
        db = self.store.service_metrics()
        with COUNTER_LOCK:
            counters = dict(COUNTERS)
        lines = [
            "# HELP recall_http_requests_total HTTP requests handled.",
            "# TYPE recall_http_requests_total counter",
            f"recall_http_requests_total {counters['http_requests']}",
            "# HELP recall_http_errors_total HTTP responses with status 4xx or 5xx.",
            "# TYPE recall_http_errors_total counter",
            f"recall_http_errors_total {counters['http_errors']}",
            "# HELP recall_http_request_duration_seconds Request handling time without route or content labels.",
            "# TYPE recall_http_request_duration_seconds summary",
            f"recall_http_request_duration_seconds_count {counters['http_duration_count']}",
            f"recall_http_request_duration_seconds_sum {counters['http_duration_sum']:.9f}",
            "# HELP recall_auth_denied_total Requests rejected by authentication.",
            "# TYPE recall_auth_denied_total counter",
            f"recall_auth_denied_total {counters['auth_denied']}",
            "# HELP recall_ingest_commits_total Newly committed batches.",
            "# TYPE recall_ingest_commits_total counter",
            f"recall_ingest_commits_total {counters['ingest_commits']}",
            "# HELP recall_ingest_replays_total Idempotent batch replays.",
            "# TYPE recall_ingest_replays_total counter",
            f"recall_ingest_replays_total {counters['ingest_replays']}",
            f"recall_source_events {db['source_events']}",
            f"recall_dead_letters {db['dead_letters']}",
            f"recall_projection_lag {db['projection_lag']}",
            f"recall_source_freshness_seconds {db['source_freshness_seconds']}",
            f"recall_embedded_items {db['embedded_items']}",
            f"recall_embedding_lag {db['embedding_lag']}",
            "",
        ]
        return "\n".join(lines).encode()

    def handle_one_request(self) -> None:
        started = time.monotonic()
        with COUNTER_LOCK:
            COUNTERS["http_requests"] += 1
        try:
            super().handle_one_request()
        finally:
            with COUNTER_LOCK:
                COUNTERS["http_duration_count"] += 1
                COUNTERS["http_duration_sum"] += time.monotonic() - started

    @staticmethod
    def public_mcp_profile() -> bool:
        return os.environ.get("RECALL_HTTP_PROFILE") == "public-mcp"

    @staticmethod
    def public_edge_profile() -> bool:
        return os.environ.get("RECALL_HTTP_PROFILE") == "public-edge"

    @staticmethod
    def admin_web_enabled() -> bool:
        return os.environ.get("RECALL_ADMIN_WEB_ENABLED") == "1"

    def hide_non_public_route(self, method: str, path: str) -> bool:
        if not (self.public_mcp_profile() or self.public_edge_profile()):
            return False
        mcp_path = path == "/mcp" or MCP_BRAIN_PATH.fullmatch(path)
        resource = os.environ.get("RECALL_MCP_RESOURCE_URI", "").rstrip("/")
        metadata_path = self.is_protected_resource_metadata_path(path, resource)
        allowed = (
            method == "POST" and bool(mcp_path)
        ) or (
            method == "GET"
            and (
                bool(mcp_path)
                or path in {"/healthz", "/readyz"}
                or metadata_path
            )
        )
        if self.public_edge_profile():
            allowed = allowed or (method == "POST" and path == WEBHOOK_PATH)
        if self.admin_web_enabled():
            allowed = (
                allowed
                or path == "/admin"
                or path.startswith("/admin/")
                or bool(INVITATION_ONBOARDING.fullmatch(path))
                or bool(INVITATION_LOGIN.fullmatch(path))
            )
        if os.environ.get("RECALL_CANONICAL_INGEST_PUBLIC") == "1":
            allowed = allowed or (
                method == "POST"
                and path in {
                    "/v2/archive/objects",
                    "/v2/ingest/canonical",
                    "/v2/ingest/status",
                }
            )
        if allowed:
            return False
        self.send_json(404, {"error": "not found"})
        return True

    def admin_principal(self, *, csrf: bool = False) -> dict[str, object] | None:
        if self.control_plane is None:
            self.send_json(503, {"error": "control_plane_unavailable"})
            return None
        browser = admin_cookies(self.headers.get("Cookie"))
        session = browser.get(SESSION_COOKIE, "")
        csrf_value = self.headers.get("X-Recall-CSRF") if csrf else None
        if csrf and (not csrf_value or csrf_value != browser.get(CSRF_COOKIE)):
            self.send_json(403, {"error": "admin_csrf_invalid"})
            return None
        try:
            return self.control_plane.authenticate_session(
                session,
                csrf=csrf_value,
            )
        except ControlError as error:
            self.send_json(error.status, {"error": error.code})
            return None

    def read_admin_json(self) -> dict | None:
        length = self.body_length(MAX_ADMIN_BODY_BYTES)
        if length is None:
            return None
        try:
            body = json.loads(self.rfile.read(length))
        except (json.JSONDecodeError, UnicodeDecodeError):
            self.send_json(400, {"error": "admin_request_invalid"})
            return None
        if not isinstance(body, dict):
            self.send_json(400, {"error": "admin_request_invalid"})
            return None
        return body

    def do_GET(self) -> None:
        parsed = urlsplit(self.path)
        if self.hide_non_public_route("GET", parsed.path):
            return
        resource = os.environ.get("RECALL_MCP_RESOURCE_URI", "").rstrip("/")
        if self.is_protected_resource_metadata_path(parsed.path, resource):
            metadata_resource_path = parsed.path.removeprefix(
                "/.well-known/oauth-protected-resource"
            )
            brain_match = MCP_BRAIN_PATH.fullmatch(metadata_resource_path)
            self.send_json(
                200,
                self.protected_resource_metadata(
                    brain_match.group(1) if brain_match else None
                ),
            )
            return
        if self.admin_web_enabled():
            if parsed.path == "/admin/api/v1/auth-methods":
                self.send_json(
                    200,
                    {
                        "oauth": bool(
                            self.control_plane
                            and self.control_plane.identity_oauth_enabled
                        ),
                        "legacy_key": True,
                    },
                )
                return
            if parsed.path == "/admin/login":
                if self.control_plane is None:
                    self.send_json(404, {"error": "not found"})
                    return
                try:
                    started = self.control_plane.start_identity_login(
                        purpose="admin"
                    )
                    self.send_redirect(started["authorization_url"])
                except ControlError as error:
                    self.send_json(error.status, {"error": error.code})
                return
            invitation_login = INVITATION_LOGIN.fullmatch(parsed.path)
            if invitation_login:
                if self.control_plane is None:
                    self.send_json(404, {"error": "not found"})
                    return
                try:
                    started = self.control_plane.start_identity_login(
                        purpose="join",
                        invitation_id=invitation_login.group(1),
                    )
                    self.send_redirect(started["authorization_url"])
                except ControlError as error:
                    self.send_json(error.status, {"error": "not found"})
                return
            if parsed.path == "/admin/oauth/callback/identity":
                if self.control_plane is None:
                    self.send_json(404, {"error": "not found"})
                    return
                query = parse_qs(parsed.query, keep_blank_values=True)
                if (
                    query.get("error")
                    or set(query) != {"state", "code"}
                    or any(len(query.get(key, [])) != 1 for key in query)
                ):
                    self.send_json(400, {"error": "identity_oauth_callback_invalid"})
                    return
                try:
                    completed = self.control_plane.complete_identity_login(
                        state=query["state"][0], code=query["code"][0]
                    )
                    headers = []
                    browser = completed.get("browser")
                    if browser:
                        headers = [
                            ("Set-Cookie", value)
                            for value in admin_session_headers(
                                browser["session"], browser["csrf"]
                            )
                        ]
                    self.send_redirect(completed["redirect"], headers)
                except ControlError as error:
                    self.send_json(error.status, {"error": error.code})
                return
            invitation_onboarding = INVITATION_ONBOARDING.fullmatch(parsed.path)
            if invitation_onboarding:
                if self.control_plane is None:
                    self.send_json(404, {"error": "not found"})
                    return
                try:
                    invitation = self.control_plane.invitation_onboarding(
                        invitation_onboarding.group(1)
                    )
                    self.send_public_html(onboarding_page(invitation))
                except ControlError as error:
                    self.send_json(error.status, {"error": "not found"})
                return
            configured_asset = admin_asset(parsed.path)
            if configured_asset is not None:
                self.send_asset(*configured_asset)
                return
            if parsed.path == "/admin/api/v1/state":
                principal = self.admin_principal()
                if principal is None:
                    return
                try:
                    self.send_json(
                        200,
                        self.control_plane.state(principal["principal_id"]),
                    )
                except ControlError as error:
                    self.send_json(error.status, {"error": error.code})
                return
            if parsed.path == "/admin/oauth/callback/google":
                if self.control_plane is None:
                    self.send_json(503, {"error": "control_plane_unavailable"})
                    return
                query = parse_qs(parsed.query, keep_blank_values=True)
                if (
                    query.get("error")
                    or set(query) - {"state", "code", "scope"}
                    or len(query.get("state", [])) != 1
                    or len(query.get("code", [])) != 1
                    or len(query.get("scope", [])) > 1
                ):
                    self.send_json(400, {"error": "oauth_callback_invalid"})
                    return
                state = query.get("state", [""])[0]
                code = query.get("code", [""])[0]
                try:
                    self.control_plane.complete_oauth(
                        provider_id="google",
                        state=state,
                        code=code,
                    )
                    self.send_redirect("/admin?oauth=connected")
                except ControlError as error:
                    self.send_json(error.status, {"error": error.code})
                return
            if parsed.path == "/admin/oauth/callback/composio":
                if self.control_plane is None:
                    self.send_json(503, {"error": "control_plane_unavailable"})
                    return
                query = parse_qs(parsed.query, keep_blank_values=True)
                if set(query) != {"state", "status", "connected_account_id"} or any(
                    len(query.get(key, [])) != 1 for key in query
                ):
                    self.send_json(400, {"error": "oauth_callback_invalid"})
                    return
                try:
                    self.control_plane.complete_oauth(
                        provider_id="composio",
                        state=query["state"][0],
                        status=query["status"][0],
                        connected_account_id=query["connected_account_id"][0],
                    )
                    self.send_redirect("/admin?oauth=connected")
                except ControlError as error:
                    self.send_json(error.status, {"error": error.code})
                return
        brain_match = MCP_BRAIN_PATH.fullmatch(parsed.path)
        if parsed.path == "/mcp" or brain_match:
            if not self.valid_mcp_origin():
                return
            if not self.valid_mcp_accept(get=True):
                return
            if not self.require(
                "read",
                requested_tenant=brain_match.group(1) if brain_match else None,
            ):
                return
            self.send_empty(405, {"Allow": "POST"})
            return
        if parsed.path == "/healthz":
            self.send_json(200, {"status": "ok"})
            return
        if parsed.path == "/readyz":
            try:
                self.send_json(200, self.store.readiness())
            except Exception:
                self.send_json(503, {"status": "not_ready"})
            return
        if parsed.path == "/metrics":
            if not self.require("metrics"):
                return
            data = self.metrics()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        if parsed.path == "/v1/receipts/resolve":
            principal = self.require("read")
            if not principal:
                return
            receipt = parse_qs(parsed.query).get("receipt", [""])[0]
            try:
                result = self.store.resolve(
                    receipt,
                    authorized_source=principal.get("source_id"),
                )
            except ValueError as exc:
                self.send_json(400, {"error": str(exc)})
                return
            self.send_json(200 if result else 404, result or {"error": "not found"})
            return
        if parsed.path == "/v1/doctor":
            if not self.require("read"):
                return
            self.send_json(200, self.store.operational_health())
            return
        self.send_json(404, {"error": "not found"})

    def do_POST(self) -> None:
        path = urlsplit(self.path).path
        if self.hide_non_public_route("POST", path):
            return
        if self.admin_web_enabled() and path == "/admin/api/v1/session":
            if self.control_plane is None:
                self.send_json(503, {"error": "control_plane_unavailable"})
                return
            body = self.read_admin_json()
            if body is None:
                return
            if set(body) != {"token"}:
                self.send_json(400, {"error": "admin_request_invalid"})
                return
            try:
                browser = self.control_plane.exchange_admin_token(body["token"])
                headers = [
                    ("Set-Cookie", value)
                    for value in admin_session_headers(
                        browser["session"], browser["csrf"]
                    )
                ]
                self.send_json(
                    201,
                    {"status": "authenticated", "expires_at": browser["expires_at"]},
                    headers,
                )
            except ControlError as error:
                self.send_json(error.status, {"error": error.code})
            return
        if self.admin_web_enabled() and path == "/admin/api/v1/oauth/start":
            principal = self.admin_principal(csrf=True)
            if principal is None:
                return
            body = self.read_admin_json()
            if body is None:
                return
            if set(body) != {"provider", "routes"}:
                self.send_json(400, {"error": "admin_request_invalid"})
                return
            try:
                result = self.control_plane.start_oauth(
                    principal_id=principal["principal_id"],
                    provider_id=body["provider"],
                    routes=body["routes"],
                )
                self.send_json(201, result)
            except ControlError as error:
                self.send_json(error.status, {"error": error.code})
            return
        if self.admin_web_enabled() and path == "/admin/api/v1/invitations":
            principal = self.admin_principal(csrf=True)
            if principal is None:
                return
            body = self.read_admin_json()
            if body is None:
                return
            if set(body) not in (
                {"tenant_id", "email", "role"},
                {"tenant_id", "email", "display_name", "role"},
            ):
                self.send_json(400, {"error": "admin_request_invalid"})
                return
            try:
                result = self.control_plane.create_brain_invitation(
                    principal_id=principal["principal_id"],
                    tenant_id=body["tenant_id"],
                    email=body["email"],
                    display_name=body.get("display_name"),
                    role=body["role"],
                )
                self.send_json(201, result)
            except ControlError as error:
                self.send_json(error.status, {"error": error.code})
            return
        invitation_revoke = (
            ADMIN_INVITATION_REVOKE.fullmatch(path)
            if self.admin_web_enabled()
            else None
        )
        if invitation_revoke:
            principal = self.admin_principal(csrf=True)
            if principal is None:
                return
            body = self.read_admin_json()
            if body is None:
                return
            if body:
                self.send_json(400, {"error": "admin_request_invalid"})
                return
            try:
                result = self.control_plane.revoke_brain_invitation(
                    principal_id=principal["principal_id"],
                    invitation_id=invitation_revoke.group(1),
                )
                self.send_json(200, result)
            except ControlError as error:
                self.send_json(error.status, {"error": error.code})
            return
        actor_identity = (
            ADMIN_ACTOR_IDENTITY.fullmatch(path)
            if self.admin_web_enabled()
            else None
        )
        if actor_identity:
            principal = self.admin_principal(csrf=True)
            if principal is None:
                return
            body = self.read_admin_json()
            if body is None:
                return
            if set(body) != {
                "tenant_id", "connector_id", "namespace", "subject"
            }:
                self.send_json(400, {"error": "admin_request_invalid"})
                return
            try:
                result = self.control_plane.register_actor_identity(
                    principal_id=principal["principal_id"],
                    tenant_id=body["tenant_id"],
                    actor_id=actor_identity.group(1),
                    connector_id=body["connector_id"],
                    namespace=body["namespace"],
                    subject=body["subject"],
                )
                self.send_json(201, result)
            except ControlError as error:
                self.send_json(error.status, {"error": error.code})
            return
        if self.admin_web_enabled() and path == "/admin/api/v1/device/installations":
            principal = self.admin_principal(csrf=True)
            if principal is None:
                return
            body = self.read_admin_json()
            if body is None:
                return
            if set(body) != {
                "connector_id",
                "tenant_id",
                "device_id",
                "source_id",
                "privacy_mode",
                "selectors",
            }:
                self.send_json(400, {"error": "admin_request_invalid"})
                return
            try:
                result = self.control_plane.create_device_installation(
                    principal_id=principal["principal_id"],
                    connector_id=body["connector_id"],
                    tenant_id=body["tenant_id"],
                    device_id=body["device_id"],
                    source_id=body["source_id"],
                    privacy_mode=body["privacy_mode"],
                    selectors=body["selectors"],
                )
                self.send_json(201, result)
            except ControlError as error:
                self.send_json(error.status, {"error": error.code})
            return
        installation_action = (
            ADMIN_INSTALLATION_ACTION.fullmatch(path)
            if self.admin_web_enabled()
            else None
        )
        if installation_action:
            principal = self.admin_principal(csrf=True)
            if principal is None:
                return
            body = self.read_admin_json()
            if body is None:
                return
            if set(body) != {"action"}:
                self.send_json(400, {"error": "admin_request_invalid"})
                return
            try:
                result = self.control_plane.transition_installation(
                    principal_id=principal["principal_id"],
                    installation_id=installation_action.group(1),
                    action=body["action"],
                )
                self.send_json(200, result)
            except ControlError as error:
                self.send_json(error.status, {"error": error.code})
            return
        connection_revoke = (
            ADMIN_CONNECTION_REVOKE.fullmatch(path)
            if self.admin_web_enabled()
            else None
        )
        if connection_revoke:
            principal = self.admin_principal(csrf=True)
            if principal is None:
                return
            body = self.read_admin_json()
            if body is None:
                return
            if body:
                self.send_json(400, {"error": "admin_request_invalid"})
                return
            try:
                result = self.control_plane.revoke_connection(
                    principal_id=principal["principal_id"],
                    connection_id=connection_revoke.group(1),
                )
                self.send_json(200, result)
            except ControlError as error:
                self.send_json(error.status, {"error": error.code})
            return
        if path == "/v2/archive/objects":
            principal = self.require("write")
            if not principal:
                return
            length = self.body_length(MAX_BODY_BYTES)
            if length is None:
                return
            try:
                body = json.loads(self.rfile.read(length))
                authority = self.canonical_authority(principal, body)
                if authority is None:
                    return
                tenant_id, principal_id, source_id = authority
                if self.archive_store is None:
                    self.send_json(503, {"error": "canonical archive unavailable"})
                    return
                payload = base64.b64decode(
                    body.get("payload_base64", ""),
                    validate=True,
                )
                gateway = CanonicalArchiveGateway(
                    self.store,
                    self.archive_store,
                    tenant_id=tenant_id,
                    principal_id=principal_id,
                )
                reference = gateway.put_raw(
                    tenant_id=tenant_id,
                    source_id=source_id,
                    native_id=body.get("native_id"),
                    payload=payload,
                    media_type=body.get("media_type"),
                    created_at=body.get("created_at"),
                )
                self.send_json(201, reference)
            except (binascii.Error, json.JSONDecodeError, TypeError, ValueError):
                self.send_json(400, {"error": "canonical archive request invalid"})
            except CanonicalLifecycleError as exc:
                status = (
                    409
                    if exc.error_code == "archive_identity_forgotten"
                    else 403
                    if exc.error_code == "archive_authority_forbidden"
                    else 400
                )
                self.send_json(status, {"error": exc.error_code})
            except Exception as exc:
                LOG.error("canonical archive failed type=%s", type(exc).__name__)
                self.send_json(503, {"error": "canonical archive unavailable"})
            return
        if path == "/v2/ingest/canonical":
            principal = self.require("write")
            if not principal:
                return
            length = self.body_length(MAX_BODY_BYTES)
            if length is None:
                return
            try:
                body = json.loads(self.rfile.read(length))
                authority = self.canonical_authority(principal, body)
                if authority is None:
                    return
                tenant_id, principal_id, source_id = authority
                events = body.get("events")
                encoded_events = body.get("events_base64")
                if encoded_events is not None:
                    if events is not None or not isinstance(encoded_events, str):
                        self.send_json(
                            400,
                            {"error": "canonical ingest request invalid"},
                        )
                        return
                    decoded_events = base64.b64decode(
                        encoded_events,
                        validate=True,
                    )
                    if len(decoded_events) > MAX_CANONICAL_EVENTS_BYTES:
                        self.send_json(413, {"error": "request body too large"})
                        return
                    events = json.loads(decoded_events)
                if not isinstance(events, list) or any(
                    not isinstance(event, dict) or event.get("source_id") != source_id
                    for event in events
                ):
                    self.send_json(403, {"error": "canonical authority forbidden"})
                    return
                if self.canonical_plane is None:
                    self.send_json(503, {"error": "canonical plane unavailable"})
                    return
                acknowledgement = self.canonical_plane.ingest_batch(
                    tenant_id=tenant_id,
                    principal_id=principal_id,
                    events=events,
                )
                with COUNTER_LOCK:
                    COUNTERS[
                        "ingest_replays"
                        if acknowledgement["replay"]
                        else "ingest_commits"
                    ] += 1
                self.send_json(
                    200 if acknowledgement["replay"] else 201,
                    acknowledgement,
                )
            except (binascii.Error, json.JSONDecodeError, TypeError, ValueError):
                self.send_json(400, {"error": "canonical ingest request invalid"})
            except CanonicalLifecycleError as exc:
                status = (
                    409
                    if exc.error_code == "canonical_identity_forgotten"
                    else 403
                    if exc.error_code
                    in {"canonical_authority_forbidden", "canonical_lineage_invalid"}
                    else 400
                )
                self.send_json(status, {"error": exc.error_code})
            except Exception as exc:
                LOG.error("canonical ingest failed type=%s", type(exc).__name__)
                self.send_json(500, {"error": "canonical ingest failed"})
            return
        if path == "/v2/ingest/status":
            principal = self.require("write")
            if not principal:
                return
            length = self.body_length(MAX_BODY_BYTES)
            if length is None:
                return
            try:
                body = json.loads(self.rfile.read(length))
                authority = self.canonical_authority(principal, body)
                if authority is None:
                    return
                tenant_id, principal_id, source_id = authority
                if self.canonical_plane is None:
                    self.send_json(503, {"error": "canonical plane unavailable"})
                    return
                self.send_json(
                    200,
                    self.canonical_plane.source_status(
                        tenant_id=tenant_id,
                        principal_id=principal_id,
                        source_id=source_id,
                    ),
                )
            except (json.JSONDecodeError, TypeError, ValueError):
                self.send_json(400, {"error": "canonical status request invalid"})
            except CanonicalLifecycleError as exc:
                status = (
                    403
                    if exc.error_code == "canonical_authority_forbidden"
                    else 400
                )
                self.send_json(status, {"error": exc.error_code})
            except Exception as exc:
                LOG.error("canonical status failed type=%s", type(exc).__name__)
                self.send_json(503, {"error": "canonical status unavailable"})
            return
        if path == WEBHOOK_PATH:
            principal = self.require("webhook")
            if not principal:
                return
            content_type = (
                self.headers.get("Content-Type", "").split(";", 1)[0].strip().casefold()
            )
            if content_type != "application/json":
                self.send_json(415, {"error": "unsupported content type"})
                return
            length = self.body_length(256 * 1024)
            if length is None:
                return
            try:
                body = json.loads(self.rfile.read(length))
                prepared = build_webhook_event(body, principal)
                if prepared.event is None:
                    self.send_json(
                        202,
                        {
                            "status": "privacy_filtered",
                            "receipt": None,
                            "replay": False,
                            "privacy": prepared.privacy,
                        },
                    )
                    return
                acknowledgement, replay = self.store.ingest(
                    prepared.idempotency_key,
                    [prepared.event],
                )
                receipts = acknowledgement.get("receipts")
                if not isinstance(receipts, list) or len(receipts) != 1:
                    raise RuntimeError("webhook acknowledgement invalid")
                self.send_json(
                    200 if replay else 201,
                    {
                        "status": acknowledgement.get("status", "committed"),
                        "receipt": receipts[0],
                        "replay": replay,
                        "privacy": prepared.privacy,
                    },
                )
            except (json.JSONDecodeError, WebhookError):
                self.send_json(400, {"error": "invalid webhook"})
            except IdempotencyConflict:
                self.send_json(409, {"error": "webhook conflict"})
            except Exception as exc:
                LOG.error("webhook failed type=%s", type(exc).__name__)
                self.send_json(500, {"error": "webhook failed"})
            return
        brain_match = MCP_BRAIN_PATH.fullmatch(path)
        if path == "/mcp" or brain_match:
            if not self.valid_mcp_origin():
                return
            if not self.valid_mcp_accept():
                return
            if not self.valid_mcp_protocol():
                return
            principal = self.require(
                "read",
                requested_tenant=brain_match.group(1) if brain_match else None,
            )
            if not principal:
                return
            content_type = (
                self.headers.get("Content-Type", "").split(";", 1)[0].strip().casefold()
            )
            if content_type != "application/json":
                self.send_json(
                    415,
                    mcp_error_response(
                        McpProtocolError(
                            -32700, "content type must be application/json"
                        )
                    ),
                )
                return
            length = self.body_length(256 * 1024)
            if length is None:
                return
            request_id = None
            try:
                body = json.loads(self.rfile.read(length))
                if isinstance(body, dict):
                    request_id = body.get("id")
                protocol_version = (
                    self.headers.get("MCP-Protocol-Version") or "2025-03-26"
                )
                if not self.valid_mcp_routing_headers(body, protocol_version):
                    return
                mcp_store = self.store
                if os.environ.get("RECALL_CANONICAL_MCP_ENABLED") == "1":
                    if self.canonical_retrieval is None:
                        raise RuntimeError("canonical retrieval unavailable")
                    mcp_store = self.canonical_retrieval.bind(principal)
                response = dispatch_mcp(
                    mcp_store,
                    principal,
                    body,
                    authorize=lambda action: self.authorize_mcp(principal, action),
                    protocol_version=protocol_version,
                )
            except json.JSONDecodeError:
                self.send_json(
                    400,
                    mcp_error_response(McpProtocolError(-32700, "invalid JSON")),
                )
                return
            except McpProtocolError as exc:
                status = (
                    404
                    if protocol_version == LATEST_PROTOCOL_VERSION
                    and exc.code == -32601
                    else 400
                    if protocol_version == LATEST_PROTOCOL_VERSION
                    and exc.code in {-32020, -32003}
                    else 200
                )
                self.send_json(status, mcp_error_response(exc, request_id))
                return
            except (ValueError, TypeError):
                self.send_json(
                    200,
                    mcp_error_response(
                        McpProtocolError(-32602, "tool arguments rejected"),
                        request_id,
                    ),
                )
                return
            except DeepInspectionError as exc:
                LOG.error("mcp deep inspection failed code=%s", exc.code)
                self.send_json(
                    200,
                    mcp_error_response(
                        McpProtocolError(-32603, "tool execution failed"),
                        request_id,
                    ),
                )
                return
            except Exception as exc:
                LOG.error("mcp tool failed type=%s", type(exc).__name__)
                self.send_json(
                    200,
                    mcp_error_response(
                        McpProtocolError(-32603, "tool execution failed"),
                        request_id,
                    ),
                )
                return
            if response is None:
                self.send_empty(202)
            else:
                self.send_json(200, bound_mcp_response(response, request_id))
            return
        if path in {"/v1/search", "/v1/show", "/v1/related", "/v1/session-export"}:
            principal = self.require("read")
            if not principal:
                return
            authorized_source = principal.get("source_id")
            length = self.body_length(256 * 1024)
            if length is None:
                return
            try:
                body = json.loads(self.rfile.read(length))
                if path == "/v1/search":
                    result = self.store.search(
                        body.get("query"),
                        body.get("filters", {}),
                        body.get("limit", 10),
                        authorized_source,
                    )
                    timing = result.get("diagnostics", {})
                    LOG.info(
                        "search timing elapsed_ms=%s deadline_ms=%s deadline_exceeded=%s legs=%s",
                        timing.get("elapsed_ms"),
                        timing.get("deadline_ms"),
                        timing.get("deadline_exceeded"),
                        ",".join(
                            str(item.get("leg")) for item in timing.get("legs", [])
                        ),
                    )
                elif path == "/v1/show":
                    result = self.store.show(
                        body.get("target", ""),
                        around=body.get("around"),
                        tail=body.get("tail", 0),
                        prompts=bool(body.get("prompts", False)),
                        authorized_source=authorized_source,
                    )
                    if result is None:
                        self.send_json(404, {"error": "not found"})
                        return
                elif path == "/v1/related":
                    result = self.store.related(
                        cwd=body.get("cwd"),
                        branch=body.get("branch"),
                        limit=body.get("limit", 10),
                        mains_only=bool(body.get("mains_only", False)),
                        fast=bool(body.get("fast", False)),
                        authorized_source=authorized_source,
                    )
                else:
                    result = self.store.session_export(
                        target=body.get("target"),
                        cursor=body.get("cursor"),
                        limit=body.get("limit", 1000),
                        authorized_source=authorized_source,
                    )
                    if result is None:
                        self.send_json(404, {"error": "not found"})
                        return
                self.send_json(200, result)
            except (ValueError, TypeError, json.JSONDecodeError) as exc:
                self.send_json(400, {"error": str(exc)})
            return
        if path != "/v1/ingest/batches":
            self.send_json(404, {"error": "not found"})
            return
        principal = self.require("write")
        if not principal:
            return
        length = self.body_length(MAX_BODY_BYTES)
        if length is None:
            return
        try:
            body = json.loads(self.rfile.read(length))
            if principal.get("kind") == "collector" and principal.get("source_id"):
                if any(
                    event.get("source_id") != principal["source_id"]
                    for event in body["events"]
                ):
                    with COUNTER_LOCK:
                        COUNTERS["auth_denied"] += 1
                    self.send_json(403, {"error": "collector source scope mismatch"})
                    return
            ack, replay = self.store.ingest(
                self.headers.get("Idempotency-Key", ""), body["events"]
            )
            with COUNTER_LOCK:
                COUNTERS["ingest_replays" if replay else "ingest_commits"] += 1
            self.send_json(200 if replay else 201, {**ack, "replay": replay})
        except IdempotencyConflict as exc:
            self.send_json(409, {"error": str(exc)})
        except (ValueError, KeyError, json.JSONDecodeError) as exc:
            LOG.warning("ingest rejected type=%s", type(exc).__name__)
            self.store.record_dead_letter(type(exc).__name__, str(exc))
            self.send_json(400, {"error": str(exc)})
        except BrokenPipeError:
            # Commit may already be durable; replaying the idempotency key returns its ack.
            return
        except Exception as exc:
            # Never let driver exceptions render payload excerpts through socketserver tracebacks.
            LOG.error("ingest failed type=%s", type(exc).__name__)
            try:
                self.store.record_dead_letter(
                    type(exc).__name__, "database rejected ingest"
                )
            except Exception:
                LOG.error("dead-letter write failed after ingest error")
            self.send_json(500, {"error": "ingest failed"})

    def do_DELETE(self) -> None:
        self.send_json(404, {"error": "not found"})

    def do_PATCH(self) -> None:
        self.send_json(404, {"error": "not found"})

    def do_PUT(self) -> None:
        self.send_json(404, {"error": "not found"})

    def do_OPTIONS(self) -> None:
        self.send_json(404, {"error": "not found"})

    def do_HEAD(self) -> None:
        self.send_empty(404)


def validate_http_profile() -> None:
    profile = os.environ.get("RECALL_HTTP_PROFILE", "")
    if profile not in {"", "public-edge", "public-mcp"}:
        raise RuntimeError("unsupported HTTP profile")
    if os.environ.get("RECALL_CANONICAL_INGEST_PUBLIC") == "1" and (
        os.environ.get("RECALL_AUTH_REQUIRED", "0") != "1"
        or os.environ.get("RECALL_CANONICAL_V2_ENABLED") != "1"
    ):
        raise RuntimeError(
            "public canonical ingest requires authentication and canonical v2"
        )
    if os.environ.get("RECALL_CANONICAL_MCP_ENABLED") == "1" and (
        os.environ.get("RECALL_AUTH_REQUIRED", "0") != "1"
        or os.environ.get("RECALL_CANONICAL_V2_ENABLED") != "1"
    ):
        raise RuntimeError("canonical MCP requires authentication and canonical v2")
    if os.environ.get("RECALL_ADMIN_WEB_ENABLED") == "1" and (
        os.environ.get("RECALL_AUTH_REQUIRED", "0") != "1"
        or not os.environ.get("RECALL_CONTROL_ENCRYPTION_KEY")
    ):
        raise RuntimeError("admin web requires auth and encryption configuration")
    google_values = (
        os.environ.get("RECALL_GOOGLE_CLIENT_ID", ""),
        os.environ.get("RECALL_GOOGLE_CLIENT_SECRET", ""),
        os.environ.get("RECALL_GOOGLE_REDIRECT_URI", ""),
    )
    if any(google_values) and not all(google_values):
        raise RuntimeError("Google OAuth configuration must be complete")
    composio_values = (
        os.environ.get("RECALL_COMPOSIO_API_KEY", ""),
        os.environ.get("RECALL_COMPOSIO_REDIRECT_URI", ""),
    )
    if any(composio_values) and not all(composio_values):
        raise RuntimeError("Composio configuration must be complete")
    if profile in {"public-edge", "public-mcp"}:
        if os.environ.get("RECALL_AUTH_REQUIRED", "0") != "1":
            raise RuntimeError("public HTTP profile requires authentication")
        if os.environ.get("RECALL_TRUST_TAILSCALE_HEADERS", "0") == "1":
            raise RuntimeError("public HTTP profile forbids trusted identity headers")
    if profile == "public-mcp":
        resource = os.environ.get("RECALL_MCP_RESOURCE_URI", "")
        authorization_servers = os.environ.get(
            "RECALL_AUTHORIZATION_SERVERS", ""
        )
        auth_provider = os.environ.get("RECALL_MCP_AUTH_PROVIDER", "").strip()
        oauth_configured = bool(resource or authorization_servers or auth_provider)
        if oauth_configured:
            parsed_resource = urlsplit(resource)
            if (
                parsed_resource.scheme != "https"
                or not parsed_resource.hostname
                or parsed_resource.username
                or parsed_resource.password
                or parsed_resource.query
                or parsed_resource.fragment
                or parsed_resource.path not in {"", "/mcp"}
            ):
                raise RuntimeError(
                    "OAuth MCP requires an exact HTTPS resource URI"
                )
        for server in authorization_servers.split(","):
            if not server.strip():
                continue
            parsed_server = urlsplit(server.strip())
            if (
                parsed_server.scheme != "https"
                or not parsed_server.hostname
                or parsed_server.username
                or parsed_server.password
                or parsed_server.query
                or parsed_server.fragment
            ):
                raise RuntimeError("authorization server URI is invalid")


def configure_runtime(dsn: str) -> None:
    Handler.store = BrainStore(dsn, semantic_runtime=SemanticRuntime.from_env())
    Handler.external_identity_verifier = OidcJwtVerifier.from_env()
    Handler.control_plane = (
        ControlPlane.from_env(Handler.store)
        if os.environ.get("RECALL_ADMIN_WEB_ENABLED") == "1"
        else None
    )
    actor_identity_index = (
        ActorIdentityIndex(Handler.control_plane.secret_box.blind_index)
        if Handler.control_plane is not None
        else None
    )
    if os.environ.get("RECALL_CANONICAL_V2_ENABLED") == "1":
        Handler.archive_store = build_archive_store()
        if os.environ.get("RECALL_EVIDENCE_ENABLED") == "1":
            evidence_tenant_id = os.environ.get("RECALL_EVIDENCE_TENANT_ID", "").strip()
            if not evidence_tenant_id:
                raise RuntimeError(
                    "RECALL_EVIDENCE_TENANT_ID is required when evidence is enabled"
                )
            Handler.evidence_archive_store = build_evidence_archive_store()
            projection = EvidenceProjectionStore(Handler.evidence_archive_store)
            Handler.evidence_projector = CanonicalEvidenceProjector(
                Handler.store,
                projection,
                bound_tenant_id=evidence_tenant_id,
            )
            Handler.deep_inspector = build_deep_inspector(projection)
        else:
            Handler.evidence_archive_store = None
            Handler.evidence_projector = None
            Handler.deep_inspector = None
        Handler.canonical_plane = CanonicalPlane(
            Handler.store,
            Handler.archive_store,
            Handler.evidence_projector,
            actor_identity_index,
        )
        Handler.canonical_retrieval = (
            CanonicalRetrieval(
                Handler.store,
                Handler.archive_store,
                evidence_projector=Handler.evidence_projector,
                deep_inspector=Handler.deep_inspector,
            )
            if os.environ.get("RECALL_CANONICAL_MCP_ENABLED") == "1"
            else None
        )
    else:
        Handler.archive_store = None
        Handler.evidence_archive_store = None
        Handler.evidence_projector = None
        Handler.deep_inspector = None
        Handler.canonical_plane = None
        Handler.canonical_retrieval = None


def serve(dsn: str, host: str = "127.0.0.1", port: int = 8788) -> None:
    validate_http_profile()
    if os.environ.get("RECALL_AUTH_REQUIRED", "0") != "1" and host not in {
        "127.0.0.1",
        "localhost",
        "::1",
    }:
        raise RuntimeError("authentication is required for a non-loopback TCP bind")
    configure_runtime(dsn)
    server = ThreadingHTTPServer((host, port), Handler)
    server.daemon_threads = True
    LOG.info("brainstore listening host=%s port=%s", host, port)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        Handler.store.close()


class ThreadingUnixHTTPServer(
    socketserver.ThreadingMixIn, socketserver.UnixStreamServer
):
    daemon_threads = True
    is_unix_socket = True


def serve_unix(dsn: str, path: str) -> None:
    validate_http_profile()
    try:
        existing = os.lstat(path)
    except FileNotFoundError:
        pass
    else:
        if not stat.S_ISSOCK(existing.st_mode):
            raise RuntimeError("refusing to replace a non-socket Unix path")
        os.unlink(path)
    configure_runtime(dsn)
    server = ThreadingUnixHTTPServer(path, Handler)
    os.chmod(path, 0o600)
    LOG.info("brainstore listening unix_socket=%s", path)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        Handler.store.close()
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"), format="%(levelname)s %(message)s"
    )
    if os.environ.get("RECALL_UNIX_SOCKET"):
        serve_unix(os.environ["RECALL_DATABASE_URL"], os.environ["RECALL_UNIX_SOCKET"])
    else:
        serve(
            os.environ["RECALL_DATABASE_URL"],
            os.environ.get("RECALL_HOST", "127.0.0.1"),
            int(os.environ.get("RECALL_PORT", "8788")),
        )
