"""Shared, tenant-aware connector control plane for web and native clients."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol
from urllib.parse import urlencode, urlsplit
import urllib.error
import urllib.request

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from .authorization import normalize_verified_email
from .invitation_email import (
    InvitationEmail,
    InvitationEmailError,
    InvitationEmailSender,
    invitation_urls,
    sender_from_env,
)
from connectors.composio_workspace_rail import (
    ComposioWorkspaceRail,
    toolkit_for_connector,
)
from connectors.workspace_rail import WorkspaceRailError
from connectors.registry import (
    ConnectorDefinitionV3,
    ConnectorRegistryError,
    REGISTRY,
    definition,
)


ADMIN_TOKEN_RE = re.compile(r"rca_[A-Za-z0-9_-]{32,128}\Z")
SESSION_TOKEN_RE = re.compile(r"rcs_[A-Za-z0-9_-]{32,128}\Z")
STATE_RE = re.compile(r"[A-Za-z0-9_-]{32,256}\Z")
AUTHORITY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9:._/@+-]{1,255}\Z")
DEVICE_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{2,63}\Z")
GOOGLE_CONNECTORS = frozenset(
    {"google.gmail", "google.calendar", "google.contacts", "google.drive"}
)
GOOGLE_SCOPE_SUPERSETS = {
    "https://www.googleapis.com/auth/gmail.readonly": frozenset(
        {
            "https://mail.google.com/",
            "https://www.googleapis.com/auth/gmail.modify",
        }
    ),
    "https://www.googleapis.com/auth/calendar.readonly": frozenset(
        {"https://www.googleapis.com/auth/calendar"}
    ),
    "https://www.googleapis.com/auth/contacts.readonly": frozenset(
        {"https://www.googleapis.com/auth/contacts"}
    ),
    "https://www.googleapis.com/auth/drive.readonly": frozenset(
        {"https://www.googleapis.com/auth/drive"}
    ),
}
INSTALLATION_STATES = frozenset(
    {"configured", "enabled", "paused", "revoked", "uninstalled"}
)
TRANSITIONS = {
    "enable": ({"configured", "paused"}, "enabled"),
    "pause": ({"enabled"}, "paused"),
    "resume": ({"paused"}, "enabled"),
    "revoke": ({"configured", "enabled", "paused"}, "revoked"),
    "uninstall": ({"configured", "enabled", "paused", "revoked"}, "uninstalled"),
}
COMPOSIO_PROBE_RETRY_DELAYS = (1.0, 2.0, 4.0, 8.0, 15.0, 15.0)
COMPOSIO_TRANSIENT_PROBE_ERRORS = frozenset(
    {"authority_forbidden", "transport_unavailable", "upstream_error"}
)
LOG = logging.getLogger("recall.control")


class ControlError(RuntimeError):
    """Closed, content-free control-plane failure."""

    def __init__(self, code: str, status: int = 400):
        self.code = code
        self.status = status
        super().__init__(code)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _scopes_cover(
    required: set[str] | tuple[str, ...],
    granted: set[str] | tuple[str, ...],
) -> bool:
    observed = set(granted)
    return all(
        scope in observed or bool(GOOGLE_SCOPE_SUPERSETS.get(scope, ()) & observed)
        for scope in required
    )


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _selector(value: Any) -> Any:
    if value is None or type(value) is bool:
        return value
    if type(value) is int and -(2**31) <= value <= 2**31 - 1:
        return value
    if isinstance(value, str) and value and "\x00" not in value:
        if len(value.encode()) <= 4096:
            return value
    if isinstance(value, list) and len(value) <= 128:
        copied = [_selector(item) for item in value]
        if (
            all(isinstance(item, str) for item in copied)
            and copied == sorted(copied)
            and len(copied) == len(set(copied))
        ):
            return copied
    raise ControlError("oauth_selectors_invalid")


def _b64decode_key(value: str) -> bytes:
    try:
        raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    except (ValueError, TypeError):
        raise ControlError("control_encryption_key_invalid", 500) from None
    if len(raw) != 32:
        raise ControlError("control_encryption_key_invalid", 500)
    return raw


class SecretBox:
    """Versioned AES-256-GCM envelope with purpose-bound associated data."""

    def __init__(self, key: bytes):
        if not isinstance(key, bytes) or len(key) != 32:
            raise ControlError("control_encryption_key_invalid", 500)
        self._key = key
        self.key_id = hashlib.sha256(key).hexdigest()[:16]

    @classmethod
    def from_env(cls) -> "SecretBox":
        value = os.environ.get("RECALL_CONTROL_ENCRYPTION_KEY", "")
        if not value:
            raise ControlError("control_encryption_key_missing", 500)
        return cls(_b64decode_key(value))

    def seal(self, value: dict[str, Any], *, purpose: str) -> bytes:
        try:
            plaintext = json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode()
        except (TypeError, ValueError):
            raise ControlError("control_secret_invalid") from None
        if len(plaintext) > 64 * 1024:
            raise ControlError("control_secret_too_large")
        nonce = os.urandom(12)
        ciphertext = AESGCM(self._key).encrypt(nonce, plaintext, purpose.encode())
        return b"\x01" + nonce + ciphertext

    def blind_index(self, value: str, *, purpose: str) -> str:
        if not isinstance(value, str) or not value or not isinstance(purpose, str):
            raise ControlError("control_secret_invalid")
        return hmac.new(
            self._key,
            f"{purpose}\0{value}".encode(),
            hashlib.sha256,
        ).hexdigest()

    def open(self, envelope: bytes, *, purpose: str) -> dict[str, Any]:
        if (
            not isinstance(envelope, bytes)
            or len(envelope) < 30
            or envelope[:1] != b"\x01"
        ):
            raise ControlError("control_secret_invalid", 500)
        try:
            plaintext = AESGCM(self._key).decrypt(
                envelope[1:13], envelope[13:], purpose.encode()
            )
            value = json.loads(plaintext)
        except Exception:
            raise ControlError("control_secret_invalid", 500) from None
        if not isinstance(value, dict):
            raise ControlError("control_secret_invalid", 500)
        return value


@dataclass(frozen=True)
class OAuthTokens:
    subject_id: str
    granted_scopes: tuple[str, ...]
    credentials: dict[str, Any]
    expires_at: datetime | None


@dataclass(frozen=True)
class ManagedOAuthStart:
    authorization_url: str
    connected_account_id: str


class OAuthProvider(Protocol):
    provider_id: str

    def authorization_url(
        self,
        *,
        state: str,
        code_challenge: str,
        scopes: tuple[str, ...],
    ) -> str: ...

    def exchange(self, *, code: str, code_verifier: str) -> OAuthTokens: ...

    def revoke(self, credentials: dict[str, Any]) -> None: ...


def _https_url(value: str, *, label: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.fragment
    ):
        raise ControlError(f"{label}_invalid", 500)
    return value


class GoogleOAuthProvider:
    provider_id = "google"

    def __init__(
        self,
        *,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
        authorization_endpoint: str = "https://accounts.google.com/o/oauth2/v2/auth",
        token_endpoint: str = "https://oauth2.googleapis.com/token",
        userinfo_endpoint: str = "https://openidconnect.googleapis.com/v1/userinfo",
        revoke_endpoint: str = "https://oauth2.googleapis.com/revoke",
    ):
        if (
            not client_id
            or not client_secret
            or len(client_id) > 512
            or len(client_secret) > 4096
        ):
            raise ControlError("google_oauth_configuration_invalid", 500)
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = _https_url(redirect_uri, label="google_redirect_uri")
        self.authorization_endpoint = _https_url(
            authorization_endpoint, label="google_authorization_endpoint"
        )
        self.token_endpoint = _https_url(token_endpoint, label="google_token_endpoint")
        self.userinfo_endpoint = _https_url(
            userinfo_endpoint, label="google_userinfo_endpoint"
        )
        self.revoke_endpoint = _https_url(
            revoke_endpoint, label="google_revoke_endpoint"
        )

    @classmethod
    def from_env(cls) -> "GoogleOAuthProvider":
        return cls(
            client_id=os.environ.get("RECALL_GOOGLE_CLIENT_ID", ""),
            client_secret=os.environ.get("RECALL_GOOGLE_CLIENT_SECRET", ""),
            redirect_uri=os.environ.get("RECALL_GOOGLE_REDIRECT_URI", ""),
        )

    @staticmethod
    def _opener() -> urllib.request.OpenerDirector:
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, req, fp, code, msg, headers, newurl):
                return None

        return urllib.request.build_opener(NoRedirect())

    @staticmethod
    def _json(response: Any) -> dict[str, Any]:
        raw = response.read(256 * 1024 + 1)
        if len(raw) > 256 * 1024:
            raise ControlError("google_oauth_upstream_invalid", 502)
        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ControlError("google_oauth_upstream_invalid", 502) from None
        if not isinstance(value, dict):
            raise ControlError("google_oauth_upstream_invalid", 502)
        return value

    @staticmethod
    def _post(url: str, fields: dict[str, str]) -> dict[str, Any]:
        request = urllib.request.Request(
            url,
            data=urlencode(fields).encode(),
            method="POST",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        try:
            with GoogleOAuthProvider._opener().open(request, timeout=20) as response:
                value = GoogleOAuthProvider._json(response)
        except (
            OSError,
            ValueError,
            urllib.error.HTTPError,
            urllib.error.URLError,
        ):
            raise ControlError("google_oauth_upstream_failed", 502) from None
        return value

    def authorization_url(
        self,
        *,
        state: str,
        code_challenge: str,
        scopes: tuple[str, ...],
    ) -> str:
        query = urlencode(
            {
                "client_id": self.client_id,
                "redirect_uri": self.redirect_uri,
                "response_type": "code",
                "scope": " ".join(scopes),
                "state": state,
                "access_type": "offline",
                "include_granted_scopes": "true",
                "prompt": "consent",
                "code_challenge": code_challenge,
                "code_challenge_method": "S256",
            }
        )
        return self.authorization_endpoint + "?" + query

    def exchange(self, *, code: str, code_verifier: str) -> OAuthTokens:
        if not code or len(code) > 4096:
            raise ControlError("oauth_code_invalid")
        token = self._post(
            self.token_endpoint,
            {
                "code": code,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "redirect_uri": self.redirect_uri,
                "grant_type": "authorization_code",
                "code_verifier": code_verifier,
            },
        )
        access_token = token.get("access_token")
        refresh_token = token.get("refresh_token")
        expires_in = token.get("expires_in")
        scope = token.get("scope", "")
        if (
            not isinstance(access_token, str)
            or not access_token
            or len(access_token) > 8192
            or not isinstance(refresh_token, str)
            or not refresh_token
            or len(refresh_token) > 8192
            or type(expires_in) is not int
            or not 1 <= expires_in <= 604800
            or not isinstance(scope, str)
        ):
            raise ControlError("google_oauth_token_invalid", 502)
        request = urllib.request.Request(
            self.userinfo_endpoint,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
            },
        )
        try:
            with self._opener().open(request, timeout=20) as response:
                userinfo = self._json(response)
        except (
            OSError,
            ValueError,
            urllib.error.HTTPError,
            urllib.error.URLError,
        ):
            raise ControlError("google_oauth_upstream_failed", 502) from None
        subject_id = userinfo.get("sub") if isinstance(userinfo, dict) else None
        if not isinstance(subject_id, str) or not 1 <= len(subject_id) <= 256:
            raise ControlError("google_oauth_subject_invalid", 502)
        granted = tuple(sorted(set(scope.split())))
        if (
            not granted
            or len(granted) > 64
            or any(not 1 <= len(value) <= 512 for value in granted)
        ):
            raise ControlError("google_oauth_scope_invalid", 502)
        credentials = {
            "type": "authorized_user",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "token_uri": self.token_endpoint,
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": token.get("token_type", "Bearer"),
        }
        return OAuthTokens(
            subject_id=subject_id,
            granted_scopes=granted,
            credentials=credentials,
            expires_at=_now() + timedelta(seconds=expires_in),
        )

    def revoke(self, credentials: dict[str, Any]) -> None:
        token = credentials.get("refresh_token") or credentials.get("access_token")
        if not isinstance(token, str) or not token:
            return
        request = urllib.request.Request(
            self.revoke_endpoint,
            data=urlencode({"token": token}).encode(),
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        try:
            with self._opener().open(request, timeout=20):
                return
        except (
            OSError,
            urllib.error.HTTPError,
            urllib.error.URLError,
        ):
            raise ControlError("google_oauth_revoke_failed", 502) from None


class ComposioConnectionBroker:
    """Hosted connect-link broker; provider credentials never enter Recall."""

    provider_id = "composio"
    managed_connection = True
    _PROBES = {
        "google.gmail": ("gmail.users.getProfile", {"userId": "me"}),
        "google.calendar": (
            "calendar.calendarList.list",
            {"maxResults": 1},
        ),
        "google.contacts": (
            "people.people.connections.list",
            {
                "resourceName": "people/me",
                "pageSize": 1,
                "personFields": "metadata",
            },
        ),
        "google.drive": (
            "drive.changes.getStartPageToken",
            {"supportsAllDrives": True},
        ),
    }

    def __init__(
        self,
        *,
        api_key: str,
        redirect_uri: str,
        auth_configs: Mapping[str, str] | None = None,
        client_factory: Any | None = None,
        rail_factory: Any | None = None,
    ):
        if (
            not isinstance(api_key, str)
            or not 10 <= len(api_key) <= 4096
            or any(character.isspace() or ord(character) < 33 for character in api_key)
        ):
            raise ControlError("composio_configuration_invalid", 500)
        redirect_uri = _https_url(redirect_uri, label="composio_redirect_uri")
        parsed = urlsplit(redirect_uri)
        if parsed.query or parsed.path != "/admin/oauth/callback/composio":
            raise ControlError("composio_redirect_uri_invalid", 500)
        normalized_configs = dict(auth_configs or {})
        if any(
            toolkit
            not in {
                "gmail",
                "googlecalendar",
                "googlecontacts",
                "googledrive",
            }
            or not isinstance(config_id, str)
            or not re.fullmatch(r"ac_[A-Za-z0-9_-]{3,252}", config_id)
            for toolkit, config_id in normalized_configs.items()
        ):
            raise ControlError("composio_configuration_invalid", 500)
        self.api_key = api_key
        self.redirect_uri = redirect_uri
        self.auth_configs = normalized_configs
        self.client_factory = client_factory
        self.rail_factory = rail_factory or ComposioWorkspaceRail
        self._client_value = None

    @classmethod
    def from_env(cls) -> "ComposioConnectionBroker":
        configs = {}
        for toolkit, variable in {
            "gmail": "RECALL_COMPOSIO_AUTH_CONFIG_GMAIL",
            "googlecalendar": "RECALL_COMPOSIO_AUTH_CONFIG_GOOGLECALENDAR",
            "googlecontacts": "RECALL_COMPOSIO_AUTH_CONFIG_GOOGLECONTACTS",
            "googledrive": "RECALL_COMPOSIO_AUTH_CONFIG_GOOGLEDRIVE",
        }.items():
            value = os.environ.get(variable, "")
            if value:
                configs[toolkit] = value
        return cls(
            api_key=os.environ.get("RECALL_COMPOSIO_API_KEY", ""),
            redirect_uri=os.environ.get("RECALL_COMPOSIO_REDIRECT_URI", ""),
            auth_configs=configs,
        )

    def _client(self):
        if self._client_value is None:
            try:
                if self.client_factory is not None:
                    self._client_value = self.client_factory(self.api_key)
                else:
                    from composio import Composio

                    self._client_value = Composio(api_key=self.api_key, timeout=60.0)
            except Exception:
                raise ControlError("composio_upstream_failed", 502) from None
        return self._client_value

    @staticmethod
    def _field(value: Any, name: str) -> Any:
        if isinstance(value, Mapping):
            return value.get(name)
        return getattr(value, name, None)

    def start_connection(
        self,
        *,
        user_id: str,
        connector_id: str,
        state: str,
    ) -> ManagedOAuthStart:
        if (
            not isinstance(user_id, str)
            or not AUTHORITY_RE.fullmatch(user_id)
            or not isinstance(state, str)
            or not STATE_RE.fullmatch(state)
        ):
            raise ControlError("oauth_state_invalid", 403)
        try:
            toolkit = toolkit_for_connector(connector_id)
        except Exception:
            raise ControlError("oauth_connector_invalid") from None
        callback_url = self.redirect_uri + "?" + urlencode({"state": state})
        options: dict[str, Any] = {
            "user_id": user_id,
            "toolkits": [toolkit],
            "manage_connections": False,
            "multi_account": {
                "enable": True,
                "require_explicit_selection": True,
            },
        }
        auth_config = self.auth_configs.get(toolkit)
        if auth_config:
            options["auth_configs"] = {toolkit: auth_config}
        try:
            session = self._client().create(**options)
            request = session.authorize(toolkit, callback_url=callback_url)
        except ControlError:
            raise
        except Exception:
            raise ControlError("composio_upstream_failed", 502) from None
        connected_account_id = self._field(request, "id")
        authorization_url = self._field(request, "redirect_url")
        parsed = (
            urlsplit(authorization_url) if isinstance(authorization_url, str) else None
        )
        if (
            not isinstance(connected_account_id, str)
            or not re.fullmatch(r"ca_[A-Za-z0-9_-]{3,252}", connected_account_id)
            or parsed is None
            or parsed.scheme != "https"
            or parsed.hostname != "connect.composio.dev"
            or parsed.username
            or parsed.password
            or parsed.fragment
            or not parsed.path.startswith("/link/")
        ):
            raise ControlError("composio_upstream_invalid", 502)
        return ManagedOAuthStart(
            authorization_url=authorization_url,
            connected_account_id=connected_account_id,
        )

    def _account(self, connected_account_id: str) -> Any:
        try:
            return self._client().connected_accounts.get(connected_account_id)
        except ControlError:
            raise
        except Exception:
            raise ControlError("composio_upstream_failed", 502) from None

    def _probe_connection(
        self,
        *,
        user_id: str,
        connected_account_id: str,
        connector_id: str,
    ) -> None:
        probe = self._PROBES.get(connector_id)
        if probe is None:
            raise ControlError("oauth_connector_invalid")
        for attempt in range(len(COMPOSIO_PROBE_RETRY_DELAYS) + 1):
            try:
                rail = self.rail_factory(
                    api_key=self.api_key,
                    user_id=user_id,
                    connected_account_id=connected_account_id,
                    connector_id=connector_id,
                )
                rail.run(*probe)
                return
            except WorkspaceRailError as error:
                retryable = str(error) in COMPOSIO_TRANSIENT_PROBE_ERRORS
                if not retryable or attempt == len(COMPOSIO_PROBE_RETRY_DELAYS):
                    raise ControlError("oauth_scope_insufficient", 403) from None
                time.sleep(COMPOSIO_PROBE_RETRY_DELAYS[attempt])
            except Exception:
                raise ControlError("oauth_scope_insufficient", 403) from None

    def complete_connection(
        self,
        *,
        user_id: str,
        connector_id: str,
        expected_connected_account_id: str,
        callback_connected_account_id: str,
        callback_status: str,
        required_scopes: tuple[str, ...],
    ) -> OAuthTokens:
        if not isinstance(user_id, str) or not AUTHORITY_RE.fullmatch(user_id):
            raise ControlError("oauth_connection_forbidden", 403)
        if callback_status != "success":
            raise ControlError("oauth_connection_inactive", 403)
        if (
            callback_connected_account_id != expected_connected_account_id
            or not re.fullmatch(
                r"ca_[A-Za-z0-9_-]{3,252}",
                callback_connected_account_id or "",
            )
        ):
            raise ControlError("oauth_connection_mismatch", 403)
        try:
            toolkit = toolkit_for_connector(connector_id)
        except Exception:
            raise ControlError("oauth_connector_invalid") from None
        account = self._account(expected_connected_account_id)
        account_toolkit = self._field(self._field(account, "toolkit"), "slug")
        if self._field(account, "id") != expected_connected_account_id:
            raise ControlError("oauth_connection_mismatch", 403)
        if self._field(account, "user_id") != user_id:
            raise ControlError("oauth_connection_forbidden", 403)
        if self._field(account, "status") != "ACTIVE":
            raise ControlError("oauth_connection_inactive", 403)
        if account_toolkit != toolkit:
            raise ControlError("oauth_connection_mismatch", 403)
        observed_scopes = self._field(account, "requested_scopes")
        if observed_scopes is not None and (
            not isinstance(observed_scopes, list)
            or not _scopes_cover(
                required_scopes,
                {
                    value
                    for value in observed_scopes
                    if isinstance(value, str)
                },
            )
        ):
            raise ControlError("oauth_scope_insufficient", 403)
        self._probe_connection(
            user_id=user_id,
            connected_account_id=expected_connected_account_id,
            connector_id=connector_id,
        )
        return OAuthTokens(
            subject_id=f"{toolkit}:{expected_connected_account_id}",
            granted_scopes=tuple(sorted(observed_scopes or required_scopes)),
            credentials={
                "user_id": user_id,
                "connected_account_id": expected_connected_account_id,
                "toolkit": toolkit,
            },
            expires_at=None,
        )

    def revoke(self, credentials: dict[str, Any]) -> None:
        if not isinstance(credentials, dict) or set(credentials) != {
            "user_id",
            "connected_account_id",
            "toolkit",
        }:
            raise ControlError("oauth_connection_invalid", 500)
        user_id = credentials["user_id"]
        connected_account_id = credentials["connected_account_id"]
        toolkit = credentials["toolkit"]
        if (
            not isinstance(user_id, str)
            or not AUTHORITY_RE.fullmatch(user_id)
            or not isinstance(connected_account_id, str)
            or not re.fullmatch(r"ca_[A-Za-z0-9_-]{3,252}", connected_account_id)
            or toolkit
            not in {"gmail", "googlecalendar", "googlecontacts", "googledrive"}
        ):
            raise ControlError("oauth_connection_invalid", 500)
        account = self._account(connected_account_id)
        account_toolkit = self._field(self._field(account, "toolkit"), "slug")
        if self._field(account, "user_id") != user_id or account_toolkit != toolkit:
            raise ControlError("oauth_connection_forbidden", 403)
        try:
            self._client().connected_accounts.delete(
                connected_account_id,
                revoke_on_delete=True,
            )
        except Exception:
            raise ControlError("composio_revoke_failed", 502) from None


class ControlPlane:
    """One control contract for browser, remote worker, and macOS clients."""

    def __init__(
        self,
        store: Any,
        secret_box: SecretBox,
        providers: dict[str, OAuthProvider],
        invitation_email_sender: InvitationEmailSender | None = None,
        mcp_resource_uri: str = "",
    ):
        self.store = store
        self.secret_box = secret_box
        self.providers = dict(providers)
        self.invitation_email_sender = invitation_email_sender
        self.mcp_resource_uri = mcp_resource_uri

    @classmethod
    def from_env(cls, store: Any) -> "ControlPlane":
        google_values = (
            os.environ.get("RECALL_GOOGLE_CLIENT_ID", ""),
            os.environ.get("RECALL_GOOGLE_CLIENT_SECRET", ""),
            os.environ.get("RECALL_GOOGLE_REDIRECT_URI", ""),
        )
        if any(google_values) and not all(google_values):
            raise ControlError("google_oauth_configuration_invalid", 500)
        composio_values = (
            os.environ.get("RECALL_COMPOSIO_API_KEY", ""),
            os.environ.get("RECALL_COMPOSIO_REDIRECT_URI", ""),
        )
        if any(composio_values) and not all(composio_values):
            raise ControlError("composio_configuration_invalid", 500)
        providers: dict[str, OAuthProvider] = {}
        if all(google_values):
            providers["google"] = GoogleOAuthProvider.from_env()
        if all(composio_values):
            providers["composio"] = ComposioConnectionBroker.from_env()
        try:
            invitation_email_sender = sender_from_env()
        except InvitationEmailError:
            raise ControlError("invitation_email_configuration_invalid", 500) from None
        return cls(
            store,
            SecretBox.from_env(),
            providers,
            invitation_email_sender=invitation_email_sender,
            mcp_resource_uri=os.environ.get("RECALL_MCP_RESOURCE_URI", ""),
        )

    def create_admin_token(
        self,
        name: str,
        *,
        principal_id: str,
        expires_in_days: int = 30,
    ) -> dict[str, Any]:
        if (
            not isinstance(name, str)
            or not 1 <= len(name) <= 128
            or not isinstance(principal_id, str)
            or not AUTHORITY_RE.fullmatch(principal_id)
            or type(expires_in_days) is not int
            or not 1 <= expires_in_days <= 365
        ):
            raise ControlError("admin_credential_invalid")
        token = "rca_" + secrets.token_urlsafe(32)
        credential_id = uuid.uuid4()
        expires_at = _now() + timedelta(days=expires_in_days)
        with self.store.connect() as connection:
            access = connection.execute(
                """SELECT 1 FROM brain_access_grants
                   WHERE principal_id=%s
                     AND permission IN ('owner','admin')
                   LIMIT 1""",
                (principal_id,),
            ).fetchone()
            if not access:
                raise ControlError("admin_access_missing", 403)
            connection.execute(
                """INSERT INTO admin_credentials(
                       id,name,token_sha256,principal_id,audience,scopes,expires_at
                   ) VALUES (%s,%s,%s,%s,'recall-admin',ARRAY['manage'],%s)""",
                (
                    credential_id,
                    name,
                    _digest(token),
                    principal_id,
                    expires_at,
                ),
            )
        return {
            "id": str(credential_id),
            "name": name,
            "principal_id": principal_id,
            "expires_at": expires_at.isoformat(),
            "token": token,
        }

    def revoke_admin_token(self, name: str) -> bool:
        with self.store.connect() as connection:
            row = connection.execute(
                """UPDATE admin_credentials SET revoked_at=now()
                   WHERE name=%s AND revoked_at IS NULL RETURNING id""",
                (name,),
            ).fetchone()
            if row:
                connection.execute(
                    """UPDATE admin_sessions SET revoked_at=now()
                       WHERE credential_id=%s AND revoked_at IS NULL""",
                    (row["id"],),
                )
        return bool(row)

    def exchange_admin_token(self, token: str) -> dict[str, str]:
        if not isinstance(token, str) or not ADMIN_TOKEN_RE.fullmatch(token):
            raise ControlError("admin_auth_invalid", 401)
        session = "rcs_" + secrets.token_urlsafe(32)
        csrf = secrets.token_urlsafe(32)
        expires_at = _now() + timedelta(hours=12)
        with self.store.connect() as connection:
            credential = connection.execute(
                """SELECT id,principal_id FROM admin_credentials
                   WHERE token_sha256=%s AND audience='recall-admin'
                     AND revoked_at IS NULL AND expires_at>now()""",
                (_digest(token),),
            ).fetchone()
            if not credential:
                raise ControlError("admin_auth_invalid", 401)
            connection.execute(
                """INSERT INTO admin_sessions(
                       id,credential_id,session_sha256,csrf_sha256,expires_at
                   ) VALUES (%s,%s,%s,%s,%s)""",
                (
                    uuid.uuid4(),
                    credential["id"],
                    _digest(session),
                    _digest(csrf),
                    expires_at,
                ),
            )
        return {
            "session": session,
            "csrf": csrf,
            "expires_at": expires_at.isoformat(),
        }

    def authenticate_session(
        self,
        token: str,
        *,
        csrf: str | None = None,
    ) -> dict[str, Any]:
        if not isinstance(token, str) or not SESSION_TOKEN_RE.fullmatch(token):
            raise ControlError("admin_session_invalid", 401)
        with self.store.connect() as connection:
            row = connection.execute(
                """SELECT session.id,credential.principal_id,session.csrf_sha256
                   FROM admin_sessions session
                   JOIN admin_credentials credential
                     ON credential.id=session.credential_id
                   WHERE session.session_sha256=%s
                     AND session.revoked_at IS NULL
                     AND session.expires_at>now()
                     AND credential.revoked_at IS NULL
                     AND credential.expires_at>now()""",
                (_digest(token),),
            ).fetchone()
        if not row:
            raise ControlError("admin_session_invalid", 401)
        if csrf is not None and (
            not isinstance(csrf, str)
            or not secrets.compare_digest(row["csrf_sha256"], _digest(csrf))
        ):
            raise ControlError("admin_csrf_invalid", 403)
        return {
            "session_id": str(row["id"]),
            "principal_id": row["principal_id"],
        }

    def _brain_rows(self, principal_id: str) -> list[dict[str, Any]]:
        with self.store.connect() as connection:
            return connection.execute(
                """SELECT space.tenant_id,space.slug,space.brain_kind,
                          organization.display_name,access.permission
                   FROM brain_access_grants access
                   JOIN brain_spaces space USING(tenant_id)
                   JOIN brain_organizations organization USING(organization_id)
                   WHERE access.principal_id=%s
                   ORDER BY space.brain_kind,space.slug""",
                (principal_id,),
            ).fetchall()

    def _brain_admin(
        self,
        connection: Any,
        *,
        principal_id: str,
        tenant_id: str,
    ) -> dict[str, Any]:
        row = connection.execute(
            """SELECT space.organization_id,space.brain_kind,space.slug,
                      organization.display_name,
                      membership.role,access.permission
               FROM brain_spaces space
               JOIN brain_organizations organization USING(organization_id)
               JOIN brain_access_grants access
                 ON access.tenant_id=space.tenant_id
                AND access.principal_id=%s
               JOIN brain_memberships membership
                 ON membership.organization_id=space.organization_id
                AND membership.principal_id=%s
               WHERE space.tenant_id=%s""",
            (principal_id, principal_id, tenant_id),
        ).fetchone()
        if (
            row is None
            or row["brain_kind"] != "company"
            or row["role"] not in {"owner", "admin"}
            or row["permission"] not in {"owner", "admin"}
        ):
            raise ControlError("brain_admin_forbidden", 403)
        return row

    def create_brain_invitation(
        self,
        *,
        principal_id: str,
        tenant_id: str,
        email: str,
        role: str,
        expires_in_days: int = 7,
    ) -> dict[str, Any]:
        normalized_email = normalize_verified_email(email)
        if (
            not isinstance(principal_id, str)
            or not AUTHORITY_RE.fullmatch(principal_id)
            or not isinstance(tenant_id, str)
            or not AUTHORITY_RE.fullmatch(tenant_id)
            or normalized_email is None
            or role not in {"admin", "member"}
            or type(expires_in_days) is not int
            or not 1 <= expires_in_days <= 30
        ):
            raise ControlError("brain_invitation_invalid")
        invitation_id = uuid.uuid4()
        expires_at = _now() + timedelta(days=expires_in_days)
        email_sha256 = self.invitation_email_index(normalized_email)
        encrypted_email = self.secret_box.seal(
            {"email": normalized_email},
            purpose=f"brain-invitation:{invitation_id}",
        )
        with self.store.connect() as connection:
            with connection.transaction():
                admin = self._brain_admin(
                    connection,
                    principal_id=principal_id,
                    tenant_id=tenant_id,
                )
                if admin["role"] == "admin" and role != "member":
                    raise ControlError("brain_role_forbidden", 403)
                existing = connection.execute(
                    """SELECT 1 FROM brain_invitations
                       WHERE tenant_id=%s AND email_sha256=%s
                         AND accepted_at IS NOT NULL AND revoked_at IS NULL""",
                    (tenant_id, email_sha256),
                ).fetchone()
                if existing:
                    raise ControlError("brain_member_exists", 409)
                connection.execute(
                    """UPDATE brain_invitations SET revoked_at=now()
                       WHERE tenant_id=%s AND email_sha256=%s
                         AND accepted_at IS NULL AND revoked_at IS NULL""",
                    (tenant_id, email_sha256),
                )
                connection.execute(
                    """INSERT INTO brain_invitations(
                           id,tenant_id,email_sha256,encrypted_email,
                           encryption_key_id,role,invited_by_principal_id,expires_at
                       ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (
                        invitation_id,
                        tenant_id,
                        email_sha256,
                        encrypted_email,
                        self.secret_box.key_id,
                        role,
                        principal_id,
                        expires_at,
                    ),
                )
                connection.execute(
                    """INSERT INTO control_audit_events(
                           id,principal_id,operation,status,target_sha256
                       ) VALUES (%s,%s,'brain.invitation.create','success',%s)""",
                    (uuid.uuid4(), principal_id, _digest(str(invitation_id))),
                )
        delivery = {"status": "disabled"}
        if self.invitation_email_sender is not None:
            try:
                brain_url, onboarding_url = invitation_urls(
                    self.mcp_resource_uri,
                    tenant_id=tenant_id,
                    invitation_id=str(invitation_id),
                )
                self.invitation_email_sender.send(
                    InvitationEmail(
                        invitation_id=str(invitation_id),
                        recipient=normalized_email,
                        organization_name=admin["display_name"],
                        brain_slug=admin["slug"],
                        role=role,
                        expires_at=expires_at,
                        brain_url=brain_url,
                        onboarding_url=onboarding_url,
                    )
                )
                delivery = {
                    "status": "sent",
                    "provider": self.invitation_email_sender.provider,
                }
            except InvitationEmailError:
                LOG.error(
                    "invitation email delivery failed provider=%s target=%s",
                    self.invitation_email_sender.provider,
                    _digest(str(invitation_id)),
                )
                delivery = {
                    "status": "failed",
                    "provider": self.invitation_email_sender.provider,
                }
        return {
            "id": str(invitation_id),
            "tenant_id": tenant_id,
            "email": normalized_email,
            "role": role,
            "status": "pending",
            "expires_at": expires_at.isoformat(),
            "delivery": delivery,
        }

    def invitation_onboarding(self, invitation_id: str) -> InvitationEmail:
        try:
            parsed_id = uuid.UUID(invitation_id)
        except (TypeError, ValueError, AttributeError):
            raise ControlError("brain_invitation_not_found", 404) from None
        with self.store.connect() as connection:
            row = connection.execute(
                """SELECT invitation.tenant_id,invitation.role,
                          invitation.expires_at,invitation.accepted_at,
                          invitation.revoked_at,space.slug,
                          organization.display_name
                   FROM brain_invitations invitation
                   JOIN brain_spaces space USING(tenant_id)
                   JOIN brain_organizations organization USING(organization_id)
                   WHERE invitation.id=%s""",
                (parsed_id,),
            ).fetchone()
        if (
            row is None
            or row["revoked_at"] is not None
            or (row["accepted_at"] is None and row["expires_at"] <= _now())
        ):
            raise ControlError("brain_invitation_not_found", 404)
        try:
            brain_url, onboarding_url = invitation_urls(
                self.mcp_resource_uri,
                tenant_id=row["tenant_id"],
                invitation_id=str(parsed_id),
            )
        except InvitationEmailError:
            raise ControlError("invitation_email_configuration_invalid", 500) from None
        return InvitationEmail(
            invitation_id=str(parsed_id),
            recipient="",
            organization_name=row["display_name"],
            brain_slug=row["slug"],
            role=row["role"],
            expires_at=row["expires_at"],
            brain_url=brain_url,
            onboarding_url=onboarding_url,
        )

    def invitation_email_index(self, email: str) -> str:
        normalized_email = normalize_verified_email(email)
        if normalized_email is None:
            raise ControlError("brain_invitation_invalid")
        return self.secret_box.blind_index(
            normalized_email,
            purpose="brain-invitation-email-v1",
        )

    def _brain_invitations(self, principal_id: str) -> list[dict[str, Any]]:
        with self.store.connect() as connection:
            rows = connection.execute(
                """SELECT invitation.id,invitation.tenant_id,
                          invitation.encrypted_email,invitation.encryption_key_id,
                          invitation.role,invitation.expires_at,
                          invitation.accepted_at,invitation.revoked_at
                   FROM brain_invitations invitation
                   JOIN brain_spaces space USING(tenant_id)
                   JOIN brain_access_grants access
                     ON access.tenant_id=invitation.tenant_id
                    AND access.principal_id=%s
                   JOIN brain_memberships membership
                     ON membership.organization_id=space.organization_id
                    AND membership.principal_id=%s
                   WHERE access.permission IN ('owner','admin')
                     AND membership.role IN ('owner','admin')
                   ORDER BY invitation.created_at DESC""",
                (principal_id, principal_id),
            ).fetchall()
        result = []
        now = _now()
        for row in rows:
            if row["encryption_key_id"] != self.secret_box.key_id:
                raise ControlError("brain_invitation_key_invalid", 500)
            private = self.secret_box.open(
                bytes(row["encrypted_email"]),
                purpose=f"brain-invitation:{row['id']}",
            )
            email = normalize_verified_email(private.get("email"))
            if email is None or set(private) != {"email"}:
                raise ControlError("brain_invitation_email_invalid", 500)
            if row["revoked_at"] is not None:
                status = "revoked"
            elif row["accepted_at"] is not None:
                status = "active"
            elif row["expires_at"] <= now:
                status = "expired"
            else:
                status = "pending"
            result.append({
                "id": str(row["id"]),
                "tenant_id": row["tenant_id"],
                "email": email,
                "role": row["role"],
                "status": status,
                "expires_at": row["expires_at"].isoformat(),
            })
        return result

    def revoke_brain_invitation(
        self,
        *,
        principal_id: str,
        invitation_id: str,
    ) -> dict[str, str]:
        try:
            parsed_id = uuid.UUID(invitation_id)
        except (ValueError, TypeError):
            raise ControlError("brain_invitation_invalid") from None
        with self.store.connect() as connection:
            with connection.transaction():
                invitation = connection.execute(
                    """SELECT invitation.tenant_id,invitation.role,
                              invitation.accepted_principal_id,
                              space.organization_id
                       FROM brain_invitations invitation
                       JOIN brain_spaces space USING(tenant_id)
                       WHERE invitation.id=%s AND invitation.revoked_at IS NULL
                       FOR UPDATE OF invitation""",
                    (parsed_id,),
                ).fetchone()
                if invitation is None:
                    raise ControlError("brain_invitation_not_found", 404)
                admin = self._brain_admin(
                    connection,
                    principal_id=principal_id,
                    tenant_id=invitation["tenant_id"],
                )
                if admin["role"] == "admin" and invitation["role"] != "member":
                    raise ControlError("brain_role_forbidden", 403)
                connection.execute(
                    "UPDATE brain_invitations SET revoked_at=now() WHERE id=%s",
                    (parsed_id,),
                )
                member = invitation["accepted_principal_id"]
                if member is not None:
                    connection.execute(
                        """UPDATE external_identity_bindings SET revoked_at=now()
                           WHERE tenant_id=%s AND principal_id=%s
                             AND revoked_at IS NULL""",
                        (invitation["tenant_id"], member),
                    )
                    connection.execute(
                        """DELETE FROM canonical_source_grants
                           WHERE tenant_id=%s AND principal_id=%s""",
                        (invitation["tenant_id"], member),
                    )
                    connection.execute(
                        """DELETE FROM brain_access_grants
                           WHERE tenant_id=%s AND principal_id=%s""",
                        (invitation["tenant_id"], member),
                    )
                    remaining = connection.execute(
                        """SELECT 1 FROM brain_access_grants access
                           JOIN brain_spaces space USING(tenant_id)
                           WHERE space.organization_id=%s
                             AND access.principal_id=%s LIMIT 1""",
                        (invitation["organization_id"], member),
                    ).fetchone()
                    if not remaining:
                        connection.execute(
                            """DELETE FROM brain_memberships
                               WHERE organization_id=%s AND principal_id=%s""",
                            (invitation["organization_id"], member),
                        )
                connection.execute(
                    """INSERT INTO control_audit_events(
                           id,principal_id,operation,status,target_sha256
                       ) VALUES (%s,%s,'brain.invitation.revoke','success',%s)""",
                    (uuid.uuid4(), principal_id, _digest(str(parsed_id))),
                )
        return {"id": str(parsed_id), "status": "revoked"}

    def state(self, principal_id: str) -> dict[str, Any]:
        brains = self._brain_rows(principal_id)
        with self.store.connect() as connection:
            installations = connection.execute(
                """SELECT installation.id,installation.tenant_id,
                          installation.connector_id,installation.source_id,
                          installation.execution,installation.device_id,
                          installation.state,
                          installation.privacy_mode,installation.selectors,
                          installation.revision,installation.last_error_code,
                          installation.last_success_at,
                          installation.last_started_at,
                          installation.run_after,
                          installation.failure_count,
                          connection.provider,connection.status AS connection_status
                   FROM connector_installations installation
                   LEFT JOIN provider_connections connection
                     ON connection.id=installation.connection_id
                   WHERE installation.principal_id=%s
                     AND installation.state<>'uninstalled'
                   ORDER BY installation.connector_id,installation.tenant_id""",
                (principal_id,),
            ).fetchall()
            connections = connection.execute(
                """SELECT id,provider,status,granted_scopes,expires_at,updated_at
                   FROM provider_connections
                   WHERE principal_id=%s AND status<>'revoked'
                   ORDER BY provider,created_at""",
                (principal_id,),
            ).fetchall()
        catalog = []
        for item in REGISTRY:
            if not isinstance(item, ConnectorDefinitionV3):
                continue
            public = item.to_public()
            catalog.append(
                {
                    "connector_id": item.connector_id,
                    "source_family": item.source_family,
                    "placement": public["placement"],
                    "auth": public["auth"],
                    "privacy_modes": public["policy"]["privacy_modes"],
                    "default_privacy_mode": public["policy"]["default_privacy_mode"],
                    "selection_fields": public["selection_fields"],
                }
            )
        return {
            "schema_version": 1,
            "mode": "connector-control-state",
            "principal": {"id": principal_id},
            "brains": brains,
            "invitations": self._brain_invitations(principal_id),
            "providers": [
                {"id": provider_id, "status": "available"}
                for provider_id in sorted(self.providers)
            ],
            "catalog": sorted(catalog, key=lambda value: value["connector_id"]),
            "connections": connections,
            "installations": installations,
        }

    def create_device_installation(
        self,
        *,
        principal_id: str,
        connector_id: str,
        tenant_id: str,
        device_id: str,
        source_id: str,
        privacy_mode: str,
        selectors: dict[str, Any],
    ) -> dict[str, Any]:
        if (
            not isinstance(connector_id, str)
            or not isinstance(tenant_id, str)
            or not AUTHORITY_RE.fullmatch(tenant_id)
            or not isinstance(device_id, str)
            or not DEVICE_RE.fullmatch(device_id)
            or not isinstance(source_id, str)
            or not AUTHORITY_RE.fullmatch(source_id)
        ):
            raise ControlError("device_route_invalid")
        try:
            item = definition(connector_id)
        except ConnectorRegistryError:
            raise ControlError("device_connector_invalid") from None
        if (
            not isinstance(item, ConnectorDefinitionV3)
            or item.placement.execution not in {"source_local", "either"}
            or privacy_mode not in item.privacy_modes
            or not isinstance(selectors, dict)
            or set(selectors) - set(item.selection_fields)
        ):
            raise ControlError("device_connector_invalid")
        normalized_selectors = {
            key: _selector(value) for key, value in sorted(selectors.items())
        }
        encoded = json.dumps(
            normalized_selectors,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        if len(encoded.encode()) > 16 * 1024:
            raise ControlError("device_selectors_invalid")
        installation_id = uuid.uuid4()
        credential_id = uuid.uuid4()
        credential_name = f"device-{credential_id}"
        token = "rcl_" + secrets.token_urlsafe(32)
        with self.store.connect() as connection:
            with connection.transaction():
                brain = connection.execute(
                    """SELECT 1 FROM brain_access_grants
                       WHERE tenant_id=%s AND principal_id=%s
                         AND permission IN ('owner','admin')
                       FOR UPDATE""",
                    (tenant_id, principal_id),
                ).fetchone()
                if not brain:
                    raise ControlError("device_brain_forbidden", 403)
                replaced = connection.execute(
                    """UPDATE connector_installations
                       SET state='revoked',revision=revision+1,updated_at=now()
                       WHERE principal_id=%s AND connector_id=%s AND device_id=%s
                         AND state NOT IN ('revoked','uninstalled')
                       RETURNING id""",
                    (principal_id, connector_id, device_id),
                ).fetchall()
                replaced_ids = [row["id"] for row in replaced]
                if replaced_ids:
                    connection.execute(
                        """UPDATE collector_credentials SET revoked_at=now()
                           WHERE installation_id=ANY(%s) AND revoked_at IS NULL""",
                        (replaced_ids,),
                    )
                connection.execute(
                    """INSERT INTO connector_installations(
                           id,tenant_id,principal_id,connector_id,source_id,
                           connection_id,execution,device_id,state,privacy_mode,
                           selectors
                       ) VALUES (%s,%s,%s,%s,%s,NULL,'source_local',%s,
                                 'enabled',%s,%s::jsonb)""",
                    (
                        installation_id,
                        tenant_id,
                        principal_id,
                        connector_id,
                        source_id,
                        device_id,
                        privacy_mode,
                        encoded,
                    ),
                )
                connection.execute(
                    """INSERT INTO collector_credentials(
                           id,name,token_sha256,tenant_id,source_id,scopes,
                           principal_id,installation_id
                       ) VALUES (%s,%s,%s,%s,%s,ARRAY['write'],%s,%s)""",
                    (
                        credential_id,
                        credential_name,
                        _digest(token),
                        tenant_id,
                        source_id,
                        principal_id,
                        installation_id,
                    ),
                )
                connection.execute(
                    """INSERT INTO control_audit_events(
                           id,principal_id,operation,status,target_sha256
                       ) VALUES (%s,%s,'device.route','success',%s)""",
                    (
                        uuid.uuid4(),
                        principal_id,
                        hashlib.sha256(str(installation_id).encode()).hexdigest(),
                    ),
                )
        return {
            "schema_version": 1,
            "mode": "device-route",
            "installation_id": str(installation_id),
            "connector_id": connector_id,
            "tenant_id": tenant_id,
            "device_id": device_id,
            "source_id": source_id,
            "privacy_mode": privacy_mode,
            "state": "enabled",
            "token": token,
            "replaced": len(replaced_ids),
        }

    def start_oauth(
        self,
        *,
        principal_id: str,
        provider_id: str,
        routes: list[dict[str, Any]],
    ) -> dict[str, str]:
        provider = self.providers.get(provider_id)
        if provider is None:
            raise ControlError("oauth_provider_unsupported")
        if (
            not isinstance(routes, list)
            or not 1 <= len(routes) <= 8
            or any(not isinstance(route, dict) for route in routes)
        ):
            raise ControlError("oauth_routes_invalid")
        brains = {
            row["tenant_id"]: row
            for row in self._brain_rows(principal_id)
            if row["permission"] in {"owner", "admin"}
        }
        normalized: list[dict[str, Any]] = []
        scopes: set[str] = set()
        if provider_id == "google":
            scopes.add("openid")
        identities: set[tuple[str, str]] = set()
        for route in routes:
            if set(route) != {
                "connector_id",
                "tenant_id",
                "privacy_mode",
                "selectors",
            }:
                raise ControlError("oauth_route_invalid")
            connector_id = route["connector_id"]
            tenant_id = route["tenant_id"]
            if (
                provider_id in {"google", "composio"}
                and connector_id not in GOOGLE_CONNECTORS
            ):
                raise ControlError("oauth_connector_invalid")
            if tenant_id not in brains:
                raise ControlError("oauth_brain_forbidden", 403)
            try:
                item = definition(connector_id)
            except ConnectorRegistryError:
                raise ControlError("oauth_connector_invalid") from None
            if (
                not isinstance(item, ConnectorDefinitionV3)
                or item.auth.kind != "oauth2"
                or item.placement.execution not in {"remote_worker", "either"}
            ):
                raise ControlError("oauth_connector_invalid")
            identity = (tenant_id, connector_id)
            if identity in identities:
                raise ControlError("oauth_route_duplicate")
            identities.add(identity)
            privacy_mode = route["privacy_mode"]
            if privacy_mode not in item.privacy_modes:
                raise ControlError("oauth_privacy_invalid")
            selectors = route["selectors"]
            if not isinstance(selectors, dict) or set(selectors) - set(
                item.selection_fields
            ):
                raise ControlError("oauth_selectors_invalid")
            selectors = {
                key: _selector(value) for key, value in sorted(selectors.items())
            }
            encoded = json.dumps(
                selectors,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            if len(encoded.encode()) > 16 * 1024:
                raise ControlError("oauth_selectors_invalid")
            scopes.update(item.auth.minimum_scopes)
            normalized.append(
                {
                    "connector_id": connector_id,
                    "tenant_id": tenant_id,
                    "privacy_mode": privacy_mode,
                    "selectors": selectors,
                }
            )
        managed_connection = bool(getattr(provider, "managed_connection", False))
        if managed_connection and len(normalized) != 1:
            raise ControlError("oauth_routes_invalid")
        code_verifier = secrets.token_urlsafe(64)
        challenge = (
            base64.urlsafe_b64encode(hashlib.sha256(code_verifier.encode()).digest())
            .rstrip(b"=")
            .decode()
        )
        state = secrets.token_urlsafe(48)
        connected_account_id = None
        authorization_url = None
        if managed_connection:
            started = provider.start_connection(
                user_id=principal_id,
                connector_id=normalized[0]["connector_id"],
                state=state,
            )
            connected_account_id = started.connected_account_id
            authorization_url = started.authorization_url
        context = self.secret_box.seal(
            {
                "provider": provider_id,
                "principal_id": principal_id,
                "routes": normalized,
                "code_verifier": code_verifier,
                "expected_connected_account_id": connected_account_id,
            },
            purpose="oauth-session",
        )
        with self.store.connect() as connection:
            connection.execute(
                """INSERT INTO oauth_sessions(
                       state_sha256,principal_id,provider,encrypted_context,
                       encryption_key_id,expires_at
                   ) VALUES (%s,%s,%s,%s,%s,%s)""",
                (
                    _digest(state),
                    principal_id,
                    provider_id,
                    context,
                    self.secret_box.key_id,
                    _now() + timedelta(minutes=10),
                ),
            )
        return {
            "provider": provider_id,
            "authorization_url": authorization_url
            or provider.authorization_url(
                state=state,
                code_challenge=challenge,
                scopes=tuple(sorted(scopes)),
            ),
        }

    def complete_oauth(
        self,
        *,
        provider_id: str,
        state: str,
        code: str | None = None,
        connected_account_id: str | None = None,
        status: str | None = None,
    ) -> dict[str, Any]:
        if not isinstance(state, str) or not STATE_RE.fullmatch(state):
            raise ControlError("oauth_state_invalid", 403)
        provider = self.providers.get(provider_id)
        if provider is None:
            raise ControlError("oauth_provider_unsupported")
        with self.store.connect() as connection:
            with connection.transaction():
                row = connection.execute(
                    """SELECT principal_id,provider,encrypted_context,
                              encryption_key_id
                       FROM oauth_sessions
                       WHERE state_sha256=%s AND consumed_at IS NULL
                         AND expires_at>now()
                       FOR UPDATE""",
                    (_digest(state),),
                ).fetchone()
                if (
                    not row
                    or row["provider"] != provider_id
                    or row["encryption_key_id"] != self.secret_box.key_id
                ):
                    raise ControlError("oauth_state_invalid", 403)
                connection.execute(
                    """UPDATE oauth_sessions SET consumed_at=now()
                       WHERE state_sha256=%s""",
                    (_digest(state),),
                )
        context = self.secret_box.open(
            bytes(row["encrypted_context"]), purpose="oauth-session"
        )
        if (
            context.get("principal_id") != row["principal_id"]
            or context.get("provider") != provider_id
        ):
            raise ControlError("oauth_state_invalid", 403)
        administrable = {
            value["tenant_id"]
            for value in self._brain_rows(row["principal_id"])
            if value["permission"] in {"owner", "admin"}
        }
        if any(
            route.get("tenant_id") not in administrable
            for route in context.get("routes", [])
        ):
            raise ControlError("oauth_brain_forbidden", 403)
        required_scopes: set[str] = set()
        for route in context.get("routes", []):
            item = definition(route["connector_id"])
            required_scopes.update(item.auth.minimum_scopes)
        if getattr(provider, "managed_connection", False):
            routes = context.get("routes", [])
            if len(routes) != 1:
                raise ControlError("oauth_state_invalid", 403)
            tokens = provider.complete_connection(
                user_id=row["principal_id"],
                connector_id=routes[0]["connector_id"],
                expected_connected_account_id=context.get(
                    "expected_connected_account_id", ""
                ),
                callback_connected_account_id=connected_account_id or "",
                callback_status=status or "",
                required_scopes=tuple(sorted(required_scopes)),
            )
        else:
            if not isinstance(code, str) or not code:
                raise ControlError("oauth_callback_invalid")
            tokens = provider.exchange(
                code=code, code_verifier=context.get("code_verifier", "")
            )
        if not _scopes_cover(required_scopes, tokens.granted_scopes):
            raise ControlError("oauth_scope_insufficient", 403)
        connection_id = uuid.uuid4()
        encrypted = self.secret_box.seal(
            tokens.credentials, purpose=f"provider-connection:{connection_id}"
        )
        installed = []
        with self.store.connect() as connection:
            with connection.transaction():
                prior = connection.execute(
                    """SELECT id FROM provider_connections
                       WHERE principal_id=%s AND provider=%s AND subject_id=%s""",
                    (row["principal_id"], provider_id, tokens.subject_id),
                ).fetchone()
                if prior:
                    connection_id = prior["id"]
                    encrypted = self.secret_box.seal(
                        tokens.credentials,
                        purpose=f"provider-connection:{connection_id}",
                    )
                    connection.execute(
                        """UPDATE provider_connections SET
                             status='connected',granted_scopes=%s,
                             encrypted_credentials=%s,encryption_key_id=%s,
                             expires_at=%s,updated_at=now(),revoked_at=NULL
                           WHERE id=%s""",
                        (
                            list(tokens.granted_scopes),
                            encrypted,
                            self.secret_box.key_id,
                            tokens.expires_at,
                            connection_id,
                        ),
                    )
                else:
                    connection.execute(
                        """INSERT INTO provider_connections(
                               id,principal_id,provider,subject_id,status,
                               granted_scopes,encrypted_credentials,
                               encryption_key_id,expires_at
                           ) VALUES (%s,%s,%s,%s,'connected',%s,%s,%s,%s)""",
                        (
                            connection_id,
                            row["principal_id"],
                            provider_id,
                            tokens.subject_id,
                            list(tokens.granted_scopes),
                            encrypted,
                            self.secret_box.key_id,
                            tokens.expires_at,
                        ),
                    )
                for route in context["routes"]:
                    installation_id = uuid.uuid4()
                    source_id = f"source:{route['connector_id']}:{uuid.uuid4().hex}"
                    connection.execute(
                        """INSERT INTO connector_installations(
                               id,tenant_id,principal_id,connector_id,source_id,
                               connection_id,execution,state,privacy_mode,selectors
                           ) VALUES (%s,%s,%s,%s,%s,%s,'remote_worker','enabled',%s,%s)
                           ON CONFLICT(tenant_id,principal_id,connector_id)
                           WHERE device_id IS NULL
                           DO UPDATE SET
                             connection_id=excluded.connection_id,
                             state='enabled',
                             privacy_mode=excluded.privacy_mode,
                             selectors=excluded.selectors,
                             revision=connector_installations.revision+1,
                             last_error_code=NULL,
                             updated_at=now()""",
                        (
                            installation_id,
                            route["tenant_id"],
                            row["principal_id"],
                            route["connector_id"],
                            source_id,
                            connection_id,
                            route["privacy_mode"],
                            json.dumps(route["selectors"]),
                        ),
                    )
                    installed.append(
                        {
                            "tenant_id": route["tenant_id"],
                            "connector_id": route["connector_id"],
                            "state": "enabled",
                        }
                    )
                connection.execute(
                    """INSERT INTO control_audit_events(
                           id,principal_id,operation,status,target_sha256
                       ) VALUES (%s,%s,'oauth.complete','success',%s)""",
                    (
                        uuid.uuid4(),
                        row["principal_id"],
                        hashlib.sha256(str(connection_id).encode()).hexdigest(),
                    ),
                )
        return {
            "provider": provider_id,
            "connection_id": str(connection_id),
            "installations": installed,
        }

    def transition_installation(
        self,
        *,
        principal_id: str,
        installation_id: str,
        action: str,
    ) -> dict[str, Any]:
        transition = TRANSITIONS.get(action)
        try:
            target_id = uuid.UUID(installation_id)
        except (ValueError, TypeError):
            raise ControlError("installation_invalid") from None
        if transition is None:
            raise ControlError("installation_action_invalid")
        allowed, target = transition
        with self.store.connect() as connection:
            with connection.transaction():
                row = connection.execute(
                    """SELECT state,revision FROM connector_installations
                       WHERE id=%s AND principal_id=%s FOR UPDATE""",
                    (target_id, principal_id),
                ).fetchone()
                if not row:
                    raise ControlError("installation_not_found", 404)
                if row["state"] == target:
                    return {
                        "installation_id": installation_id,
                        "state": target,
                        "revision": row["revision"],
                        "replay": True,
                    }
                if row["state"] not in allowed:
                    raise ControlError("installation_transition_invalid", 409)
                revision = row["revision"] + 1
                connection.execute(
                    """UPDATE connector_installations
                       SET state=%s,revision=%s,updated_at=now()
                       WHERE id=%s""",
                    (target, revision, target_id),
                )
                if target in {"revoked", "uninstalled"}:
                    connection.execute(
                        """UPDATE collector_credentials SET revoked_at=now()
                           WHERE installation_id=%s AND revoked_at IS NULL""",
                        (target_id,),
                    )
        return {
            "installation_id": installation_id,
            "state": target,
            "revision": revision,
            "replay": False,
        }

    def revoke_connection(
        self,
        *,
        principal_id: str,
        connection_id: str,
    ) -> dict[str, Any]:
        try:
            target_id = uuid.UUID(connection_id)
        except (TypeError, ValueError):
            raise ControlError("connection_invalid") from None
        with self.store.connect() as connection:
            row = connection.execute(
                """SELECT provider,encrypted_credentials,encryption_key_id,status
                   FROM provider_connections
                   WHERE id=%s AND principal_id=%s""",
                (target_id, principal_id),
            ).fetchone()
        if not row:
            raise ControlError("connection_not_found", 404)
        if row["status"] == "revoked":
            return {"connection_id": connection_id, "status": "revoked", "replay": True}
        if row["encryption_key_id"] != self.secret_box.key_id:
            raise ControlError("control_secret_key_unavailable", 500)
        credentials = self.secret_box.open(
            bytes(row["encrypted_credentials"]),
            purpose=f"provider-connection:{target_id}",
        )
        provider = self.providers.get(row["provider"])
        if provider is None:
            raise ControlError("oauth_provider_unsupported", 500)
        provider.revoke(credentials)
        empty = self.secret_box.seal({}, purpose=f"provider-connection:{target_id}")
        with self.store.connect() as connection:
            with connection.transaction():
                connection.execute(
                    """UPDATE provider_connections SET
                         status='revoked',encrypted_credentials=%s,
                         updated_at=now(),revoked_at=now()
                       WHERE id=%s AND principal_id=%s""",
                    (empty, target_id, principal_id),
                )
                changed = connection.execute(
                    """UPDATE connector_installations
                       SET state='revoked',revision=revision+1,updated_at=now()
                       WHERE connection_id=%s AND principal_id=%s
                         AND state NOT IN ('revoked','uninstalled')
                       RETURNING id""",
                    (target_id, principal_id),
                ).fetchall()
        return {
            "connection_id": connection_id,
            "status": "revoked",
            "installations_revoked": len(changed),
            "replay": False,
        }


__all__ = [
    "ComposioConnectionBroker",
    "ControlError",
    "ControlPlane",
    "GoogleOAuthProvider",
    "ManagedOAuthStart",
    "OAuthTokens",
    "SecretBox",
]
