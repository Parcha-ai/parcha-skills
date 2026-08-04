"""Browser OAuth identity for Recall onboarding.

Descope proves who the browser user is. Recall still owns every brain, role,
source grant, and revocation decision.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
import re
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit, urlunsplit
import urllib.request


CLIENT_ID_RE = re.compile(r"[A-Za-z0-9._~+/=-]{8,512}\Z")


class IdentityOAuthError(RuntimeError):
    """Content-free OAuth identity failure."""


class _RejectRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _https_url(value: str, *, callback: bool = False) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or (callback and parsed.path != "/admin/oauth/callback/identity")
    ):
        raise IdentityOAuthError("identity_oauth_configuration_invalid")
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", ""))


@dataclass(frozen=True)
class BrowserIdentity:
    subject: str
    email: str
    email_verified: bool


class DescopeIdentityOAuth:
    """Small confidential-client PKCE adapter for Descope Inbound Apps."""

    provider_id = "descope"

    def __init__(
        self,
        *,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
        canonical_issuer: str,
        authorization_endpoint: str = "https://api.descope.com/oauth2/v1/apps/authorize",
        token_endpoint: str = "https://api.descope.com/oauth2/v1/apps/token",
        userinfo_endpoint: str = "https://api.descope.com/oauth2/v1/apps/userinfo",
        timeout_seconds: float = 10.0,
        opener: Any = None,
    ) -> None:
        if (
            not CLIENT_ID_RE.fullmatch(client_id)
            or not client_secret
            or len(client_secret) > 8192
            or not 1 <= timeout_seconds <= 30
        ):
            raise IdentityOAuthError("identity_oauth_configuration_invalid")
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = _https_url(redirect_uri, callback=True)
        self.canonical_issuer = _https_url(canonical_issuer)
        self.authorization_endpoint = _https_url(authorization_endpoint)
        self.token_endpoint = _https_url(token_endpoint)
        self.userinfo_endpoint = _https_url(userinfo_endpoint)
        self.timeout_seconds = timeout_seconds
        self.opener = opener or urllib.request.build_opener(_RejectRedirect())

    @classmethod
    def from_env(cls) -> "DescopeIdentityOAuth | None":
        values = (
            os.environ.get("RECALL_IDENTITY_OAUTH_CLIENT_ID", "").strip(),
            os.environ.get("RECALL_IDENTITY_OAUTH_CLIENT_SECRET", "").strip(),
            os.environ.get("RECALL_IDENTITY_OAUTH_REDIRECT_URI", "").strip(),
        )
        if not any(values):
            return None
        if not all(values):
            raise IdentityOAuthError("identity_oauth_configuration_invalid")
        return cls(
            client_id=values[0],
            client_secret=values[1],
            redirect_uri=values[2],
            canonical_issuer=os.environ.get("RECALL_OIDC_ISSUER", ""),
            authorization_endpoint=os.environ.get(
                "RECALL_IDENTITY_OAUTH_AUTHORIZATION_ENDPOINT",
                "https://api.descope.com/oauth2/v1/apps/authorize",
            ),
            token_endpoint=os.environ.get(
                "RECALL_IDENTITY_OAUTH_TOKEN_ENDPOINT",
                "https://api.descope.com/oauth2/v1/apps/token",
            ),
            userinfo_endpoint=os.environ.get(
                "RECALL_IDENTITY_OAUTH_USERINFO_ENDPOINT",
                "https://api.descope.com/oauth2/v1/apps/userinfo",
            ),
        )

    def authorization_url(self, *, state: str, code_challenge: str) -> str:
        return self.authorization_endpoint + "?" + urlencode(
            {
                "client_id": self.client_id,
                "redirect_uri": self.redirect_uri,
                "response_type": "code",
                # The app-local permission makes consent explicit while the
                # standard OIDC scopes provide only the identity attributes
                # Recall needs. No brain/API authorization is delegated here.
                "scope": "openid email recall.identity",
                "state": state,
                "code_challenge": code_challenge,
                "code_challenge_method": "S256",
            }
        )

    @staticmethod
    def _json(response: Any) -> dict[str, Any]:
        raw = response.read(256 * 1024 + 1)
        if len(raw) > 256 * 1024:
            raise IdentityOAuthError("identity_oauth_upstream_invalid")
        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise IdentityOAuthError("identity_oauth_upstream_invalid") from None
        if not isinstance(value, dict):
            raise IdentityOAuthError("identity_oauth_upstream_invalid")
        return value

    def exchange(self, *, code: str, code_verifier: str) -> BrowserIdentity:
        if (
            not isinstance(code, str)
            or not 1 <= len(code) <= 4096
            or not isinstance(code_verifier, str)
            or not 43 <= len(code_verifier) <= 128
        ):
            raise IdentityOAuthError("identity_oauth_callback_invalid")
        token_request = urllib.request.Request(
            self.token_endpoint,
            data=urlencode(
                {
                    "grant_type": "authorization_code",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "code": code,
                    "redirect_uri": self.redirect_uri,
                    "code_verifier": code_verifier,
                }
            ).encode(),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": "recall-core/identity-oauth",
            },
            method="POST",
        )
        try:
            with self.opener.open(
                token_request, timeout=self.timeout_seconds
            ) as response:
                token = self._json(response)
            access_token = token.get("access_token")
            if (
                not isinstance(access_token, str)
                or not 1 <= len(access_token) <= 16 * 1024
                or token.get("token_type", "Bearer").casefold() != "bearer"
            ):
                raise IdentityOAuthError("identity_oauth_upstream_invalid")
            user_request = urllib.request.Request(
                self.userinfo_endpoint,
                headers={
                    "Accept": "application/json",
                    "Authorization": f"Bearer {access_token}",
                    "User-Agent": "recall-core/identity-oauth",
                },
            )
            with self.opener.open(
                user_request, timeout=self.timeout_seconds
            ) as response:
                user = self._json(response)
        except IdentityOAuthError:
            raise
        except (HTTPError, URLError, TimeoutError, OSError, ValueError):
            raise IdentityOAuthError("identity_oauth_upstream_failed") from None
        subject = user.get("sub")
        email = user.get("email")
        if (
            not isinstance(subject, str)
            or not 1 <= len(subject) <= 512
            or not isinstance(email, str)
            or not 3 <= len(email) <= 320
            or user.get("email_verified") is not True
        ):
            raise IdentityOAuthError("identity_oauth_identity_invalid")
        return BrowserIdentity(subject, email, True)


__all__ = [
    "BrowserIdentity",
    "DescopeIdentityOAuth",
    "IdentityOAuthError",
]
