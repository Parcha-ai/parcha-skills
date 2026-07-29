#!/usr/bin/env python3
"""Browser-shaped E2E for unified personal/company connector administration."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import http.client
import json
import os
from pathlib import Path
import sys
import threading
from http.server import ThreadingHTTPServer
from urllib.parse import parse_qs, urlencode, urlsplit


ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT), str(ROOT / "server")]

from recall_server.app import Handler
from recall_server.control import (
    ControlPlane,
    ManagedOAuthStart,
    OAuthTokens,
    SecretBox,
)
from recall_server.db import BrainStore


OWNER = "principal:owner:e2e-control"
OUTSIDER = "principal:outsider:e2e-control"
ADMIN = "principal:admin:e2e-control"
PERSONAL = "tenant:personal:e2e-control"
COMPANY = "tenant:company:e2e-control"
SECRET_CANARY = "synthetic-refresh-token-never-render"


class FakeGoogle:
    provider_id = "google"

    def __init__(self):
        self.revoked = False
        self.last_verifier = None
        self.exchanges = 0

    def authorization_url(self, *, state, code_challenge, scopes):
        return "https://oauth.synthetic.invalid/authorize?" + urlencode(
            {
                "state": state,
                "code_challenge": code_challenge,
                "code_challenge_method": "S256",
                "scope": " ".join(scopes),
            }
        )

    def exchange(self, *, code, code_verifier):
        assert code == "synthetic-code"
        assert len(code_verifier) >= 64
        self.last_verifier = code_verifier
        self.exchanges += 1
        return OAuthTokens(
            subject_id="synthetic-google-subject",
            granted_scopes=(
                "https://www.googleapis.com/auth/calendar.readonly",
                "https://www.googleapis.com/auth/gmail.readonly",
                "openid",
            ),
            credentials={
                "access_token": "synthetic-access-token",
                "refresh_token": SECRET_CANARY,
                "token_type": "Bearer",
            },
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        )

    def revoke(self, credentials):
        assert credentials["refresh_token"] == SECRET_CANARY
        self.revoked = True


class FakeComposio:
    provider_id = "composio"
    managed_connection = True

    def __init__(self):
        self.revoked = False
        self.completed = 0

    def start_connection(self, *, user_id, connector_id, state):
        assert user_id == OWNER
        assert connector_id == "google.drive"
        return ManagedOAuthStart(
            authorization_url=(
                "https://connect.composio.dev/link/synthetic?"
                + urlencode({"state": state})
            ),
            connected_account_id="ca_synthetic_drive_123",
        )

    def complete_connection(
        self,
        *,
        user_id,
        connector_id,
        expected_connected_account_id,
        callback_connected_account_id,
        callback_status,
        required_scopes,
    ):
        assert user_id == OWNER
        assert connector_id == "google.drive"
        assert expected_connected_account_id == "ca_synthetic_drive_123"
        assert callback_connected_account_id == expected_connected_account_id
        assert callback_status == "success"
        self.completed += 1
        return OAuthTokens(
            subject_id="googledrive:ca_synthetic_drive_123",
            granted_scopes=tuple(required_scopes),
            credentials={
                "user_id": user_id,
                "connected_account_id": expected_connected_account_id,
                "toolkit": "googledrive",
            },
            expires_at=None,
        )

    def revoke(self, credentials):
        assert set(credentials) == {
            "user_id",
            "connected_account_id",
            "toolkit",
        }
        self.revoked = True


class FakeInvitationEmailSender:
    provider = "synthetic"

    def __init__(self):
        self.messages = []

    def send(self, invitation):
        self.messages.append(invitation)


def request(server, method, path, *, body=None, cookie=None, csrf=None):
    payload = None if body is None else json.dumps(body).encode()
    headers = {"Accept": "application/json"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
        headers["Content-Length"] = str(len(payload))
    if cookie:
        headers["Cookie"] = cookie
    if csrf:
        headers["X-Recall-CSRF"] = csrf
    connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=10)
    connection.request(method, path, body=payload, headers=headers)
    response = connection.getresponse()
    raw = response.read()
    response_headers = response.getheaders()
    connection.close()
    content_type = dict(response_headers).get("Content-Type", "")
    parsed = json.loads(raw) if "application/json" in content_type else raw
    return response.status, response_headers, parsed


def cookie_value(headers, name):
    for key, value in headers:
        if key.casefold() == "set-cookie" and value.startswith(name + "="):
            return value.split(";", 1)[0]
    raise AssertionError(f"{name} cookie missing")


def login(server, token):
    status, headers, body = request(
        server,
        "POST",
        "/admin/api/v1/session",
        body={"token": token},
    )
    assert status == 201 and body["status"] == "authenticated"
    session = cookie_value(headers, "recall_admin_session")
    csrf = cookie_value(headers, "recall_admin_csrf")
    return session + "; " + csrf, csrf.split("=", 1)[1]


def main():
    store = BrainStore(os.environ["RECALL_DATABASE_URL"])
    store.migrate()
    tables = (
        "authorization_audit_events,brain_invitations,external_identity_bindings,"
        "control_audit_events,connector_installations,provider_connections,"
        "oauth_sessions,admin_sessions,admin_credentials,"
        "canonical_source_grants,brain_access_grants,brain_memberships,"
        "brain_spaces,brain_organizations,brain_principals,brain_tenants"
    )
    with store.connect() as connection:
        connection.execute(f"TRUNCATE {tables} CASCADE")
    store.provision_brain(
        organization_id="org:personal:e2e-control",
        organization_kind="personal",
        display_name="Synthetic Personal",
        tenant_id=PERSONAL,
        brain_kind="personal",
        slug="personal-control",
        owner_principal_id=OWNER,
    )
    store.provision_brain(
        organization_id="org:company:e2e-control",
        organization_kind="company",
        display_name="Synthetic Company",
        tenant_id=COMPANY,
        brain_kind="company",
        slug="company-control",
        owner_principal_id=OWNER,
    )
    with store.connect() as connection:
        for principal_id in (OUTSIDER, ADMIN):
            connection.execute(
                """INSERT INTO brain_principals(tenant_id,principal_id)
                   VALUES (%s,%s)""",
                (COMPANY, principal_id),
            )
            connection.execute(
                """INSERT INTO brain_access_grants(
                       tenant_id,principal_id,permission
                   ) VALUES (%s,%s,'admin')""",
                (COMPANY, principal_id),
            )
        connection.execute(
            """INSERT INTO brain_memberships(
                   organization_id,principal_id,role
               ) VALUES ('org:company:e2e-control',%s,'admin')""",
            (ADMIN,),
        )

    provider = FakeGoogle()
    composio = FakeComposio()
    invitation_email_sender = FakeInvitationEmailSender()
    box = SecretBox(b"e" * 32)
    control = ControlPlane(
        store,
        box,
        {"google": provider, "composio": composio},
        invitation_email_sender=invitation_email_sender,
        mcp_resource_uri="https://recall.synthetic.invalid/mcp",
    )
    owner_token = control.create_admin_token(
        "owner-control-e2e",
        principal_id=OWNER,
    )["token"]
    outsider_token = control.create_admin_token(
        "outsider-control-e2e",
        principal_id=OUTSIDER,
    )["token"]
    admin_token = control.create_admin_token(
        "admin-control-e2e",
        principal_id=ADMIN,
    )["token"]
    previous = {
        key: os.environ.get(key)
        for key in (
            "RECALL_ADMIN_WEB_ENABLED",
            "RECALL_AUTH_REQUIRED",
            "RECALL_HTTP_PROFILE",
        )
    }
    os.environ.update(
        {
            "RECALL_ADMIN_WEB_ENABLED": "1",
            "RECALL_AUTH_REQUIRED": "1",
            "RECALL_HTTP_PROFILE": "public-mcp",
        }
    )
    Handler.store = store
    Handler.control_plane = control
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        status, headers, page = request(server, "GET", "/admin")
        assert status == 200
        assert b"Recall Switchboard" in page
        assert b"Choose what your" in page
        assert b"Company access" in page
        assert b"Open a door" in page
        assert b"Admin access key" in page
        security = dict(headers)
        assert security["X-Frame-Options"] == "DENY"
        assert security["Content-Security-Policy"].startswith("default-src 'none'")

        status, _, stylesheet = request(server, "GET", "/admin/assets/admin.css")
        assert status == 200 and b"--acid: #c8ff52" in stylesheet
        status, _, script = request(server, "GET", "/admin/assets/admin.js")
        assert status == 200 and b"/mcp/brains/" in script
        assert b"navigator.clipboard.writeText" in script
        assert b'left.brain_kind === "personal" ? 0 : 1' in script
        assert b"Company brains are shared." in script
        status, _, denied = request(server, "GET", "/admin/api/v1/state")
        assert status == 401 and denied["error"] == "admin_session_invalid"
        status, _, denied = request(
            server,
            "POST",
            "/admin/api/v1/session",
            body={"token": "not-a-real-key"},
        )
        assert status == 401 and denied["error"] == "admin_auth_invalid"

        owner_cookie, owner_csrf = login(server, owner_token)
        outsider_cookie, outsider_csrf = login(server, outsider_token)
        admin_cookie, admin_csrf = login(server, admin_token)
        status, _, initial = request(
            server, "GET", "/admin/api/v1/state", cookie=owner_cookie
        )
        assert status == 200
        assert len(initial["brains"]) == 2
        assert initial["providers"] == [
            {"id": "composio", "status": "available"},
            {"id": "google", "status": "available"},
        ]
        assert {item["connector_id"] for item in initial["catalog"]}.issuperset(
            {"google.gmail", "google.calendar", "custom.webhook"}
        )
        rendered = json.dumps(initial)
        assert SECRET_CANARY not in rendered
        assert "token_sha256" not in rendered
        assert initial["invitations"] == []

        invitation_body = {
            "tenant_id": COMPANY,
            "email": "Teammate@Example.com",
            "role": "member",
        }
        status, _, denied = request(
            server,
            "POST",
            "/admin/api/v1/invitations",
            body=invitation_body,
            cookie=owner_cookie,
        )
        assert status == 403 and denied["error"] == "admin_csrf_invalid"
        status, _, denied = request(
            server,
            "POST",
            "/admin/api/v1/invitations",
            body={**invitation_body, "tenant_id": PERSONAL},
            cookie=owner_cookie,
            csrf=owner_csrf,
        )
        assert status == 403 and denied["error"] == "brain_admin_forbidden"
        status, _, denied = request(
            server,
            "POST",
            "/admin/api/v1/invitations",
            body=invitation_body,
            cookie=outsider_cookie,
            csrf=outsider_csrf,
        )
        assert status == 403 and denied["error"] == "brain_admin_forbidden"
        status, _, invitation = request(
            server,
            "POST",
            "/admin/api/v1/invitations",
            body=invitation_body,
            cookie=owner_cookie,
            csrf=owner_csrf,
        )
        assert status == 201
        assert invitation["email"] == "teammate@example.com"
        assert invitation["status"] == "pending"
        assert invitation["delivery"] == {
            "status": "sent",
            "provider": "synthetic",
        }
        assert len(invitation_email_sender.messages) == 1
        delivered = invitation_email_sender.messages[0]
        assert delivered.recipient == "teammate@example.com"
        assert delivered.organization_name == "Synthetic Company"
        assert (
            delivered.brain_url
            == "https://recall.synthetic.invalid/mcp/brains/"
            "tenant:company:e2e-control"
        )
        status, headers, onboarding = request(
            server,
            "GET",
            f"/join/{invitation['id']}",
        )
        assert status == 200
        assert b"npm install -g @openai/codex" in onboarding
        assert b"npm install -g @anthropic-ai/claude-code" in onboarding
        assert b"codex mcp login recall-company-control" in onboarding
        assert b"claude mcp login recall-company-control" in onboarding
        assert b"teammate@example.com" not in onboarding
        security = dict(headers)
        assert security["Referrer-Policy"] == "no-referrer"
        assert security["X-Frame-Options"] == "DENY"
        status, _, denied = request(
            server,
            "POST",
            f"/admin/api/v1/invitations/{invitation['id']}/revoke",
            body={},
            cookie=outsider_cookie,
            csrf=outsider_csrf,
        )
        assert status == 403 and denied["error"] == "brain_admin_forbidden"
        with store.connect() as connection:
            stored = connection.execute(
                """SELECT encrypted_email,email_sha256
                   FROM brain_invitations WHERE id=%s""",
                (invitation["id"],),
            ).fetchone()
            assert b"teammate@example.com" not in bytes(stored["encrypted_email"])
            assert stored["email_sha256"] != "teammate@example.com"
            assert stored["email_sha256"] != hashlib.sha256(
                b"teammate@example.com"
            ).hexdigest()
        status, _, invited_state = request(
            server, "GET", "/admin/api/v1/state", cookie=owner_cookie
        )
        assert status == 200
        assert invited_state["invitations"] == [
            {
                "id": invitation["id"],
                "tenant_id": COMPANY,
                "email": "teammate@example.com",
                "role": "member",
                "status": "pending",
                "expires_at": invitation["expires_at"],
            }
        ]
        status, _, revoked = request(
            server,
            "POST",
            f"/admin/api/v1/invitations/{invitation['id']}/revoke",
            body={},
            cookie=owner_cookie,
            csrf=owner_csrf,
        )
        assert status == 200 and revoked["status"] == "revoked"
        status, _, hidden = request(
            server,
            "GET",
            f"/join/{invitation['id']}",
        )
        assert status == 404 and hidden["error"] == "not found"
        status, _, denied = request(
            server,
            "POST",
            "/admin/api/v1/invitations",
            body={
                "tenant_id": COMPANY,
                "email": "escalation@example.com",
                "role": "admin",
            },
            cookie=admin_cookie,
            csrf=admin_csrf,
        )
        assert status == 403 and denied["error"] == "brain_role_forbidden"
        status, _, delegated = request(
            server,
            "POST",
            "/admin/api/v1/invitations",
            body={
                "tenant_id": COMPANY,
                "email": "member@example.com",
                "role": "member",
            },
            cookie=admin_cookie,
            csrf=admin_csrf,
        )
        assert status == 201 and delegated["role"] == "member"
        assert delegated["delivery"]["status"] == "sent"
        assert len(invitation_email_sender.messages) == 2
        status, _, revoked = request(
            server,
            "POST",
            f"/admin/api/v1/invitations/{delegated['id']}/revoke",
            body={},
            cookie=admin_cookie,
            csrf=admin_csrf,
        )
        assert status == 200 and revoked["status"] == "revoked"

        device_body = {
            "connector_id": "local.codex",
            "tenant_id": PERSONAL,
            "device_id": "mac-synthetic-e2e",
            "source_id": "codex:mac:synthetic-e2e",
            "privacy_mode": "scrub",
            "selectors": {},
        }
        status, _, denied = request(
            server,
            "POST",
            "/admin/api/v1/device/installations",
            body=device_body,
            cookie=owner_cookie,
        )
        assert status == 403 and denied["error"] == "admin_csrf_invalid"
        outsider_device = {**device_body, "tenant_id": PERSONAL}
        status, _, denied = request(
            server,
            "POST",
            "/admin/api/v1/device/installations",
            body=outsider_device,
            cookie=outsider_cookie,
            csrf=outsider_csrf,
        )
        assert status == 403 and denied["error"] == "device_brain_forbidden"
        status, _, device_route = request(
            server,
            "POST",
            "/admin/api/v1/device/installations",
            body=device_body,
            cookie=owner_cookie,
            csrf=owner_csrf,
        )
        assert status == 201 and device_route["state"] == "enabled"
        assert store.authenticate_bearer(device_route["token"], "write")
        device_action = (
            "/admin/api/v1/installations/"
            + device_route["installation_id"]
            + "/actions"
        )
        status, _, paused_device = request(
            server,
            "POST",
            device_action,
            body={"action": "pause"},
            cookie=owner_cookie,
            csrf=owner_csrf,
        )
        assert status == 200 and paused_device["state"] == "paused"
        assert store.authenticate_bearer(device_route["token"], "write") is None
        status, _, resumed_device = request(
            server,
            "POST",
            device_action,
            body={"action": "resume"},
            cookie=owner_cookie,
            csrf=owner_csrf,
        )
        assert status == 200 and resumed_device["state"] == "enabled"
        reroute_body = {**device_body, "tenant_id": COMPANY}
        status, _, rerouted = request(
            server,
            "POST",
            "/admin/api/v1/device/installations",
            body=reroute_body,
            cookie=owner_cookie,
            csrf=owner_csrf,
        )
        assert status == 201 and rerouted["replaced"] == 1
        assert store.authenticate_bearer(device_route["token"], "write") is None
        credential = store.authenticate_bearer(rerouted["token"], "write")
        assert credential and credential["tenant_id"] == COMPANY
        rerouted_action = (
            "/admin/api/v1/installations/" + rerouted["installation_id"] + "/actions"
        )
        status, _, revoked_device = request(
            server,
            "POST",
            rerouted_action,
            body={"action": "revoke"},
            cookie=owner_cookie,
            csrf=owner_csrf,
        )
        assert status == 200 and revoked_device["state"] == "revoked"
        assert store.authenticate_bearer(rerouted["token"], "write") is None
        with store.connect() as connection:
            connection.execute(
                """DELETE FROM collector_credentials
                   WHERE installation_id IN (
                       SELECT id FROM connector_installations
                       WHERE device_id='mac-synthetic-e2e'
                   )"""
            )
            connection.execute(
                """DELETE FROM connector_installations
                   WHERE device_id='mac-synthetic-e2e'"""
            )

        routes = [
            {
                "connector_id": "google.gmail",
                "tenant_id": PERSONAL,
                "privacy_mode": "scrub",
                "selectors": {},
            },
            {
                "connector_id": "google.calendar",
                "tenant_id": COMPANY,
                "privacy_mode": "scrub",
                "selectors": {},
            },
        ]
        status, _, denied = request(
            server,
            "POST",
            "/admin/api/v1/oauth/start",
            body={"provider": "google", "routes": routes},
            cookie=owner_cookie,
        )
        assert status == 403 and denied["error"] == "admin_csrf_invalid"
        status, _, started = request(
            server,
            "POST",
            "/admin/api/v1/oauth/start",
            body={"provider": "google", "routes": routes},
            cookie=owner_cookie,
            csrf=owner_csrf,
        )
        assert status == 201
        authorization = urlsplit(started["authorization_url"])
        authorization_query = parse_qs(authorization.query)
        assert authorization.scheme == "https"
        assert authorization_query["code_challenge_method"] == ["S256"]
        assert len(authorization_query["code_challenge"][0]) == 43
        state = authorization_query["state"][0]

        outsider_route = [
            {
                "connector_id": "google.gmail",
                "tenant_id": PERSONAL,
                "privacy_mode": "scrub",
                "selectors": {},
            }
        ]
        status, _, denied = request(
            server,
            "POST",
            "/admin/api/v1/oauth/start",
            body={"provider": "google", "routes": outsider_route},
            cookie=outsider_cookie,
            csrf=outsider_csrf,
        )
        assert status == 403 and denied["error"] == "oauth_brain_forbidden"

        callback = "/admin/oauth/callback/google?" + urlencode(
            {"state": state, "code": "synthetic-code"}
        )
        status, headers, raw = request(server, "GET", callback)
        assert status == 303 and dict(headers)["Location"] == "/admin?oauth=connected"
        assert raw == b""
        assert provider.last_verifier is not None
        status, _, denied = request(server, "GET", callback)
        assert status == 403 and denied["error"] == "oauth_state_invalid"

        status, _, reauth = request(
            server,
            "POST",
            "/admin/api/v1/oauth/start",
            body={"provider": "google", "routes": [routes[1]]},
            cookie=owner_cookie,
            csrf=owner_csrf,
        )
        assert status == 201
        reauth_state = parse_qs(urlsplit(reauth["authorization_url"]).query)["state"][0]
        with store.connect() as connection:
            connection.execute(
                """UPDATE brain_access_grants SET permission='read'
                   WHERE tenant_id=%s AND principal_id=%s""",
                (COMPANY, OWNER),
            )
        denied_callback = "/admin/oauth/callback/google?" + urlencode(
            {"state": reauth_state, "code": "synthetic-code"}
        )
        status, _, denied = request(server, "GET", denied_callback)
        assert status == 403 and denied["error"] == "oauth_brain_forbidden"
        assert provider.exchanges == 1
        with store.connect() as connection:
            connection.execute(
                """UPDATE brain_access_grants SET permission='owner'
                   WHERE tenant_id=%s AND principal_id=%s""",
                (COMPANY, OWNER),
            )

        status, _, connected = request(
            server, "GET", "/admin/api/v1/state", cookie=owner_cookie
        )
        assert status == 200
        assert len(connected["connections"]) == 1
        assert len(connected["installations"]) == 2
        assert {
            (item["tenant_id"], item["connector_id"], item["state"])
            for item in connected["installations"]
        } == {
            (PERSONAL, "google.gmail", "enabled"),
            (COMPANY, "google.calendar", "enabled"),
        }
        assert SECRET_CANARY not in json.dumps(connected)

        composio_route = [
            {
                "connector_id": "google.drive",
                "tenant_id": PERSONAL,
                "privacy_mode": "scrub",
                "selectors": {},
            }
        ]
        status, _, denied = request(
            server,
            "POST",
            "/admin/api/v1/oauth/start",
            body={"provider": "composio", "routes": composio_route + routes[:1]},
            cookie=owner_cookie,
            csrf=owner_csrf,
        )
        assert status == 400 and denied["error"] == "oauth_routes_invalid"
        status, _, hosted = request(
            server,
            "POST",
            "/admin/api/v1/oauth/start",
            body={"provider": "composio", "routes": composio_route},
            cookie=owner_cookie,
            csrf=owner_csrf,
        )
        assert status == 201
        hosted_state = parse_qs(urlsplit(hosted["authorization_url"]).query)["state"][0]
        invalid_hosted_callback = "/admin/oauth/callback/composio?" + urlencode(
            {
                "state": hosted_state,
                "status": "success",
                "connected_account_id": "ca_synthetic_drive_123",
                "unexpected": "blocked",
            }
        )
        status, _, denied = request(server, "GET", invalid_hosted_callback)
        assert status == 400 and denied["error"] == "oauth_callback_invalid"
        hosted_callback = "/admin/oauth/callback/composio?" + urlencode(
            {
                "state": hosted_state,
                "status": "success",
                "connected_account_id": "ca_synthetic_drive_123",
            }
        )
        status, headers, raw = request(server, "GET", hosted_callback)
        assert status == 303 and dict(headers)["Location"] == "/admin?oauth=connected"
        assert raw == b"" and composio.completed == 1
        status, _, denied = request(server, "GET", hosted_callback)
        assert status == 403 and denied["error"] == "oauth_state_invalid"

        status, _, with_hosted = request(
            server, "GET", "/admin/api/v1/state", cookie=owner_cookie
        )
        assert status == 200
        assert len(with_hosted["connections"]) == 2
        assert len(with_hosted["installations"]) == 3
        assert SECRET_CANARY not in json.dumps(with_hosted)

        first = connected["installations"][0]
        action_path = f"/admin/api/v1/installations/{first['id']}/actions"
        status, _, paused = request(
            server,
            "POST",
            action_path,
            body={"action": "pause"},
            cookie=owner_cookie,
            csrf=owner_csrf,
        )
        assert status == 200 and paused["state"] == "paused"
        status, _, replay = request(
            server,
            "POST",
            action_path,
            body={"action": "pause"},
            cookie=owner_cookie,
            csrf=owner_csrf,
        )
        assert status == 200 and replay["replay"]

        connection_id = next(
            item["id"]
            for item in with_hosted["connections"]
            if item["provider"] == "google"
        )
        status, _, revoked = request(
            server,
            "POST",
            f"/admin/api/v1/connections/{connection_id}/revoke",
            body={},
            cookie=owner_cookie,
            csrf=owner_csrf,
        )
        assert status == 200
        assert revoked["installations_revoked"] == 2
        assert provider.revoked
        with store.connect() as connection:
            row = connection.execute(
                """SELECT encrypted_credentials,encryption_key_id,status
                   FROM provider_connections WHERE id=%s""",
                (connection_id,),
            ).fetchone()
            consumed = connection.execute(
                """SELECT count(*) AS count FROM oauth_sessions
                   WHERE consumed_at IS NOT NULL"""
            ).fetchone()["count"]
        assert row["status"] == "revoked"
        assert row["encryption_key_id"] == box.key_id
        assert (
            box.open(
                bytes(row["encrypted_credentials"]),
                purpose=f"provider-connection:{connection_id}",
            )
            == {}
        )
        assert consumed == 3

        hosted_connection_id = next(
            item["id"]
            for item in with_hosted["connections"]
            if item["provider"] == "composio"
        )
        status, _, hosted_revoked = request(
            server,
            "POST",
            f"/admin/api/v1/connections/{hosted_connection_id}/revoke",
            body={},
            cookie=owner_cookie,
            csrf=owner_csrf,
        )
        assert status == 200
        assert hosted_revoked["installations_revoked"] == 1
        assert composio.revoked
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        Handler.control_plane = None
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        store.close()

    print(
        json.dumps(
            {
                "status": "pass",
                "brains": 2,
                "routes": 5,
                "device_token_rotations": 1,
                "paused_device_authority_denials": 1,
                "cross_brain_admin_writes": 0,
                "mid_flow_authority_revocations": 1,
                "csrf_denials": 1,
                "oauth_state_replays": 0,
                "hosted_account_binding_replays": 0,
                "plaintext_secret_renders": 0,
                "encrypted_secret_wiped_on_revoke": True,
                "browser_assets": 3,
                "invitation_emails": len(invitation_email_sender.messages),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
