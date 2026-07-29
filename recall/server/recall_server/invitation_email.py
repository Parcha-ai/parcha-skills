"""Invitation delivery and onboarding instructions for human MCP access."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from html import escape
import json
import os
import re
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit, urlunsplit
import urllib.request


DESCOPE_INVITE_URL = "https://api.descope.com/v1/mgmt/user/create"
RESEND_EMAIL_URL = "https://api.resend.com/emails"
PROJECT_ID_RE = re.compile(r"P[A-Za-z0-9]{10,63}\Z")
SAFE_NAME_RE = re.compile(r"[^a-z0-9]+")


class InvitationEmailError(RuntimeError):
    """Content-free invitation delivery failure."""


class _RejectRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


@dataclass(frozen=True)
class InvitationEmail:
    invitation_id: str
    recipient: str
    organization_name: str
    brain_slug: str
    role: str
    expires_at: datetime
    brain_url: str
    onboarding_url: str

    @property
    def server_name(self) -> str:
        suffix = SAFE_NAME_RE.sub("-", self.brain_slug.casefold()).strip("-")
        return f"recall-{suffix or 'company'}"[:64]


class InvitationEmailSender(Protocol):
    provider: str

    def send(self, invitation: InvitationEmail) -> None: ...


def _https_url(value: str, *, path: str | None = None) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or (path is not None and parsed.path.rstrip("/") != path)
    ):
        raise InvitationEmailError("invitation_email_configuration_invalid")
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", ""))


def invitation_urls(
    resource_uri: str,
    *,
    tenant_id: str,
    invitation_id: str,
) -> tuple[str, str]:
    resource = _https_url(resource_uri, path="/mcp")
    parsed = urlsplit(resource)
    brain_url = f"{resource}/brains/{quote(tenant_id, safe=':._@+-')}"
    onboarding_url = urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            f"/join/{quote(invitation_id, safe='-')}",
            "",
            "",
        )
    )
    return brain_url, onboarding_url


def installation_commands(invitation: InvitationEmail) -> dict[str, tuple[str, ...]]:
    return {
        "codex": (
            "npm install -g @openai/codex",
            f"codex mcp add {invitation.server_name} --url {invitation.brain_url}",
            f"codex mcp login {invitation.server_name}",
        ),
        "claude": (
            "npm install -g @anthropic-ai/claude-code",
            (
                "claude mcp add --scope user --transport http "
                f"{invitation.server_name} {invitation.brain_url}"
            ),
            f"claude mcp login {invitation.server_name}",
        ),
    }


def invitation_message(invitation: InvitationEmail) -> tuple[str, str, str]:
    commands = installation_commands(invitation)
    expires = invitation.expires_at.strftime("%B %d, %Y at %H:%M UTC").replace(
        " 0", " "
    )
    organization = invitation.organization_name.replace("\r", " ").replace("\n", " ")
    subject = f"Join {organization} on Recall"
    text = f"""You have been invited to the {invitation.organization_name} company brain as {invitation.role}.

Open the setup page:
{invitation.onboarding_url}

Codex
{chr(10).join(commands["codex"])}

Claude Code
{chr(10).join(commands["claude"])}

Sign in with this email address when OAuth opens. The invitation expires {expires}.
"""
    sections = []
    for label, key in (("Codex", "codex"), ("Claude Code", "claude")):
        rendered = "\n".join(escape(command) for command in commands[key])
        sections.append(f"<h2>{label}</h2><pre>{rendered}</pre>")
    html = f"""<!doctype html>
<html lang="en">
<body style="margin:0;background:#eee9d7;color:#121a14;font-family:Arial,sans-serif">
  <main style="max-width:680px;margin:auto;padding:48px 24px">
    <p style="font:700 12px monospace;letter-spacing:.14em">RECALL / COMPANY BRAIN</p>
    <h1 style="font:500 42px Georgia,serif">Join {escape(organization)}</h1>
    <p>You have been invited as <strong>{escape(invitation.role)}</strong>.</p>
    <p><a href="{escape(invitation.onboarding_url, quote=True)}"
       style="display:inline-block;background:#121a14;color:#c8ff52;padding:16px 20px;text-decoration:none">
       Open setup page</a></p>
    {"".join(sections)}
    <p>Sign in with this email address when OAuth opens.</p>
    <p style="color:#657064">Invitation expires {escape(expires)}.</p>
  </main>
</body>
</html>"""
    return subject, text, html


def onboarding_page(invitation: InvitationEmail) -> bytes:
    commands = installation_commands(invitation)
    cards = []
    for number, (label, key) in enumerate(
        (("Codex", "codex"), ("Claude Code", "claude")), start=1
    ):
        rendered = "\n".join(escape(command) for command in commands[key])
        cards.append(
            f"""<section>
  <span>0{number}</span>
  <h2>{label}</h2>
  <pre>{rendered}</pre>
</section>"""
        )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="color-scheme" content="light">
  <title>Join {escape(invitation.organization_name)} on Recall</title>
  <style>
    :root {{ --paper:#eee9d7;--ink:#121a14;--acid:#c8ff52;--blue:#1c55ff; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0;background:var(--paper);color:var(--ink);font-family:Arial,sans-serif; }}
    main {{ max-width:1040px;margin:auto;padding:clamp(40px,8vw,100px) 24px; }}
    .eyebrow,section>span {{ font:800 11px monospace;letter-spacing:.16em; }}
    h1 {{ margin:24px 0 18px;font:500 clamp(48px,8vw,100px)/.88 Georgia,serif;letter-spacing:-.05em; }}
    h1 em {{ color:var(--blue);font-weight:400; }}
    .lede {{ max-width:620px;font-size:20px;line-height:1.5;margin-bottom:54px; }}
    .grid {{ display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:1px;background:var(--ink);border:1px solid var(--ink); }}
    section {{ min-width:0;background:#faf7eb;padding:32px; }}
    section>span {{ color:var(--blue); }}
    h2 {{ font:500 34px Georgia,serif; }}
    pre {{ overflow:auto;background:var(--ink);color:var(--acid);padding:20px;font:13px/1.65 monospace;white-space:pre-wrap;word-break:break-word; }}
    footer {{ margin-top:32px;color:#657064;font-size:14px; }}
  </style>
</head>
<body>
  <main>
    <p class="eyebrow">RECALL / VERIFIED COMPANY ACCESS</p>
    <h1>Join {escape(invitation.organization_name)}.<br><em>Remember together.</em></h1>
    <p class="lede">Run one setup block, then sign in with the same email address that received the invitation. OAuth activates access automatically.</p>
    <div class="grid">{"".join(cards)}</div>
    <footer>Access is limited to this company brain. No bearer token is copied into either client.</footer>
  </main>
</body>
</html>""".encode()


class DescopeInvitationEmailSender:
    provider = "descope"

    def __init__(
        self,
        *,
        project_id: str,
        management_key: str,
        endpoint: str = DESCOPE_INVITE_URL,
        timeout_seconds: float = 10.0,
        opener=None,
    ):
        if (
            not PROJECT_ID_RE.fullmatch(project_id)
            or not management_key
            or len(management_key) > 8192
        ):
            raise InvitationEmailError("invitation_email_configuration_invalid")
        self.project_id = project_id
        self.management_key = management_key
        self.endpoint = _https_url(endpoint)
        self.timeout_seconds = timeout_seconds
        self.opener = opener or urllib.request.build_opener(_RejectRedirect())

    def send(self, invitation: InvitationEmail) -> None:
        body = {
            "loginId": invitation.recipient,
            "email": invitation.recipient,
            "invite": True,
            "inviteUrl": invitation.onboarding_url,
            "sendMail": True,
            "sendSMS": False,
            "roleNames": [],
            "userTenants": [],
            "test": False,
        }
        request = urllib.request.Request(
            self.endpoint,
            data=json.dumps(body, separators=(",", ":")).encode(),
            headers={
                "Authorization": (f"Bearer {self.project_id}:{self.management_key}"),
                "Content-Type": "application/json",
                "User-Agent": "recall-core/1",
                "X-Descope-Project-Id": self.project_id,
            },
            method="POST",
        )
        try:
            with self.opener.open(request, timeout=self.timeout_seconds) as response:
                if not 200 <= response.status < 300:
                    raise InvitationEmailError("invitation_email_delivery_failed")
        except (HTTPError, URLError, TimeoutError, OSError):
            raise InvitationEmailError("invitation_email_delivery_failed") from None


class ResendInvitationEmailSender:
    provider = "resend"

    def __init__(
        self,
        *,
        api_key: str,
        from_address: str,
        endpoint: str = RESEND_EMAIL_URL,
        timeout_seconds: float = 10.0,
        opener=None,
    ):
        if (
            not api_key
            or len(api_key) > 8192
            or not from_address
            or len(from_address) > 512
            or "\r" in from_address
            or "\n" in from_address
        ):
            raise InvitationEmailError("invitation_email_configuration_invalid")
        self.api_key = api_key
        self.from_address = from_address
        self.endpoint = _https_url(endpoint)
        self.timeout_seconds = timeout_seconds
        self.opener = opener or urllib.request.build_opener(_RejectRedirect())

    def send(self, invitation: InvitationEmail) -> None:
        subject, text, html = invitation_message(invitation)
        request = urllib.request.Request(
            self.endpoint,
            data=json.dumps(
                {
                    "from": self.from_address,
                    "to": [invitation.recipient],
                    "subject": subject,
                    "text": text,
                    "html": html,
                },
                separators=(",", ":"),
            ).encode(),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "Idempotency-Key": f"recall-invitation/{invitation.invitation_id}",
                "User-Agent": "recall-core/1",
            },
            method="POST",
        )
        try:
            with self.opener.open(request, timeout=self.timeout_seconds) as response:
                if not 200 <= response.status < 300:
                    raise InvitationEmailError("invitation_email_delivery_failed")
        except (HTTPError, URLError, TimeoutError, OSError):
            raise InvitationEmailError("invitation_email_delivery_failed") from None


def sender_from_env() -> InvitationEmailSender | None:
    provider = os.environ.get("RECALL_INVITATION_EMAIL_PROVIDER", "").casefold().strip()
    if provider in {"", "disabled"}:
        return None
    _https_url(os.environ.get("RECALL_MCP_RESOURCE_URI", ""), path="/mcp")
    if provider == "descope":
        return DescopeInvitationEmailSender(
            project_id=os.environ.get("RECALL_DESCOPE_PROJECT_ID", ""),
            management_key=os.environ.get("RECALL_DESCOPE_MGMT_KEY", ""),
        )
    if provider == "resend":
        return ResendInvitationEmailSender(
            api_key=os.environ.get("RECALL_INVITATION_EMAIL_API_KEY", ""),
            from_address=os.environ.get("RECALL_INVITATION_EMAIL_FROM", ""),
        )
    raise InvitationEmailError("invitation_email_provider_invalid")


__all__ = [
    "DescopeInvitationEmailSender",
    "InvitationEmail",
    "InvitationEmailError",
    "InvitationEmailSender",
    "ResendInvitationEmailSender",
    "installation_commands",
    "invitation_message",
    "invitation_urls",
    "onboarding_page",
    "sender_from_env",
]
