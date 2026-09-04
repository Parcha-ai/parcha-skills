"""Minimal Slack Web API client: urllib only, bot token from the gateway env.

Tether needs four calls -- post, thread replies, channel history, identity --
and nothing the Slack SDK adds is worth a dependency the gateway does not
already carry. Every method raises SlackError with Slack's own error string.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

API = "https://slack.com/api/"


class SlackError(RuntimeError):
    def __init__(self, code: str, message: str | None = None):
        super().__init__(message or code)
        self.code = code


class SlackEgress:
    def __init__(self, token: str | None = None, *, timeout: float = 20.0, opener: Any = None):
        self.token = token if token is not None else os.environ.get("SLACK_BOT_TOKEN", "")
        self.timeout = timeout
        self._open = opener or urllib.request.urlopen
        self._identity: dict[str, Any] | None = None

    @property
    def configured(self) -> bool:
        return bool(self.token)

    def _call(self, method: str, payload: dict[str, Any] | None = None, *, get: bool = False) -> dict[str, Any]:
        if not self.token:
            raise SlackError("no_bot_token")
        payload = {k: v for k, v in (payload or {}).items() if v is not None}
        if get:
            url = API + method + ("?" + urllib.parse.urlencode(payload) if payload else "")
            request = urllib.request.Request(url, headers={"Authorization": f"Bearer {self.token}"})
        else:
            request = urllib.request.Request(
                API + method,
                data=json.dumps(payload).encode(),
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json; charset=utf-8",
                },
                method="POST",
            )
        try:
            with self._open(request, timeout=self.timeout) as response:  # nosec B310 - fixed https host
                body = json.loads(response.read().decode("utf-8", errors="replace"))
        except urllib.error.URLError as error:
            raise SlackError("transport", str(error.reason)) from error
        except ValueError as error:
            raise SlackError("malformed_response") from error
        if not isinstance(body, dict) or not body.get("ok"):
            raise SlackError(str((body or {}).get("error") or "unknown_error"))
        return body

    def identity(self) -> dict[str, Any]:
        if self._identity is None:
            body = self._call("auth.test", {})
            self._identity = {
                "team_id": str(body.get("team_id") or ""),
                "user_id": str(body.get("user_id") or ""),
                "user": str(body.get("user") or ""),
            }
        return self._identity

    def post(self, channel_id: str, text: str, *, thread_ts: str | None = None) -> str:
        body = self._call(
            "chat.postMessage",
            {"channel": channel_id, "text": text, "thread_ts": thread_ts},
        )
        return str(body.get("ts") or "")

    def thread_replies(self, channel_id: str, thread_ts: str, *, limit: int = 50) -> list[dict[str, Any]]:
        body = self._call(
            "conversations.replies",
            {"channel": channel_id, "ts": thread_ts, "limit": limit},
            get=True,
        )
        return _messages(body)

    def history(self, channel_id: str, *, limit: int = 20) -> list[dict[str, Any]]:
        body = self._call("conversations.history", {"channel": channel_id, "limit": limit}, get=True)
        return _messages(body)

    def react(self, channel_id: str, message_ts: str, emoji: str) -> bool:
        """Best effort; a reaction is presence, never delivery. Duplicates are fine."""
        try:
            self._call("reactions.add", {"channel": channel_id, "timestamp": message_ts, "name": emoji})
            return True
        except SlackError as error:
            return error.code == "already_reacted"

    def unreact(self, channel_id: str, message_ts: str, emoji: str) -> bool:
        try:
            self._call("reactions.remove", {"channel": channel_id, "timestamp": message_ts, "name": emoji})
            return True
        except SlackError as error:
            return error.code == "no_reaction"

    def membership(self, channel_id: str) -> str:
        try:
            body = self._call("conversations.info", {"channel": channel_id}, get=True)
        except SlackError as error:
            return "unknown" if error.code == "transport" else "not_member"
        channel = body.get("channel") or {}
        return "member" if channel.get("is_member") else "not_member"


def _messages(body: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for message in body.get("messages", []) or []:
        if not isinstance(message, dict):
            continue
        out.append({
            key: message.get(key)
            for key in ("ts", "text", "user", "bot_id", "thread_ts")
            if message.get(key) is not None
        })
    return out
