"""Minimal Slack Web API client: urllib only, bot token from the gateway env.

Tether needs four calls -- post, thread replies, channel history, identity --
and nothing the Slack SDK adds is worth a dependency the gateway does not
already carry. Every method raises SlackError with Slack's own error string.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
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

    def upload(
        self, channel_id: str, path: str, *, thread_ts: str | None = None,
        initial_comment: str | None = None, title: str | None = None,
    ) -> dict[str, Any]:
        """Post a file natively (files.getUploadURLExternal → upload → completeUploadExternal).

        Returns ``{"file_id", "ts"}``; ``ts`` is the share message in the channel or
        thread, the same thing a person gets when they drag a file into Slack.
        """
        file_path = Path(path).expanduser()
        try:
            data = file_path.read_bytes()
        except OSError as error:
            raise SlackError("file_unreadable", f"{file_path}: {error.strerror or error}") from error
        if not data:
            raise SlackError("file_empty", str(file_path))
        ticket = self._call(
            "files.getUploadURLExternal", {"filename": file_path.name, "length": len(data)}, get=True,
        )
        upload_url = str(ticket.get("upload_url") or "")
        file_id = str(ticket.get("file_id") or "")
        if not upload_url or not file_id:
            raise SlackError("upload_url_missing")
        request = urllib.request.Request(
            upload_url, data=data, method="POST",
            headers={"Content-Type": "application/octet-stream", "Content-Length": str(len(data))},
        )
        try:
            with self._open(request, timeout=max(self.timeout, 300.0)) as response:  # nosec B310 - Slack-issued https URL
                response.read()
        except urllib.error.URLError as error:
            raise SlackError("transport", str(error.reason)) from error
        done = self._call(
            "files.completeUploadExternal",
            {"files": [{"id": file_id, "title": title or file_path.name}], "channel_id": channel_id,
             "thread_ts": thread_ts, "initial_comment": initial_comment},
        )
        ts = ""
        for item in done.get("files") or []:
            shares = (item.get("shares") or {})
            for scope in ("public", "private"):
                for _channel, entries in (shares.get(scope) or {}).items():
                    for entry in entries or []:
                        ts = ts or str(entry.get("ts") or "")
        if not ts:
            ts = self._message_carrying(channel_id, file_id, thread_ts)
        return {"file_id": file_id, "ts": ts}

    def _message_carrying(self, channel_id: str, file_id: str, thread_ts: str | None, attempts: int = 4) -> str:
        """Slack shares appear a moment after completeUploadExternal; find the message by file id."""
        for attempt in range(attempts):
            try:
                if thread_ts:
                    body = self._call("conversations.replies", {"channel": channel_id, "ts": thread_ts, "limit": 20}, get=True)
                else:
                    body = self._call("conversations.history", {"channel": channel_id, "limit": 20}, get=True)
            except SlackError:
                body = {}
            for message in reversed(body.get("messages") or []):
                if any((f or {}).get("id") == file_id for f in message.get("files") or []):
                    return str(message.get("ts") or "")
            if attempt < attempts - 1:
                time.sleep(1.0)
        return ""

    def thread_replies(self, channel_id: str, thread_ts: str, *, limit: int = 2000) -> list[dict[str, Any]]:
        """Up to ``limit`` of the newest replies, returned oldest-first.

        Slack pages a thread from its newest reply backwards whatever ``oldest``
        says (checked live 2026-09-10 on a 600-message thread), so the cursor
        is followed until ``limit`` and the result is sorted. One page of 50
        was what a lead saw of that build thread: every real delivery had
        scrolled out behind status notices.
        """
        out: list[dict[str, Any]] = []
        cursor: str | None = None
        seen: set[str] = set()
        while len(out) < limit:
            params: dict[str, Any] = {"channel": channel_id, "ts": thread_ts,
                                      "limit": min(200, limit - len(out))}
            if cursor:
                params["cursor"] = cursor
            body = self._call("conversations.replies", params, get=True)
            for message in _messages(body):
                if message.get("ts") in seen:
                    continue
                seen.add(str(message.get("ts")))
                out.append(message)
            cursor = str(((body.get("response_metadata") or {}).get("next_cursor")) or "")
            if not body.get("has_more") or not cursor:
                break
        out.sort(key=lambda m: float(m.get("ts") or 0))
        return out

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
        entry = {
            key: message.get(key)
            for key in ("ts", "text", "user", "bot_id", "thread_ts")
            if message.get(key) is not None
        }
        files = [
            {k: f.get(k) for k in ("id", "name", "permalink", "size") if f.get(k) is not None}
            for f in (message.get("files") or []) if isinstance(f, dict)
        ]
        if files:
            entry["files"] = files
        out.append(entry)
    return out
