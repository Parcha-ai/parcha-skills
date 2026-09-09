"""Minimal, content-free MCP client used by the systems card probes.

Every call records wall-clock latency and HTTP outcome. The client never logs
request or response bodies; probes decide which content-free fields to keep.
"""
from __future__ import annotations

import json
import os
import stat
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

MCP_PROTOCOL_VERSION = "2025-11-25"
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
DEFAULT_TIMEOUT_SECONDS = 90.0


class McpClientError(RuntimeError):
    """A transport or protocol failure; the message is content-free."""


@dataclass
class CallOutcome:
    tool: str
    ok: bool
    elapsed_ms: float
    http_status: int | None = None
    error: str | None = None
    result: dict[str, Any] | None = None
    response_bytes: int = 0
    attempts: int = 1
    started_at: float = field(default_factory=time.time)


def _private_json(path: Path, *, max_bytes: int = 4096) -> dict:
    info = os.lstat(path)
    if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) & 0o077:
        raise McpClientError("private file must be a mode-0600 regular file")
    raw = path.read_bytes()
    if len(raw) > max_bytes:
        raise McpClientError("private file is too large")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise McpClientError("private file is not an object")
    return value


def load_profile(
    *,
    url: str | None = None,
    token_file: str | None = None,
) -> tuple[str, str | None]:
    """Resolve the MCP URL and bearer token from flags, env, or the client profile."""
    profile: dict = {}
    profile_path = Path(os.environ.get("RECALL_CLIENT_CONFIG", "~/.config/recall-brain/client.json")).expanduser()
    if profile_path.exists():
        profile = _private_json(profile_path)
    base = url or os.environ.get("RECALL_URL") or profile.get("url") or ""
    if not isinstance(base, str) or not base.startswith("https://") or not base.rstrip("/").endswith("/mcp"):
        raise McpClientError("an https MCP URL ending in /mcp is required")
    token_path = token_file or os.environ.get("RECALL_TOKEN_FILE") or profile.get("token_file")
    token = None
    if token_path:
        value = _private_json(Path(token_path).expanduser())
        token = value.get("token")
        if not isinstance(token, str) or not token:
            raise McpClientError("token file has no token")
    return base, token


class McpClient:
    def __init__(
        self,
        base: str,
        token: str | None,
        *,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        opener: Any = None,
    ) -> None:
        self.base = base
        self._token = token
        self.timeout_seconds = timeout_seconds
        self._opener = opener or urllib.request.urlopen
        self._next_id = 1
        self.calls: list[CallOutcome] = []

    def _headers(self) -> dict[str, str]:
        headers = {
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
            "MCP-Protocol-Version": MCP_PROTOCOL_VERSION,
        }
        if self._token:
            headers["Authorization"] = "Bearer " + self._token
        return headers

    def _post(self, message: dict) -> tuple[int, dict]:
        request = urllib.request.Request(
            self.base,
            data=json.dumps(message, sort_keys=True).encode(),
            method="POST",
            headers=self._headers(),
        )
        with self._opener(request, timeout=self.timeout_seconds) as response:
            raw = response.read(MAX_RESPONSE_BYTES + 1)
            if len(raw) > MAX_RESPONSE_BYTES:
                raise McpClientError("response too large")
            body = json.loads(raw)
            if not isinstance(body, dict):
                raise McpClientError("non-object response")
            return response.status, body

    def ping(self) -> CallOutcome:
        return self._call("ping", {"jsonrpc": "2.0", "id": self._id(), "method": "ping", "params": {}})

    def call_tool(self, name: str, arguments: dict[str, Any], *, timeout_seconds: float | None = None) -> CallOutcome:
        message = {
            "jsonrpc": "2.0",
            "id": self._id(),
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        }
        return self._call(name, message, timeout_seconds=timeout_seconds)

    def _id(self) -> int:
        value = self._next_id
        self._next_id += 1
        return value

    def _call(self, tool: str, message: dict, *, timeout_seconds: float | None = None) -> CallOutcome:
        started = time.monotonic()
        outcome = CallOutcome(tool=tool, ok=False, elapsed_ms=0.0)
        saved_timeout = self.timeout_seconds
        if timeout_seconds is not None:
            self.timeout_seconds = timeout_seconds
        try:
            status, body = self._post(message)
            outcome.http_status = status
            outcome.response_bytes = len(json.dumps(body))
            error = body.get("error")
            if isinstance(error, dict):
                outcome.error = f"jsonrpc:{error.get('code')}"
            elif body.get("id") != message["id"]:
                outcome.error = "id_mismatch"
            elif message["method"] == "ping":
                outcome.ok = True
                outcome.result = {"status": "ok"}
            else:
                result = body.get("result")
                structured = result.get("structuredContent") if isinstance(result, dict) else None
                if not isinstance(result, dict) or result.get("isError") is True or not isinstance(structured, dict):
                    outcome.error = "tool_error" if isinstance(result, dict) and result.get("isError") else "invalid_result"
                else:
                    outcome.ok = True
                    outcome.result = structured
        except urllib.error.HTTPError as exc:
            outcome.http_status = exc.code
            outcome.error = f"http:{exc.code}"
            try:
                exc.read(MAX_RESPONSE_BYTES + 1)
            except OSError:
                pass
        except (OSError, urllib.error.URLError, json.JSONDecodeError, ValueError, McpClientError) as exc:
            outcome.error = type(exc).__name__
        finally:
            self.timeout_seconds = saved_timeout
            outcome.elapsed_ms = (time.monotonic() - started) * 1000.0
        self.calls.append(outcome)
        return outcome
