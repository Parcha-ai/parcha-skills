"""Herdr as a place where Tether-spawned sessions live.

Herdr (https://herdr.dev) is the terminal workspace manager the team runs their coding agents
in. Its ``herdr`` CLI prints JSON over a per-session Unix socket. This module is the thin
client Tether needs: open a tab in a workspace, start a harness in it, learn which native
session it became, submit a prompt and wait for it to settle, read a blocking dialog, answer
it with keys. The CLI is the contract; the socket protocol is never reimplemented here.

Herdr is optional everywhere: ``Herdr.discover()`` returns None when the binary or a live
session is missing, and every caller falls back to what Tether did before.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import socket
import subprocess  # nosec B404 - fixed argv, no shell
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)

AGENT_NAME = re.compile(r"[^a-z0-9_-]+")
READY_STATES = ("idle", "done")


class HerdrError(RuntimeError):
    def __init__(self, code: str, message: str | None = None):
        super().__init__(message or code)
        self.code = code


def agent_name_for(label: str) -> str:
    """A Herdr agent name (``[a-z][a-z0-9_-]{0,31}``) derived from a human label."""
    slug = AGENT_NAME.sub("-", label.lower()).strip("-")
    if not slug or not slug[0].isalpha():
        slug = f"t-{slug}" if slug else "tether"
    return slug[:32].rstrip("-") or "tether"


@dataclass
class Herdr:
    binary: str = "herdr"
    session: str = ""
    socket_path: str = ""
    runner: Callable[..., subprocess.CompletedProcess] = subprocess.run
    env: dict[str, str] | None = None  # None: the process environment at call time, minus HERDR_*

    # -- discovery ------------------------------------------------------------------

    @classmethod
    def discover(cls, *, binary: str = "herdr", session: str = "", home: Path | None = None,
                 runner: Callable[..., subprocess.CompletedProcess] = subprocess.run) -> "Herdr | None":
        """The running Herdr session on this box, or None.

        Order: an explicit ``session``; the session this process runs in (``HERDR_SOCKET_PATH``);
        else the newest live socket under ``~/.config/herdr/sessions/*/herdr.sock`` or the
        default ``~/.config/herdr/herdr.sock``.
        """
        found = shutil.which(binary)
        if not found:
            return None
        root = Path(home or os.environ.get("HERDR_CONFIG_HOME") or Path.home() / ".config" / "herdr")
        candidates: list[tuple[float, str, str]] = []
        if session:
            path = root / "sessions" / session / "herdr.sock"
            if session == "default":
                path = root / "herdr.sock"
            candidates.append((0.0, session, str(path)))
        elif os.environ.get("HERDR_SOCKET_PATH"):
            path = Path(os.environ["HERDR_SOCKET_PATH"])
            candidates.append((0.0, os.environ.get("HERDR_SESSION") or path.parent.name, str(path)))
        else:
            for path in sorted((root / "sessions").glob("*/herdr.sock")) + [root / "herdr.sock"]:
                try:
                    candidates.append((path.stat().st_mtime, "default" if path.parent == root else path.parent.name, str(path)))
                except OSError:
                    continue
            candidates.sort(reverse=True)
        for _, name, path in candidates:
            if _socket_alive(path):
                return cls(binary=found, session=name, socket_path=path, runner=runner)
        return None

    def available(self) -> bool:
        return bool(self.socket_path) and _socket_alive(self.socket_path)

    # -- transport ------------------------------------------------------------------

    def _run(self, *args: str, text_ok: bool = False) -> Any:
        argv = [self.binary]
        if self.session:
            argv += ["--session", self.session]
        argv += list(args)
        env = dict(self.env) if self.env is not None else {k: v for k, v in os.environ.items() if not k.startswith("HERDR_")}
        if self.socket_path:
            env["HERDR_SOCKET_PATH"] = self.socket_path
        done = self.runner(argv, env=env, capture_output=True, text=True, check=False)  # nosec B603
        out = (done.stdout or "").strip()
        if done.returncode != 0:
            raise HerdrError(*_error_of(done.stderr or out or f"exit {done.returncode}"))
        if text_ok:
            return out
        try:
            payload = json.loads(out) if out else {}
        except ValueError as exc:
            raise HerdrError("protocol", f"herdr printed no JSON for {' '.join(args[:2])}") from exc
        if isinstance(payload, dict) and payload.get("error"):
            raise HerdrError(*_error_of(json.dumps(payload["error"])))
        return payload.get("result", payload) if isinstance(payload, dict) else payload

    # -- layout ---------------------------------------------------------------------

    def workspaces(self) -> list[dict[str, Any]]:
        return list(self._run("workspace", "list").get("workspaces") or [])

    def find_workspace(self, label_or_id: str) -> dict[str, Any] | None:
        wanted = label_or_id.strip().lower()
        for ws in self.workspaces():
            if ws.get("workspace_id") == label_or_id or str(ws.get("label") or "").lower() == wanted:
                return ws
        return None

    def workspace_for_cwd(self, cwd: str) -> dict[str, Any] | None:
        """The workspace whose worktree checkout is ``cwd`` (or contains it), if any."""
        path = Path(cwd).resolve()
        best: tuple[int, dict[str, Any]] | None = None
        for ws in self.workspaces():
            checkout = (ws.get("worktree") or {}).get("checkout_path")
            if not checkout:
                continue
            root = Path(checkout).resolve()
            if path == root or root in path.parents:
                depth = len(root.parts)
                if best is None or depth > best[0]:
                    best = (depth, ws)
        return best[1] if best else None

    def workspace_create(self, *, cwd: str, label: str) -> dict[str, Any]:
        result = self._run("workspace", "create", "--cwd", cwd, "--label", label, "--no-focus")
        return {"workspace_id": result["workspace"]["workspace_id"], "tab_id": result["tab"]["tab_id"],
                "pane_id": result["root_pane"]["pane_id"]}

    def tab_create(self, *, workspace_id: str, cwd: str, label: str) -> dict[str, Any]:
        result = self._run("tab", "create", "--workspace", workspace_id, "--cwd", cwd, "--label", label, "--no-focus")
        return {"workspace_id": workspace_id, "tab_id": result["tab"]["tab_id"], "pane_id": result["root_pane"]["pane_id"]}

    def pane_report_metadata(self, pane_id: str, *, source: str, tokens: dict[str, str]) -> None:
        args = ["pane", "report-metadata", pane_id, "--source", source]
        for key, value in tokens.items():
            args += ["--token", f"{key}={value}"]
        self._run(*args)

    # -- agents ---------------------------------------------------------------------

    def agent_start(self, name: str, *, kind: str, pane_id: str, args: tuple[str, ...] = (),
                    timeout_ms: int = 60000) -> dict[str, Any]:
        """Start ``kind`` in ``pane_id``; returns {"status": "ready"|"blocked", "agent": {...}}.

        ``agent_not_ready`` is not a failure: Claude Code asks whether to trust a fresh folder,
        and the name stays addressable for ``agent_read`` / ``send_keys``.
        """
        argv = ["agent", "start", name, "--kind", kind, "--pane", pane_id, "--timeout", str(timeout_ms)]
        if args:
            argv += ["--", *args]
        try:
            result = self._run(*argv)
        except HerdrError as exc:
            if exc.code == "agent_not_ready":
                return {"status": "blocked", "agent": self.agent_get(name)}
            raise
        return {"status": "ready", "agent": result.get("agent") or {}}

    def agent_get(self, target: str) -> dict[str, Any]:
        return dict(self._run("agent", "get", target).get("agent") or {})

    def agent_list(self) -> list[dict[str, Any]]:
        return list(self._run("agent", "list").get("agents") or [])

    def session_id(self, target: str) -> str:
        return str(((self.agent_get(target).get("agent_session") or {}).get("value")) or "")

    def find_agent_by_session(self, session_id: str) -> dict[str, Any] | None:
        for agent in self.agent_list():
            if (agent.get("agent_session") or {}).get("value") == session_id:
                return agent
        return None

    def find_agent_by_name(self, name: str) -> dict[str, Any] | None:
        for agent in self.agent_list():
            if agent.get("name") == name or agent.get("pane_id") == name:
                return agent
        return None

    def agent_prompt(self, target: str, text: str, *, wait: bool = True, timeout_ms: int | None = None) -> dict[str, Any]:
        """Submit ``text`` as one turn; with ``wait`` returns after idle, done, or blocked."""
        argv = ["agent", "prompt", target, text]
        if wait:
            argv.append("--wait")
        if timeout_ms:
            argv += ["--timeout", str(timeout_ms)]
        return dict(self._run(*argv).get("agent") or {})

    def agent_wait(self, target: str, *, until: tuple[str, ...] = (), timeout_ms: int | None = None) -> dict[str, Any]:
        argv = ["agent", "wait", target]
        for state in until:
            argv += ["--until", state]
        if timeout_ms:
            argv += ["--timeout", str(timeout_ms)]
        return dict(self._run(*argv).get("agent") or {})

    def agent_read(self, target: str, *, source: str = "detection", lines: int | None = None) -> str:
        argv = ["agent", "read", target, "--source", source, "--format", "text"]
        if lines:
            argv += ["--lines", str(lines)]
        return str(self._run(*argv, text_ok=True))

    def send_keys(self, target: str, *keys: str) -> None:
        for key in keys:
            self._run("agent", "send-keys", target, key)


def _socket_alive(path: str) -> bool:
    try:
        if not Path(path).is_socket():
            return False
        probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        probe.settimeout(1.0)
        try:
            probe.connect(path)
        finally:
            probe.close()
        return True
    except OSError:
        return False


def _error_of(text: str) -> tuple[str, str]:
    """(code, message) from the CLI's JSON error on stderr, or a generic pair."""
    for line in reversed((text or "").strip().splitlines()):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except ValueError:
            continue
        error = payload.get("error") if isinstance(payload, dict) else None
        if isinstance(error, dict):
            return str(error.get("code") or "herdr_error"), str(error.get("message") or "")[:300]
        if isinstance(payload, dict) and payload.get("code"):
            return str(payload["code"]), str(payload.get("message") or "")[:300]
    return "herdr_error", (text or "").strip()[:300]
