"""One harness process per binding, many turns over stream-json.

Claude Code: ``claude -p --resume <sid> --input-format stream-json --output-format
stream-json`` accepts user turns on stdin and emits one ``result`` event per turn
with the session id; verified on greppy3 2026-09-06 (two turns, same id, turn 2
recalled turn 1, 1.4 s). The process idles between turns and is dropped after
``idle_seconds``; the next turn relaunches with ``--resume``.

Codex: v1 keeps one ``codex exec resume`` process per turn behind the same
``run_turn`` interface. The app-server JSON-RPC driver replaces it once its
turn/completed path is proven with MCP servers disabled.

The driver owns no durability of its own: the Store records the attempt and its
terminal state; the reply text is written to a file under ``work_root`` and the
Store keeps the absolute path as ``response_ref``.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import shutil
import subprocess  # nosec B404 - fixed argv, no shell
import threading
import time
from pathlib import Path
from typing import Any, Callable

from .store import Store, is_no_reply

logger = logging.getLogger("hermes_plugins.tether_next.session_driver")

LaunchPlan = Callable[[list[str], Path, dict[str, str], Any], tuple[list[str], dict[str, str], str]]


class SessionProcess:
    """A live ``claude -p`` stream-json child for one binding.

    A reader thread feeds parsed events into a queue: select() on a text-mode
    pipe is unreliable because readline() buffers past the line it returns.
    """

    def __init__(self, binding_id: str, process: subprocess.Popen, session_id: str):
        self.binding_id = binding_id
        self.process = process
        self.session_id = session_id
        self.last_used = time.monotonic()
        self.lock = threading.Lock()
        self._events: queue.Queue[dict[str, Any] | None] = queue.Queue()
        self._reader = threading.Thread(target=self._pump, name=f"tether-session-{binding_id[:8]}", daemon=True)
        self._reader.start()

    def _pump(self) -> None:
        stdout = self.process.stdout
        if stdout is None:  # pragma: no cover - Popen always opens it here
            self._events.put(None)
            return
        try:
            for line in stdout:
                try:
                    event = json.loads(line)
                except ValueError:
                    continue
                if isinstance(event, dict):
                    self._events.put(event)
        except (OSError, ValueError):
            pass
        finally:
            self._events.put(None)  # EOF sentinel

    def alive(self) -> bool:
        return self.process.poll() is None

    def send(self, text: str) -> None:
        if self.process.stdin is None:  # pragma: no cover
            raise OSError("session process has no stdin")
        frame = {"type": "user", "message": {"role": "user", "content": [{"type": "text", "text": text}]}}
        self.process.stdin.write(json.dumps(frame) + "\n")
        self.process.stdin.flush()

    def read_result(self, timeout: float) -> dict[str, Any] | None:
        """Consume events until a ``result``; None on timeout or process exit."""
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            try:
                event = self._events.get(timeout=min(remaining, 1.0))
            except queue.Empty:
                continue
            if event is None:
                return None
            if event.get("type") == "result":
                return event

    def stderr_tail(self) -> str:
        try:
            if self.process.stderr is not None and not self.alive():
                data = self.process.stderr.read() or ""
                return data.strip().splitlines()[-1][:200] if data.strip() else ""
        except (OSError, ValueError):
            pass
        return ""

    def terminate(self) -> None:
        try:
            if self.process.stdin is not None:
                self.process.stdin.close()
        except (OSError, ValueError):
            pass
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()


class SessionDriver:
    """``run_turn`` drives one attempt to a terminal state and records it in the Store."""

    def __init__(
        self,
        store: Store,
        work_root: Path,
        settings: Any,
        *,
        launch_plan: LaunchPlan,
        child_env: Callable[..., dict[str, str]],
        popen: Callable[..., subprocess.Popen] = subprocess.Popen,
        idle_seconds: float = 900.0,
    ):
        self.store = store
        self.work_root = Path(work_root)
        self.blob_root = self.work_root / "replies"
        self.blob_root.mkdir(parents=True, exist_ok=True)
        self.settings = settings
        self._launch_plan = launch_plan
        self._child_env = child_env
        self._popen = popen
        self.idle_seconds = idle_seconds
        self._sessions: dict[str, SessionProcess] = {}
        self._lock = threading.Lock()

    # -- lifecycle ------------------------------------------------------------------

    def idle_sweep(self) -> int:
        """Drop session processes idle longer than ``idle_seconds``. Returns count."""
        dropped = 0
        now = time.monotonic()
        with self._lock:
            for binding_id, session in list(self._sessions.items()):
                if not session.alive() or now - session.last_used > self.idle_seconds:
                    session.terminate()
                    del self._sessions[binding_id]
                    dropped += 1
        return dropped

    def close_binding(self, binding_id: str) -> None:
        with self._lock:
            session = self._sessions.pop(binding_id, None)
        if session is not None:
            session.terminate()

    def shutdown(self) -> None:
        with self._lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
        for session in sessions:
            session.terminate()

    # -- one turn -------------------------------------------------------------------

    def run_turn(
        self,
        attempt: dict[str, Any],
        context: dict[str, Any],
        prompt: str,
        cwd: Path,
        timeout_seconds: float,
    ) -> dict[str, Any]:
        """Send the prompt to the binding's session and return the terminal result.

        Result: ``{"state": completed_with_response|no_reply|failed, "text": str,
        "error_code": str|None, "session_id": str|None}``. The Store is updated.
        """
        self.idle_sweep()
        attempt_id = attempt["attempt_id"]
        binding_id = attempt["binding_id"]
        source = context.get("source") or {}
        session_id = str(source.get("session_id") or "")
        if context.get("source_kind") == "codex_session":
            return self._run_codex_turn(attempt_id, session_id, prompt, cwd, timeout_seconds)
        try:
            session = self._session_for(binding_id, session_id, cwd)
        except OSError as exc:
            return self._finish(attempt_id, "failed", "", error_code=f"spawn_failed:{type(exc).__name__}")
        with session.lock:
            try:
                session.send(prompt)
            except (OSError, ValueError):
                # the process died between turns: relaunch once and retry
                self.close_binding(binding_id)
                session = self._session_for(binding_id, session_id, cwd)
                session.send(prompt)
            event = session.read_result(timeout_seconds)
            session.last_used = time.monotonic()
        if event is None:
            reason = session.stderr_tail() if not session.alive() else "turn timed out"
            code = "harness_exited" if not session.alive() else "timeout"
            self.close_binding(binding_id)
            return self._finish(attempt_id, "failed", reason, error_code=code)
        text = str(event.get("result") or "")
        if event.get("session_id") and event["session_id"] != session_id:
            logger.warning("tether: session %s answered as %s (fork?)", session_id, event["session_id"])
        if event.get("is_error"):
            self.close_binding(binding_id)
            return self._finish(attempt_id, "failed", text, error_code="harness_error")
        if is_no_reply(text):
            return self._finish(attempt_id, "no_reply", text, session_id=event.get("session_id"))
        return self._finish(attempt_id, "completed_with_response", text, session_id=event.get("session_id"))

    def _session_for(self, binding_id: str, session_id: str, cwd: Path) -> SessionProcess:
        with self._lock:
            session = self._sessions.get(binding_id)
            if session is not None and session.alive():
                return session
            if session is not None:
                self._sessions.pop(binding_id, None)
            binary = shutil.which(self.settings.claude_binary) or self.settings.claude_binary
            command = [
                binary, "-p", "--resume", session_id, "--input-format", "stream-json",
                "--output-format", "stream-json", "--verbose", *self.settings.claude_resume_args,
            ]
            env = self._child_env(passthrough=self.settings.harness_env)
            argv, popen_env, launcher = self._launch_plan(command, cwd, env, self.settings)
            logger.info("tether: session process for %s launcher=%s", binding_id, launcher)
            process = self._popen(  # nosec B603 - fixed argv, no shell
                argv, cwd=str(cwd), env=popen_env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True, bufsize=1, start_new_session=True,
            )
            session = SessionProcess(binding_id, process, session_id)
            self._sessions[binding_id] = session
            return session

    def _run_codex_turn(
        self, attempt_id: str, session_id: str, prompt: str, cwd: Path, timeout_seconds: float,
    ) -> dict[str, Any]:
        binary = shutil.which(self.settings.codex_binary) or self.settings.codex_binary
        command = [binary, "exec", "resume", *self.settings.codex_resume_args, session_id, prompt]
        env = self._child_env(passthrough=self.settings.harness_env)
        argv, popen_env, _ = self._launch_plan(command, cwd, env, self.settings)
        try:
            completed = subprocess.run(  # nosec B603 - fixed argv, no shell
                argv, cwd=str(cwd), env=popen_env, capture_output=True, text=True,
                timeout=timeout_seconds, stdin=subprocess.DEVNULL,
            )
        except subprocess.TimeoutExpired:
            return self._finish(attempt_id, "failed", "turn timed out", error_code="timeout")
        except OSError as exc:
            return self._finish(attempt_id, "failed", str(exc), error_code="spawn_failed")
        text = completed.stdout.strip()
        if completed.returncode != 0:
            tail = (completed.stderr.strip().splitlines() or [text or ""])[-1][:200]
            return self._finish(attempt_id, "failed", tail, error_code=f"exit_{completed.returncode}")
        if is_no_reply(text):
            return self._finish(attempt_id, "no_reply", text)
        return self._finish(attempt_id, "completed_with_response", text)

    def _finish(
        self, attempt_id: str, state: str, text: str, *, error_code: str | None = None,
        session_id: str | None = None,
    ) -> dict[str, Any]:
        response_ref: str | None = None
        if state == "completed_with_response":
            path = self.blob_root / f"{attempt_id}.txt"
            path.write_text(text, encoding="utf-8")
            os.chmod(path, 0o600)
            response_ref = str(path)
        elif state == "failed" and text:
            path = self.blob_root / f"{attempt_id}.stderr"
            path.write_text(text, encoding="utf-8")
            os.chmod(path, 0o600)
        self.store.finish_attempt(attempt_id, state=state, response_ref=response_ref, error_code=error_code)
        return {"state": state, "text": text, "error_code": error_code, "session_id": session_id,
                "response_ref": response_ref}

    # ActiveSlice reads failure reasons from the attempt directory of the old driver;
    # keep the same shape so the notice code needs no branch.
    def _attempt_dir(self, attempt_id: str) -> Path:
        return self.blob_root

    def failure_reason(self, attempt_id: str) -> str:
        path = self.blob_root / f"{attempt_id}.stderr"
        try:
            return path.read_text(encoding="utf-8", errors="replace").strip().splitlines()[-1][:200]
        except (OSError, IndexError):
            return ""
