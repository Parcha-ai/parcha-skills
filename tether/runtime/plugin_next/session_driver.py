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

import base64
import json
import logging
import os
import queue
import shutil
import socket
import struct
import subprocess  # nosec B404 - fixed argv, no shell
import threading
import time
from pathlib import Path
from typing import Any, Callable

from .herdr import Herdr, HerdrError
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
        """Consume events until a ``result``; None when the process exits or goes silent.

        ``timeout`` is idle time: every stream event (a delta, a tool call, a tool result)
        restarts it. A turn that keeps producing events is work, however long; only a
        process that has said nothing for ``timeout`` seconds is wedged.
        """
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
            deadline = time.monotonic() + timeout
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


def codex_daemon_socket() -> Path | None:
    """The local ``codex app-server`` daemon's control socket, if one is running here.

    The ChatGPT desktop app and ``codex app-server proxy`` talk to this daemon, and it
    holds the writer lock of every thread it has loaded. Driving a thread through it
    is driving the same session the human sees in the app.
    """
    home = Path(os.environ.get("CODEX_HOME") or Path.home() / ".codex")
    path = home / "app-server-control" / "app-server-control.sock"
    return path if path.is_socket() else None


def ws_frame(data: bytes, *, mask: bool, opcode: int = 0x1) -> bytes:
    n = len(data)
    head = bytes([0x80 | opcode])
    flag = 0x80 if mask else 0
    if n < 126:
        head += bytes([flag | n])
    elif n < 65536:
        head += bytes([flag | 126]) + struct.pack(">H", n)
    else:
        head += bytes([flag | 127]) + struct.pack(">Q", n)
    if not mask:
        return head + data
    key = os.urandom(4)
    return head + key + bytes(b ^ key[i % 4] for i, b in enumerate(data))


def ws_read_frame(sock: socket.socket) -> tuple[int, bytes] | None:
    """One frame as (opcode, payload); None when the peer closed. See ws_read_message for fragments."""
    def exact(n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("closed")
            buf += chunk
        return buf
    try:
        head = exact(2)
        opcode, n = head[0] & 0x0F, head[1] & 0x7F
        masked = bool(head[1] & 0x80)
        if n == 126:
            n = struct.unpack(">H", exact(2))[0]
        elif n == 127:
            n = struct.unpack(">Q", exact(8))[0]
        key = exact(4) if masked else b""
        payload = exact(n)
        if masked:
            payload = bytes(b ^ key[i % 4] for i, b in enumerate(payload))
        return opcode, payload
    except (OSError, ConnectionError):
        return None


class WsReader:
    """Reassembles one WebSocket message at a time (FIN=0 ... opcode 0 FIN=1).

    Control frames (ping/pong/close) may interleave with fragments; they are returned on
    their own and the partial message survives across calls.
    """

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self._opcode: int | None = None
        self._parts: list[bytes] = []

    def read(self) -> tuple[int, bytes] | None:
        """One complete message or control frame as (opcode, payload); None when closed."""
        while True:
            frame = _ws_read_frame_fin(self.sock)
            if frame is None:
                return None
            fin, op, payload = frame
            if op >= 0x8:
                return op, payload
            if op != 0x0:
                self._opcode, self._parts = op, [payload]
            else:
                self._parts.append(payload)
            if fin:
                opcode, parts = (self._opcode if self._opcode is not None else 0x1), self._parts
                self._opcode, self._parts = None, []
                return opcode, b"".join(parts)


def ws_read_message(sock: socket.socket) -> tuple[int, bytes] | None:
    """One complete message from a fresh reader (tests and one-off probes)."""
    return WsReader(sock).read()


def _ws_read_frame_fin(sock: socket.socket) -> tuple[bool, int, bytes] | None:
    def exact(n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("closed")
            buf += chunk
        return buf
    try:
        head = exact(2)
        fin, opcode, n = bool(head[0] & 0x80), head[0] & 0x0F, head[1] & 0x7F
        masked = bool(head[1] & 0x80)
        if n == 126:
            n = struct.unpack(">H", exact(2))[0]
        elif n == 127:
            n = struct.unpack(">Q", exact(8))[0]
        key = exact(4) if masked else b""
        payload = exact(n)
        if masked:
            payload = bytes(b ^ key[i % 4] for i, b in enumerate(payload))
        return fin, opcode, payload
    except (OSError, ConnectionError):
        return None


class _StdioTransport:
    """JSON lines over a child process's pipes (Tether's own ``codex app-server``)."""

    def __init__(self, argv: list[str], cwd: Path, env: dict[str, str], popen: Callable[..., subprocess.Popen]):
        self.process = popen(  # nosec B603 - fixed argv, no shell
            argv, cwd=str(cwd), env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, bufsize=1, start_new_session=True,
        )
        self.kind = "child"

    def lines(self):
        stdout = self.process.stdout
        if stdout is None:  # pragma: no cover
            return
        try:
            yield from stdout
        except (OSError, ValueError):
            return

    def send(self, line: str) -> None:
        if self.process.stdin is None:  # pragma: no cover
            raise OSError("app-server has no stdin")
        self.process.stdin.write(line + "\n")
        self.process.stdin.flush()

    def alive(self) -> bool:
        return self.process.poll() is None

    def pids(self) -> set[int]:
        return {self.process.pid}

    def close(self) -> None:
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


class _DaemonTransport:
    """JSON-RPC over a WebSocket on the daemon's Unix control socket (RFC 6455, text frames)."""

    def __init__(self, path: Path, timeout: float = 10.0):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(timeout)
        self.sock.connect(str(path))
        key = base64.b64encode(os.urandom(16)).decode()
        self.sock.sendall(
            ("GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
             f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n").encode())
        reply = b""
        while b"\r\n\r\n" not in reply:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise OSError("codex daemon closed the handshake")
            reply += chunk
        if b" 101 " not in reply.split(b"\r\n", 1)[0]:
            raise OSError("codex daemon refused the websocket: " + reply.split(b"\r\n", 1)[0].decode(errors="replace"))
        self.sock.settimeout(None)
        self._open = True
        self._wlock = threading.Lock()
        self.kind = "daemon"

    def lines(self):
        reader = WsReader(self.sock)
        while self._open:
            frame = reader.read()
            if frame is None:
                break
            opcode, payload = frame
            if opcode == 0x8:
                break
            if opcode == 0x9:
                with self._wlock:
                    try:
                        self.sock.sendall(ws_frame(payload, mask=True, opcode=0xA))
                    except OSError:
                        break
                continue
            if opcode in (0x1, 0x2):
                yield payload.decode("utf-8", errors="replace")
        self._open = False

    def send(self, line: str) -> None:
        with self._wlock:
            self.sock.sendall(ws_frame(line.encode("utf-8"), mask=True))

    def alive(self) -> bool:
        return self._open

    def pids(self) -> set[int]:
        return set()

    def close(self) -> None:
        self._open = False
        try:
            self.sock.close()
        except OSError:
            pass


class CodexAppServer:
    """One Codex app-server per gateway, JSON-RPC over a transport.

    Preferred transport: the machine's ``codex app-server`` daemon (the one the ChatGPT
    desktop app drives), so a Slack turn runs inside the very session the human has
    open and shows up there. Fallback: Tether's own child app-server over stdio.

    Probe on greppy3 (2026-09-08): with ``-c mcp_servers={}`` a child turn completes in
    ~7 s; the reply is the ``agentMessage`` items on ``turn/completed``. Threads are
    resumed once per binding with ``thread/resume`` and then driven with ``turn/start``.
    """

    QUIET_AFTER = 10.0  # seconds of silence after a final answer that end a turn without turn/completed
    LEAK_GUARD = 12 * 3600.0  # a turn never ends on a clock; this only frees a wedged waiter


    def __init__(self, transport: Any):
        self.transport = transport
        self.kind = getattr(transport, "kind", "child")
        self._events: queue.Queue[dict[str, Any] | None] = queue.Queue()
        self._next_id = 1
        self._resumed: set[str] = set()
        self.lock = threading.Lock()
        self.last_used = time.monotonic()
        threading.Thread(target=self._pump, name="tether-codex-app-server", daemon=True).start()
        self._call("initialize", {"clientInfo": {"name": "tether", "title": "Tether", "version": "0.4.0"},
                                  "capabilities": {}}, 30)
        self._notify("initialized", {})

    @classmethod
    def spawn(cls, argv: list[str], cwd: Path, env: dict[str, str], popen: Callable[..., subprocess.Popen]) -> "CodexAppServer":
        return cls(_StdioTransport(argv, cwd, env, popen))

    @classmethod
    def connect(cls, path: Path) -> "CodexAppServer":
        return cls(_DaemonTransport(path))

    def _pump(self) -> None:
        try:
            for line in self.transport.lines():
                try:
                    event = json.loads(line)
                except ValueError:
                    continue
                if isinstance(event, dict):
                    self._events.put(event)
        finally:
            self._events.put(None)

    def pids(self) -> set[int]:
        return set(self.transport.pids())

    def alive(self) -> bool:
        return bool(self.transport.alive())

    def _send(self, payload: dict[str, Any]) -> None:
        self.transport.send(json.dumps(payload))

    def _notify(self, method: str, params: dict[str, Any]) -> None:
        self._send({"jsonrpc": "2.0", "method": method, "params": params})

    def _call(self, method: str, params: dict[str, Any], timeout: float) -> dict[str, Any]:
        request_id = self._next_id
        self._next_id += 1
        self._send({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(method)
            try:
                event = self._events.get(timeout=min(remaining, 1.0))
            except queue.Empty:
                continue
            if event is None:
                raise OSError("app-server exited")
            if event.get("id") == request_id:
                if event.get("error"):
                    raise RuntimeError(str(event["error"].get("message") or event["error"])[:200])
                return event.get("result") or {}

    def turn(self, thread_id: str, text: str, cwd: Path, *, sandbox: str, approval: str,
             timeout: float | None = None) -> dict[str, Any]:
        """Run one turn on ``thread_id``; returns {"text", "status", "error"}.

        A turn ends when Codex ends it (``turn/completed``), however long the work takes:
        an agent fixing a planner for an hour is the job, not a hang. ``timeout`` is only
        a leak guard (default LEAK_GUARD); the old 30-minute cap came from the days of one
        child process per turn and posted "could not take this turn" over live work.
        """
        if thread_id not in self._resumed:
            self._call("thread/resume", {"threadId": thread_id, "cwd": str(cwd), "approvalPolicy": approval,
                                         "sandbox": sandbox}, 60)
            self._resumed.add(thread_id)
        self._call("turn/start", {"threadId": thread_id, "cwd": str(cwd), "approvalPolicy": approval,
                                  "input": [{"type": "text", "text": text}]}, 30)
        deadline = time.monotonic() + (timeout or self.LEAK_GUARD)
        messages: list[str] = []          # final-phase agentMessage texts as items complete
        last_event_at = time.monotonic()  # any event on this thread: the turn is alive
        quiet_after = self.QUIET_AFTER
        while True:
            now = time.monotonic()
            if now >= deadline:
                return {"text": "\n".join(messages), "status": "timeout", "error": "turn timed out"}
            if messages and now - last_event_at > quiet_after:
                # Some providers never send turn/completed: a final answer followed by silence
                # is the reply. A preamble ("I'll read the thread...") is commentary, never
                # counted, and any event (a command running, a delta) keeps the turn alive.
                return {"text": "\n".join(messages), "status": "completed", "error": None}
            try:
                event = self._events.get(timeout=min(deadline - now, 1.0))
            except queue.Empty:
                continue
            if event is None:
                return {"text": "\n".join(messages), "status": "exited", "error": "app-server exited"}
            method = event.get("method")
            params = event.get("params") or {}
            if params.get("threadId") not in (None, thread_id):
                continue
            last_event_at = time.monotonic()
            if method == "item/completed":
                item = params.get("item") or {}
                if (item.get("type") == "agentMessage" and (item.get("text") or "").strip()
                        and item.get("phase") in (None, "", "final_answer")):
                    messages.append(str(item["text"]))
            elif method == "turn/completed":
                turn = params.get("turn") or {}
                items = [i for i in turn.get("items") or [] if i.get("type") == "agentMessage" and (i.get("text") or "").strip()]
                final = [i["text"] for i in items if i.get("phase") == "final_answer"] or [i["text"] for i in items]
                error = turn.get("error")
                text = "\n".join(final) if final else (messages[-1] if messages else "")
                return {"text": text, "status": str(turn.get("status") or "completed"),
                        "error": (error.get("message") if isinstance(error, dict) else error) if error else None}

    def terminate(self) -> None:
        self.transport.close()


def claude_transcript(session_id: str) -> Path | None:
    """Claude Code's transcript for a session: ~/.claude/projects/<cwd-slug>/<id>.jsonl."""
    if not session_id:
        return None
    root = Path(os.environ.get("CLAUDE_CONFIG_DIR") or Path.home() / ".claude") / "projects"
    try:
        matches = sorted(root.glob(f"*/{session_id}.jsonl"), key=lambda p: p.stat().st_mtime)
    except OSError:
        return None
    return matches[-1] if matches else None


def claude_reply_after(transcript: Path, offset: int) -> str:
    """The last assistant text block written after ``offset`` bytes of the transcript."""
    last = ""
    try:
        with transcript.open("rb") as handle:
            handle.seek(offset)
            for raw in handle:
                try:
                    record = json.loads(raw)
                except ValueError:
                    continue
                if record.get("type") != "assistant":
                    continue
                content = (record.get("message") or {}).get("content") or []
                text = "\n".join(str(block.get("text") or "") for block in content
                                 if isinstance(block, dict) and block.get("type") == "text").strip()
                if text:
                    last = text
    except OSError:
        return ""
    return last


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
        self._codex: CodexAppServer | None = None
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
            codex, self._codex = self._codex, None
        for session in sessions:
            session.terminate()
        if codex is not None:
            codex.terminate()

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
            if getattr(self.settings, "codex_driver", "app-server") == "exec":
                return self._run_codex_turn(attempt_id, session_id, prompt, cwd, timeout_seconds)
            return self._run_codex_app_server_turn(attempt_id, session_id, prompt, cwd, timeout_seconds)
        placement = source.get("herdr") if isinstance(source.get("herdr"), dict) else None
        if placement:
            # The session is interactive in a Herdr pane: prompt it there (a headless
            # `claude -p --resume` on the same id would fork the conversation).
            result = self._run_herdr_turn(attempt_id, session_id, prompt, placement)
            if result is not None:
                return result
            logger.warning("tether: herdr pane %s for %s is gone; resuming headless", placement.get("pane_id"), session_id)
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

    def herdr_factory(self, session: str) -> Herdr | None:
        return Herdr.discover(session=session)

    def _run_herdr_turn(self, attempt_id: str, session_id: str, prompt: str,
                        placement: dict[str, Any]) -> dict[str, Any] | None:
        """One turn through Herdr's agent surface; None when the pane is no longer there.

        The reply is the transcript's last assistant text written after the prompt, never the
        terminal (a screen scrape) and never narration. A blocked dialog is the reply itself:
        the thread sees it and answers it.
        """
        herdr = self.herdr_factory(str(placement.get("session") or ""))
        if herdr is None:
            return None
        try:
            agent = (herdr.find_agent_by_name(str(placement.get("agent") or "")) if placement.get("agent") else None) \
                or herdr.find_agent_by_session(session_id)
        except HerdrError:
            return None
        if agent is None:
            return None
        target = str(agent.get("name") or agent.get("pane_id"))
        transcript = claude_transcript(session_id)
        offset = transcript.stat().st_size if transcript is not None else 0
        try:
            settled = herdr.agent_prompt(target, prompt)
        except HerdrError as exc:
            if exc.code == "agent_blocked":
                settled = {"agent_status": "blocked"}
            else:
                return self._finish(attempt_id, "failed", str(exc)[:200], error_code=f"herdr_{exc.code}")
        if settled.get("agent_status") == "blocked":
            try:
                screen = herdr.agent_read(target).strip()
            except HerdrError:
                screen = ""
            tail = "\n".join(screen.splitlines()[-12:]).strip()
            text = "The session is waiting on a dialog. Reply here to answer it.\n```\n" + tail + "\n```"
            return self._finish(attempt_id, "completed_with_response", text)
        transcript = transcript or claude_transcript(session_id)
        text = claude_reply_after(transcript, offset) if transcript is not None else ""
        if not text:
            return self._finish(attempt_id, "failed", "no assistant reply found in the session transcript",
                                error_code="herdr_no_reply")
        if is_no_reply(text):
            return self._finish(attempt_id, "no_reply", text)
        return self._finish(attempt_id, "completed_with_response", text)

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

    def _codex_server(self, cwd: Path) -> CodexAppServer:
        with self._lock:
            if self._codex is not None and self._codex.alive():
                return self._codex
            daemon = codex_daemon_socket()
            if daemon is not None:
                try:
                    self._codex = CodexAppServer.connect(daemon)
                    logger.info("tether: codex turns run on the machine daemon at %s", daemon)
                    return self._codex
                except (OSError, RuntimeError, TimeoutError) as exc:
                    logger.warning("tether: codex daemon at %s unusable (%s); starting a child app-server", daemon, exc)
            binary = shutil.which(self.settings.codex_binary) or self.settings.codex_binary
            command = [binary, "app-server", "-c", "mcp_servers={}"]
            env = self._child_env(passthrough=self.settings.harness_env)
            argv, popen_env, launcher = self._launch_plan(command, cwd, env, self.settings)
            logger.info("tether: codex app-server launcher=%s", launcher)
            self._codex = CodexAppServer.spawn(argv, cwd, popen_env, self._popen)
            return self._codex

    def codex_pids(self) -> set[int]:
        """PIDs of Tether's own Codex app-server child, if one is running (none for the daemon)."""
        with self._lock:
            return self._codex.pids() if self._codex is not None and self._codex.alive() else set()

    def _run_codex_app_server_turn(
        self, attempt_id: str, session_id: str, prompt: str, cwd: Path, timeout_seconds: float,
    ) -> dict[str, Any]:
        bypass = any("bypass" in a for a in self.settings.codex_resume_args)
        sandbox = "danger-full-access" if bypass else "workspace-write"
        try:
            server = self._codex_server(cwd)
        except (OSError, RuntimeError, TimeoutError) as exc:
            return self._finish(attempt_id, "failed", str(exc)[:200], error_code="codex_app_server_unavailable")
        with server.lock:
            try:
                # No clock on the turn: Codex decides when it is done (see CodexAppServer.turn).
                result = server.turn(session_id, prompt, cwd, sandbox=sandbox, approval="never")
            except (OSError, RuntimeError, TimeoutError) as exc:
                with self._lock:
                    self._codex = None
                server.terminate()
                return self._finish(attempt_id, "failed", str(exc)[:200], error_code="codex_app_server_error")
            server.last_used = time.monotonic()
        if result["status"] in ("timeout", "exited"):
            with self._lock:
                self._codex = None
            server.terminate()
            return self._finish(attempt_id, "failed", result["error"] or "", error_code=result["status"])
        if result["status"] != "completed" or result.get("error"):
            return self._finish(attempt_id, "failed", str(result.get("error") or result["status"])[:200], error_code="codex_turn_failed")
        text = result["text"]
        if is_no_reply(text):
            return self._finish(attempt_id, "no_reply", text)
        return self._finish(attempt_id, "completed_with_response", text)

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
