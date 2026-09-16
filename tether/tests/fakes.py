"""Shared test doubles: a scriptable stand-in for `claude -p` speaking stream-json."""

from __future__ import annotations

import json
import os
import stat
import textwrap
from pathlib import Path

# Per user turn: if FAKE_REPLY is set, run it with /bin/sh and use its stdout as the
# result (non-zero exit -> the harness exits with that code after writing stderr);
# otherwise echo a counter so tests can prove one process served several turns.
FAKE_CLAUDE = textwrap.dedent(
    """\
    #!/usr/bin/env python3
    import json, os, subprocess, sys, time
    sid = sys.argv[sys.argv.index("--resume") + 1] if "--resume" in sys.argv else "fresh"
    n = 0
    log = os.environ.get("FAKE_LOG")
    prompts = os.environ.get("FAKE_PROMPTS")
    script = os.environ.get("FAKE_REPLY")
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        n += 1
        text = json.loads(line)["message"]["content"][0]["text"]
        if log:
            open(log, "a").write(f"{os.getpid()} {n} {text[:40]}\\n")
        if prompts:
            open(prompts, "a").write(text + "\\n===\\n")
        if script:
            done = subprocess.run(["/bin/sh", "-c", script], capture_output=True, text=True)
            if done.returncode != 0:
                sys.stderr.write(done.stdout + done.stderr); sys.stderr.flush(); sys.exit(done.returncode)
            reply = done.stdout
        else:
            if "SLEEP" in text:
                time.sleep(5)
            for _ in range(int(os.environ.get("FAKE_TICKS") or 0)):
                # a working session: stream events keep coming while the job runs
                print(json.dumps({"type": "stream_event", "session_id": sid})); sys.stdout.flush()
                time.sleep(0.4)
            if "CRASH" in text:
                sys.stderr.write("You've hit your session limit\\n"); sys.exit(1)
            if "SILENT" in text:
                reply = "NO_REPLY\\n\\nNO_REPLY"
            elif "ERRORFLAG" in text:
                print(json.dumps({"type": "result", "is_error": True, "result": "API overloaded", "session_id": sid})); sys.stdout.flush(); continue
            else:
                reply = f"turn {n} of pid {os.getpid()}: {text.splitlines()[-1][:60]}"
        print(json.dumps({"type": "assistant", "session_id": sid}))
        print(json.dumps({"type": "result", "result": reply, "session_id": sid, "is_error": False}))
        sys.stdout.flush()
    """
)


def write_fake_claude(directory: Path) -> Path:
    path = Path(directory) / "fake-claude"
    path.write_text(FAKE_CLAUDE, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return path


def direct_launch(command, cwd, env, settings):
    return command, env, "direct"


def child_env(passthrough=()):
    keys = ("PATH", "FAKE_LOG", "FAKE_PROMPTS", "FAKE_REPLY", "FAKE_CODEX_REPLY", "FAKE_CODEX_FAIL", "FAKE_CODEX_NO_COMPLETE",
            "FAKE_CODEX_PREAMBLE", "FAKE_TICKS", *passthrough)
    return {k: os.environ[k] for k in keys if k in os.environ}


class Descriptor:
    def __init__(self, owners=("U12345678",), workspace_id="T12345678"):
        self.authorized_owner_ids = tuple(owners)
        self.canonical_owner_ids = tuple(owners)
        self.workspace_id = workspace_id
        self.persona_id = "primary"
        self.policy_generation = 1


# A stand-in for `codex app-server`: JSON-RPC over stdio. Answers initialize and
# thread/resume, and turns every turn/start into turn/started, an agentMessage
# item and turn/completed. FAKE_CODEX_REPLY overrides the text; FAKE_CODEX_FAIL
# makes the turn complete with status "failed".
FAKE_CODEX = textwrap.dedent(
    """\
    #!/usr/bin/env python3
    import json, os, sys, time
    n = 0
    log = os.environ.get("FAKE_LOG")
    def out(o):
        print(json.dumps(o)); sys.stdout.flush()
    for line in sys.stdin:
        try:
            req = json.loads(line)
        except ValueError:
            continue
        method, rid, params = req.get("method"), req.get("id"), req.get("params") or {}
        if method == "initialize":
            out({"id": rid, "result": {"userAgent": "fake-codex"}})
        elif method == "thread/resume":
            out({"id": rid, "result": {"thread": {"id": params["threadId"]}}})
        elif method == "turn/start":
            n += 1
            tid = params["threadId"]; text = params["input"][0]["text"]
            if log:
                open(log, "a").write(f"{os.getpid()} {n} {text[:40]}\\n")
            out({"id": rid, "result": {"turn": {"id": f"turn-{n}"}}})
            out({"method": "turn/started", "params": {"threadId": tid, "turn": {"id": f"turn-{n}", "status": "inProgress"}}})
            if os.environ.get("FAKE_CODEX_FAIL"):
                out({"method": "turn/completed", "params": {"threadId": tid, "turn": {"id": f"turn-{n}", "items": [], "status": "failed", "error": {"message": "model refused"}}}})
                continue
            if os.environ.get("FAKE_CODEX_PREAMBLE"):
                # Codex narrates before it works: a commentary message, then a long silent tool call.
                pre = {"type": "agentMessage", "id": f"pre-{n}", "text": "I'll read the thread first.", "phase": "commentary"}
                out({"method": "item/completed", "params": {"threadId": tid, "turnId": f"turn-{n}", "item": pre}})
                out({"method": "item/started", "params": {"threadId": tid, "turnId": f"turn-{n}", "item": {"type": "commandExecution", "id": f"cmd-{n}"}}})
                time.sleep(float(os.environ["FAKE_CODEX_PREAMBLE"]))
            reply = os.environ.get("FAKE_CODEX_REPLY") or f"codex turn {n} of pid {os.getpid()}"
            item = {"type": "agentMessage", "id": f"msg-{n}", "text": reply, "phase": "final_answer"}
            out({"method": "item/completed", "params": {"threadId": tid, "turnId": f"turn-{n}", "item": item}})
            if os.environ.get("FAKE_CODEX_NO_COMPLETE"):
                continue
            out({"method": "turn/completed", "params": {"threadId": tid, "turn": {"id": f"turn-{n}", "items": [item], "status": "completed", "error": None}}})
    """
)


def write_fake_codex(directory: Path) -> Path:
    path = Path(directory) / "fake-codex"
    path.write_text(FAKE_CODEX, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return path


class FakeCodexDaemon:
    """A stand-in for the machine's ``codex app-server`` daemon: WebSocket JSON-RPC on a
    Unix control socket. Records every turn it ran; answers like FAKE_CODEX."""

    def __init__(self, codex_home: Path):
        import base64
        import hashlib
        import socket
        import threading
        from runtime.plugin_next.session_driver import ws_frame, ws_read_frame

        control = Path(codex_home) / "app-server-control"
        control.mkdir(parents=True, exist_ok=True)
        self.path = control / "app-server-control.sock"
        self.turns: list[tuple[str, str]] = []
        self.clients = 0
        self._server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._server.bind(str(self.path))
        self._server.listen(4)

        def serve(conn):
            self.clients += 1
            buf = b""
            while b"\r\n\r\n" not in buf:
                buf += conn.recv(4096)
            key = [line.split(b":", 1)[1].strip() for line in buf.split(b"\r\n") if line.lower().startswith(b"sec-websocket-key")][0]
            accept = base64.b64encode(hashlib.sha1(key + b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest())
            conn.sendall(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                         b"Sec-WebSocket-Accept: " + accept + b"\r\n\r\n")

            def out(o):
                conn.sendall(ws_frame(json.dumps(o).encode(), mask=False))
            n = 0
            while True:
                frame = ws_read_frame(conn)
                if frame is None or frame[0] == 0x8:
                    break
                if frame[0] != 0x1:
                    continue
                req = json.loads(frame[1])
                method, rid, params = req.get("method"), req.get("id"), req.get("params") or {}
                if method == "initialize":
                    out({"id": rid, "result": {"userAgent": "fake-codex-daemon"}})
                elif method == "thread/resume":
                    out({"id": rid, "result": {"thread": {"id": params["threadId"]}}})
                elif method == "turn/start":
                    n += 1
                    tid = params["threadId"]
                    text = params["input"][0]["text"]
                    self.turns.append((tid, text))
                    out({"id": rid, "result": {"turn": {"id": f"turn-{n}"}}})
                    out({"method": "turn/started", "params": {"threadId": tid, "turn": {"id": f"turn-{n}", "status": "inProgress"}}})
                    item = {"type": "agentMessage", "id": f"msg-{n}", "text": f"daemon turn {n}", "phase": "final_answer"}
                    out({"method": "item/completed", "params": {"threadId": tid, "turnId": f"turn-{n}", "item": item}})
                    out({"method": "turn/completed", "params": {"threadId": tid, "turn": {"id": f"turn-{n}", "items": [item], "status": "completed", "error": None}}})
            conn.close()

        def accept_loop():
            while True:
                try:
                    conn, _ = self._server.accept()
                except OSError:
                    return
                threading.Thread(target=serve, args=(conn,), daemon=True).start()

        threading.Thread(target=accept_loop, daemon=True).start()

    def close(self):
        try:
            self._server.close()
        except OSError:
            pass


# A stand-in for the `herdr` CLI: keeps its state in FAKE_HERDR_STATE (JSON) so calls compose
# across processes, logs every argv line to FAKE_HERDR_LOG. Behaviours are steered by env:
#   FAKE_HERDR_TRUST=1        first `agent start` of a claude agent returns agent_not_ready
#                             (folder-trust dialog) until `send-keys enter` arrives
#   FAKE_HERDR_AFTER=<state>  state reported after a prompt (default idle)
#   FAKE_HERDR_SCREEN=<text>  what `agent read` prints
#   FAKE_HERDR_CODEX_SESSION=1 codex agents report a session id too (needs the hook in real life)
FAKE_HERDR = textwrap.dedent(
    """\
    #!/usr/bin/env python3
    import json, os, sys
    argv = sys.argv[1:]
    if argv[:1] == ["--session"]:
        argv = argv[2:]
    state_path = os.environ.get("FAKE_HERDR_STATE") or "/tmp/fake-herdr-state.json"
    try:
        state = json.load(open(state_path))
    except Exception:
        state = {"workspaces": [{"workspace_id": "w1", "label": "grep.ai", "worktree": {"checkout_path": os.environ.get("FAKE_HERDR_W1_CWD", "/nonexistent")}}],
                 "tabs": {}, "agents": {}, "n": 0, "metadata": {}}
    log = os.environ.get("FAKE_HERDR_LOG")
    if log:
        open(log, "a").write(" ".join(argv) + "\\n")
    def save():
        json.dump(state, open(state_path, "w"))
    def out(result):
        print(json.dumps({"id": "cli", "result": result})); sys.exit(0)
    def fail(code, message=""):
        sys.stderr.write(json.dumps({"id": "cli", "error": {"code": code, "message": message}}) + "\\n"); sys.exit(1)
    def agent_view(a):
        v = {"agent": a["kind"], "agent_status": a["status"], "cwd": a["cwd"], "pane_id": a["pane_id"],
             "tab_id": a["tab_id"], "workspace_id": a["workspace_id"], "name": a["name"]}
        if a.get("session"):
            v["agent_session"] = {"agent": a["kind"], "kind": "id", "source": f"herdr:{a['kind']}", "value": a["session"]}
        return v
    def resolve(target):
        for a in state["agents"].values():
            if a["name"] == target or a["pane_id"] == target:
                return a
        fail("agent_not_found", f"no agent {target}")
    def opt(name, default=None):
        return argv[argv.index(name) + 1] if name in argv else default
    group, cmd = (argv + [None, None])[:2]
    if group == "workspace" and cmd == "list":
        out({"type": "workspace_list", "workspaces": state["workspaces"]})
    if group == "workspace" and cmd == "create":
        state["n"] += 1; wid = f"w{state['n'] + 1}"
        state["workspaces"].append({"workspace_id": wid, "label": opt("--label", "")})
        state["tabs"][f"{wid}:t1"] = {"workspace_id": wid, "cwd": opt("--cwd", ""), "label": "1"}
        save(); out({"workspace": {"workspace_id": wid}, "tab": {"tab_id": f"{wid}:t1"}, "root_pane": {"pane_id": f"{wid}:p1"}})
    if group == "tab" and cmd == "create":
        wid = opt("--workspace"); state["n"] += 1; tid = f"{wid}:t{state['n'] + 1}"
        state["tabs"][tid] = {"workspace_id": wid, "cwd": opt("--cwd", ""), "label": opt("--label", "")}
        save(); out({"tab": {"tab_id": tid, "label": opt("--label", "")}, "root_pane": {"pane_id": f"{wid}:p{state['n'] + 1}"}})
    if group == "pane" and cmd == "report-metadata":
        pane = argv[2]; tokens = state["metadata"].setdefault(pane, {})
        for i, a in enumerate(argv):
            if a == "--token":
                k, _, v = argv[i + 1].partition("="); tokens[k] = v
            if a == "--clear-token":
                tokens.pop(argv[i + 1], None)
        save(); out({"type": "ok"})
    if group == "agent" and cmd == "start":
        name = argv[2]; kind = opt("--kind"); pane = opt("--pane"); state["n"] += 1
        wid = pane.split(":")[0]
        tab = next((t for t, v in state["tabs"].items() if v["workspace_id"] == wid), f"{wid}:t1")
        blocked = kind == "claude" and os.environ.get("FAKE_HERDR_TRUST") == "1"
        session = f"{kind}-sess-{state['n']}" if (kind == "claude" or os.environ.get("FAKE_HERDR_CODEX_SESSION") == "1") else ""
        state["agents"][name] = {"name": name, "kind": kind, "pane_id": pane, "tab_id": tab, "workspace_id": wid,
                                 "cwd": state["tabs"].get(tab, {}).get("cwd", ""), "status": "blocked" if blocked else "idle",
                                 "session": "" if blocked else session, "pending_session": session, "prompts": [],
                                 "args": argv[argv.index("--") + 1:] if "--" in argv else []}
        save()
        if blocked:
            fail("agent_not_ready", f"agent {name} is blocked during startup and is not ready for prompts")
        out({"agent": agent_view(state["agents"][name])})
    if group == "agent" and cmd == "get":
        out({"type": "agent_info", "agent": agent_view(resolve(argv[2]))})
    if group == "agent" and cmd == "list":
        out({"type": "agent_list", "agents": [agent_view(a) for a in state["agents"].values()]})
    if group == "agent" and cmd == "send-keys":
        a = resolve(argv[2]); key = argv[3]
        a.setdefault("keys", []).append(key)
        if a["status"] == "blocked" and a.get("prompts"):
            a["answered"] = True  # a dialog inside a turn: `agent wait` reports what happened next
        elif key == "enter" and a["status"] == "blocked":
            a["status"] = "idle"; a["session"] = a.get("pending_session", "")
        save(); out({"type": "ok"})
    if group == "agent" and cmd == "prompt":
        a = resolve(argv[2]); text = argv[3]
        if a["status"] == "blocked":
            fail("agent_blocked", "agent is blocked")
        a["prompts"].append(text); a["status"] = os.environ.get("FAKE_HERDR_AFTER", "idle")
        transcript = os.environ.get("FAKE_HERDR_TRANSCRIPT")
        if transcript and a["status"] != "blocked":
            # what Claude Code writes to ~/.claude/projects/<slug>/<sid>.jsonl during the turn
            reply = os.environ.get("FAKE_HERDR_REPLY") or f"herdr reply {len(a['prompts'])}"
            with open(transcript, "a") as fh:
                fh.write(json.dumps({"type": "assistant", "message": {"content": [{"type": "thinking", "thinking": "hm"}]}}) + "\\n")
                fh.write(json.dumps({"type": "assistant", "message": {"content": [{"type": "text", "text": "Let me look."}]}}) + "\\n")
                fh.write(json.dumps({"type": "assistant", "message": {"content": [{"type": "text", "text": reply}]}}) + "\\n")
        save(); out({"agent": agent_view(a)})
    if group == "agent" and cmd == "wait":
        a = resolve(argv[2])
        if a.get("answered"):
            a["answered"] = False
            a["status"] = os.environ.get("FAKE_HERDR_AFTER", "idle")
            transcript = os.environ.get("FAKE_HERDR_TRANSCRIPT")
            if transcript and a["status"] != "blocked":
                reply = os.environ.get("FAKE_HERDR_REPLY") or "resumed"
                with open(transcript, "a") as fh:
                    fh.write(json.dumps({"type": "assistant", "message": {"content": [{"type": "text", "text": reply}]}}) + "\\n")
            save()
        out({"agent": agent_view(a)})
    if group == "pane" and cmd == "send-text":
        pane = argv[2]
        for a in state["agents"].values():
            if a["pane_id"] == pane:
                a.setdefault("typed", []).append(argv[3]); a["answered"] = True
        save(); out({"type": "ok"})
    if group == "agent" and cmd == "read":
        resolve(argv[2]); print(os.environ.get("FAKE_HERDR_SCREEN", "\\u276f")); sys.exit(0)
    fail("unknown_command", " ".join(argv))
    """
)


def write_fake_herdr(directory: Path) -> Path:
    path = Path(directory) / "herdr"
    path.write_text(FAKE_HERDR, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return path
