#!/usr/bin/env python3
"""Collect the Agent Hub Board dataset.

Reads each gateway's live tether store over ssh, plus `hermes --version`,
`tether version` and `tether doctor` check counts, and emits one board.json
matching board/schema.json.

Every host is probed by the same remote script (REMOTE_PROBE): it runs on the
gateway, reads the sqlite store read-only, and prints one JSON object on
stdout. The local side only merges. A host that cannot be reached becomes a
machine row with `error` set and null metrics -- the board degrades, it does
not lie.

Usage:
    python3 board/collect.py                 # -> board/board.json
    python3 board/collect.py -o /tmp/b.json  # explicit output
    python3 board/collect.py --hosts m       # subset, for a fast check
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

BOARD_DIR = Path(__file__).resolve().parent
REPO_ROOT = BOARD_DIR.parent

# Gateways reached over ssh. "m" is greppy3 itself via its tailnet alias; the
# local claudio identity is collected without ssh (see LOCAL_HOST).
SSH_HOSTS = ["greppy-cr", "greppy-sam", "greppy-bc", "greppy-mg", "m"]
LOCAL_HOST = "greppy3"

# Tailnet HostName -> the name the roster and board use.
HOST_ALIASES = {"ns1026182": "m"}

# Accounts whose tether store lives outside the login user and must be read
# through `sudo runuser`. Absent on this fleet today (every host runs a single
# `ubuntu` user); declared so adding one is a data change, not a code change.
ISOLATED_USERS: dict[str, list[str]] = {}

SSH_TIMEOUT_S = 45
WINDOW_H = 24

# Runs on the gateway. Self-contained: stdlib only, no repo checkout needed.
REMOTE_PROBE = r'''
import glob, json, os, re, shutil, sqlite3, subprocess, sys
from datetime import datetime, timedelta, timezone

WINDOW_H = int(sys.argv[1]) if len(sys.argv) > 1 else 24
HOME = os.path.expanduser("~")
out = {"agents": [], "machine": {}}


def run(cmd, timeout=20):
    """Run through a login shell so ~/.local/bin is on PATH, as on a real session."""
    try:
        p = subprocess.run(["bash", "-lc", cmd], capture_output=True,
                           text=True, timeout=timeout)
        return p.returncode, p.stdout.strip(), p.stderr.strip()
    except Exception as exc:
        return 127, "", str(exc)


def parse_ts(value):
    """Store timestamps are naive UTC 'YYYY-MM-DD HH:MM:SS'."""
    if not value:
        return None
    try:
        return datetime.strptime(value[:19], "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc)
    except ValueError:
        return None


def store_path():
    """Live 0.4.x store; fall back to the pre-0.4 bridges.db if that is all there is."""
    live = os.path.join(HOME, ".hermes", "plugin-data", "tether", "domain.db")
    if os.path.exists(live):
        return live, "domain"
    legacy = os.path.join(HOME, ".hermes", "bridges.db")
    if os.path.exists(legacy):
        return legacy, "legacy"
    return None, None


def connect_ro(path):
    return sqlite3.connect("file:%s?mode=ro" % path, uri=True)


def identity():
    code, stdout, _ = run("tether identity --json")
    if code == 0 and stdout:
        try:
            data = json.loads(stdout)
            return data.get("user"), data.get("user_id")
        except ValueError:
            pass
    return None, None


def agent_metrics():
    """live_threads / last_reply_at / latency_median_s / replies_24h for this host's agent."""
    path, flavor = store_path()
    metrics = {"live_threads": 0, "last_reply_at": None,
               "latency_median_s": None, "replies_24h": 0, "store": flavor}
    if not path:
        metrics["store"] = "missing"
        return metrics

    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(hours=WINDOW_H)).strftime("%Y-%m-%d %H:%M:%S")
    conn = connect_ro(path)
    try:
        if flavor == "domain":
            # A live thread is a binding still in a non-terminal state.
            metrics["live_threads"] = conn.execute(
                "select count(distinct channel_id || ':' || thread_ts) "
                "from thread_bindings where state in ('active','pending')"
            ).fetchone()[0]

            # A reply is an attempt that reached a terminal state carrying a
            # response; `no_reply` attempts are turns we deliberately did not
            # answer and must not count as latency samples.
            replied = (
                "select a.created_at, a.terminal_at from native_attempts a "
                "where a.terminal_at is not null and a.state not in "
                "('no_reply','cancelled','failed')"
            )
            rows = list(conn.execute(replied))
            last = max((r[1] for r in rows if r[1]), default=None)
            metrics["last_reply_at"] = (
                parse_ts(last).isoformat().replace("+00:00", "Z") if last else None)
            recent = [r for r in rows if r[1] and r[1] >= cutoff]
            metrics["replies_24h"] = len(recent)
            samples = []
            for started, ended in recent:
                a, b = parse_ts(started), parse_ts(ended)
                if a and b and b >= a:
                    samples.append((b - a).total_seconds())
        else:
            metrics["live_threads"] = conn.execute(
                "select count(distinct channel_id || ':' || thread_ts) "
                "from bridges where status in ('active','pending')"
            ).fetchone()[0]
            rows = list(conn.execute(
                "select r.created_at, b.channel_id, b.thread_ts "
                "from bridge_replies r join bridges b using(bridge_id) "
                "where r.state='sent'"))
            last = max((r[0] for r in rows), default=None)
            metrics["last_reply_at"] = (
                parse_ts(last).isoformat().replace("+00:00", "Z") if last else None)
            recent = [r for r in rows if r[0] >= cutoff]
            metrics["replies_24h"] = len(recent)
            samples = []
            for created, channel, thread in recent:
                # Latency = reply time minus the newest inbound message that
                # preceded it in the same thread.
                got = conn.execute(
                    "select max(created_at) from thread_ingress "
                    "where channel_id=? and thread_ts=? and created_at<=?",
                    (channel, thread, created)).fetchone()[0]
                a, b = parse_ts(got), parse_ts(created)
                if a and b and b >= a:
                    samples.append((b - a).total_seconds())

        if samples:
            samples.sort()
            mid = len(samples) // 2
            median = (samples[mid] if len(samples) % 2
                      else (samples[mid - 1] + samples[mid]) / 2)
            metrics["latency_median_s"] = round(median, 1)
    finally:
        conn.close()
    return metrics


def machine():
    info = {}
    # `hostname` is unreliable across this fleet (greppy3 reports "g", the m
    # box reports "ns1026182"), so identify a machine by its tailnet name and
    # fall back to `hostname` only when Tailscale is not answering.
    code, stdout, _ = run(
        "tailscale status --self --json 2>/dev/null "
        "| python3 -c 'import sys,json; print(json.load(sys.stdin)[\"Self\"][\"HostName\"])'")
    tailnet = stdout.strip() if code == 0 else ""
    if not tailnet:
        _, tailnet, _ = run("hostname")
    info["hostname"] = tailnet or None

    with open("/proc/uptime") as handle:
        seconds = float(handle.read().split()[0])
    days, rem = divmod(int(seconds), 86400)
    hours, minutes = divmod(rem // 60, 60)
    info["uptime"] = ("%d days, %d:%02d" % (days, hours, minutes) if days
                      else "%d:%02d" % (hours, minutes))

    with open("/proc/loadavg") as handle:
        info["load"] = [float(x) for x in handle.read().split()[:3]]

    usage = shutil.disk_usage("/")
    info["disk_free"] = "%dG" % (usage.free // (1024 ** 3))

    code, stdout, _ = run("hermes --version")
    match = re.search(r"v?(\d+\.\d+\.\d+[^\s)]*)", stdout) if code == 0 else None
    info["hermes_version"] = match.group(1) if match else None

    code, stdout, _ = run("tether version")
    match = re.search(r"(\d+\.\d+\.\d+[^\s)]*)", stdout) if code == 0 else None
    info["tether_version"] = match.group(1) if match else None

    # `tether doctor --json` exits non-zero when checks fail, so parse stdout
    # regardless of the exit code and only fall back when it is unparseable.
    ok = fail = 0
    _, stdout, _ = run("tether doctor --json", timeout=40)
    try:
        checks = json.loads(stdout).get("checks", [])
        for line in checks:
            if str(line).strip().lower().startswith("ok"):
                ok += 1
            else:
                fail += 1
    except ValueError:
        info["doctor_error"] = "unparseable"
    info["doctor_ok"] = ok
    info["doctor_fail"] = fail
    return info


name, slack_id = identity()
metrics = agent_metrics()
metrics["name"] = name
metrics["slack_id"] = slack_id
out["agents"].append(metrics)
out["machine"] = machine()
print(json.dumps(out))
'''


def probe_remote(host: str, window_h: int) -> dict:
    """Run REMOTE_PROBE on `host` over ssh and return its JSON, or an error dict."""
    cmd = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", host,
           "python3 - %d" % window_h]
    try:
        proc = subprocess.run(cmd, input=REMOTE_PROBE, capture_output=True,
                              text=True, timeout=SSH_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        return {"error": "ssh timeout after %ds" % SSH_TIMEOUT_S}
    if proc.returncode != 0:
        return {"error": (proc.stderr.strip() or "ssh exit %d" % proc.returncode)[:200]}
    try:
        return json.loads(proc.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return {"error": "unparseable probe output: %s" % proc.stdout[:120]}


def probe_local(window_h: int) -> dict:
    """Same probe, no ssh -- for the claudio identity on this box."""
    try:
        proc = subprocess.run([sys.executable, "-", str(window_h)],
                              input=REMOTE_PROBE, capture_output=True,
                              text=True, timeout=SSH_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        return {"error": "local probe timeout"}
    if proc.returncode != 0:
        return {"error": (proc.stderr.strip() or "exit %d" % proc.returncode)[:200]}
    try:
        return json.loads(proc.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return {"error": "unparseable probe output"}


def probe_isolated(host: str, user: str, window_h: int) -> dict:
    """Isolated account on `host`: same probe under `sudo runuser -u <user>`.

    Used for accounts whose tether store is not readable by the login user.
    """
    remote = ("sudo -n runuser -u %s -- python3 - %d" % (user, window_h))
    cmd = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", host, remote]
    try:
        proc = subprocess.run(cmd, input=REMOTE_PROBE, capture_output=True,
                              text=True, timeout=SSH_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        return {"error": "ssh timeout"}
    if proc.returncode != 0:
        return {"error": (proc.stderr.strip() or "exit %d" % proc.returncode)[:200]}
    try:
        return json.loads(proc.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return {"error": "unparseable probe output"}


def shipped_this_week(days: int = 7) -> list[dict]:
    """Work landed on origin/main in the trailing `days`, newest first.

    This repo squash-merges, so merge commits do not exist; a shipped item is
    a commit on origin/main whose subject carries a `(#NNN)` PR reference.
    """
    since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    fmt = "%H%x1f%cI%x1f%an%x1f%s"
    try:
        proc = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "log", "origin/main",
             "--since", since, "--pretty=format:" + fmt],
            capture_output=True, text=True, timeout=30)
    except (subprocess.TimeoutExpired, OSError):
        return []
    if proc.returncode != 0:
        return []

    shipped = []
    for line in proc.stdout.splitlines():
        parts = line.split("\x1f")
        if len(parts) != 4:
            continue
        _, committed, author, subject = parts
        # %cI carries each committer's local offset; normalise to UTC so the
        # board can sort and compare these as plain strings.
        try:
            merged_at = datetime.fromisoformat(committed).astimezone(
                timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        except ValueError:
            continue
        pr_match = re.search(r"\(#(\d+)\)\s*$", subject)
        if not pr_match:
            continue  # not a landed PR: a direct push, a revert, a fixup
        pr = int(pr_match.group(1))
        title = subject[:pr_match.start()].strip()
        shipped.append({
            "title": title,
            "merged_at": merged_at,
            "pr": pr,
            "author": author,
            "url": ("https://github.com/Parcha-ai/parcha-skills/pull/%d" % pr
                    if pr else None),
        })
    return shipped


def build(hosts: list[str], window_h: int) -> dict:
    roster = json.loads((BOARD_DIR / "lanes.json").read_text())["agents"]
    by_host = {entry["host"]: entry for entry in roster}

    targets: list[tuple[str, str, callable]] = []
    for host in hosts:
        if host == LOCAL_HOST:
            targets.append((host, "local", lambda h=host: probe_local(window_h)))
        else:
            targets.append((host, "ssh", lambda h=host: probe_remote(h, window_h)))
    for host, users in ISOLATED_USERS.items():
        for user in users:
            targets.append(("%s:%s" % (host, user), "isolated",
                            lambda h=host, u=user: probe_isolated(h, u, window_h)))

    results: dict[str, dict] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(targets) or 1) as pool:
        futures = {pool.submit(fn): label for label, _, fn in targets}
        for future in concurrent.futures.as_completed(futures):
            results[futures[future]] = future.result()

    agents, machines = [], []
    for label, _, _ in targets:
        probe = results.get(label, {"error": "no result"})
        # Identify by tailnet name; the ssh alias is only how we got there.
        # The m box's tailnet HostName is "ns1026182" while its ssh alias is
        # "m", so normalise it to the name the roster uses.
        reported = probe.get("machine", {}).get("hostname") or label
        host_name = HOST_ALIASES.get(reported, reported)

        if "error" in probe:
            machines.append({
                "host": host_name, "uptime": None, "load": None, "disk_free": None,
                "hermes_version": None, "tether_version": None,
                "doctor_ok": 0, "doctor_fail": 0, "error": probe["error"],
            })
            continue

        machine = probe["machine"]
        machines.append({
            "host": host_name,
            "uptime": machine.get("uptime"),
            "load": machine.get("load"),
            "disk_free": machine.get("disk_free"),
            "hermes_version": machine.get("hermes_version"),
            "tether_version": machine.get("tether_version"),
            "doctor_ok": machine.get("doctor_ok", 0),
            "doctor_fail": machine.get("doctor_fail", 0),
        })

        for found in probe.get("agents", []):
            entry = by_host.get(host_name, {})
            name = found.get("name") or entry.get("name")
            if not name:
                continue
            agents.append({
                "name": name,
                "slack_id": found.get("slack_id") or entry.get("slack_id", ""),
                "host": host_name,
                "lane": entry.get("lane", ""),
                "live_threads": found.get("live_threads", 0),
                "last_reply_at": found.get("last_reply_at"),
                "latency_median_s": found.get("latency_median_s"),
                "replies_24h": found.get("replies_24h", 0),
            })

    agents.sort(key=lambda a: a["name"])
    machines.sort(key=lambda m: m["host"])
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(
            timespec="seconds").replace("+00:00", "Z"),
        "agents": agents,
        "machines": machines,
        "shipped_this_week": shipped_this_week(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-o", "--output", default=str(BOARD_DIR / "board.json"))
    parser.add_argument("--hosts", nargs="*", default=SSH_HOSTS + [LOCAL_HOST])
    parser.add_argument("--window-hours", type=int, default=WINDOW_H)
    args = parser.parse_args()

    board = build(args.hosts, args.window_hours)

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    tmp = output.with_suffix(output.suffix + ".tmp")
    tmp.write_text(json.dumps(board, indent=2) + "\n")
    os.replace(tmp, output)  # atomic: a reader never sees a half-written board

    unreachable = [m["host"] for m in board["machines"] if m.get("error")]
    print("wrote %s: %d agents, %d machines, %d shipped%s" % (
        output, len(board["agents"]), len(board["machines"]),
        len(board["shipped_this_week"]),
        (" (unreachable: %s)" % ", ".join(unreachable)) if unreachable else ""))
    return 1 if len(unreachable) == len(board["machines"]) else 0


if __name__ == "__main__":
    raise SystemExit(main())
