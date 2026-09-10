"""Structural tests for the Agent Hub Board collector.

These assert the data contract in board/schema.json without needing a
jsonschema dependency or a reachable fleet: the probe is exercised locally and
the merge logic is fed synthetic probe results.
"""

from __future__ import annotations

import importlib.util
import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pytest

BOARD_DIR = Path(__file__).resolve().parent.parent / "board"


def load_collect():
    spec = importlib.util.spec_from_file_location(
        "board_collect", BOARD_DIR / "collect.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


collect = load_collect()

ISO_Z = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")


def check_board(board: dict) -> None:
    """Assert `board` satisfies every required-field rule in schema.json."""
    schema = json.loads((BOARD_DIR / "schema.json").read_text())
    assert set(board) <= set(schema["properties"])
    for key in schema["required"]:
        assert key in board, key

    assert ISO_Z.match(board["generated_at"]), board["generated_at"]

    agent_props = schema["properties"]["agents"]["items"]
    for agent in board["agents"]:
        assert set(agent) == set(agent_props["required"]), agent
        assert isinstance(agent["name"], str) and agent["name"]
        assert re.match(r"^U[A-Z0-9]+$", agent["slack_id"]), agent
        assert isinstance(agent["live_threads"], int) and agent["live_threads"] >= 0
        assert isinstance(agent["replies_24h"], int) and agent["replies_24h"] >= 0
        if agent["last_reply_at"] is not None:
            assert ISO_Z.match(agent["last_reply_at"]), agent
        if agent["latency_median_s"] is not None:
            assert agent["latency_median_s"] >= 0

    machine_props = schema["properties"]["machines"]["items"]
    for machine in board["machines"]:
        assert set(machine) <= set(machine_props["properties"]), machine
        for key in machine_props["required"]:
            assert key in machine, key
        assert isinstance(machine["doctor_ok"], int)
        assert isinstance(machine["doctor_fail"], int)
        if machine["load"] is not None:
            assert len(machine["load"]) == 3
            assert all(isinstance(x, float) for x in machine["load"])

    for item in board["shipped_this_week"]:
        assert item["title"]
        assert datetime.fromisoformat(item["merged_at"])
        assert item["pr"] is None or isinstance(item["pr"], int)


def test_schema_is_valid_json():
    schema = json.loads((BOARD_DIR / "schema.json").read_text())
    assert schema["required"] == [
        "generated_at", "agents", "machines", "shipped_this_week"]


def test_lanes_roster_matches_schema_identity_rules():
    roster = json.loads((BOARD_DIR / "lanes.json").read_text())["agents"]
    assert roster, "roster must not be empty"
    seen = set()
    for entry in roster:
        assert re.match(r"^U[A-Z0-9]+$", entry["slack_id"]), entry
        assert entry["name"] not in seen, "duplicate agent %s" % entry["name"]
        seen.add(entry["name"])
        assert entry["lane"], "every agent needs a lane: %s" % entry["name"]


def test_remote_probe_runs_locally_and_reports_this_machine():
    """The probe is the whole collector contract; run it for real, here."""
    proc = subprocess.run([sys.executable, "-", "24"], input=collect.REMOTE_PROBE,
                          capture_output=True, text=True, timeout=90)
    assert proc.returncode == 0, proc.stderr
    probe = json.loads(proc.stdout.strip().splitlines()[-1])

    machine = probe["machine"]
    assert machine["hostname"]
    assert re.match(r"^(\d+ days, )?\d+:\d{2}$", machine["uptime"]), machine["uptime"]
    assert len(machine["load"]) == 3
    assert machine["disk_free"].endswith("G")
    assert machine["doctor_ok"] + machine["doctor_fail"] > 0

    agent = probe["agents"][0]
    assert agent["live_threads"] >= 0
    assert agent["replies_24h"] >= 0


def test_build_merges_probes_into_schema_shaped_board(monkeypatch):
    fake = {
        "agents": [{"name": "sam_franchesko", "slack_id": "U0BFC6ZRRQX",
                    "live_threads": 3, "last_reply_at": "2026-09-09T10:00:00Z",
                    "latency_median_s": 12.5, "replies_24h": 4}],
        "machine": {"hostname": "greppy-sam", "uptime": "2 days, 1:00",
                    "load": [0.1, 0.2, 0.3], "disk_free": "483G",
                    "hermes_version": "0.21.1", "tether_version": "0.4.0",
                    "doctor_ok": 14, "doctor_fail": 0},
    }
    monkeypatch.setattr(collect, "probe_remote", lambda host, window: fake)
    board = collect.build(["greppy-sam"], 24)

    check_board(board)
    assert board["agents"][0]["lane"], "lane must come from lanes.json"
    assert board["machines"][0]["host"] == "greppy-sam"


def test_unreachable_host_degrades_instead_of_lying(monkeypatch):
    monkeypatch.setattr(collect, "probe_remote",
                        lambda host, window: {"error": "ssh timeout after 45s"})
    board = collect.build(["greppy-bc"], 24)

    check_board(board)
    assert board["agents"] == [], "a dead host contributes no agent metrics"
    machine = board["machines"][0]
    assert machine["error"].startswith("ssh timeout")
    assert machine["uptime"] is None and machine["doctor_ok"] == 0


def test_m_box_is_reported_under_its_roster_name(monkeypatch):
    """The m box's tailnet HostName is ns1026182; the board calls it `m`."""
    fake = {
        "agents": [{"name": "q2", "slack_id": "U0BJV7GNXML", "live_threads": 1,
                    "last_reply_at": None, "latency_median_s": None,
                    "replies_24h": 0}],
        "machine": {"hostname": "ns1026182", "uptime": "1:00",
                    "load": [0.1, 0.2, 0.3], "disk_free": "155G",
                    "hermes_version": "0.21.1", "tether_version": "0.4.0",
                    "doctor_ok": 14, "doctor_fail": 0},
    }
    monkeypatch.setattr(collect, "probe_remote", lambda host, window: fake)
    board = collect.build(["m"], 24)

    assert board["machines"][0]["host"] == "m"
    assert board["agents"][0]["host"] == "m"
    assert board["agents"][0]["lane"].startswith("QA")


def test_shipped_this_week_reads_squash_merged_prs():
    shipped = collect.shipped_this_week(days=7)
    assert shipped, "expected landed PRs in the last 7 days"
    for item in shipped:
        assert item["pr"] is not None, item
        assert "(#" not in item["title"], "PR ref must be stripped from the title"
        assert item["url"].endswith("/%d" % item["pr"])
    stamps = [item["merged_at"] for item in shipped]
    assert stamps == sorted(stamps, reverse=True), "newest first"


def test_collector_writes_atomically(tmp_path, monkeypatch):
    fake = {"agents": [], "machine": {"hostname": "greppy-cr", "uptime": "1:00",
            "load": [0.0, 0.0, 0.0], "disk_free": "216G",
            "hermes_version": "0.21.1", "tether_version": "0.4.0",
            "doctor_ok": 14, "doctor_fail": 0}}
    monkeypatch.setattr(collect, "probe_remote", lambda host, window: fake)
    out = tmp_path / "nested" / "board.json"
    monkeypatch.setattr(sys, "argv",
                        ["collect.py", "-o", str(out), "--hosts", "greppy-cr"])

    assert collect.main() == 0
    check_board(json.loads(out.read_text()))
    assert not list(out.parent.glob("*.tmp")), "temp file must be renamed away"
