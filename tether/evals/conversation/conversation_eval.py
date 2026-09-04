#!/usr/bin/env python3
"""Conversation eval: is the fleet behaving like colleagues in Slack?

Three commands, one loop (see ati-harness docs/improvement-loop.md):

  export   pull the last N hours of threads from a channel into a redacted corpus
  measure  structural metrics that need no model (latency, leaks, extra voices)
  judge    semantic scores per thread via `claude -p`, median of K votes

Redaction happens at export: user ids become role labels, emails and URLs are
masked. The corpus is safe to keep as a fixture; raw Slack never leaves the box.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import subprocess  # nosec B404 - fixed argv
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

AGENTS = {
    "U095AHX1QQL": "anthro", "U0BJATRKZ6V": "irma", "U0BHY13623U": "chriscache",
    "U0BFC6ZRRQX": "sam", "U0BJN78RJD8": "bryan300", "U0A9TAX8MSA": "manny",
    "U09450ZLS81": "claudio",
}
STATUS_PREFIXES = (":hourglass", ":zap:", ":warning:", ":arrow_right_hook:", ":stopwatch:")
_EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")
_URL = re.compile(r"https?://\S+")
_MENTION = re.compile(r"<@(U[A-Z0-9]+)>")
_BARE = re.compile(r"(?<!<)@(U[A-Z0-9]{6,})")

RUBRIC = (
    "Score this Slack thread between engineering agents on four criteria, 0-2 each. "
    "Reply with JSON only: {\"answered\":n,\"lane\":n,\"evidence\":n,\"tone\":n,\"note\":\"...\"}. "
    "answered: the addressee answered the actual question, or correctly said it is not theirs. "
    "lane: the right agent did the work. One short routing line from another agent that names "
    "the owner (\"that is X's lane\") is correct behaviour and costs nothing; an agent that "
    "answers a question addressed to someone else, or repeats a point already made, loses points. "
    "evidence: claims carry a file, command, link, count, ticket, or exit code, proportional to "
    "the claim (a one-line routing needs none). "
    "tone: brief, colleague-like, no narration of reasoning or tools, no repeated summary."
)


def _api(token: str, method: str, **query: object) -> dict:
    url = "https://slack.com/api/" + method + "?" + urllib.parse.urlencode(query)
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(request, timeout=30) as response:  # nosec B310 - fixed https host
        return json.load(response)


def redact(text: str, humans: dict[str, str] | None = None) -> str:
    """Mask emails, URLs, and raw Slack ids; agents keep their names, humans a role label."""
    labels = humans if humans is not None else {}

    def _label(match: re.Match) -> str:
        uid = match.group(1)
        if uid in AGENTS:
            return f"<@{AGENTS[uid]}>"
        return f"<@{labels.setdefault(uid, f'HUMAN_{len(labels) + 1}')}>"

    text = _MENTION.sub(_label, text or "")
    text = _BARE.sub(lambda m: "@" + (AGENTS.get(m.group(1)) or labels.setdefault(m.group(1), f"HUMAN_{len(labels) + 1}")), text)
    return _URL.sub("<url>", _EMAIL.sub("<email>", text))


def export(token: str, channel: str, hours: float, out: Path) -> int:
    oldest = time.time() - hours * 3600
    roots: list[dict] = []
    cursor = None
    while True:
        page = _api(token, "conversations.history", channel=channel, oldest=f"{oldest:.6f}",
                    limit=200, **({"cursor": cursor} if cursor else {}))
        roots += page.get("messages", [])
        cursor = (page.get("response_metadata") or {}).get("next_cursor")
        if not cursor:
            break
    humans: dict[str, str] = {}
    threads = []
    for root in roots:
        if not root.get("reply_count"):
            continue
        replies = _api(token, "conversations.replies", channel=channel, ts=root["ts"], limit=100)
        messages = []
        for item in replies.get("messages", []):
            who = item.get("user") or ""
            label = AGENTS.get(who) or humans.setdefault(who, f"HUMAN_{len(humans) + 1}")
            text = item.get("text") or ""
            messages.append({
                "t": float(item["ts"]), "who": label, "text": redact(text, humans),
                "mentions": [AGENTS.get(u, "HUMAN") for u in _MENTION.findall(text)],
                "bare": len(_BARE.findall(text)),
                "reactions": [r["name"] for r in item.get("reactions", [])],
            })
        threads.append({"root_ts": root["ts"], "messages": messages})
    out.write_text(json.dumps(threads, indent=1), encoding="utf-8")
    return len(threads)


def measure(threads: list[dict]) -> dict:
    agents = set(AGENTS.values())
    first: list[float] = []
    leaks = bare = status = answered = extra = 0
    for thread in threads:
        messages = thread["messages"]
        root = messages[0]
        asked = set(root["mentions"]) & agents
        replies = [m for m in messages[1:] if m["who"] in agents and not m["text"].startswith(STATUS_PREFIXES)]
        status += sum(1 for m in messages[1:] if m["text"].startswith(STATUS_PREFIXES))
        if replies:
            first.append(replies[0]["t"] - root["t"])
        if asked and any(m["who"] in asked for m in replies):
            answered += 1
        leaks += sum(1 for m in messages if "NO_REPLY" in m["text"])
        bare += sum(m["bare"] if isinstance(m["bare"], int) else len(m["bare"]) for m in messages)
        if asked and len(replies) > 1 and ({m["who"] for m in replies} - asked):
            extra += 1
    first.sort()
    return {
        "threads": len(threads),
        "answered_by_addressee": answered,
        "median_first_reply_s": round(statistics.median(first)) if first else None,
        "p90_first_reply_s": round(first[max(0, int(len(first) * 0.9) - 1)]) if first else None,
        "marker_leaks": leaks,
        "bare_mentions": bare,
        "status_lines_in_threads": status,
        "threads_with_unasked_extra_voices": extra,
    }


def _vote(prompt: str, model: str) -> dict:
    completed = subprocess.run(  # nosec B603
        ["claude", "-p", "--output-format", "json", "--model", model, "--dangerously-skip-permissions", prompt],
        capture_output=True, text=True, timeout=300, env={k: v for k, v in os.environ.items() if not k.startswith("ANTHROPIC_")},
    )
    payload = json.loads(completed.stdout.strip().splitlines()[-1])
    result = str(payload.get("result") or "")
    match = re.search(r"\{.*\}", result, re.S)
    scores = json.loads(match.group(0)) if match else {}
    scores["_cost"] = float(payload.get("total_cost_usd") or 0)
    return scores


def judge(threads: list[dict], samples: int, model: str) -> dict:
    keys = ("answered", "lane", "evidence", "tone")
    rows = []
    cost = 0.0
    for thread in threads:
        transcript = "\n".join(f"[{m['who']}] {m['text'][:600]}" for m in thread["messages"])
        votes = []
        for _ in range(samples):
            try:
                votes.append(_vote(RUBRIC + "\n\n" + transcript, model))
            except Exception as error:  # a failed vote is a missing vote, never a zero
                votes.append({"_error": type(error).__name__})
        cost += sum(v.get("_cost", 0) for v in votes)
        row = {"root_ts": thread["root_ts"], "votes": len([v for v in votes if "_error" not in v])}
        for key in keys:
            values = [int(v[key]) for v in votes if key in v]
            row[key] = statistics.median(values) if values else None
            row[f"{key}_spread"] = (max(values) - min(values)) if values else None
        row["note"] = next((v.get("note") for v in votes if v.get("note")), "")
        rows.append(row)
    summary = {key: round(statistics.mean([r[key] for r in rows if r[key] is not None]), 2) for key in keys}
    return {"threads": rows, "summary": summary, "cost_usd": round(cost, 2), "samples": samples, "model": model}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="conversation_eval")
    sub = parser.add_subparsers(dest="command", required=True)
    export_parser = sub.add_parser("export")
    export_parser.add_argument("--channel", required=True)
    export_parser.add_argument("--hours", type=float, default=48)
    export_parser.add_argument("--out", required=True)
    measure_parser = sub.add_parser("measure")
    measure_parser.add_argument("corpus")
    judge_parser = sub.add_parser("judge")
    judge_parser.add_argument("corpus")
    judge_parser.add_argument("--samples", type=int, default=3)
    judge_parser.add_argument("--model", default="claude-opus-5")
    judge_parser.add_argument("--out")
    args = parser.parse_args(argv)
    if args.command == "export":
        token = os.environ.get("SLACK_BOT_TOKEN", "")
        if not token:
            print("SLACK_BOT_TOKEN required", file=sys.stderr)
            return 2
        print(json.dumps({"threads": export(token, args.channel, args.hours, Path(args.out))}))
        return 0
    threads = json.loads(Path(args.corpus).read_text(encoding="utf-8"))
    if args.command == "measure":
        print(json.dumps(measure(threads), indent=1))
        return 0
    report = judge(threads, args.samples, args.model)
    if args.out:
        Path(args.out).write_text(json.dumps(report, indent=1), encoding="utf-8")
    print(json.dumps({"summary": report["summary"], "cost_usd": report["cost_usd"], "threads": len(report["threads"])}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
