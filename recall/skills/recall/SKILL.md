---
name: recall
description: Find prior Claude Code and Codex sessions with an indexed local search engine, then continue the work, repeat it with fresh inputs, or distill it into a skill. Runs from Claude Code, Codex, or pi; the current index covers Claude Code and Codex transcripts. Use when the user names Recall, says "find that conversation where…", "what did we do last time about…", "continue what we did yesterday on X", "what did codex do on this branch", "turn what we did about Y into a skill", or "remember when you…". Not for searching code (use grep on the repo) or for facts already in MEMORY.md.
---

# /recall: session memory engine

Claude Code and Codex sessions on this machine are indexed into a local
SQLite engine. Query the index and read only the best-supported session instead
of searching entire transcripts. All commands go through one CLI:

```bash
python3 scripts/recall.py <command>       # relative to this skill directory
```

If Recall MCP tools are available in the current agent, immediately
read [references/central-brain.md](references/central-brain.md)
before running commands and use those tools directly. Do not run the local CLI
as a parallel or fallback retrieval path. The same central instructions apply
when `RECALL_URL` is set, `RECALL_MODE` is `remote` or `shadow`, or
`~/.config/recall-brain/client.json` exists. Otherwise everything below is fully local and nothing
touches a network.

## No index yet? Search anyway

If `search` reports the index does not exist (or `doctor` shows
`db exists=False`), search the raw JSONL transcripts immediately while the
first index builds:

```bash
rg -l -i "<terms>" ~/.claude/projects ~/.codex/sessions   # candidate files
ls -t <hits>                                              # newest first
rg -n -i -C3 "<terms>" <best-hit>                         # read the window
```

Choose terms and regular expressions based on the request. Exact identifiers
are stronger evidence than general prose. If `rg` is unavailable, use
`grep -rl`. Start the index in the background at the same time:

```bash
setsid nohup python3 scripts/recall.py index >/dev/null 2>&1 &
```

The first build over a large history can take many minutes; later runs are
incremental and fast. Tell the user when an answer came from a cold scan of the
raw transcripts. Once `doctor` shows a healthy db, switch to indexed `search`,
which ranks results, explains the `WHY` for each match, and matches identifiers
exactly.

## First: pick the outcome

1. **Find / verify:** answer "did we…", "which session…", "how did we…".
   Search, read the best hit's relevant window, answer with the session path
   as the receipt.
2. **Continue:** resume in-progress work. This needs the session's tail plus its
   branch and worktree.
3. **Repeat:** redo the same kind of task with fresh inputs. This needs the
   original driving prompts, verbatim.
4. **Skill-ify:** turn the recipe into a reusable skill. This needs the steps that
   worked, minus one-off data. Chain into the harness's skill creator when one
   is installed; otherwise write the standard `SKILL.md` package directly.

Ask only if the outcome is genuinely ambiguous.

## Search

```bash
python3 scripts/recall.py search "<what the user said>" [filters]
```

- Pass the user's phrasing plus any identifier you have. Identifiers (job
  UUIDs, PR numbers, pod names, error strings, filenames) are the strongest
  evidence and are matched exactly, including inside tool output.
- Apply filters as flags rather than approximating them in the query text:
  | ask | flag |
  |---|---|
  | "last 48h", "back in May" | `--since 2026-05-01 --until 2026-06-01` (UTC; both bounds include the specified instant. To cover a full local day, use the NEXT day's date as `--until`; convert the user's local day first) |
  | "in the other worktree/checkout" | `--cwd <any-cwd-substring>` |
  | "what did codex do" | `--harness codex` |
  | branch-scoped | `--branch <substring>` |
- Output is ranked sessions with date, cwd, slot, branch, a matched snippet,
  and `WHY` it matched. Empty output means nothing cleared the evidence gate;
  it does not prove the work never happened. Retry once with a distinctive
  identifier or a wider window. If output is still empty, tell the user what
  you searched and that no supported match was found.
- `--paths` prints bare file paths (for scripting); `--limit N` widens.

## Read the best-supported hit

Check the `WHY` line on the top few results first. Matches on only generic words
are weak evidence. Prefer a result whose `WHY` includes an identifier, phrase,
or exact-entity match rather than selecting rank 1 automatically.

```bash
python3 scripts/recall.py show <path> --prompts                    # user prompts only
python3 scripts/recall.py show <path> --around 2026-07-03T14:20    # ±3-turn window; use the date printed in the search result
python3 scripts/recall.py show <path> --tail 30                    # the session's final turns (for Continue)
```

Pass the session file path from the search output. Use `show` rather than
`cat`: sessions can reach 80 MB, and `show` parses and prints only the requested
content.

## Related work (no query needed)

```bash
python3 scripts/recall.py related --cwd "$(pwd)" --branch "$(git branch --show-current)"
```

This returns sessions that share the project, branch, or touched files, ranked
by overlap and recency. Use it at session start when the user references prior
work without naming it.

## Outcome playbooks

**Find / verify:** search → `show --around` the matched timestamp → answer
with evidence. This usually requires two commands.

**Continue:** search → `show --tail 30` for the final state (last actions,
tool results, open errors) → check the session's branch/slot still exists
(`git -C <cwd> branch --show-current`) → summarize: "Found `<session>` in
`<cwd>` on `<date>`, last action `<x>`, branch `<b>`; resume there or here?"

**Repeat:** search → `show --prompts` → present the driving prompts verbatim
and confirm fresh inputs (dates, scope) before re-running.

**Skill-ify:** search → `show` the working window → separate the durable
recipe (commands, endpoints, auth patterns) from one-off data (specific IDs,
dates) → invoke the available skill creator with the recipe and a proposed
name, or create a standard Agent Skills directory when none is installed.

## Export one exact session for another skill

Use the machine-readable session export when `/recap` or another evidence consumer needs complete,
ordered coverage rather than a human window:

```bash
python3 scripts/recall.py session-export --current --limit 1000
python3 scripts/recall.py session-export --target <exact-path-or-receipt> --limit 1000
python3 scripts/recall.py session-export --cursor <opaque-next-cursor> --limit 1000
```

Each JSON page contains stable evidence IDs, redacted text and digests, sanitized typed entities
(including native tool identity when observed), native session identity, projection/privacy
versions, a boundary receipt, a content-free page receipt, and `complete` plus `next_cursor`.
Consume pages in sequence and accept immutable-snapshot completeness only on the final page; inspect
`source_snapshot_stable` before claiming a live source did not advance. Cursors are stored
owner-private under `~/.recall` and never encode transcript text or a path.

`--current` resolves Codex only through exact `CODEX_THREAD_ID`, and Claude through exact
`CLAUDE_SESSION_ID` when the harness exposes it. Otherwise it fails closed with content-free ranked
candidate receipts; pass the exact path found by Recall rather than guessing. Child and continuation
sessions are separate boundaries by default. A standalone local export stamps an explicit
`local:<harness>` source.

To resolve a local native relationship graph for Recap without reading transcript prose, use:

```bash
python3 scripts/recall.py session-relations --current --include-children
python3 scripts/recall.py session-relations --target <exact-path> --chain
python3 scripts/recall.py session-relations --target <exact-path> --chain --include-children
```

The closed `recall.session-relations.v1` JSON uses Claude `sessionId`/`agentId` sidechain metadata
and Codex `parent_thread_id`/`forked_from_id` metadata. It excludes merely adjacent or similar
sessions and fails when a requested native link is missing or ambiguous. This command is
local-only.

## Index health

```bash
python3 scripts/recall.py index      # incremental; run if results look stale
python3 scripts/recall.py doctor     # coverage, index age, retention watchdog
```

Never run `index --rebuild` without the user's explicit request; large session
histories can require gigabytes of temporary WAL and substantial CPU time.

`doctor` warning about `cleanupPeriodDays` means transcript retention got
re-enabled. Surface that to the user immediately because history is being
deleted.

## Gotchas

- The engine indexes user text, assistant text, and tool input/output, but
  reasoning/thinking blocks are never stored, and secret-shaped lines are
  redacted at ingest. If the only trace of something was a thinking block, it
  is not findable.
- Codex sessions are one file per rollout under a date tree; `show` handles
  both schemas transparently.
- Running Recall from pi is supported, but pi's own session format is not yet
  indexed. Do not claim that a cold result proves no pi session exists.
- A query about work that never happened can still return lexically-adjacent
  sessions. The ranked `WHY` line tells you what actually matched; read it
  before asserting the session answers the question.
- Subagent and workflow transcripts are indexed as their own sessions and
  live under the parent session's directory (`<session-uuid>/subagents/…`);
  the path itself tells you which main session spawned them.

## Upgrade: central Recall Brain (optional)

Recall can sync into a private central Brain service that provides deliberate
memory writes, cross-device search, consented ChatGPT-export import, a Cowork
collector, pull connectors, and MCP capture. The service is off unless
explicitly configured. Setup, mode routing, and all Brain commands are in
[references/central-brain.md](references/central-brain.md); `doctor` prints
the current mode.

## References

- [references/query-cookbook.md](references/query-cookbook.md): worked
  examples per stratum: identifiers, error strings, time windows,
  cross-worktree, cross-harness, paraphrase.
- [references/central-brain.md](references/central-brain.md): optional
  central Brain upgrade: setup, modes, deliberate writes, connectors,
  privacy, export inbox, MCP capture.
