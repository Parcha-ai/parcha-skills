# recall

[![GitHub](https://img.shields.io/badge/source-Parcha--ai%2Fparcha--skills-181717?logo=github)](https://github.com/Parcha-ai/parcha-skills/tree/main/recall)

Claude Code and Codex keep detailed local transcripts of each session,
including prompts, responses, commands, tool results, branches, and working
directories. As the history grows, finding earlier work otherwise requires
knowing roughly when it happened and searching large JSONL files.

**recall** indexes that history locally. Queries can describe the work in
ordinary language:

```text
"find the session where the staging pod kept OOMing"
"what did Codex do on this branch?"
"continue the Greptile review work from back in May"
```

Recall ranks matching sessions, reports **why** each one matched, and reads the
relevant turns without loading an entire transcript. The same index covers
Claude Code and Codex, so either harness can find work recorded by the other.

```text
query         -> describe the earlier work
recall        -> rank sessions and report why they matched
agent         -> read the relevant window and continue the task
```

## What it does

- **Find and verify:** search old sessions by natural language, exact IDs, error
  strings, date, worktree, branch, or harness.
- **Continue:** recover the last actions, open problems, branch, and worktree
  from an unfinished session.
- **Repeat:** extract the prompts that drove an earlier task and run it again
  with fresh inputs.
- **Find related work:** surface sessions connected to the current repo or
  branch at session start.
- **Turn work into a skill:** extract the reusable method from a successful
  session while excluding its task-specific data.

The transcripts remain the source of truth, and the SQLite index is disposable
and fully rebuildable. A first index build does not block search: until it
finishes in the background, the skill scans the raw JSONL transcripts with
`rg`.

No-answer detection is lexical. A query about work that never happened can
therefore return a session containing similar words. Each result includes a
`WHY` line so the agent can inspect the evidence before claiming a match.

## Install

[skills.sh](https://skills.sh/miguelrios/unc-skills/recall):

[View Recall on skills.sh](https://skills.sh/Parcha-ai/parcha-skills/recall).

```bash
npx skills add miguelrios/unc-skills --skill recall
```

Claude Code:

```bash
claude plugin marketplace add Parcha-ai/parcha-skills
claude plugin install recall@unc-skills
```

Codex:

```bash
codex plugin marketplace add Parcha-ai/parcha-skills
codex plugin add recall@unc-skills
```

pi (installs the complete unc-skills collection):

```bash
pi install git:github.com/Parcha-ai/parcha-skills
```

Start a new session, invoke Recall using the harness's normal skill syntax, and
ask it to `index my session history`. The skill runs its engine relative to its
installed directory, so you do not need to find a plugin-cache path.

For a direct/manual Claude install:

```bash
git clone https://github.com/Parcha-ai/parcha-skills.git
cd parcha-skills/recall
./install.sh
```

Then build the local index directly:

```bash
python3 ~/.claude/skills/recall/scripts/recall.py index
python3 ~/.claude/skills/recall/scripts/recall.py doctor
```

To surface related sessions automatically at the beginning of Claude Code
sessions, run `./install.sh --hook`. It prints the `settings.json` hook
configuration for you to review and add. Search works from Codex and pi without
the hook. Recall currently indexes Claude Code and Codex transcripts; pi can run
the search, but pi's own transcript format is not indexed yet.

## How it works

- `skills/recall/scripts/recall.py` is a stdlib-only Python engine backed by one
  SQLite database, FTS5, an entity index, and evidence-tiered ranking.
- `session-export` gives evidence consumers such as Recap an exact, redacted,
  ordered session snapshot with stable IDs and opaque local/central pagination;
  it never relies on scraping Recall's human-readable `show` output.
- `session-relations` resolves local Claude sidechains and Codex child/fork
  edges from bounded native metadata. It does not infer relationships from
  time, cwd, filenames, or transcript prose.
- `skills/recall/SKILL.md` teaches the agent when to search, how to judge a
  match, and how to find, continue, repeat, or skill-ify prior work.
- `skills/recall/scripts/recall-hook.sh` is an optional SessionStart hook. It is
  bounded, fail-open, and keeps the index fresh without a daemon or cron job.
- `tests/` contains unit and synthetic-fixture tests. Private transcript-derived
  evaluation corpora are deliberately kept outside the repository.

Indexing and search run on the local machine. Secret-shaped lines are redacted
during indexing, thinking blocks are not indexed, and the index directory is
created with user-only permissions.

## Optional upgrade: Recall Brain

Recall is local by default. The separate, self-hosted **Recall Brain** service
adds deliberate memory writes, cross-device search over the tailnet, consented
ChatGPT-export import, a Cowork collector, pull connectors, and MCP capture.
Recall does not contact a network until you explicitly configure
`RECALL_URL` or `~/.config/recall-brain/client.json`; `RECALL_MODE=local` is
the rollback setting. Agent-facing details are in
`skills/recall/references/central-brain.md`; operator docs in `client/`,
`connectors/`, and `server/deploy/`.

## Requirements

- Python 3.10+ with SQLite FTS5 (included in stock Python on Debian, Ubuntu,
  and macOS).
- Claude Code and/or Codex CLI session history on disk. The operating harness may also be pi.
- Linux or macOS.

Retrieval architecture was informed by
[garrytan/gbrain](https://github.com/garrytan/gbrain), especially its
deterministic substrate, hybrid ranking, and treatment of doctor/eval as
first-class tools. The session catalog pattern was borrowed from Codex's own
`state_5.sqlite`.

MIT © Miguel Rios
