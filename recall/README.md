# recall

**Your coding agents remember everything. They just can't find it.**

[![skills.sh](https://skills.sh/b/miguelrios/unc-skills)](https://skills.sh/miguelrios/unc-skills/recall)

Claude Code and Codex already keep detailed local transcripts of every session:
your prompts, their answers, commands, tool results, branches, and working
directories. Once those transcripts pile up, finding one piece of old work means
guessing when it happened and grepping gigabytes of JSONL.

**recall** turns that history into a local search engine your agent can actually
use. Ask the way you remember the work:

```text
"find the session where the staging pod kept OOMing"
"what did Codex do on this branch?"
"continue the Greptile review work from back in May"
```

recall returns ranked sessions, explains **why** each one matched, and lets the
agent read the relevant turns instead of loading an 80 MB transcript. It also
works across Claude Code and Codex, so work started in one harness is available
to the other.

```text
you           -> describe the work you remember
recall        -> rank the sessions and show why they matched
your agent    -> read the winning window and get back to work
```

## What it does

- **Find and verify:** search old sessions by natural language, exact IDs, error
  strings, date, worktree, branch, or harness.
- **Continue:** recover the last actions, open problems, branch, and worktree
  from an unfinished session.
- **Repeat:** extract the prompts that drove an earlier task and run it again
  with fresh inputs.
- **Find related work:** surface sessions connected to the current repo or
  branch at session start, before you remember to ask.
- **Turn work into a skill:** pull the reusable method out of a successful
  session without dragging along its one-off data.

The transcripts remain the source of truth. The SQLite index is disposable and
fully rebuildable.

The honest limit: no-answer detection is still lexical. A query about work that
never happened can return a nearby session with similar words. That is why every
result includes a `WHY` line and the skill tells the agent to inspect the
evidence before claiming a match.

## Install

skills.sh:

```bash
npx skills add miguelrios/unc-skills --skill recall
```

Claude Code:

```bash
claude plugin marketplace add miguelrios/unc-skills
claude plugin install recall@unc-skills
```

Codex:

```bash
codex plugin marketplace add miguelrios/unc-skills
codex plugin add recall@unc-skills
```

pi (installs the complete unc-skills collection):

```bash
pi install git:github.com/miguelrios/unc-skills
```

Start a new session, invoke Recall using the harness's normal skill syntax, and ask it to
`index my session history`. The skill runs its engine relative to its installed directory, so
you do not need to find a plugin-cache path.

For a direct/manual Claude install:

```bash
git clone https://github.com/miguelrios/unc-skills.git
cd unc-skills/recall
./install.sh
```

Then build the local index directly:

```bash
python3 ~/.claude/skills/recall/scripts/recall.py index
python3 ~/.claude/skills/recall/scripts/recall.py doctor
```

To surface related sessions automatically at the beginning of Claude Code sessions, run
`./install.sh --hook`. It prints the `settings.json` hook configuration for you to review and
add. Search works from Codex and pi without the hook. Recall currently indexes Claude Code and
Codex transcripts; pi can run the search, but pi's own transcript format is not indexed yet.

## How it works

- `skills/recall/scripts/recall.py` is a stdlib-only Python engine backed by one
  SQLite database, FTS5, an entity index, and evidence-tiered ranking.
- `session-export` gives evidence consumers such as Recap an exact, redacted,
  ordered session snapshot with stable IDs and opaque local/central pagination;
  it never relies on scraping Recall's human-readable `show` output.
- `session-relations` resolves local Claude sidechains and Codex child/fork edges from bounded native
  metadata. It never guesses relationships from time, cwd, filenames, or transcript prose.
- `skills/recall/SKILL.md` teaches the agent when to search, how to judge a
  match, and how to find, continue, repeat, or skill-ify prior work.
- `skills/recall/scripts/recall-hook.sh` is an optional SessionStart hook. It is
  bounded, fail-open, and keeps the index fresh without a daemon or cron job.
- `tests/` contains unit and synthetic-fixture tests. Private transcript-derived
  evaluation corpora are deliberately kept outside the repository.

Everything stays on your machine. Secret-shaped lines are redacted during
indexing, thinking blocks are not indexed, and the index directory is created
with user-only permissions.

## Optional upgrade: Recall Brain

recall is fully local by default. A separate, self-hosted **Recall Brain**
service adds deliberate memory writes, cross-device search over the tailnet,
consented ChatGPT-export import, a Cowork collector, pull connectors, and MCP
capture. Nothing contacts a network until you explicitly configure
`RECALL_URL` or `~/.config/recall-brain/client.json`; `RECALL_MODE=local` is
the instant rollback. Agent-facing details live in
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
