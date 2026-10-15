# Configure your colleagues

The team manifest gives a Tether instance its own identity, colleague roster
and project references. The same section is used in Hermes's system prompt
and native continuation prompts. The bundled collaboration contract is
neutral; no Parcha account IDs or private team are injected by default.

## Select a manifest

Set `team_config = "team.toml"` in Tether's config at
`$XDG_CONFIG_HOME/tether/config.toml` (default `~/.config/tether/config.toml`).
Relative paths resolve from the config file's directory; absolute paths and
`~` work too. The gateway environment's `TETHER_TEAM_CONFIG` takes precedence;
a relative environment path resolves from its working directory.

An absent manifest setting supplies the generic collaboration contract. An
explicit path that is missing, malformed or too large produces an actionable
configuration error instead of silently choosing a different team.

Use [the example](../examples/team.toml) to start. Each instance may share the
same roster while selecting its own colleague with `self`. The manifest
contains context, not credentials.

## Format

| Location | Fields |
| --- | --- |
| Root | Optional `version` (defaults to `1`), optional `self` (a colleague ID) |
| `[[colleagues]]` | Required `id`, `name`, `role`; optional `slack_id`, `computer`, `projects` |
| `[[projects]]` | Required `id`, `ref` |

IDs start with a lowercase letter and contain lowercase letters, digits, `_`
or `-`, up to 48 characters. Names allow 80 characters, roles 240, computer
labels 80, and project references 320. Text fields use one line without
surrounding whitespace or control characters. A Slack member ID uses plain
`U…` or `W…` syntax; rendering adds the mention markup.

Colleague and project IDs must be unique. Slack member IDs cannot be assigned
to two colleagues. A colleague's `projects` array names defined project IDs;
`self` must name a configured colleague. Unknown fields and incorrect types
are rejected, so a misspelled option does not silently disappear.

The file is bounded to 64 KiB, at most 20 colleagues and 20 projects. The full
rendered section must fit Hermes's 4,000-character section budget, including
the collaboration contract. Tether rejects overflow with the actual size
and an instruction to shorten the roster; it does not cut off a colleague.

## What the settings do

Names, roles, mentions and project references tell the model who it is working
with and where to look. Roles guide collaboration; useful findings can come
from any colleague. The generic contract asks colleagues to act on requests,
report evidence, incorporate corrections, avoid acknowledgment loops and
distinguish promises from finished work.

`computer` is a preference in context. It does not select a provider or model,
authenticate an account, create a session or replace an attached computer.
Project references are literal context; this slice does not grant filesystem
access or resolve relative paths into a runtime working directory. Use existing
Tether/Hermes/runtime configuration for execution, authorization and model
selection. A roster entry is not an allowlist.

## Preview the workflow

From the repository root:

```bash
node tether/bin/tether.js demo --team-config tether/examples/team.toml --json
```

The offline demo uses configured names for its two scripted colleagues and
simulated identities/computers. It writes a buggy median artifact, reviews it
against independent cases, routes a correction, reviews the fix and continues
the same owner session. The JSON receipt separates five completed transport
attempts from one accepted sample task. The artifact source and review evidence
remain in the receipt after temporary files are cleaned up.

This is a repeatable engineering example. A real implementer/reviewer task
service, stable Hermes task references, current artifact context and dependency
wakes belong to the [next product slice](ROADMAP.md).


## Link work to an existing Hermes task

Configure only the existing boards this instance should read. In Tether's
config.toml, place top-level settings before the task_boards table:

```toml
# Optional exact tenant guard, including linked dependencies.
task_tenant = "my-team"

[task_boards]
project = "/absolute/path/to/existing/kanban.db"
```

The path is an operator setting, not a value accepted from a Slack message.
Tether never discovers a current board or falls back to HERMES_KANBAN_DB. It
opens the configured database read-only and requires the host's canonical
Hermes task read functions. Missing functions or an incompatible schema give
a task-context error. The local contract checks use Hermes 0.21.1 source;
this does not establish compatibility with every stock host release.

After attaching a coding session, use its actual bridge ID and existing Hermes
task ID. Inspect the current generation, then link it explicitly:

```bash
tether task show --bridge-id "$BRIDGE_ID" --json
tether task link --bridge-id "$BRIDGE_ID" --board project \
  --task-id "$TASK_ID" --expected-generation "$GENERATION" --json
```

Set GENERATION from the binding returned by show. Changing the association
advances it. A stale generation or queued/running turns prevents reassociation;
read the current binding again after the active work finishes. To remove the
link, use task unlink with the current generation.

Before each linked continuation, Tether reads the current task, relevant
comments, runs, review events, attachments and direct dependency summaries.
It records a digest and the exact projected evidence on that attempt. A later
review finding or artifact reference appears on the next turn, including after
a Store restart. The latest human message follows the task context in the
prompt so the current request remains explicit.

Task show returns fresh context and the last attempt's context digest. Its
context_changed_since_last_attempt field tells you when the evidence has
changed. It does not imply that a new review has happened or invalidate a host
review automatically. Artifact pointers and acceptance receipts are evidence to
inspect; a local path is not automatically accessible on another computer.

The rendered context is limited to 32 KiB of UTF-8. Recent history is retained
with explicit omission counts. Essential task text is not silently truncated;
if it exceeds the budget, the linked turn fails before starting the computer.
An unavailable task or scope mismatch also prevents execution and produces a
failure notice. Unlinked conversations keep their existing continuation path.

A task link grants no dispatcher claim or run ownership. It does not inject
HERMES_KANBAN_TASK, complete a task from a sent answer, change the model, or
start another computer. Hermes remains responsible for task writes, review
state, scheduling and dependency wakes. Linking and showing context are the
implemented connection; exact-session wakes and a live two-colleague task
completion episode remain the next build.
