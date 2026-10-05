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
