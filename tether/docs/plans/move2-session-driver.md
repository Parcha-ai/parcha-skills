# Move 2 — one session process per binding; delete the durability core

Status: design, 2026-09-08. Plan: `.cascade/colleague.md` C6→C8. Assessment: Colleague (2026-09-05).

## Why

Tether's 7,116-line durability core (`domain_schema`, `domain_runtime`, `domain_control`) and the
432-line `native_driver` exist to prove that a *forked* `claude --print` process delivered one turn:
attempts, leases, receipts, journals, uncertain-state classification, generation fences. The premise
was that continuing a session meant spawning a new process per Slack message.

Verified on greppy3 (2026-09-06): one `claude -p --resume <sid> --input-format stream-json
--output-format stream-json` process takes many user turns on stdin; every `result` event carries
the same session id; turn 2 recalled turn 1; second-turn latency 1.4 s. Codex: `codex app-server`
over stdio answers `thread/list`, `thread/resume`, `turn/start`. With a pipe there is nothing to
prove after the fact: the reply is a line we read, and delivery is the Slack message id Hermes returns.

## What stays (the three seams)

| Seam | Module | Change |
|---|---|---|
| binding: thread ↔ session | new `store.py` (~250 lines) | tables `bindings`, `turns`; replaces the 7k core |
| admission: who wakes it | `admission.py`, `journal.py` | unchanged |
| launcher: how it runs | `launch_plan` in `active.py` | unchanged (systemd-user / direct) |
| ingress/egress | `__init__.py` hook, `slack_egress.py`, `broker.py` | unchanged protocol v6; same ops |

## What changes

### `store.py` — the whole persistent model

```
bindings(binding_id PK, team_id, channel_id, thread_ts, source_kind, session_id, session_name,
         cwd, owner_user_id, state{active,closed}, generation, created_at, updated_at)
turns(event_key PK, binding_id, ordered_at, actor, text, state{ready,running,done,failed,cancelled},
      reply_ts, error, created_at, updated_at)
```
Idempotency: `event_key` = `slack:<team>:<channel>:<message_ts>` (already the case). `admit_turn`
is `INSERT OR IGNORE`. `close_binding` refuses while `ready`/`running` turns exist (same contract as
today's `binding_has_ready_turns`). `recent_turn_actors` keeps the peer-chain cap. `counts()` keeps
`tether status` shape. One SQLite file, WAL, `check_same_thread=False` + a lock (Hermes 0.21 hooks
run on worker threads).

### `session_driver.py` — one process per binding

- `SessionProcess(binding)` owns a `claude -p --resume <sid> --input-format stream-json
  --output-format stream-json --verbose <claude_resume_args>` child, launched through
  `launch_plan` (so it runs in the operator's systemd user session), cwd = binding cwd,
  env = `child_env(harness_env)`.
- `send(turn)`: writes one `{"type":"user","message":{...}}` line. Turns for a binding are strictly
  serial (a lock per binding); different bindings run concurrently.
- `read()`: consumes events until `type == "result"`; returns `result`, `session_id`,
  `is_error`, `duration_ms`. Text deltas can be forwarded to Hermes native streaming later; v1 posts
  the final only (same as today).
- Idle policy: process stays up `session_idle_seconds` (default 900) after the last result, then exits
  (`stdin.close()`); next turn relaunches with `--resume`. Close binding = terminate.
- Failure: non-zero exit or `is_error` → turn `failed`, post the one-line failure notice with the
  last stdout/stderr line (existing behaviour), process dropped; next turn relaunches.
- Timeout: `native_timeout_seconds` per turn → terminate, notice, relaunch on next turn.
- Session id drift: if a `result` carries a different `session_id` (fork), update the binding and log.
- Codex: `CodexThreadDriver` speaks JSON-RPC to one `codex app-server` stdio child per gateway:
  `thread/resume` on first use, `turn/start` with `input=[{type:text}]`, wait for `turn/completed`,
  collect `item/*` text. Same `send/read` interface. Ephemeral probe on 2026-09-06 started a turn but
  MCP servers configured in `~/.codex/config.toml` stalled it; the driver passes
  `-c mcp_servers={}` for Tether turns.
- Discovery: `tether sessions` = `claude agents --json` + Codex `thread/list` (cwd filter). `bind`
  accepts a session **name** and resolves it via that list.

### Hooks in (later, optional)
A Claude Code `Stop`/`Notification` hook (`type: http` to a local listener, or a command hook calling
`tether` on the broker socket) lets a session that is *waiting for input* ask in its thread. Not
needed for v1: the stream-json `result` already tells us when a turn ends.

## Migration

1. `driver = "native" | "session"` in `config.toml`; default `native` until proven, then `session`.
2. Bindings table is rebuilt from `thread_bindings` at first start (only active rows; ids kept).
3. Old attempts/receipts are not migrated; the old `domain.db` is left in place until C8.
4. Fleet flip = config change + gateway restart, per box, canary first (claudio).

## Deletion ledger (C8)

| Delete | Lines |
|---|---|
| `domain_schema.py`, `domain_runtime.py`, `domain_control.py` | 7,116 |
| `native_driver.py` | 432 |
| `security.py` (keep only what admission imports) | ~800 of 926 |
| `tether_notify.py` zellij/herdr stubs, legacy doctor paths | ~800 of 1,113 |
| tests bound to the core (`test_domain_*`, `test_native_driver`, `test_multi_agent_chaos` parts) | ~2,300 |
| `install.sh` manifest machinery if `hermes plugins install` covers it | TBD |

Target after C8: under 3,000 lines of Python, same CLI verbs, same broker protocol, same doctor.

## Exit checks

- C7: unit tests for store + drivers (fake process speaking stream-json / JSON-RPC); bound turn on
  claudio through `driver = session` answers in-thread with evidence; peer cap, failure notice,
  close-refuses-while-busy, idempotent admit all pass; 230+ tests green.
- C8: `wc -l` Python < 3,000; fleet deploy clean; conversation eval not worse than C4.
