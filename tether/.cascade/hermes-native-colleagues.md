# Hermes-native colleagues — living plan

Assessment: https://claude.ai/code/artifact/c3b586c2-3b09-4451-9d23-55e438cf6098 (2026-09-10).
Authorized by Miguel 2026-09-10: adopt items 1–8 of the assessment ("no 9"); cards and buttons are
wanted for QA and similar flows. Standing principles: leverage Hermes, build on top only when
necessary, zen; the agents are harnesses.

## Outcome and acceptance
Every primary bot is a Claude Code (or Codex) session inside Hermes, addressed as a named profile
in Slack, coordinated through Hermes' own board, goals and peer channels; Tether is a thin plugin
that only attaches outside-started sessions and launches with operator privileges.
**Final acceptance:** (a) the eight-agent board build re-run on kanban, read cold, shows only work
in the thread: zero status lines, zero marker leaks, every lane closed with evidence; (b) Tether
Python runtime under 800 lines and no session driver, store, or Slack client of its own; (c) six
upstream gaps either merged in Hermes or carried as shims listed in `docs/upstream/` with the PR link;
(d) a QA report thread carries a native task card for the run and an Approve button that a human
can press to trigger the next step.

## Context to resume
- Workspace: `~/worktrees/tether-l2d/tether` (parcha-skills), this branch `cascade/hermes-native-2026-09-10`
  (`.cascade/` gitignored; this file force-added and pushed at every boundary).
- Hermes work: `~/worktrees/hermes-agent` (clone of ~/.hermes/hermes-agent at 6e2b8e070d), branch
  `feat/claude-code-runtime` (commit 7387930a73 as applied on greppy-bc). Patch + PR recipe in
  `docs/upstream/hermes-claude-code-runtime.md` (PR 482, merged). The machine App cannot fork
  NousResearch or create repos: upstream PRs and the Parcha fork are Miguel's to open; until then,
  patches ride as `git am` on each gateway checkout and are re-applied after `hermes update`.
- Fleet: claudio (greppy3), mikael/irma (greppy3 isolated, secondary, no claude login), greppy-cr
  (chris), greppy-sam, greppy-bc (bryan, CANARY on the runtime since 2026-09-10 17:16 UTC), greppy-mg
  (manny, codex app-server natively), m (q2). Tether main at ed8089f deployed everywhere (installer only).
- Constraints: test only in #agent-hub C095VU95XQR; never ping Miguel from tests; tether/opus-5 model
  policy; no Northflank; never restart a gateway with a live thread in flight; validate YAML after
  every config write; deploys via installer / `hermes update`, never hand copies.
- Runtime config block (Bryan, proven): `model.anthropic_runtime: claude_code`,
  `claude_code: {permission_mode: acceptEdits, allowed_tools: [Bash, Read, Edit, Write, Grep, Glob,
  WebFetch, WebSearch, Task], model: claude-opus-5, turn_timeout_seconds: 1800}`.
- Known Hermes gaps (from the 2026-09-10 survey): no per-thread cwd (gateway/run.py:4083 never passes
  cwd; session_context.py:116 accepts it); images not passed as media to claude_code turns
  (claude_code_session.py:350); status notices unconditional (run_busy.py:410, run_shutdown.py:932,
  run_turn.py:852); NO_REPLY exact-match only (response_filters.py:50); no trusted-peer allowlist or
  loop guard (authz_mixin.py:447); inbound dedupe in-memory (slack adapter:917); native task cards
  send markdown_text+chunks and Slack rejects (docs/upstream/slack-native-task-cards-append-payload.md).
- Decisions: skip community plugins (item 9) except none; keep anthro/irma sandboxed and on Hermes'
  native loop; peer allowlist semantics = one peer repeating with nobody else in between is capped.

## Tasks
| ID | Deliverable | Depends | Exit check | Status | Evidence |
|---|---|---|---|---|---|
| H1 | Bryan soak: 24 h on the claude_code runtime across all his conversations | — | zero failed turns in gateway.log (`claude code turn failed`, `should_retire`), no duplicate replies, one Claude process per session at a time; note anything a human colleague would not do | doing | started 2026-09-10 17:16 UTC; two smoke turns (Write tool, terminal rm) in 6 s each |
| H2 | Runtime on claudio, sam, chris (same config block; manny stays codex) | H1 | each gateway: mention in #agent-hub answered through a Claude Code tool (tool rows in state.db), `hermes doctor` clean, restart with no live thread | todo | |
| H3 | Per-session cwd in Hermes: persist `session_cwd`, `/cwd <path>` slash command, and a `start a session here` path so a thread can own a worktree | — | two #agent-hub threads on one gateway edit files in two different worktrees; cwd survives a gateway restart; tests | todo | plumbing: gateway/session_context.py:116, hermes_state_sessions.py:486 |
| H4 | Media into claude_code turns: image content blocks; documents keep the annotated path | — | a Slack image mention is described correctly by the bot; a JSON upload is read from its path; tests | todo | |
| H5 | TEAM.md as a `register_system_prompt_section` block; delete `tether team apply` and SOUL splicing | — | fresh session prompt dump (`hermes prompt-size` / debug) contains the block once; splice code removed; tests | todo | |
| H6 | `hermes send` replaces `tether post`/`notify` in the tether skill for one-shot posts and files; the skill documents when a post must be a turn (then `tether reply`) | — | skill round-trip: text post, `MEDIA:` file post from a Claude Code session land in a thread; `tether post` retired from the skill | todo | verified `hermes send --list slack` works on claudio without a gateway |
| H7 | Profile routing: one profile per colleague on multi-bot hosts (greppy3: claudio + two secondaries), `profile_routes` for #agent-hub threads; retire Tether's binding table for Slack-originated work | H2 | a thread routed to a profile answers with that profile's SOUL/memory; `tether status` shows no Slack-originated bindings; store.py deleted or reduced to attach-only | todo | |
| H8 | Kanban board for the team: profiles as lanes (claude_code runtime = worker), `hermes project` worktree convention, decomposer on | H2,H3 | a 3-card chain (schema → impl → test) runs on two machines with dispatcher election; cards reach `review` with a PR link; `hermes kanban diagnostics` clean | todo | kanban.db already initialized on claudio, empty |
| H9 | Board build re-run on kanban (final acceptance a) | H5,H7,H8 | same brief as 2026-09-10, eight agents; thread read cold: 0 status lines, 0 marker leaks, every lane closed with evidence; compare against `agent-hub-build-postmortem-2026-09-10` | todo | |
| H10 | A2A + `hermes peer`: api_server on each primary with per-peer tokens, `a2a_agents` roster, cron `deliver=a2a` | H2 | chris asks bryan for numbers via `a2a_call` with a context_id and gets a multi-turn answer; `hermes peer dm` from greppy3 to greppy-bc round-trips | todo | |
| H11 | Goals, loops, heartbeats in the team rules: lead uses `/heartbeat` to chase open lanes, `/goal` for iterate-until-green cards | H5 | a lane left open gets one chase from the lead within the heartbeat interval, no human nudge; `/goal` closes a lint-fix card on its own | todo | |
| H12 | Native task cards: fix the appendStream markdown_text+chunks payload upstream, re-enable `native_task_cards` on one gateway | — | a tool-heavy turn shows one card with per-tool rows and no "Hermes is working" fallback; patch staged in docs/upstream | todo | |
| H13 | Slack buttons via `register_slack_action_handler`: Approve / Re-run on QA report threads (delegate-grep-code-review and Manny's verification report) | H12 | pressing Approve posts the decision and triggers the next step (merge or rerun) as a turn; unauthorized user click is refused; tests | todo | |
| H14 | Upstream small PRs: status-notice knob, NO_REPLY+content, peer allowlist + loop guard, inbound journal; delete the matching Tether shim as each merges | — | each PR opened (Miguel's fork) and the shim removed after merge; `docs/upstream/` index links them | blocked | blocker: fork/PRs need Miguel's GitHub account |
| H15 | Tether reduced: keep attach + launcher + remaining shims; delete session driver, store bindings for Slack work, Slack egress, presence, team apply | H7,H14 | runtime Python < 800 lines; doctor clean fleet-wide; attach smoke (terminal session → Slack thread) passes | todo | |

## Owners, blockers, next actions
- Owner: claudio session (this one). Workers: Explore/parable subagents for H3/H4/H5/H12 code in the
  Hermes worktree, serialized on files they share (claude_code_session.py, gateway/run.py).
- Blocked: H14 (and the runtime upstream PR) on Miguel opening the Parcha fork of hermes-agent.
- Next: H3 and H4 in the Hermes worktree now (independent of the soak); H5 and H6 in Tether now;
  H2 after Bryan's 24 h (2026-09-11 ~17:00 UTC); then H7, H8, H10.
