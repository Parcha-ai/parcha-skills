# Herdr tabs for Tether — living plan

Exploration: https://claude.ai/artifact/JVK32VoRnDEaDT28d1bjCc (2026-09-16). Authorized by Miguel
2026-09-16: "go plan this proper AND MAKE IT DOPE". Principles stand: leverage Hermes and Herdr,
build on top only when necessary, zen. Herdr's own CLI (JSON out) is the client; no socket
protocol reimplementation.

## Outcome and acceptance
A session a colleague starts from Slack lives in a Herdr tab on that box, in the workspace the
human named (or the repo's), labelled after the thread, visible the moment anyone opens Herdr.
Its replies land in the thread that asked, its pane shows the thread link, a dialog that blocks
it reaches the thread and is answered from there, and an existing Herdr session can be attached
to a thread by name. Codex and Claude Code both. Hosts without Herdr behave as today.

**Final acceptance (all live, read cold from Slack and Herdr):**
(a) In C07QDVCPWS1-style thread: `@claudio start a herdr tab called MCP in the grep.ai space
with claude code doing X` → tab "MCP" in workspace grep.ai, agent named `mcp`, first report in
that thread within the turn, pane sidebar shows `slack=<permalink>`; zero posts outside the thread.
(b) Miguel replies in the thread → the turn is visible in the pane as it runs, the final answer
lands in the thread, no clock.
(c) A dialog (folder trust, permission, question) blocks the pane → the dialog text is posted in
the thread; Miguel's reply answers it; work resumes; nothing auto-answered except folder trust on
a Tether-managed worktree.
(d) Same as (a)–(b) with `--harness codex`; the pane and the ChatGPT app both show the turn.
(e) `@claudio attach this thread to my hvrt session` binds the live pane named `hvrt`.
(f) On greppy-bc/mg/m (no Herdr) every op behaves exactly as before; on greppy-cr/sam a headless
Herdr session serves the tabs until the human attaches. `tether doctor` reports Herdr state.
(g) Tests: fake `herdr` binary covering every call; suite green; ruff clean.

## Context to resume
- Workspace: `~/worktrees/tether-herdr/tether` (parcha-skills), branch `cascade/herdr-tabs-2026-09-16`
  (carries the earlier plans; `.cascade/` gitignored, force-add at every boundary). Code branches
  off `main` (≥ 4b64243 + PR 602).
- Herdr 0.9.0 on greppy3 (client+server, session `pilot`, socket
  `~/.config/herdr/sessions/pilot/herdr.sock`). greppy-cr 0.8.2 and greppy-sam 0.9.0 installed,
  no server running. bc/mg/m: not installed. Update Herdr only outside a pane (unset `HERDR_*`),
  `herdr update --handoff`. Docs: https://herdr.dev/llms.txt (raw mdx at tag v0.9.0); skill:
  `herdr --skill`. Never experiment in Miguel's workspace `w1` (grep.ai); use a throwaway
  workspace and close it.
- Proven 2026-09-16 (lab workspace, closed): `tab create` → `agent start --kind claude|codex`
  → `agent get` (Claude reports `agent_session.value`; Codex needs the SessionStart hook approved:
  `[hooks.state."/home/ubuntu/.codex/hooks.json:session_start:0:0"]` lacks `enabled = true`, Miguel's
  call) → `agent prompt --wait` → reply from the native transcript. Claude shows a folder-trust
  dialog on a fresh cwd (`agent_not_ready`, state blocked): `send-keys down`, `send-keys enter`.
- Codex 0.154 TUIs are clients of the machine's app-server daemon: a Codex started in a pane holds
  no writer lock; the daemon does. Tether's daemon path (PR 529) already drives it and the pane
  shows the turn. Only Claude Code needs the Herdr prompt path (interactive `claude` and a headless
  `claude -p --resume` on one id would fork the session).
- Reply source per harness: Claude `~/.claude/projects/<cwd-slug>/<sid>.jsonl`, last assistant
  text block after the prompt's timestamp; Codex rollout final `agentMessage` (already in
  `find_transcript`). Never post narration; the final answer only (same rule as dd1818e).
- Turn clocks are gone (PR 547): a Herdr `agent prompt --wait` runs without `--timeout`; the
  12 h leak guard is the only bound.
- Thread routing rules (PR 602, deployed 16:40 UTC): a thread id without its channel is refused,
  a bind verifies the root exists, `--task-stdin`. The 2026-09-16 15:46 stray-root incident is the
  regression test for (a).
- Constraints: test only in #agent-hub C095VU95XQR; never ping Miguel from tests; deploys via
  the installer only; never restart a gateway with a live turn (check `turns.state='running'`);
  GitHub via claudio-michel[bot]; broker text via stdin from a file; opus-5 policy.
- Decisions: CLI wrapper over `herdr` (not the socket protocol); Herdr is optional everywhere;
  the human's workspace label is the contract (`--herdr-workspace "grep.ai"`), defaulting to the
  gateway's `herdr_workspace` setting, else the repo's workspace if Herdr has one for that cwd,
  else a workspace named after the repo; tab label = thread title (first 48 chars of the ask);
  agent name = slug of the tab label; folder trust auto-accepted only for cwd under
  `~/worktrees` or `~/parcha` (Tether-managed), any other dialog goes to the thread.

## Tasks
| ID | Deliverable | Depends | Exit check | Status | Evidence |
|---|---|---|---|---|---|
| T1 | `runtime/plugin_next/herdr.py`: thin client over the `herdr` CLI (`HERDR_SOCKET_PATH`, session discovery from `~/.config/herdr/sessions/*/herdr.sock`), typed calls: workspaces, tab_create, agent_start, agent_get, agent_list, agent_prompt(wait, no timeout), agent_wait, agent_read(detection), send_keys, pane_report_metadata, find_agent_by_session(sid), find_agent_by_name; `available()` false when no binary or no live socket | — | `tests/fakes.py` gains a fake `herdr` script (JSON responses, records calls); unit tests for every call and for `available()` on missing binary / dead socket | done | commit 6f208cf on feat/tether-herdr-tabs: runtime/plugin_next/herdr.py + fake herdr + tests/test_herdr.py (9 tests: layout ids, cwd→workspace, start/prompt/session id, trust dialog blocked-not-failed, codex id absent until hook, CLI error codes, discover needs binary+live socket, TETHER_HERDR=off) |
| T2 | Spawn into a tab: `op_spawn` + CLI (`--herdr-workspace`, `--tab`) + `tether_spawn` tool args (`workspace`, `tab`): tab create → agent start (claude/codex, trust dialog handled per decisions) → session id (Claude from Herdr; Codex from Herdr when hooked, else newest daemon `thread/list` entry for that cwd) → bind with `source.herdr={"session":…, "pane_id":…, "agent":…}` → pane token `slack=<permalink>`; falls back to today's spawn when Herdr is unavailable, never fails a spawn because Herdr is missing | T1 | broker tests: spawn with Herdr available creates tab+agent+binding and records the pane; spawn without Herdr unchanged; the 15:46 incident shape (thread id, wrong channel) still refused | done | commit 323c98c: op_spawn places the session (workspace by name → by cwd → create), agent name from tab label (unique), harness args from config, trust auto-accept only under managed roots, session id from Herdr (Codex: newest rollout for cwd fallback), bind source.herdr, pane token slack=<channel>/<thread>; CLI --herdr-workspace/--tab/--no-herdr; tool workspace/tab. Live canary 17:03 UTC: workspace w8 tether-lab, pane w8:p1, agent canary, session 19873e79 reported by Herdr, thread 1789578226.924649 in #agent-hub |
| T3 | Claude-in-pane turns: the session driver resolves `source.herdr.pane_id` (or `find_agent_by_session`) → `agent prompt --wait` → settled state → reply from the transcript after the prompt timestamp → post; Codex untouched (daemon) | T1 | driver tests with the fake herdr + a fake transcript: reply text equals the last final assistant block, narration excluded; a pane that disappeared falls back to `claude -p --resume` with a log line | done | commit 323c98c: SessionDriver._run_herdr_turn (agent by name/session → agent prompt --wait → transcript reply after offset via claude_transcript/claude_reply_after; blocked → dialog text is the reply; pane gone → headless fallback). Live: canary task ran in the pane, reply CANARY-OK + branch + $HERDR_PANE_ID=w8:p1 posted in-thread 4 s later; no headless claude started |
| T4 | Blocked dialogs in Slack: settled `blocked` → post the detection-source text of the dialog (trimmed, code block) with "reply here to answer"; the next admitted thread message is delivered as `send-keys` when it matches a known control (y/n/enter/esc/number) else as `agent prompt`; then wait again; folder-trust auto-accept per decisions | T3 | tests: blocked → posted dialog; reply "2" → send-keys sequence; reply text → prompt; auto-trust only under managed roots | done (canary pending) | PR 604 commit ebf8004: blocked pane → dialog tail + DIALOG_HELP posted, raised_hand; next thread message → dialog_answer(): digits/esc/enter/y/n as keys, other text typed + Enter; agent_wait then transcript reply. Tests: keys, typed text, esc mapping |
| T5 | Attach by name: `tether attach --herdr <agent-name>` and tool `tether_attach` (name or pane id) → session id from Herdr → bind (source.herdr set); refuse an agent Herdr cannot resolve or one already bound | T1 | test: attach by name binds; unknown name refused; skill documents the phrasing "attach this thread to my <name> session" | done | PR 604: op_attach herdr_agent → _resolve_herdr_agent (name or pane id; Codex id via rollout fallback), source.herdr, pane token; `tether attach --herdr NAME`; tool tether_attach; close clears the token. Tests: bind by name, unknown, unsupported kind, close, no Herdr |
| T6 | Presence both ways: Herdr state → thread reaction (working 👀 / blocked 🙋 / done ✅) through the existing presence path; pane token `slack=` set at bind and cleared at close; `tether status --json` lists pane and workspace per binding | T2,T3 | live: reactions flip on a lab thread in #agent-hub; `herdr agent list` shows the token; close clears it | partial | PR 604: blocked_emoji raised_hand on the asking message; slack token set at bind/attach, cleared at close. Not done: state→reaction while a pane works outside a turn (needs a watcher; deferred, low value) |
| T7 | Headless Herdr where installed: doctor check + installer step that starts the named session server (`herdr --session pilot` headless, user unit) on greppy-cr/sam so tabs exist before a human attaches; documented in greppy-ops | T2 | on greppy-sam: `herdr status` shows a running server without a client; a spawn creates a tab; `herdr` from an SSH shell attaches and shows it | done | greppy-sam 19:10 UTC: `systemd-run --user --unit herdr-pilot --property=Restart=on-failure ~/.local/bin/herdr --session pilot server` (linger=yes) → unit active, `herdr --session pilot status` running, API answers. greppy-cr runs a live `default` session (0.8.2, 11 agents, workspaces chris-cache/codex); discover picks ~/.config/herdr/herdr.sock and `--session default` works there. Doctor check for Herdr state: not added |
| T8 | Codex session identity: Miguel approves the SessionStart hook; drop the daemon `thread/list` fallback once `agent get` reports Codex ids on greppy3 | Miguel | `herdr agent get` on a Codex pane shows `agent_session`; fallback code removed with its test | blocked | blocker: config.toml hook approval is Miguel's |
| T9 | Team prompt + skill: spawn rules (tool over CLI; `--task-stdin`; channel+thread; workspace phrasing "in the <label> space"; tab naming), attach-by-name phrasing, what a blocked dialog looks like in Slack; team.md stays under the 4000 cap (guard test) | T2,T4,T5 | prompt-size test; skill round-trip section reads clean | done | PR 604: team.md step 4 names workspace/tab and tether_attach; body under the 4000 cap (guard test); SKILL.md spawn section covers Herdr flags (PR 603) |
| T10 | Canary + fleet: claudio first (re-run the exact 2026-09-16 15:45 ask in a fresh thread), then greppy-sam and greppy-cr with T7, then bc/mg/m (fallback path); installer only; no restart with a turn running | T2–T7,T9 | acceptance (a)–(g) each with a thread link or command output pasted here | todo | |

## Owners, blockers, next actions
- Owner: claudio (this session). T1–T3 in PR (feat/tether-herdr-tabs, package manifest updated in f18824d);
  claudio runs 603 since 17:02 UTC; PR 604 (T4–T6, T9) open, installed on claudio pending a restart
  (Miguel's MCP thread turn in flight). Next: dialog canary in #agent-hub, merge 604, fleet rollout (T10).
- Blocked: T8 on Miguel (Codex hook approval). Everything else proceeds; T2 uses the daemon
  `thread/list` fallback for Codex ids until then.
- Open question for T7: whether `herdr --session <name>` can run headless under a user unit
  (`herdr server` is listed as "Run as headless server"); verify on greppy-sam before designing
  the installer step.
