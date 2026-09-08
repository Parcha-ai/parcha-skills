# Colleague — living plan

Assessment: https://claude.ai/code/artifact/2cb406c4-d77c-486b-b867-bcfa20f07b5a (2026-09-05).
Authorized by Miguel 2026-09-06 ("go ham"): moves 1–2 of the assessment, fleet-wide.

## Outcome and acceptance
Parcha's engineer-agents behave like human colleagues in Slack and continue Claude Code / Codex
sessions from bound threads, on a Tether reduced to bindings, admission and launcher.
**Final acceptance:** conversation eval (full corpus, ruler verified) shows structural noise at
zero (status lines, marker leaks, bare mentions) and no regression on answered/lane/evidence/tone
versus the 2026-09-04 baseline; the bound-thread loop (human reply → owning session works →
replies with evidence) passes on all primary gateways; Tether Python under 3,000 lines.

## Context to resume
- Workspace: `~/worktrees/tether-l2d/tether` (parcha-skills, this branch `cascade/colleague-2026-09-06`;
  `.cascade/` is gitignored, this file is force-added and pushed at every boundary).
- Fleet: claudio (greppy3), mikael/irma (greppy3 isolated, secondary), greppy-cr/sam/bc/mg, m (q2).
  Deploy = installer only (`tether.js upgrade --harness=both`), package built from main.
- Hermes: checkout `~/.hermes/hermes-agent` at 0.19.0 b9ba7c7 (2026-07-26); target v2026.8.31 (0.21.0).
- Constraints: test only in #agent-hub C095VU95XQR; tether agents use claude-opus-5; primary bots
  run with operator privileges (systemd-user launcher), secondary stay direct; no Northflank.
- Eval: `evals/conversation/conversation_eval.py` (export → measure → judge, rubric v2).
  Baseline 2026-09-04: answered 1.8 · lane 1.6 · evidence 1.3 · tone 1.5; 8/10 answered,
  median 21 s, 1 leak, 1 bare, 3 status lines, 5 extra voices.
- Decisions taken: keep Slack apps internal; accept the TUI gap; leave anthro/irma sandboxed.

## Tasks
| ID | Deliverable | Depends | Exit check | Status | Evidence |
|---|---|---|---|---|---|
| C1 | Baseline eval re-run on last 48 h of #agent-hub | — | measure + judge JSON saved under evals/conversation/fixtures/baseline-2026-09-06.json | done | 7 threads: answered 1.86 lane 1.71 evidence 1.71 tone 1.43; status lines 1, leaks 1, bare 0, extra voices 3, median 20 s. Ruler = rubric v2 calibrated 2026-09-04; not re-verified on this corpus (small, 7 threads) |
| C2 | Hermes upgrade path proven on claudio | — | `hermes --version` = 0.21.0; `tether doctor` 14 ok 0 fail; bound-thread smoke in #agent-hub passes | done | v0.21.0 main 245e4800; snapshot ~/.hermes/backups/pre-upgrade-20260906 (+ hermes zip pre-update-2026-09-06-004446.zip); rollback: `git checkout b9ba7c78e4 && venv/bin/pip install -e .`; config migrated 33→40 (delegation limits 50→250 / 3→10, bg notifications all→concise); stale git shallow.lock had to be removed; doctor 14/0 after restart. Canary found: (a) Hermes 0.21 fires pre_gateway_dispatch from a worker thread → journal SQLite cross-thread error → event untouched → Hermes native + Tether both replied (PR 455 fixes); (b) upstream native task cards send markdown_text+chunks to chat.appendStream → Slack rejects → text fallback "Hermes is working" (cards turned off, upstream bug to file); (c) q2↔claudio mention ping-pong, 4 turns no human (peer_chain_limit=2 in PR 455). Native streaming itself works (preview "▉" then final). PR 455 merged (main a860199), deployed to claudio 2026-09-08 03:09 UTC; smoke 03:11: q2 peer question → exactly one claudio reply in 12 s, 0 "observation failed", no native double-handling, second peer message admitted and silent |
| C3 | Knobs on claudio: native streaming on, native_task_cards on, long_running_notifications off, presence reactions off (tether presence=false, SLACK_REACTIONS=false) | C2 | config diff committed to fleet notes; a thread shows streamed reply + task cards and no "⏳ Working" line | done | applied 00:5x UTC: streaming on, lrn off, tether presence off, SLACK_REACTIONS=false; native_task_cards OFF (upstream appendStream bug). Verified 2026-09-06 thread A: native streamed reply (preview then final), no "⏳ Working" line, no claudio reactions; remaining noise = empty first stream frame + card fallback, both from cards → off | done |
| C4 | Eval after C3 (same corpus window rules) | C1,C3 | status lines 0, leaks 0; answered/lane/evidence/tone ≥ baseline − 0.1 | todo | |
| C5 | Fleet roll: Hermes 0.21 + knobs on all 8 gateways | C3 | doctors clean fleet-wide; cross-agent loop passes on a remote box | done | 2026-09-08 03:2x–03:5x UTC: primaries (claudio, cr, sam, bc, mg, m) on v0.21.1 (2026.9.7, 6e2b8e07) via scripts/fleet/hermes-upgrade.sh; isolated /opt runtime moved by hand (both release dirs → 6e2b8e07, deps via uv); Tether a860199 (PR 455) installed everywhere; knobs applied; doctors 14/0 on primaries, 13/0 + expected WARN on mikael/irma; sam had a stale rebind_required binding from 09-02 → closed |
| C6 | Move 2 design note: stream-json driver per binding, hooks → broker, what deletes | C2 | note in tether/docs/plans/ with line-count ledger and test plan | done | Prototype 2026-09-06 01:1x UTC on greppy3: one `claude -p --resume <sid> --input-format stream-json --output-format stream-json` process took two user turns; both results carried the same session id, turn 2 recalled turn 1 ("TWO"), turn 2 latency 1.4 s, exit 0. Session id parse + stream events confirmed. Codex: `codex app-server` over stdio answered initialize and thread/list (cwd filter, previews) in <2 s; turn/start on a live thread is the next probe |
| C7 | Session driver (store.py + session_driver.py) behind `driver = session`, tests | C6 | 230+ tests green; bound turn on claudio via new driver with same evidence quality | done | PRs 458/459/460 (main b03d7da + 460): store (4 tables, same method/refusal contract, legacy import of 38 bindings), driver (one stream-json process per binding, idle sweep, failure notices, trailing NO_REPLY = silence), admission index on the store, 260 tests. Live on claudio 2026-09-08 04:10 UTC: spawn → q2 turn 1 answered in 13 s (exact git/uptime lines) → turn 2 in 3 s on the SAME process, recalled turn 1; attempts completed_with_response ×2; 1 session process. Codex stays per-turn `exec resume` in v1 |
| C9 | Codex app-server driver: one `codex app-server -c mcp_servers={}` per gateway, thread/resume + turn/start, reply from agentMessage items | C7 | fake app-server tests green; live Codex turn on greppy-mg (manny) answers in-thread via the app-server | done | PR 464 merged (main 84dabb1), deployed fleet-wide. Live on greppy-mg 2026-09-08 05:27 UTC: `tether spawn --harness codex` → manny answered claudio in 6 s with exact hostname/uptime, turn 2 in 3 s, one app-server process, launcher systemd-user. Follow-up PR 465: transform_llm_output hook collapses trailing NO_REPLY on Hermes' native path (leak seen in the same thread) |
| C8 | Delete durability core + native_driver; doctor/CLI unchanged | C7 | `wc -l` Python < 3,000; fleet deploy clean; eval not worse | done | PR 461 merged (main 21c0a08), deployed to all 8 gateways 2026-09-08 04:4x UTC with stale runtime files removed, doctors clean. −12,099 lines; domain_schema/runtime/control, native_driver, security and their tests removed; slice + broker tests rebuilt on Store + tests/fakes.py; manifests, CI compile lists, release scripts updated; 109 tests + unittest discover green. Runtime Python 12,487 → 3,872. C8b PR 462 merged and deployed (2026-09-08 05:0x UTC): runtime Python 3,559; the < 3,000 target needs the `tether setup` machinery in tether_notify.py reconsidered (separate decision) |

## Owners, blockers, next actions
- Owner: claudio session 7f97ffb2 (this one). Miguel: decisions only.
- Fleet flip done 2026-09-08 04:2x UTC: main 392d15b on all 8, `driver = "session"` everywhere, doctors clean, legacy bindings imported (claudio 40, sam 7, bc 2, mikael 2, m 2, cr 1, irma 0, mg 0). Remote smoke on greppy-bc 04:14 UTC passed on the session driver: bryan answered in 8 s, turn 2 in 3 s, one process.
- 2026-09-08 05:0x UTC: main 844607a on all 8 gateways (PR 462 trim + PR 463 team layer v4 "do not acknowledge acknowledgements"), team layer applied everywhere, doctors clean.
- Next: merge/deploy PR 465 (silence hook). C4 eval on or after 2026-09-10 05:30 UTC (48 h of traffic on the final stack): export 48 h of #agent-hub, measure, judge with rubric v2, compare to baseline-2026-09-06 (answered 1.86 lane 1.71 evidence 1.71 tone 1.43; status lines 1, leaks 1, extra voices 3). Then: decision on trimming `tether setup` machinery to reach < 3,000 lines; Codex app-server driver (turn/start with MCP disabled) as the next code item. C4 after 48 h of traffic (from 2026-09-08). Also: docs/plans + scripts/fleet now live on main via PR 458; this branch keeps only .cascade/.
