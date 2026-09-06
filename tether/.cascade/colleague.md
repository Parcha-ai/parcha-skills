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
| C1 | Baseline eval re-run on last 48 h of #agent-hub (ruler verified first) | — | measure + judge JSON saved under evals/conversation/fixtures/baseline-2026-09-06.json | todo | |
| C2 | Hermes upgrade path proven on claudio: snapshot, upgrade to v2026.8.31, plugin loads, doctor 14/0, rollback recipe written | — | `hermes --version` = 0.21.0; `tether doctor` 14 ok 0 fail; bound-thread smoke in #agent-hub passes | todo | |
| C3 | Knobs on claudio: native streaming on, native_task_cards on, long_running_notifications off, presence reactions off, peers via config | C2 | config diff committed to fleet notes; a thread shows streamed reply + task cards and no "⏳ Working" line | todo | |
| C4 | Eval after C3 (same corpus window rules) | C1,C3 | status lines 0, leaks 0; answered/lane/evidence/tone ≥ baseline − 0.1 | todo | |
| C5 | Fleet roll: Hermes v2026.8.31 + knobs on all 8 gateways | C4 | doctors clean fleet-wide; q2/claudio cross-agent loop passes | todo | |
| C6 | Move 2 design note: stream-json driver per binding, hooks → broker, what deletes | C2 | note in tether/docs/plans/ with line-count ledger and test plan | todo | |
| C7 | Stream-json driver + Codex app-server driver behind `launcher`/`driver` config, tests | C6 | 230+ tests green; bound turn on claudio via new driver with same evidence quality | todo | |
| C8 | Delete durability core + native_driver + zellij/herdr stubs; doctor/CLI unchanged | C7 | `wc -l` Python < 3,000; fleet deploy clean; eval not worse | todo | |

## Owners, blockers, next actions
- Owner: claudio session 7f97ffb2 (this one). Miguel: decisions only.
- Next: C1 and C2 in parallel.
