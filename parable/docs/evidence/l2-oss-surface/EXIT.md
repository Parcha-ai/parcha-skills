# L2 — PORTABLE PARABLE LAUNCHER AND NAMED-AGENT SYNC · EXIT (2026-07-19)

## Status: COMPLETE

## The headline evidence

Parable 0.1.9 now turns the L1 manual wiring into a credential-free TOML
contract plus two small commands. The hermetic suite passes 81/81, including a
fake localhost proxy and fake Claude binary that prove exact Sol selection,
ordinary argv forwarding, Kimi agent generation, environment isolation, and
safe reconciliation. A separate live smoke used the new launcher with stock
Claude Code 2.1.215 and the pinned CLIProxyAPI/OpenAI OAuth route; the
authenticated catalog exposed ten models and the Sol turn exited successfully.

The live smoke is `n=1` route evidence, not a reliability or latency claim.

## What shipped

| Piece | Where |
|---|---|
| Strict `[claude]` config, proxy preflight, environment builder, and launcher | `skills/parable/scripts/parable.py` |
| Packaged `parable claude` / `parable agents sync` commands | `bin/parable.js` |
| Sol + Kimi credential-free config | `examples/parable.claude-subscriptions.toml` |
| Hermetic mechanism and fake-success tests | `tests/test_parable.py`, `tests/test_integration.py` |
| Sanitized hermetic/live attestation | `docs/evidence/l2-oss-surface/attestation.json` |
| Operator and agent-routing references | `README.md`, `skills/parable/SKILL.md`, `skills/parable/references/` |

## Bound accounting (honest)

- Correctly wired PROVE runs: 2, both passed (one hermetic 81-test run and one
  live Sol launcher smoke).
- Evidence failures: 0.
- Review rounds: 1. The staged diff, JSON validation, content-safety scan,
  package dry run, focused Ruff slice, GitHub diff, and mergeability check had
  no L2 findings. This repository reported no automated checks or configured
  reviewer on PR #117.
- Instrument setup note 1: a broad process search matched its own command, the
  already-known L0 failure mode. The dedicated proxy start banner and bound
  listener supplied the real process evidence; no proof attempt was consumed.
- Instrument setup note 2: the first temporary-directory reset command was
  rejected by the shell's destructive-command guard. A fresh directory was
  used without deletion; no proof attempt was consumed.
- Non-gate lint note: a whole-file Ruff probe found three pre-existing import
  findings outside the L2 additions in `test_parable.py`. The changed runtime
  and integration-test files pass Ruff, and the repository's declared
  `npm test` gate passes.

ZEN check: the surface remains a thin `config → preflight → sync → exec`
adapter. It delegates OAuth, refresh, request translation, and provider
catalogs to CLIProxyAPI and delegates task/model judgment to Parable's prose
cast rather than adding deterministic routing policy.

## Accept criteria → evidence

1. Minimal credential-free TOML selects Sol and Kimi — ✅
   `examples/parable.claude-subscriptions.toml` contains only the loopback
   endpoint, local client-token environment-variable name, and model ids.
2. Launcher selects Sol, forwards args, and leaves global Claude config alone —
   ✅ `TestClaudeSubscriptionLauncher.test_launcher_routes_sol_forwards_args_and_scrubs_global_override`
   verifies the packaged Node-to-Python command; `attestation.json` records the
   separate live stock-Claude smoke.
3. Named Kimi agent is exact, namespaced, idempotent, and user-safe — ✅
   `TestClaudeSubscriptionLauncher.test_agent_sync_is_idempotent_cleans_stale_and_preserves_unrelated`
   plus `TestClaudeLaunch.test_custom_model_agent_is_namespaced_and_exact`.
4. Inherited global subagent override cannot defeat heterogeneous routing — ✅
   the launcher test seeds `CLAUDE_CODE_SUBAGENT_MODEL=gpt-5.6-sol` and records
   that it is absent from the fake Claude child while `parable-kimi` still
   carries `model: "kimi-k3"`.
5. No tokens or secret artifacts are published — ✅ the test token is generated
   at runtime, the fake binary records booleans rather than values, the TOML and
   generated agent are asserted token-free, and the content-safety scan found
   no credential or identifying-path material. Parable writes no
   secret-bearing file in this flow.
6. Unit/integration suite and focused PR — ✅ 81/81 tests and package dry run
   pass; [PR #117](https://github.com/miguelrios/unc-skills/pull/117) is merged,
   and its resulting `main` HEAD is checked immediately before L3 advances.

## The running delta table (L0→L2)

| Loop | P1: Sol session | P2: Sol → Kimi subagent | OSS clean-room |
|---|---:|---:|---:|
| L0 | NOT RUN | NOT RUN | NOT RUN |
| L1 | PASS (`n=1`) | NOT RUN | NOT RUN |
| L2 | PASS retained + launcher smoke | NOT RUN | SURFACE PASS; full guide pending L4 |

MEASURE is skipped for L2: it moves a product surface and binary route gate, not
a latency, cost, or quality metric. The test count moved from 70 to 81 because
eleven mechanism/fake-success cases were added; that is coverage, not a product
performance delta.

## exit → L3

After PR #117 is merged and verified at `main`, pause at the declared human
gate for the operator to complete CLIProxyAPI's Kimi OAuth flow. Then run the
isolated Sol-parent → `parable-kimi` child canary and require separate upstream
receipts for `gpt-5.6-sol` and `kimi-k3`.
