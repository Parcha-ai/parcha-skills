# Routing playbook

**Note:** This section applies to multi-model Parable mode only. When running in solo mode (`parable --solo`), routing is not applicable — the selected model handles all work directly. See `SKILL.md` Solo Mode section.

## Classifying tasks

| Class | Signature | Typical executor traits |
|---|---|---|
| `mechanical` | Precise spec, zero judgment: renames, boilerplate, config churn, test scaffolds, applying a known pattern N times | cheapest available; literal instruction-following is an asset |
| `data_transform` | Structured extraction, normalization, migrations, and deterministic data reshaping | strong data-transformation model at modest effort |
| `frontend` | React, visual UI, interaction polish, and browser-visible fixes | frontend-specialized coding model |
| `feature` | Bounded new behavior with clear acceptance criteria | mid-tier coding-tuned model |
| `refactor_wide` | Many files, needs large working context, mechanical-per-site but global consistency matters | large-context model |
| `gnarly` | Ambiguous scope, deep debugging, cross-cutting design tension | frontier model |
| `review` | Judge a diff for defects | different model than the author. Routine diffs: cheap first pass. High-blast-radius diffs — money movement, destructive data paths, auth, security, dispatch/timeout behavior a green suite can pass while wrong — skip the cheap pass and go straight to a frontier adversarial reviewer |
| `smoke_test` | Exercise the running app and gather evidence | tool-heavy model with explicit execute-instructions |
| `architecture` | Decompose ambiguous work or make high-blast-radius system decisions | strongest planning and architectural reasoning model |

When genuinely unsure between two classes, take the cheaper one. Escalation after a cheap
failure costs one run; over-provisioning costs every run.

## Selection

1. Classify the work, then take that class's routing menu. The list is a capable-peer menu,
   not a priority ladder; `routing.escalation` is the only ordered list.
2. Skip unavailable or disabled executors (`parable-config.sh` prints `ready`,
   `missing <ENV_VAR>`, or `disabled`). Also skip any executor whose exact model is the current
   parent model. For review, skip the diff author's model too.
3. Apply each candidate's `use_for`/`avoid_for`; this prose overrides tag heuristics. If the
   task does not fit, remove the candidate even when its tag looks plausible.
4. Read `parable-usage.sh --all`. A pool at 80% or more used is not a default. Among the
   remaining capable candidates, choose the pool with the most headroom while preserving the
   pool funding the parent session. An unreadable pool reports `unknown`; keep it eligible
   rather than inventing scarcity.
5. Use lower `cost.in` only to break a remaining tie between metered executors. Subscription
   headroom and task fit come first; an executor without `cost` loses a true cost tie-break.

## Escalation ladder

- **First failure:** diagnose from the run summary + verify report. If the plan was
  underspecified, tighten it (smaller scope, explicit file list, exact commands) and rerun
  ONCE on the same executor.
- **Second failure on the tier:** move to the next rung of `routing.escalation`. Write a
  fresh plan; include what the previous model produced and concretely what was wrong with
  it (sessions do not transfer between executors — escalation starts fresh).
- **TIMEOUT (wall-clock kill):** distinct from provider failure — the run exhausted its
  budget with no verdict, only partial working-tree state. Budget exhaustion is a capacity
  signal: escalate to the next `routing.escalation` rung with a fresh plan that carries the
  partial tree forward. Resume the same executor only when its recorded session was
  demonstrably near-complete and one bounded follow-up closes it; a timeout after a resume
  escalates, it does not resume again.
- **Provider failure (stream errors, 5xx) rather than model failure:** if the working tree
  passes verification, accept the work. Otherwise retry the same model on a different
  provider before escalating tiers — don't burn frontier budget on infrastructure noise.
- **Silent reviewer (no output within its budget):** infrastructure failure — fall through
  to the next `routing.review` entry immediately rather than polling or re-dispatching the
  same one.
- **Fallback floor:** when the orchestrating harness exposes native agent spawning, native
  subagents need no external key and can complete in-session. Without that capability, the
  floor is the last eligible configured CLI-backed executor. If neither exists, stop and ask
  for a cast configuration; do not silently implement the batch in the brain session.

## Effort per role

| Role | Effort | Why |
|---|---|---|
| Implementer | `high` (default), `xhigh` for gnarly | tool-use and self-verification scale with effort |
| Mechanical | `low`/`medium` | strict scoping at low effort is a feature here |
| First-pass reviewer | `medium` | coverage rubric does the work |
| Adversarial reviewer | `high`+ | recall on subtle defects |
| Smoke tester | `high` | tool-call triggering scales with effort; pair with explicit execute-instructions |

The config pins each executor's baseline effort; when the task's role calls for a different level
(a mechanical task on a high-effort implementer), override per dispatch with
`parable-run.sh --effort <level>` instead of editing the config.

`max` and `ultra` (GPT-5.6-class models) sit above `xhigh`. `ultra` is not a bigger `max`:
it flips codex into proactive multi-agent delegation — the executor spawns its own parallel
subagent threads inside one dispatch. Use it for deliberately handed-over batch clusters with
disjoint per-subtask file ownership, never as an escalation rung; it carries the highest
per-turn burn of any setting.

## Cost accounting

Run summaries report tokens (and provider-reported cost when available); `cost` fields in
the config price the rest. When a class consistently escalates past its first rung, change
the routing default rather than paying the escalation tax every task.
