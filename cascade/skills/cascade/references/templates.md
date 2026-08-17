# Cascade templates

Use these skeletons after reading `SKILL.md`. Keep raw artifacts ignored under `.cascade/` and use
the validator as a structural check, never as the acceptance judge.

## Chain document

`.cascade/LOOP_CHAIN_<YYYY-MM-DD>_<SLUG>.md`

```markdown
---
cascade_version: 2
episode_id: <stable-id>
pacing: autonomous | checkpointed
status: ACTIVE
current_loop: L0
authority: <plan-only/local/GitHub/publish/deploy/destructive grants and exclusions>
budget: <time, spend, valid attempts, context limits, or none>
human_gates: <named gates or none>
target: <repository/worktree; branch; HEAD/deployed revision; dirty/diff digest if uncommitted; runtime>
---

# <Project> — Cascade

## Current

<!-- This is the only mutable block. Keep it small. -->
L0 is ready. Latest receipt: none. <Any active wait/blocker.>

## Authority and budgets

- May: <actions explicitly authorized>.
- May not: <writes, targets, publication, deployment, destructive actions not authorized>.
- User-absence preflight: <credentials/choices/approvals resolved now, or none>.
- Stop conditions: <budget, human gate, target drift, external block>.

## Order rationale

<Why these 2–4 loops; why each is one state change; why this risk order. Name the re-plan gate.>

## Chain

### L0 — <NAME>

- **goal:** <one-sentence state change>
- **prompt:** <self-contained task, named inputs, target/runtime, proof protocol, rollback>
- **accept:**
  1. **L0.1** — <observable claim>; evidence: <artifact>; falsifier: <negative case>.
  2. **L0.2** — <observable claim>; evidence: <artifact>; falsifier: <negative case>.
- **bound:** <valid evidence attempts and review/fix rounds>
- **at_bound ->** STOP | R0 <predeclared localized repair inside authority>
- **exit ->** L1

### L1 — <NAME OR RE-PLAN>

- **goal:** ...
- **prompt:** ...
- **accept:** ...
- **bound:** ...
- **at_bound ->** STOP
- **exit ->** COMPLETE | <short successor chain>

## Native task mirror

| Task | Blocked by | Loop | Receipt | Mirror status |
|---|---|---|---|---|
| <task> | — | L0 | `.cascade/evidence/l0-<slug>/EXIT.md` | pending |

Never mark a mirrored task complete until its receipt exists. The chain and evidence win on conflict.

## TAKEOVER snapshot

Read `Current`, the current prompt, latest receipt, actual HEAD/deployed revision, runtime, authority,
budget, and native mirror. Use Recap if installed for session context. Recheck noise-band priors and
stop/re-plan on target drift.

## Invariants

- Only `COMPLETE` follows normal `exit ->`; `AT_BOUND` uses only its declared repair or stops.
- No acceptance weakening after observation; no advance without a boundary receipt.
- Receipts and closed-loop history are immutable; replans append a named successor.
- ZEN applies during BUILD; architecture-changing exits must pass POST-ZEN.
```

Validate with:

```bash
python3 <skill-dir>/scripts/validate_cascade.py chain <chain-path>
```

## Boundary receipt

`.cascade/evidence/<loop-id>-<slug>/EXIT.md`

```markdown
---
cascade_version: 2
episode_id: <stable-id>
loop: L0
status: COMPLETE | AT_BOUND | WAITING_HUMAN | BLOCKED_EXTERNAL | SUPERSEDED
target_head: <verified commit/deployed revision, or base HEAD plus candidate diff digest>
next: L1 | R0 | STOP | WAIT | <successor-chain path>
---

# L0 boundary receipt — <name>

## Bound accounting

- Valid evidence attempts: <used>/<bound>, with verdicts.
- Review/fix rounds: <used>/<bound>.
- Instrumentation failures: <count and diagnosis; do not charge as evidence attempts>.

## Accept criteria → evidence

| ID | Verdict | Evidence |
|---|---|---|
| L0.1 | PASS | <artifact and exact observation> |
| L0.2 | PASS | <artifact and exact observation> |

`COMPLETE` requires every row to be `PASS`. Use `FAIL` for an exhausted claim and `WAIT` for a
declared human gate. Never write “complete except” or defer a current criterion into the next loop.

## Evidence manifest

| Criterion | Command/action | Runtime/environment | Target | Timestamp | Artifact/digest | Falsifier / negative | Cleanup / rollback |
|---|---|---|---|---|---|---|---|
| L0.1 | `<command>` | <pinned runtime> | <HEAD/revision> | <UTC ISO-8601> | <path + digest> | <negative checked> | <cleanup/rollback> |

## POST-ZEN

<For architecture change: what became simpler, what was deleted, one authoritative path, ownership,
tested rollback, and any temporary scaffold's removal gate. Otherwise: `N/A — <reason>`.>

## Transition

<Exactly one:
- `COMPLETE`: follow normal `exit ->`.
- `AT_BOUND`: enter the declared repair successor or STOP/page; never follow normal `exit ->`.
- `WAITING_HUMAN`: name the declared decision and wait.
- `BLOCKED_EXTERNAL`: name unavailable state/authority and stop.
- `SUPERSEDED`: point to the append-forward successor chain.>
```

Validate with:

```bash
python3 <skill-dir>/scripts/validate_cascade.py exit <receipt-path>
```

## Autonomous continuation prompt

Use only with a recurring-wake or lifecycle mechanism the harness actually provides:

```text
Continue <episode-id>. Read <chain-path>: Current, current loop prompt, latest receipt, authority,
budget, and target; compare them with the actual worktree/HEAD/runtime and native task mirror. If they
disagree, stop or append a SUPERSEDED re-plan. Advance one ribbon step. At a boundary, write and
validate the receipt. COMPLETE follows exit ->. AT_BOUND uses only its declared repair successor or
stops. WAITING_HUMAN and BLOCKED_EXTERNAL stop. Never widen authority, weaken acceptance, fabricate
background execution, or mark a native task complete without its receipt. Reply with position,
boundary evidence if any, and the next permitted action.
```
