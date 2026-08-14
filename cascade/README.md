# Cascade

Cascade is an Agent Skill for turning a consequential project into a short, evidence-gated chain
that can survive unattended work, context compaction, and handoff between Claude Code, Codex, and pi.

[![skills.sh](https://skills.sh/b/miguelrios/unc-skills)](https://skills.sh/miguelrios/unc-skills/cascade)

The central promise is simple: plan before building, make one state change per loop, and never call a
loop complete without fresh proof at the real target. “Go ham” can make the chain autonomous; it
cannot grant GitHub, deployment, destructive-action, or spending authority.

```text
task → operating envelope → short chain → build/prove → boundary receipt
                                                │
                  COMPLETE ─────────────────────┴─→ normal successor
                  AT_BOUND ───────────────────────→ declared repair or stop
                  WAITING_HUMAN / BLOCKED_EXTERNAL → stop
```

## Install

skills.sh:

```bash
npx skills add miguelrios/unc-skills --skill cascade
```

Claude Code:

```bash
claude plugin marketplace add miguelrios/unc-skills
claude plugin install cascade@unc-skills
```

Codex:

```bash
codex plugin marketplace add miguelrios/unc-skills
codex plugin add cascade@unc-skills
```

pi (installs the complete unc-skills collection):

```bash
pi install git:github.com/miguelrios/unc-skills
```

## Use it

```text
/cascade Migrate our job runner from Redis queues to Postgres. Keep going autonomously, but do not
push, deploy, or make destructive data changes.
```

Invocation syntax is `/cascade` in Claude Code, `$cascade` or a direct request in Codex, and
`/skill:cascade` in pi. To inspect every boundary, request checkpointed mode:

```text
Cascade this in checkpointed mode. Stop for my go/no-go after every loop receipt.
```

Before touching code, Cascade records pacing, mutation authority, budgets, human gates, exact target
identity, and current position. It also asks for any credentials or product decisions you need to
supply before leaving an autonomous run unattended.

## The shape of a good chain

Cascade prefers two to four substantive loops before a re-plan gate. A loop has one state change,
one primary acceptance story, and six fields:

```text
goal          what state changes
prompt        enough context for a fresh session
accept        criterion IDs, evidence, and falsifiers
bound         valid attempts and review/fix rounds
at_bound ->   a predeclared localized repair, or STOP
exit ->       the normal successor after COMPLETE only
```

For a queue migration, the first chain might be:

```text
L0  pin current queue semantics     → behavior fixtures and baseline pass
L1  add Postgres queue core         → contract + concurrency falsifiers pass
L2  integrate one worker slice      → resulting HEAD passes live recovery proof
L3  re-plan from accumulated facts  → successor or migration-stop verdict
```

Cutting a successor after L3 is deliberate. A ten-loop roadmap written before the first experiment
usually encodes guesses as dependencies.

## Inside a loop

```text
RE-PLAN → BUILD → PIN → PROVE → MEASURE → REVIEW/INTEGRATE → EXIT
```

- `RE-PLAN` re-grounds from the chain, latest receipt, actual HEAD/revision, and runtime.
- `BUILD` applies ZEN: simple, general, agentic where judgment is the work, beautiful, and dope.
- `PIN` tests the mechanism and likely fake-success modes.
- `PROVE` exercises the real claim and retains raw evidence.
- `MEASURE` records a comparable delta, or a justified `N/A`.
- `REVIEW/INTEGRATE` verifies the actual merge candidate or resulting target, not a commit message.
- `EXIT` maps every criterion to fresh evidence and runs POST-ZEN when architecture changed.

Instrumentation failures are diagnosed separately. They do not consume a claim-attempt bound unless
the run actually exercised the claim.

## Exact boundary states

- `COMPLETE`: every current criterion passed at the verified target; follow normal `exit ->`.
- `AT_BOUND`: valid attempts are exhausted; use only the predeclared repair successor or stop.
- `WAITING_HUMAN`: a declared decision or approval is required.
- `BLOCKED_EXTERNAL`: required state or authority is unavailable.
- `SUPERSEDED`: an append-forward re-plan replaced the remaining chain.

There is no “complete except,” partial completion, or deferred acceptance criterion. A failed loop
can transition into a bounded repair loop, but cannot silently retry forever or advance normally.

## Portable evidence and takeover

```text
.cascade/
├── LOOP_CHAIN_<date>_<slug>.md
└── evidence/
    ├── l0-baseline/
    │   ├── raw-result.json
    │   └── EXIT.md
    └── l1-core/
        └── EXIT.md
```

The chain file is authoritative; native task UIs mirror it. A native task cannot be complete before
its receipt exists. Each receipt records the criterion verdict, command/action, runtime, exact target,
timestamp, artifact/digest, negative case, and rollback or cleanup.

For an uncommitted local candidate, “exact target” means base HEAD plus a working-tree or diff digest;
HEAD by itself cannot identify the code that was tested.

On takeover, the agent reads the chain's small `Current` block, current loop, latest receipt, real
target identity, and native task mirror. Recap can recover conversational context when installed, but
cannot overrule the repository evidence. Target drift or contradictory state causes a stop or an
append-forward re-plan—not a guess.

Keep `.cascade/` ignored because raw evidence may contain local paths, identifiers, or operational
details. Publish a deliberately redacted summary when needed.

## Validate the artifacts

Cascade ships a dependency-free structural validator:

```bash
python3 skills/cascade/scripts/validate_cascade.py chain .cascade/LOOP_CHAIN_2026-08-13.md
python3 skills/cascade/scripts/validate_cascade.py exit .cascade/evidence/l0/EXIT.md
```

It checks the v2 envelope, loop fields, boundary sections, and obvious fake completion such as a
`COMPLETE` receipt containing a failed or waiting criterion. It does not judge whether evidence is
true or sufficient; the executing agent still owns that semantic review.

Cascade is worthwhile for migrations, broad refactors, measured agent/prompt changes, and unattended
work with real boundaries. It is unnecessary overhead for a small, obvious edit.

The complete contract is in [`skills/cascade/SKILL.md`](skills/cascade/SKILL.md), with copy-ready
artifacts in [`skills/cascade/references/templates.md`](skills/cascade/references/templates.md).
