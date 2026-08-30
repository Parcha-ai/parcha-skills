---
name: cascade
description: Turn a blunt, risky, or multi-cycle task into a short chain of bounded development loops with explicit authority, evidence-gated exits, honest bound handling, and portable takeover. Use when the user names Cascade, says "cascading loops", "work in loops", or "go ham", or asks to plan, advance, resume, or take over a consequential project that should not be attempted as one unstructured push.
---

# Cascade

Plan before building. Replace one large push with a short chain of bounded loops. Each loop makes
one state change, proves it at the actual target, and writes an immutable boundary receipt before
anything advances. The chain file is the portable contract across Codex, Claude, pi, compaction,
and handoff; native task systems only mirror it.

## Choose the mode

- **PLAN** — create the chain and task mirrors before BUILD.
- **ADVANCE** — execute the current loop's next ribbon step.
- **TAKEOVER** — after a session boundary or handoff, re-ground before advancing.

At PLAN, front-load anything the user must provide before becoming unavailable: accounts,
credentials, target choices, destructive-action approval, or product decisions. Never bury a known
human dependency in a later autonomous loop.

## Declare the operating envelope

The chain frontmatter records:

- pacing: `autonomous` or `checkpointed`;
- mutation authority: plan-only, local edits, GitHub writes, publishing, deployment, and destructive
  actions are separate grants;
- budget: relevant time, spend, iteration, and context limits;
- human gates;
- exact target identity: repository/worktree, branch, HEAD or deployed revision, and runtime;
- chain status and current loop.

Infer pacing only when clear. “Go ham” means autonomous pacing; it never expands mutation authority,
budget, target scope, or permission to cross a human gate. In checkpointed mode, stop after every
boundary receipt for one concrete go/no-go/redirect decision.

## Cut a short chain

Prefer **2–4 substantive loops before a re-plan gate**. Use agent judgment rather than a mechanical
line-count rule: one loop should have one state change, one primary acceptance story, and roughly one
reviewable concern. Split a loop whose proof depends on unrelated subsystems; combine bookkeeping
that cannot independently change the verdict. Put measurement before intervention when later loops
claim a delta. Put lower-risk, semantics-preserving changes before riskier prompt or model changes.
Move a long roadmap into a successor chain instead of pre-authoring a brittle mega-chain.

Every loop has exactly six fields:

| Field | Contract |
|---|---|
| `goal` | One-sentence state change. |
| `prompt` | Self-contained instructions and named inputs for a fresh session. |
| `accept` | Criterion IDs with checkable evidence and explicit falsifiers. |
| `bound` | Maximum valid attempts or review/fix rounds. |
| `at_bound ->` | A predeclared localized repair successor, or `STOP`. |
| `exit ->` | The normal successor after `COMPLETE` only. |

An `at_bound ->` repair must remain inside the declared authority and diagnose or repair the failed
mechanism; it cannot quietly repeat the same loop, weaken acceptance, or jump to the normal successor.

## Execute the ribbon

1. **RE-PLAN** — read the chain, current prompt, latest receipt, target HEAD/revision, and native task
   mirror. Plan only this loop.
2. **BUILD** — implement one concern. Apply ZEN: simple, general, prompt/agentic-oriented where
   judgment is the work, beautiful, and dope.
3. **PIN** — test the mechanism and its plausible fake-success modes.
4. **PROVE** — run the real claim and retain raw, ignored evidence.
5. **MEASURE** — record the comparable delta, or an explicit justified `N/A`.
6. **REVIEW / INTEGRATE** — resolve findings and verify the actual merge candidate or resulting
   HEAD. A stale, conflicted, unmerged, or differently deployed change is unmet.
7. **EXIT** — map every criterion to fresh evidence, write the boundary receipt, then transition.

Use background execution only when the harness actually supports it: stock pi has no background bash,
so keep work foreground there or split it into resumable bounded steps. An infrastructure or
instrumentation failure does not consume an evidence-attempt bound, but diagnose it and record why;
only a run that exercised the claim consumes the bound.

## Use exact boundary states

Every loop ends in exactly one state:

- `COMPLETE` — every current acceptance criterion passed at the verified target. This is the only
  state that follows `exit ->`.
- `AT_BOUND` — valid attempts are exhausted with one or more criteria unmet. Write the receipt and
  either enter the predeclared `at_bound ->` repair or stop/page. Never follow the normal successor.
- `WAITING_HUMAN` — a declared human decision or approval is required; wait without inventing a
  timeout or approval.
- `BLOCKED_EXTERNAL` — required external state or authority is unavailable.
- `SUPERSEDED` — an append-forward re-plan replaced the unfinished remainder with a named successor.

There is no “complete except”, “complete with deferred criteria”, partial completion, or retroactive
weakening of acceptance. Regression means the criterion is unmet.

## Make evidence portable and fresh

For every criterion, the receipt records: criterion ID, verdict, command/action, runtime/environment,
target HEAD or deployed revision, timestamp, artifact path and digest when practical, the negative
case or falsifier checked, and cleanup/rollback notes. Point to artifacts, not commit messages or
summaries. Keep raw evidence under ignored `.cascade/evidence/<loop>/`; publish only deliberate,
redacted summaries. When commits are forbidden, identify the candidate as base HEAD plus a
working-tree or diff digest; HEAD alone does not identify uncommitted proof.

The chain file owns definitions, current position, and append-forward history. Only its small
`Current` block is mutable. Boundary receipts are immutable. A native task must link to its loop and
receipt; it cannot be marked complete before that receipt exists, and disagreement resolves in favor
of the file-backed evidence.

## Take over skeptically

On TAKEOVER, read the chain's `Current` block, current loop prompt, latest boundary receipt, exact
HEAD/deployed revision, and native task mirror. If Recap is installed, use it to recover session or
compaction context, but treat the chain and target as authoritative. Recheck baseline and noise-band
priors rather than inheriting conclusions. Confirm repository, worktree, branch, merge freshness,
runtime, authority, and remaining budget before changing anything. If they disagree, stop or append
a `SUPERSEDED` re-plan; do not guess the position.

## Close cleanly

At every architecture-changing EXIT, run **POST-ZEN** after ZEN: leave the whole system smaller,
boring to operate, and maintainable. Prefer one authoritative path, conventional packaging, full
verification, reusable evals, clear ownership, and tested rollback. Delete superseded paths and
abstractions. Temporary scaffolding needs an owner and removal gate. For a non-architectural loop,
record why POST-ZEN is `N/A`.

No loop advances without its receipt. Autonomous chains report at boundaries and continue only when
the resulting state permits it. The final loop closes with evidence or creates a short successor
chain from the remaining unknowns; it does not leave a permanent migration tail.

Use [references/templates.md](references/templates.md) for chain and receipt skeletons. Run
`python3 scripts/validate_cascade.py chain <path>` or `... exit <path>` for structural checks; the
validator cannot replace semantic review.
