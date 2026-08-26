---
cascade_version: 2
episode_id: tether-rewrite-2026-08-17
pacing: autonomous
status: ACTIVE
current_loop: L2
authority: local code, tests, documentation, commits, GitHub branches/pushes/PRs/review responses for Parcha-ai/parcha-skills, upstream Hermes proposals, and sanitized ~/docs publication granted; canary, credential/host-policy changes, Slack sends, deployment, fleet rollout, and destructive actions excluded until separately granted
budget: four substantive loops before a mandatory re-plan; loop-specific valid-attempt and review bounds below; no direct model-provider traffic or unapproved spend
human_gates: upstream merge or release may block L2 externally; authorize credential-policy, host-control, named canary, Slack traffic, deployment, rollback, fleet rollout, and deletion actions before L3 or its successor
target: plan worktree (RECONSTRUCTED 2026-08-22 — see RECONSTRUCTION.md; original worktree deleted by machine cleanup) at Parcha-ai/parcha-skills origin/main 547d8f5db48f99fbfe61ca6f918ee385fcdee30e; current Tether source PR 381 at b1e8b36d8ba418142a3b30b376dd062df1df0d74; deployed Tether files match 0fa83fb3766ca95d3efec0185bb138f07f4fd4c7 and b1e8b36; clean fleet evidence worktree Parcha-ai/parcha origin/main 9b3acf4c39553ce0af3fb7b0ce03361f538e861b; deployed Hermes 0.19.0 at b9ba7c78e41b5d187e2c8fb446655c4b71c42aa5
---

# Tether replacement — Cascade

This chain replaces the current failure-prone Tether implementation with a smaller durable
continuation system that works as a stock Hermes plugin, supports one native endpoint bound to many
Slack threads, survives missing model callbacks and restarts, and safely gives tightly authorized
interactive agents the machine capabilities they need. It covers the open-source data plane and the
private Greppy control plane; it does not turn fleet coordination into a message-routing dependency.

## Current

L0 completed at candidate `e37126b0a9adcef1b851102878bc1ca71d90d0c5`; receipt:
`evidence/L0-canonical-baseline/EXIT.md`. L1a's offline expand/compatibility checkpoint at
`a469d464a895100453f370b1ba001dea0f5ce851` in PR 392 (CHECKPOINT-L1A.md). L1b's read-only
control/readiness checkpoint at `43d12219536dfeab93571c6b300e4a4e0280f60d` in stacked PR 393
(CHECKPOINT-L1B-CONTROL.md). L1c's kill-safe internal rehearsal coordinator at
`c248e08c2c2337a67596f5f718b36bafd50958da` in stacked PR 395 (CHECKPOINT-L1C.md). L1d's schema-18
`DomainRuntime`, detached exact-turn driver, August 14 journey regression, and rehearsal synthetic
cycle at `a6c8e19` in stacked PR 396 (PLAN-L1D.md; CHECKPOINT-L1D.md). L1e (production
`default_gateway_controller`, populated-store rehearsal with contract-defined preservation, live
two-thread trace) at `9ade9d1` in stacked PR 397. **L1 is COMPLETE** — exit receipt
`evidence/L1-durable-domain-core/EXIT.md`, all six criteria PASS at `9ade9d1`; the schema-17
transport/state layer is the named compatibility adapter with its deletion gate owned by L2. The
shipped broker stays schema 17 until L2's cutover.

**L2 is ACTIVE**, re-grounded 2026-08-19 against hermes-agent origin/main `6575507` (v2026.8.18):
see `evidence/L2-stock-hermes-plugin/PLAN-L2.md`. L2a (grounding + ADR Amendment A2 + upstream
proposal drafts) at PR 398. L2b (shadow-mode plugin `runtime/plugin_next/` on public seams only +
shadow parity harness vs the legacy router — no over/under-claim, one explained difference) at PR
399. L2c: both upstream changes IMPLEMENTED and tested against hermes-agent main 2026-08-22
(patches in `tether/docs/upstream/`; branches `feat/plugin-send-delivery-ledger` and
`feat/slack-gateway-platform-events` in `~/worktrees/hermes-upstream-l2c*`); FILING BLOCKED ON
CREDENTIALS — the claudio-michel App cannot fork or PR outside its installations; Miguel or any
user account applies each patch to a fork and opens the PR with its proposal document as the body.
After filing: external gate, `BLOCKED_EXTERNAL` honest while waiting. Next: L2d shadow deploy on
the greppy3 gateway, live parity, single-writer cutover, deletion gate.
PR stack 392←393←395←396←397←398←399 awaits human review; work keeps stacking.

Canary, credential/host-policy changes, Slack sends, deployment, fleet rollout, and destructive
actions remain separate gates.

Amendment A1 (2026-08-18) supersedes the capability-grant constraints on agents: admission stays
strict and fail-closed, agents run at full machine capability, the authority plane narrows to
admission + audit + bridge-credential isolation. See the Amendment A1 section before Invariants.

## Authority and budgets

- May now: inspect repositories and deployed read-only state; edit/test/commit in fresh worktrees;
  push and open or update Parcha-ai/parcha-skills branches/PRs; prepare upstream Hermes proposals;
  and publish sanitized self-contained HTML under `~/docs`.
- May not now: send Slack messages, change credentials/authorization/host policy, restart services,
  mutate live databases, deploy, roll back, roll out to the fleet, or delete legacy paths.
- L0 requires fresh isolated worktrees. GitHub writes and tailnet document publication are granted;
  all outward evidence must remain sanitized and content-addressed.
- L1 requires: local implementation and migration-fixture authority. No live deployment is implied.
- L2 requires: GitHub writes to `Parcha-ai/parcha-skills` and permission to propose changes upstream
  to `NousResearch/hermes-agent`. An upstream merge or release is external state, never assumed.
- L3 requires: explicit approval for the named canary, Slack identity, credential-broker policy,
  GitHub App exercise, host-control operations, service changes, synthetic Slack traffic, rollback,
  and any deletion. Fleet rollout is not authorized by approval of this chain.
- User-absence preflight: product choices are already fixed—endpoint-to-thread is one-to-many;
  Tether-specific code belongs in `parcha-skills`; Hermes integration must use released public APIs;
  every Greppy must converge eventually; and privileged agent access is allowed only behind tight
  Slack authorization (see Amendment A1 for the superseded machine-capability clause).
  No accounts, credentials, or destructive approvals are presumed.
- Evidence bounds: each substantive loop permits at most 2 valid PROVE attempts and 3 review/fix
  rounds. A repair loop permits 1 valid PROVE attempt and 1 review/fix round. Instrumentation failures
  do not consume a valid attempt but must be diagnosed in the receipt.
- Stop conditions: target revision drift without re-grounding; a dirty implementation worktree;
  missing authority; upstream unavailability; any secret in a prompt, argv, log, artifact, or diff;
  a plausible lost/duplicate/misrouted Slack event; an authorization bypass; an untested rollback;
  or an exhausted bound.

## Fixed product contract

Tether's purpose is narrow: authenticate and route a Slack event to exactly one writer, bind a Slack
thread to a continuation endpoint, serialize turns per endpoint, and return a durable outcome to the
originating thread. It does not reason, own agent tool policy, distribute consensus across machines,
or become the Greppy fleet control plane.

The target ownership model is:

```text
Endpoint 1  <──  N ThreadBinding
Endpoint 1  ──  0..1 active EndpointLease
ThreadBinding 1 ── N ordered QueuedTurn
NativeAttempt 1 ── 1 binding generation + reply token + terminal outcome
Slack principal + event/thread context ── admission decision + root-owned audit (per Amendment A1)
```

Authorization is derived from authenticated Slack provenance. Capability gating of agent actions is
superseded by Amendment A1; the bridge's own integrity (credentials, admission ledger, audit) is
the remaining machine boundary.

## Amendment A1 — 2026-08-18 — expanded agent capability, unchanged admission rigor

Product-owner directive (Miguel, 2026-08-18 session takeover): **agents must not be
capability-constrained on their machines.** An admitted turn from an authorized Slack principal
runs the interactive agent at the agent's full machine capability; the agent's own harness
permission mode — not a Tether/authority-plane grant — governs what it does on the host. The
compensating control is admission: the Slack bridge must be maximally strict about *who* can reach
an agent, and fail closed on any provenance doubt.

This supersedes the fine-grained per-operation capability-grant design wherever the two conflict:

- **Superseded:** per-operation expiring `CapabilityGrant` gating of agent actions; opaque one-shot
  handles returning "the minimum result needed for the one operation"; the named-value-only
  credential broker as a *restriction* on agents; constrained `greppy-hostctl` as the only host
  path; L3.3's framing of broker/hostctl as agent ceilings; the "User-absence preflight" clause
  "machine-enforced capability boundaries"; the invariant bullet limiting privileged agents to
  "approved named Interactive Agents values … and constrained host operations".
- **Retained and tightened (bridge integrity, not agent constraint):** strict Slack admission —
  exact workspace/app/user/channel/thread/event provenance against an explicit authorized-user
  allowlist, denial of unknown/removed users, untrusted bots, wrong workspace/app, replays, and
  stale generations (L3.2 stands in full). The bridge's *own* credentials and state (Slack bot
  token, gateway environment, admission ledger, root-owned audit log) remain unreadable and
  unwritable by agent UIDs — leaking the bot token would let anything impersonate the bridge and
  void the allowlist, so this is the bridge defending itself, not a cap on agents.
- **Retained (non-negotiable secret hygiene, orthogonal to agent freedom):** the 1Password
  service-account token and whole-environment injection stay forbidden; model traffic stays on
  `greppy-llm-proxy`; no secret in prompts/argv/logs/artifacts. Agents may still *use* the
  Interactive Agents environment, GitHub App helper, and host operations — now without a Tether-
  side gate — subject only to those hygiene rules.
- **Authority plane scope narrows:** authorityd's job becomes (1) authenticated admission receipts,
  (2) root-owned audit of who commanded what from which thread, (3) protecting bridge credentials.
  It is not an agent-capability arbiter. ADR-001's capability-grant sections must be revised to
  match before L3 build starts (done: ADR Amendment A2 additionally re-grounds against 2026.8
  main); L1a–L1d are unaffected (domain core carries no capability logic).

L3 acceptance rewrites required at L3 planning time: L3.1 keeps root-owned audit + bridge-secret
isolation, drops "no path to ambient credentials" as applied to agents; L3.3 becomes "an authorized
turn exercises full machine capability and every use is audit-attributable to its Slack principal";
adversarial same-UID tests target the *gateway's* isolation from agents, not agents' isolation from
the machine.

**Witnessed data point (2026-08-19, Miguel):** a Claude session initiated from Tether through the
Hermes agent today ran with degraded machine access relative to a user-opened Claude Code session —
sudo, `op`, docker, and the preview manager all failed for it, and it could not even distinguish
its own restricted environment from a machine fault (it misdiagnosed a healthy host as a read-only
filesystem incident before correcting). This is the concrete failure A1 exists to prevent. Binding
acceptance criterion for the rewrite (testable at L3, design-constraining from L1d onward): **a
Tether-spawned interactive agent session must be capability-indistinguishable from the same user
opening that harness directly on the machine** — same UID, login environment, PATH, sudo policy,
1Password/`op` interactive-agents access, docker group, and tool availability. The only permitted
deltas are the bridge-integrity carve-outs above. The parity check itself belongs in the canary
matrix: run the same probe script in a user-launched session and a Tether-launched session and
diff the results to empty (modulo the named carve-outs).

## Amendment A3 — 2026-08-26 — Tether agent model policy

Product-owner directive (Miguel): **Tether-spawned Claude agents run `claude-opus-5`, not
Fable.** Enforced today via `claude_resume_args = ["--model", "claude-opus-5"]` in the machine
Tether config (per-attempt read, no restart). The rewrite must carry this as configuration, not
code: the detached-native driver's launch command composes from operator config, and the L3
canary matrix includes one resume proving the configured model is honored. Background: the
machine LiteLLM (llm-dev-eng) routes `claude-fable-5` to a Vertex publisher that 403s
(data-sharing opt-in missing) — discovered 2026-08-22 when every resume of a bound Claude
session failed before its first turn; the route stays broken and needs no fix for Tether.

## Chain (loops L0–L3 and repair loops)

The full loop prompts and acceptance tables for L0/R0/L1/R1/L2/R2/L3/R3 are preserved in the
completed receipts and plans under `evidence/`; the load-bearing remaining definitions:

### L2 — STOCK HERMES CONTRACT AND SELF-CONTAINED PLUGIN (ACTIVE)

- **goal:** Run Tether as one self-contained plugin on released stock Hermes using public durable
  gateway contracts and no private Slack adapter patches.
- **plan:** `evidence/L2-stock-hermes-plugin/PLAN-L2.md` (grounded 2026-08-19; supersedes the
  original L2 prompt's interface list per ADR Amendment A2 — Hermes 2026.8 main already ships the
  ingress/interactivity/CLI/lifecycle seams; only two upstream widenings remain).
- **accept (unchanged):** L2.1 upstream contracts merged/released or BLOCKED_EXTERNAL with PR URLs;
  L2.2 clean stock install of one immutable plugin, no Node, no XDG code loading; L2.3 no Slack
  SDK/credentials in Tether, all egress through the one Hermes path; L2.4 shadow parity on the
  frozen corpus at zero unexplained differences; L2.5 full matrix + rehearsed rollback to the L1
  package; L2.6 POST-ZEN zero private Hermes symbols, one ingress journal, one egress ledger, one
  CLI/install path.
- **bound:** 2 valid PROVE attempts and 3 review/fix rounds; time waiting on upstream does not
  consume an attempt.
- **at_bound ->** R2 HERMES CAPABILITY REPAIR. **exit ->** L3 only after upstream release and
  stock-package proof; otherwise BLOCKED_EXTERNAL or WAITING_HUMAN.

### L3 — AUTHORITY PLANE AND ONE-GREPPY CANARY (blocked on L2)

As originally planned, amended by A1 (admission + audit + bridge-credential isolation; agent
capability parity criterion above) and requiring explicit approvals for canary, Slack identity,
host control, service changes, synthetic traffic, rollback, and deletion.

## Native task mirror

| Task | Blocked by | Loop | Receipt | Mirror status |
|---|---|---|---|---|
| Canonicalize source and freeze executable contract | clean commit + local/matrix/GitHub evidence | L0 | `evidence/L0-canonical-baseline/EXIT.md` | complete |
| Replace domain core and completion protocol | PRs 392/393/395/396/397 (stacked, human review pending) | L1 | CHECKPOINT-L1A/L1B-CONTROL/L1C/L1D + `EXIT.md` under `evidence/L1-durable-domain-core/` | complete |
| Land stock Hermes contract and self-contained plugin | L1 COMPLETE + GitHub/upstream authority; upstream FILING blocked on credentials | L2 | `evidence/L2-stock-hermes-plugin/EXIT.md` | active |
| Prove privileged one-Greppy canary | L2 COMPLETE + named canary/security/deploy authority | L3 | `evidence/L3-authority-canary/EXIT.md` | blocked |
| Cut evidence-based fleet rollout successor | L3 COMPLETE | successor PLAN | successor chain path determined at L3 EXIT | blocked |

Never mark a mirrored task complete until its immutable receipt exists. If a task system and this
file disagree, the chain and evidence win.

## TAKEOVER snapshot

Read `Current`, the active plan (PLAN-L2), its latest receipts, actual repository/worktree HEADs,
deployed Tether/Hermes identities, upstream Hermes release/main, authority, budgets, and the task
mirror. The planning pins are observations, not permission to ignore drift. Re-fetch with the
Claudio Michel GitHub App and create fresh worktrees. **This chain now lives on the
`cascade/tether-rewrite-2026-08-17` branch of Parcha-ai/parcha-skills — commit and push every
state change; never keep cascade state only in an uncommitted worktree** (see RECONSTRUCTION.md).

## Invariants

- Exactly one writer owns each admitted Slack event; separate Greppy installations never pretend to
  provide distributed consensus.
- A Slack event is not "handled" until it has a durable end-to-end terminal receipt or a visible
  unresolved state. Transport acceptance and queue transfer are not delivery.
- Possible execution is never retried blindly. Safety cannot mean an immortal silent lock; ambiguity
  must be visible, bounded operationally, and evidence-resolvable.
- One endpoint may bind many Slack threads, but has at most one active lease. Reply and authority
  remain bound to the originating thread and binding generation.
- Attempt completion is owned by the driver/harness, never by remembering a prompt command.
- Authorization is derived from authenticated Slack provenance and scoped to event/thread. It never
  follows a pane, endpoint, model session, or generic post.
- Admission receipts and audit records are root-owned and outside agent-writable Tether state
  (Amendment A1: they gate admission and attribute actions; they do not cap agent capability).
- Tether contains no Slack credentials or SDK transport and uses only released public Hermes APIs.
- Hermes owns one ingress handoff and one canonical egress ledger; Tether owns only binding,
  scheduling, native attempts, and local control state.
- Payloads needed after process exit are durable owner-private blobs, never ephemeral path handoffs.
- Fleet reconciliation deploys packages/config/drop-ins and observes content-free health; it never
  routes messages or centralizes thread state.
- Model and judge traffic uses the local `greppy-llm-proxy`; failure is closed.
- No loop advances without a validated boundary receipt. `COMPLETE` requires every criterion PASS;
  `AT_BOUND` uses only its declared repair; external and human gates stop honestly.
- ZEN applies during BUILD. Every architecture-changing EXIT applies POST-ZEN, deletes superseded
  paths after proof, names temporary-scaffold owners/removal gates, and verifies rollback at the
  actual candidate or deployed revision.
