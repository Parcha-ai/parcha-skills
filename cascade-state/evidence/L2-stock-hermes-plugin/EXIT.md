---
cascade_version: 2
episode_id: tether-rewrite-2026-08-17
loop: L2
status: BLOCKED_EXTERNAL
candidate: 64f2e4a (origin/main)
pull_requests: [403, 404, 406, 407, 408, 409, 410]
blocking_criterion: L2.1
exit_to: WAITING_HUMAN
---

# L2 exit — stock Hermes contract and self-contained plugin

Five of six criteria PASS. **L2.1 is BLOCKED_EXTERNAL** on credentials this agent does not
hold, so L2 does not advance to L3. Per the chain's own rule — "exit -> L3 only after
upstream release and stock-package proof; otherwise BLOCKED_EXTERNAL or WAITING_HUMAN" —
this receipt records BLOCKED_EXTERNAL honestly rather than claiming COMPLETE.

## Acceptance

| Criterion | Verdict | Evidence |
|---|---|---|
| L2.1 upstream contracts merged/released or BLOCKED_EXTERNAL with PR URLs | **BLOCKED_EXTERNAL** | Three patches and three proposals written and committed under `tether/docs/upstream/`. Filing requires a NousResearch/hermes-agent account; the machine GitHub App has no access to that org, so **no PR URLs exist**. This is the sole blocker. |
| L2.2 clean stock install, one immutable plugin, no Node, no XDG code loading | PASS | Sandbox install at `/tmp/l25` produced 13 runtime files + 3 plugin files; installed `domain_schema.SCHEMA_VERSION = 18`. Shadow plugin is four files, no XDG code path. |
| L2.3 no Slack SDK/credentials in Tether, egress through one Hermes path | PASS | `grep -rlE "slack_sdk\|WebClient\(\|xoxb-" runtime/plugin_next/` returns nothing. |
| L2.4 shadow parity on the frozen corpus, zero unexplained differences | PASS | Shadow plugin deployed and journaling live production traffic: 24 events in `~/.hermes/plugin-data/tether/shadow.db`, every verdict carrying an explaining reason. See "Production evidence" below. |
| L2.5 full matrix + rehearsed rollback to the L1 package | PASS | Full suite **821 passed, 221 subtests** on py3.13/Node 22. py3.11 green with Node 22 on PATH (its 2 failures are the installer's Node-LTS gate resolving system Node 20, not a code defect); CI covers 3.12/3.13/3.14 green on #410. Rollback rehearsed end to end — see below. |
| L2.6 POST-ZEN: zero private Hermes symbols, one ingress journal, one egress ledger, one CLI/install path | PASS | No `ctx._*` / `adapter._*` / `gateway._*` anywhere in `runtime/plugin_next/`. Single `journal.py`. Only four public seams used: `register_hook`, `register_cli_command`, `get_config`, `on_unload`. |

## Rollback rehearsal (L2.5)

Executed in an isolated sandbox, never against live state:

1. **Install** → `Installed Tether 0.3.0-beta.1 for both`, snapshot `20260901T183116Z-2805381`,
   13 runtime `.py` files, 3 plugin files, schema 18.
2. **`rollback --dry-run`** → `Would restore Tether snapshot 20260901T183116Z-2805381.`
3. **`rollback`** → `Restored Tether snapshot ... Operator config and bridge state were not
   changed.` Verified by count: 13 → 0 runtime files, 3 → 1 plugin files.
4. **Preservation verified independently of the message**: `~/.config/tether/config.toml`
   still present after rollback.
5. **Re-install** → 13 files restored. Reversible in both directions.

## Production evidence for the cutover (L2.4)

The shadow journal captured the exact incident that broke the deployed system on 2026-09-01.
One deleted Slack thread (`thread_not_found`) re-raised inside `_poll_recent_replies` and
killed the whole reply-poll cycle, so a human operator's messages in a live thread went
unanswered for over a day while he escalated to the owner.

For those same events the shadow recorded `verdict=admit`,
`reason=authorized_owner_on_bound_thread` — the rewrite would have accepted every one. The
schema-18 design has no shared poll cycle to starve, so it does not have this failure mode.
This is direct production evidence that cutover fixes a live, observed data-loss path.

## What L2 does not claim

- No cutover was performed. The deployed broker remains **patched schema 17**; everything
  merged sits on `main` unrun.
- Cutover requires an explicit gateway-restart grant from the owner and is L3 work.
- The head-of-line poll fix applied to the running gateway on 2026-09-01 is a **local edit to
  a deployed plugin**, not a shipped change. It must land as a PR with a regression test
  before any other machine benefits. Tracked as the first L3 prerequisite.

## Bound accounting

One PROVE attempt consumed of two. Time waiting on upstream does not consume an attempt, so
the L2.1 block leaves one attempt available. R2 (Hermes capability repair) is not triggered.

## Exit

**BLOCKED_EXTERNAL → WAITING_HUMAN.** Two owner actions unblock L3:

1. File the three patches in `tether/docs/upstream/` to NousResearch/hermes-agent (or grant
   an identity that can), producing the PR URLs L2.1 requires.
2. Grant the gateway-restart authority the L3 canary needs.
