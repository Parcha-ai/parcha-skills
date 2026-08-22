---
cascade_version: 2
episode_id: tether-rewrite-2026-08-17
loop: L2
slice: L2-stock-hermes-plugin
status: ACTIVE
grounded_against: hermes-agent origin/main 657550716f370bd5d1e848a57fc24b9c404cf982 (2026-08-19), release v2026.8.18; deployed Hermes 0.19.0 (b9ba7c7, 2026-07-20)
base_candidate: 9ade9d19 (claude/tether-rewrite-l1e-20260819)
---

# L2 plan — Tether as a stock-Hermes plugin, leveraging what Hermes already ships

## Re-grounding verdict (supersedes the ADR's 0.19.0-era assumptions)

Upstream moved 7,048 commits since the deployed release. The plugin surface on current main is
far richer than at planning time, and the correct posture inverts: **consume Hermes's public
seams first; propose upstream only the two genuinely missing generic widenings.** Placement
policy is explicit (CONTRIBUTING.md:88-103): third-party product plugins never land in
hermes-agent — Tether lives in parcha-skills, installed to `~/.hermes/plugins/tether`, and the
only sanctioned upstream PR shape is "widen the generic plugin surface, with a stated consumer".

### What Hermes now provides that the ADR assumed we would have to propose

| ADR requirement | Current-main reality | Consequence |
|---|---|---|
| Ingress middleware v1 (normalized event before dispatch, skip/rewrite/consume) | `pre_gateway_dispatch` directive hook (plugins.py:221-228, fired run.py:16343-16385): skip / rewrite / allow, first directive wins, fires before auth and agent dispatch | **Consume as-is.** `consume` = journal + `{"action":"skip"}` from Tether. |
| Exclusive owner + local retry from a pre-ACK ingress ledger | Does not exist and will not: slack_bolt acks before anything (deliberate — un-acked events trip Slack's auto-disable, adapter.py:2062-2096); only guard is an in-memory 300s dedup. Upstream rejected a silent outbox once already (#61790). | **Deliberate ADR deviation (Amendment ADR-A2):** Tether journals inbound itself — durable write to `plugin_db("tether")` inside `pre_gateway_dispatch` before returning a directive. The residual ack→hook crash window is Hermes's own, documented, and no worse than today's baseline. No upstream ask. |
| Egress + delivery ledger v2 (receipts, get/watch/reconcile) | A durable `delivery_obligations` ledger exists (gateway/delivery_ledger.py) with honest at-least-once + visible RECOVERED_MARKER — but it covers ONLY the gateway's final response. The plugin send path (`ctx.dispatch_tool("send_message")` → SlackAdapter.send → chat_postMessage) is completely unledgered: no obligation row, no retry, no receipt. | **Upstream ask #1 (small, generic):** ledger plugin/tool-path sends in the existing `delivery_obligations` table and return the `obligation_id` receipt from send_message (message_id is already returned). Until merged, Tether's L1 attempt/outbox model remains the receipt authority. |
| Platform read (edits/deletes/thread events) | `gateway_platform_event` observer exists but **Slack never fires it** — Discord and Telegram do. | **Upstream ask #2 (small, precedented parity):** Slack fire-sites for `message_changed`, `message_deleted`, reaction parity. Clears the anti-speculation bar because Tether is the stated consumer (AGENTS.md:98-101). |
| Plugin lifecycle: start, supervised tasks, quiesce, readiness contribution | `register(ctx)` + `ctx.spawn_task` (supervised, cancelled on unload) + `ctx.on_unload` (reverse-order teardown) + ownership ledger. No plugin readiness contribution exists. | **Consume as-is; drop the readiness-contribution requirement** — `tether doctor` and `schema status` already carry operator health. No upstream ask. |
| CLI surface `hermes tether ...` | `ctx.register_cli_command` + `ctx.register_command` for `/tether` slash | **Consume as-is.** Retires the Node CLI distribution path at the deletion gate. |
| No raw SDK/token across the boundary | `MessageEvent` normalized envelope; `register_slack_action_handler` (documented as THE replacement for patching `SlackAdapter.connect`); `dispatch_tool("send_message")`; `inject_message` (double-gated) | **Consume as-is.** Directly retires all 13 private/replaced adapter methods and both hook-registry mutations from the L0 baseline. |
| Capability negotiation by version range | No API-version handshake exists by design (AGENTS.md:800-823): additive behavior contracts, signature-inspected callbacks, manifest v2 fields advisory. | **Adapt:** declare `manifest_version: 2`; compatibility asserted by behavior tests through the real discovery path, not version literals. All hook callbacks declare `**kwargs`. |

### Hard rules learned from the house style (pro-style = their style)

- Persist to `<hermes home>/plugin-data/tether/` (`plugin_storage.plugin_db`) — never the install
  dir, which `hermes plugins update/remove` destroys.
- All behavioral config under `plugins.entries.tether.settings` via `config_schema`; no new
  `HERMES_*` env vars (secrets only).
- Never block `/stop`//`approve` paths; `pre_gateway_dispatch` directives must be fast and
  fail-open to "allow" on internal Tether errors EXCEPT for events Tether has already journaled
  as owned.
- One logical change per upstream PR; Conventional Commits; `scripts/run_tests.sh` green;
  real-HERMES_HOME temp-dir E2E tests; no change-detector tests; lead with the stated consumer.

## Sub-slices

### L2a — grounding receipt, ADR amendment, upstream proposal texts — DONE (PR 398)

### L2b — the plugin, on public seams only (shadow mode) — DONE (PR 399)
`runtime/plugin_next/`: manifest v2; fail-closed admission (exact workspace, authorized owner,
bound thread, no bots, event identity required; unbound/not-Slack = not_ours; unconfigured claims
nothing); durable idempotent journal under plugin-data/tether/; forced shadow (hook always returns
None, shadow_mode=false refused loudly); `hermes tether` CLI status; floor-compatible with
deployed v2026.7.20 (spawn_task/on_unload/get_config feature-detected). Shadow parity harness vs
the legacy pure router: NO over-claim, NO under-claim vs the legacy NATIVE set; all pairs map
through an explicit equivalence table (`not_ours` = ownership transfer to ordinary Hermes
dispatch); one allowlisted explained difference (edits invisible until ask #2; both systems admit
no turn). The wrong-workspace case proved EQUIVALENT (legacy carries team_id in message and
thread identity).

### L2c — upstream PRs
STATUS 2026-08-22: both asks IMPLEMENTED and tested against hermes-agent main, packaged as
patches in tether/docs/upstream/ (branches feat/plugin-send-delivery-ledger @ d8de9c773 on
261a4efb9, feat/slack-gateway-platform-events @ 7b6abe2bd on 657550716, in worktrees
~/worktrees/hermes-upstream-l2c*). Ask #1: 6 new tests + 68 send_message + 24 ledger green.
Ask #2: 11 new tests + 563 Slack/platform-event gateway green. FILING BLOCKED ON CREDENTIALS:
the claudio-michel App cannot fork or PR outside its installations — Miguel (or any user
account) applies each patch to a fork and opens the PR with the proposal doc as body. After
filing: external gate, `BLOCKED_EXTERNAL` honest while waiting; shadow work proceeds
(edits/deletes degrade gracefully until ask #2 lands).

### L2d — cutover and the deletion gate
Only after L2b shadow parity is proven LIVE on the greppy3 gateway and both asks (or documented
workarounds) resolve: single-writer cutover per binding kind, then delete the named compatibility
adapter — v17 `Store` transport, four Slack outbox/reconciliation tables' write paths,
`hermes_compat.py` private coupling, Node CLI direct-broker paths, `$XDG_DATA_HOME/tether` code
loading — with packaged rollback to the L1 system rehearsed first (chain L2.5/L2.6). Shadow
deploy on the live gateway requires the service-restart grant (currently excluded — human gate).

## Acceptance mapping (chain L2.1–L2.6)

- L2.1 external contracts: asks #1/#2 merged+released upstream, or `BLOCKED_EXTERNAL` recorded
  with the PR URLs; conformance tests run against a stock install of that release.
- L2.2 clean install: `hermes plugins install` (or pack) from parcha-skills produces a working
  plugin on a fresh HERMES_HOME with no Node, no XDG code loading. Node CLI retirement is L2d.
- L2.3 no Slack SDK/credentials in Tether: static boundary check over the plugin tree; egress
  only via dispatch_tool/platform_actions.
- L2.4 shadow parity on the frozen corpus: zero unexplained decision diffs.
- L2.5 full matrix + rehearsed rollback to the L1 package.
- L2.6 POST-ZEN: zero private Hermes symbols (from 13+2 today), one ingress journal, one egress
  path, one CLI/install path.

## ZEN gate

Tether adds no second event bus, no readiness framework, no version-literal handshake, and no
egress ledger duplicating `delivery_obligations` once ask #1 lands — until then the L1 attempt
ledger is the one receipt authority and says so in code comments. Anything patching a private
`_method` or reordering hook registries fails the gate outright.
