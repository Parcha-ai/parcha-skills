# Changelog

## 0.3.0-beta.1

- Adds BindingV3 Herdr protocol-19 live endpoints with official native-session
  references, occupant-bound agent names, private socket validation, and exact
  process-incarnation checks.
- Routes generation-fenced native delivery, acknowledgment, edit interruption,
  cancellation, and ambiguous-outcome recovery through Herdr `agent.prompt`
  and `agent.send_keys` primitives.
- Captures Herdr bindings from its session environment without requiring a
  duplicate Codex or Claude session environment variable.
- Adds the `parcha.tether` Herdr plugin with contextual actions, Slack-link
  handling, and a keyboard-driven popup cockpit.
- Opens the cockpit through Herdr's active-pane popup contract while carrying
  the independently verified target in a private single-use handoff record.
- Adds broker protocol 6 contextual inspection without exposing credentials,
  native session values, socket paths, process fingerprints, or message text.
- Makes lifecycle `--restart` detect an active system-level Hermes gateway and
  use Hermes's documented non-interactive system restart path.
- Preserves the canonical default Herdr session, follows pane moves through the
  occupant identity, and survives live handoff terminal-ID rotation only when
  the native session and process incarnation remain unchanged.
- Stages legacy Zellij follow-up instructions in bounded terminal writes so
  Claude Code keeps the attempt marker visible instead of collapsing it into an
  opaque pasted-text placeholder before Enter.

## Unreleased

- Makes explicit local attach and rebind operations durably claim their exact
  Slack thread for the current binding generation, so allowlisted humans can
  continue long or multi-bot threads without mentioning Tether. Peer bots stay
  mention-gated and stale generations remain fenced.
- Allows one Codex, Claude Code, Herdr, Zellij, Hermes, or headless endpoint to
  own multiple independent Slack threads. Exact thread routing and reply keys
  remain per bridge, while the durable ledger serializes agent turns across the
  shared endpoint and wakes the next queued sibling after acknowledgment.
- Keeps Herdr bindings attached across a host or Herdr restart when the exact
  pane, official native-session reference, agent runtime, and trusted live
  executable still agree, even if Herdr rotates its terminal ID and agent name.
- Separates read-only Herdr preflight failures from ambiguous terminal writes,
  safely requeues legacy preflight failures, and prevents one such record from
  permanently blocking every later thread reply.
- Makes `tether status` and `tether doctor` fail visibly on unresolved delivery
  blockers and report queued, uncertain, and blocked-thread counts.
- Posts a Slack control notice when a bridge reply fails so threads are not
  silently abandoned.
- Adds idempotent `team_id` backfill migration for legacy bridges, and skips
  polling threads with an empty workspace identity instead of retrying and
  failing every cycle.
- Distinguishes local credential-helper misconfiguration from model
  authentication failure in continuation error messages, and surfaces
  `HermesCompatibilityError` detail in poll logs.

## 0.2.0-beta.1

- Introduces BindingV2 and database schema 15.
- Adds generation-bound delivery attempts and explicit ambiguous-delivery
  states.
- Adds durable, single-writer Slack ingress and outboxes for roots, replies,
  generic thread posts, Hermes text posts and edits, and external file uploads.
- Adds durable thread ownership, bounded paginated reconciliation, shared
  Slack rate-limit coordination, and operator `unresolved`/`resolve` recovery.
- Adds exact Zellij process revalidation immediately before input and verified
  cancellation of the bound pane.
- Adds staged install and upgrade, per-user locking, managed-file checksums,
  crash journals, rollback snapshots, safe uninstall, and unified command
  discovery.
- Adds bounded retention for completed delivery and deduplication records.
- Adds restart-safe Slack ingress, delivery, and attachment state with
  generation-bound recovery.
- Guards and redacts Hermes native media sends while documenting their
  best-effort delivery boundary.
- Makes Socket Mode authoritative and treats Slack polling as best-effort only.
- Adds stdin/file-descriptor text transport and deprecates message text in
  process arguments.
- Documents Linux and Hermes 0.19.0 support, the Unix-account authority
  boundary, schema rollback limits, attachment controls, and at-least-once
  Slack delivery.
- Adds a signed, exact-commit release workflow with npm OIDC provenance and
  immutable package verification.
- Makes explicit Slack mentions authoritative across humans and bots, suppresses
  terminal `NO_REPLY` control output, and keeps continuation failures out of
  conversation threads.
