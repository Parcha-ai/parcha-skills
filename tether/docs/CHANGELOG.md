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

- Adds one schema-18 `BlockingCondition` projection for native uncertainty and
  endpoint/binding rebind blockers. The August 14 accepted-without-ack shape is
  visible with its age and blocked-turn count. Operator actions remain hidden
  until the isolated authority capability is attested; blind retry is never
  advertised.
- Adds a fenced, idempotent operator-resolution domain transaction that
  requires an external authority verifier, terminalizes exact attempt members,
  and releases the endpoint lease atomically. It is not exposed on the
  same-UID agent socket.
- Disables the legacy same-UID broker and CLI recovery mutation until an
  OS-distinguishable operator authority channel exists.
- Adds read-only `tether schema status` with database/runtime compatibility,
  logical-manifest digest, explicit security-domain readiness, incomplete
  receipt detection, and schema-18 blocker reporting. Schema migration remains
  disabled until the coupled lifecycle/runtime cutover is implemented.
- Extracts one authoritative schema-operation receipt module with monotonic
  compare-and-swap phases, fsync-bound writes, and a redacted public view.
  Broker and plugin startup now fail closed on any invalid or incomplete
  receipt before opening the database.
- Adds a side-effect-free `validate-store` runtime attestation (read-only
  SQLite, exact-artifact build digest, logical-manifest digest) that never
  instantiates the migrating `Store`. It is internal; the public CLI remains
  `tether schema status`.
- Adds the internal kill-safe schema rehearsal coordinator: installer
  lifecycle lock, maintenance gate, independent quiesce attestation, database
  singleton, receipt-bound verified backup, disposable 17→18→17 transforms
  validated by pinned target and predecessor artifacts in a sanitized
  environment, and single-classified crash recovery at every durable phase.
  Rehearsal results are evidence only; no public migrate or rollback command
  exists and `migration_ready` stays false.
- Makes the installer refuse install/upgrade/rollback/uninstall while the
  schema maintenance flag is armed.
- Adds the sole schema-18 `DomainRuntime`: single-transaction admission, fair
  oldest-ready scheduling across sibling bindings, fenced single-lease
  allocation, and a driver-receipt-owned attempt lifecycle in which stale
  fences, replayed request ids, and out-of-order sequences are dead, and
  terminalization, turn outcomes, and lease release commit atomically. Herdr
  and Zellij endpoints stay ineligible for automatic scheduling. The shipped
  schema-17 broker does not use it.
- Adds the detached-native exact-turn driver: durable spawn intent before
  `Popen`, process identity (pid + starttime) captured, durable `accepted`
  receipt before any wait; a crash before acceptance recovers `uncertain` and
  is never re-executed; exactly stripped `NO_REPLY` is the only silence,
  empty output fails, responses become owner-private content-addressed
  blobs; unobservable kill outcomes stay `uncertain`, never `cancelled`.
- Proves the August 14 accepted-without-terminal journey end to end on
  schema 18: one typed blocker with age and blocked-turn count, retry never
  advertised, and capability-gated operator resolution freeing the endpoint.
- Extends the internal schema rehearsal to boot the target `DomainRuntime`
  against a disposable copy of the migrated store and run one synthetic
  admit/schedule/receipt/terminal cycle before rollback validation.

- Adds a fail-closed `TETHER_AMBIENT_BOT_CHANNELS` grant for trusted
  automations that intentionally create unmentioned root events. Each grant is
  bound to one exact Slack bot/app identity and one exact shared channel.
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
