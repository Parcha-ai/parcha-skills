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
