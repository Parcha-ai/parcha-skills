# Security Policy

## Supported versions

Tether is pre-release software. Security fixes apply to the latest tagged
`0.2.0` pre-release. Older `0.x` builds are unsupported.

## Private reporting

Use [GitHub private vulnerability reporting](https://github.com/Parcha-ai/parcha-skills/security/advisories/new).
Do not open a public issue for a suspected vulnerability.

Include the affected version, operating system, impact, reproduction steps, and
the smallest redacted evidence needed to confirm the issue. Do not submit Slack
tokens, provider credentials, `bridges.db`, customer messages, session
transcripts, or unredacted Hermes logs.

Maintainers will assess complete reports and coordinate disclosure when
available. This project does not promise a fixed acknowledgment or remediation
time.

## Trust model

- Tether must run with Hermes as the same dedicated non-root Unix user.
- The broker refuses root, uses a mode-`0600` Unix socket, and verifies the
  connecting peer's Linux UID with `SO_PEERCRED`.
- Hermes holds the Slack token. Tether does not inject it into local publishers
  or resumed agent processes, and child environments are allowlisted.
- The Unix account is trusted. Tether does not isolate mutually hostile
  processes running under that account or protect state from them.
- Slack human and peer-bot identities require explicit allowlists. Channel
  membership and message text do not grant authority.
- Durable Slack text posts and edits are at least once. Idempotency controls
  reduce duplicates but do not provide a distributed exactly-once transaction;
  ephemeral notices and native media uploads are best-effort.
- The local database contains Slack text, session identifiers, paths, and
  delivery state. It and its backups are sensitive.
- Attachments are disabled without approved roots and pass structural and
  secret-pattern checks. Those checks do not classify customer, legal, or
  regulated data.

Tether is a routing and credential-separation layer, not an agent sandbox,
endpoint-security product, Slack data-loss-prevention system, or authorization
boundary between processes sharing one Unix account.

See [Compatibility](../tether/docs/COMPATIBILITY.md) and
[Operations](../tether/docs/OPERATIONS.md) before deployment.
