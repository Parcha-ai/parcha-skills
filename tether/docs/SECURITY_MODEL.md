# Tether security boundary

This describes the current `0.4.0` source. It does not claim the retired
schema-17/18 isolation or migration design is active. The
[architecture](ARCHITECTURE.md) names the actual implementation modules.

## Local authority

Hermes and Tether run as a non-root Unix user. The broker uses a private
Unix-domain socket and verifies peer identity. The installer rejects root and
records owner-private managed state. Processes sharing the same UID share the
local authority boundary; a private socket does not isolate a model process
from another process under that user.

Use separate OS accounts or hosts when computers must not share that local
authority. Tool execution permissions and sandboxing belong to the host and
native runtime. Tether's launcher context describes the actual execution
boundary to the continued session; it does not create a new sandbox.

## Slack identity and instruction authority

The gateway owns Slack credentials. CLI clients send bounded, newline-framed
JSON through the broker, not direct Slack calls. Native child environments are
allowlisted and do not need the gateway's Slack credential.

Admission uses the configured workspace, explicit human operator identities,
trusted peer identities, thread association and mentions. A colleague roster
is prompt context and grants no authority. Owned thread replies from allowed
humans can continue without a mention. Tether admits authenticated trusted
peers on bound threads; Hermes's configured mention-aware ingress supplies an
upstream filter. Tether's current admission layer does not independently require
a peer mention.

A thread is attached to an exact computer session. Binding generations fence
queued work when it is intentionally rebound or closed. A replacement session
must be explicit; a fresh session or another account is not invisible recovery.
Provider authentication and model selection remain computer configuration.

## Persistence and external effects

The current Store persists endpoints, bindings, turns, attempts and thread
origins. The session driver stores terminal response blobs separately from
platform delivery. SQLite writes, native actions and Slack acknowledgment are
different boundaries. Ambiguous external actions must not be interpreted as
safe-to-repeat work solely because a local receipt is absent.

Several gateway paths still perform direct Slack operations through
`SlackEgress`; the roadmap consolidates delivery under supported Hermes APIs.
The source does not provide exactly-once Slack effects, a schema migration
orchestrator or an independently isolated privileged recovery writer. The
legacy `resolve` mutation remains unavailable.

## Context and diagnostics

Team manifests are operator-controlled context: names, roles, project references
and computer preferences. They must not contain secrets. Strict type/reference
validation prevents malformed configuration from silently becoming another
team; rendered-size validation prevents truncating identity or the shared
contract. It is not sanitization of an untrusted prompt or access control.

Slack content, attachments, source metadata, saved replies and error text can
contain private work. Keep credentials and sensitive findings out of messages,
and restrict access to runtime state. Redaction is a diagnostic aid, not a
complete data-classification system. A model's claim that a task is done is not
independent verification of the artifact.

## Offline development

The demo uses scripted fake computers and fake delivery. It never discovers
Herdr, launches a native process or calls a provider. Tests run per file with
private HOME and an explicit environment; regressions patch process/network
operations to fail if the demo crosses that boundary. This proves tested
mechanics, not a live host's configuration or a model's security behavior.
