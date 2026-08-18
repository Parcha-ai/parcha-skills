# ADR-001: Replace Tether with a durable continuation service

- Status: accepted target for staged implementation
- Date: 2026-08-18
- Owners: Tether maintainers and Greppy platform owners
- Decision scope: open-source Tether data plane, Hermes extension boundary, and private Greppy authority plane
- Evidence baseline: `evals/incident-corpus.json`, schema 17 runtime, Tether `0.3.0-beta.1`, Hermes `0.19.0`

## Decision

Tether will become a small continuation service with one job: admit a Slack
event to exactly one writer, serialize work for the selected native endpoint,
and durably return the outcome to the originating Slack thread.

The replacement has three boundaries:

1. Stock Hermes owns Slack transport, the authoritative ingress handoff, and
   the one durable platform-egress ledger.
2. The self-contained Tether plugin owns thread bindings, endpoint scheduling,
   native attempt state, payload durability, and operator resolution.
3. A separate root-owned Greppy authority service owns privileged capability
   grants. Tether and the agent can request and consume grants but cannot mint,
   widen, or persist them. The Tether database is service-writable and is never
   writable by an endpoint or model process.

The core relationship is:

```text
Endpoint 1 <── N ThreadBinding
Endpoint 1 ── 0..1 active EndpointLease
ThreadBinding 1 ── N ordered QueuedTurn
NativeAttempt 1 ── 1 binding generation + reply token + terminal outcome
Slack principal + event/thread context ── 0..N expiring CapabilityGrant
```

A capability grant never belongs to an endpoint. Reusing one Claude Code,
Codex, Herdr, or Zellij session from another Slack thread carries continuity,
not authority.

## Purpose and non-goals

Tether authenticates and routes an event, binds a thread to a continuation
endpoint, serializes turns per endpoint, and records a durable outcome. It is
not an agent, model router, tool-policy engine, Slack SDK wrapper, fleet
orchestrator, distributed consensus system, or general job queue.

The fleet control plane may deploy and observe Tether, but it does not enter the
message path and never becomes the owner of thread state.

## Why the current system fails

The current implementation has good local safety mechanisms—durable outboxes,
generation fences, same-UID socket checks, uncertain states, and endpoint-wide
serialization—but its component boundaries are wrong.

- Native completion depends on a prompt telling the model to call an exact
  reply command. A successfully accepted prompt can remain `awaiting_ack`
  forever when the model omits the callback.
- One immortal attempt blocks its thread and every sibling thread sharing the
  endpoint. Recovery deliberately refuses to age it out, while the advertised
  `unresolved` command cannot list it.
- Generic Slack posting can succeed during an open attempt without closing or
  rejecting that attempt.
- Each bridge stores a duplicated endpoint snapshot. Sibling bindings can
  diverge after rebind even though scheduling treats them as one endpoint.
- Tether replaces private Hermes Slack methods, reorders private hooks, and
  requires an exact clean Hermes Git SHA. Ordinary upstream releases cannot be
  adopted through a stable plugin contract.
- Tether duplicates platform delivery machinery already partly present in
  Hermes and distributes runtime code across an external data directory, a
  Hermes plugin directory, a Node CLI, skills, and fleet wrappers.
- Cross-repository wrappers drifted from required `reply_key` and
  `idempotency_key` fields and hid command failures.
- Path-only `/run` handoffs can outlive neither their writer nor cleanup. A path
  is not a durable payload reference.
- Same-UID trust remains appropriate for one Hermes/Tether instance and its
  bound agent endpoints, but it cannot safely mint machine privilege. Slack
  authorization and host privilege need a separate, auditable root boundary.

The executable baseline is in `evals/incident-corpus.json`. It distinguishes a
defect reproducer from a passing safety control so a future implementation
cannot claim success by merely changing an assertion.

## Target components and ownership

### Stock Hermes

Hermes owns Socket Mode, Slack identity, reconnects, normalized earliest
ingress, normal agent dispatch, and platform egress. Its existing delivery
ledger is extended rather than paralleled. Tokens and raw Slack clients never
cross the public plugin API.

### Tether plugin

One installable Python plugin contains migrations, router policy, endpoint and
binding store, scheduler, native adapters, local control service/client,
operator commands, skill, and tests. It uses only released Hermes capability
interfaces. It does not import the Slack SDK, load executable code from
`$XDG_DATA_HOME/tether`, or require Node.

### Native driver

Each adapter implements one typed contract:

```text
capture() -> EndpointRef
validate(endpoint) -> valid | rebind_required
submit(endpoint, expected_incarnation, lease_fence, request_id, attempt)
  -> not_started | accepted | completed | uncertain
watch(attempt_id, fence, cursor) -> ordered durable lifecycle receipts
reconcile(attempt_id, fence) -> latest authenticated receipt or uncertain
cancel(attempt_id, expected_incarnation, lease_fence, request_id)
  -> not_started | cancelled | uncertain
health(endpoint) -> typed state
```

The driver—not the model—must emit the terminal lifecycle receipt. A semantic
response is optional. A normally completed turn without one closes through a
driver-owned `NO_REPLY`. Each receipt carries receipt ID, attempt ID, lease
fence, monotonic sequence, driver incarnation, state, observed time, and an
optional durable response reference. Duplicate delivery is idempotent and a
stale fence cannot close a newer attempt. `watch` resumes from a cursor after
either side restarts; `reconcile` proves the latest durable driver state. An
accepted turn whose completion still cannot be proven becomes operator-visible
`uncertain`; it is never blindly replayed.

`submit` and `cancel` condition the operation atomically on the persisted
endpoint incarnation and current lease fence; a prior standalone `validate`
result is never authority to perform I/O. The request ID makes a repeated call
idempotent. Herdr is supported only after it exposes an exact-turn acceptance,
status, terminal-result, and reconciliation API. Its current pane-lifecycle
wait is insufficient. Zellij terminal heuristics cannot prove `NO_REPLY`, so
Zellij remains a legacy, uncertain-only adapter until it has a native lifecycle
source. Existing `hermes_session` and `headless_run` sources migrate to explicit
Hermes-dispatch and headless-job adapters rather than disappearing implicitly.

Hermes/Tether is the sole state writer. Native endpoint and model processes may
share an instance UID only when an OS-enforced per-process boundary—such as a
MAC profile or mount namespace—prevents them from opening the Tether database
directory (including WAL and SHM), durable payload store, operator socket, and
authority-registration channel. Otherwise the service writer and endpoint run
under distinct non-root UIDs, with a narrow authenticated driver/control IPC.
The root authority service never proxies agent prompts. Mutually untrusted
personas always use separate endpoint security domains and service instances.

Mode 0600 and `SO_PEERCRED` do not isolate processes that share a UID. Before
loading Slack or authority credentials, the supervised service must also make
itself non-dumpable, prevent ptrace and `/proc` environment or descriptor
inspection, close every unneeded descriptor, and launch native children through
an explicit environment and FD allowlist. Native routing and privileged
readiness are false unless a canary running as the real endpoint process proves
that DB/WAL/SHM open or mutation, payload and operator-socket access, authority
channel access, environment/FD inspection, and ptrace all fail while ordinary
agent control calls and service-owned writes still succeed. A same-UID file
mode or peer check alone never satisfies this gate.

### Root-owned authority service

The private Greppy control plane runs as root, separately from each non-root
instance UID. It does not trust provenance asserted by an agent. When Tether
admits a privileged turn, it registers the authenticated Slack principal,
event/thread identity, binding generation, allowed capability class, expiry,
and nonce over a pre-established authority channel that only the supervised
Hermes/Tether process receives. Systemd passes that connected channel at
service start; Tether marks it close-on-exec and the native child FD allowlist
excludes it. There is no filesystem socket an agent can use to mint a grant.
The service fails closed unless the channel peer, service unit, and configured
instance identity match.

Authorityd stores the registration in its root-owned database and returns an
opaque, random, single-use handle. The agent can present that handle only on a
separate request socket. Authorityd looks up the protected registration and
applies machine policy; it never accepts caller-supplied Slack identity or
thread provenance. Handle scope, operation, target, expiry, and replay nonce
are checked at use time.

The service exposes constrained operations, not an unrestricted root shell:

- read one named value from the machine's Interactive Agents 1Password
  Environment without exposing its service-account token or the whole
  environment;
- mint a short-lived GitHub App installation token for the repository owner
  allowed by policy, without exposing the App private key;
- execute an allowlisted host-management operation with bounded arguments,
  timeout, working directory, output, and audit metadata; and
- report capability readiness without returning credentials.

Model traffic remains on the local `greppy-llm-proxy`. The authority service
cannot return model-provider keys or route around the broker.

Grant records and security audit events live in the root-owned authority store,
not SQLite owned by the instance UID. Every grant is scoped to principal,
workspace, channel, thread, ingress event, operation, target, expiry, and nonce.
The agent receives only an opaque handle and then the minimum result needed for
the one operation. A handle cannot be widened or moved to another thread and is
invalid after use or expiry. A returned bearer secret is inherently copyable;
the design does not claim erasure. Prefer brokered operations, mint short-lived
GitHub App tokens, and reveal a long-lived named 1Password value only when an
explicit policy says that exact principal may receive that exact value. Never
return the whole environment, its service-account token, the GitHub App private
key, or any model-provider credential.

An endpoint is an information-sharing boundary: model context, terminal scrollback,
and tool output can persist across all of its bindings. Authority policy therefore
treats every binding in the endpoint security domain as able to observe a returned
value. Reusable secrets should be consumed by a brokered operation. A revealable
long-lived named value requires an explicit policy for the whole security domain,
not merely the current thread.

## Public Hermes interfaces required

### Ingress middleware v1

Hermes supplies the earliest safe normalized event before lossy channel,
mention, edit/delete, bot, or in-memory deduplication branches. The envelope
contains platform, app/workspace/account, channel/thread, event/message IDs,
create/edit/delete kind, actor ID/type, text, mentions, blocks, bounded content
handles, and timestamps. No raw SDK object crosses the boundary.

Before acknowledging Socket Mode, Hermes records the normalized immutable
envelope, event key, mutation target and revision, and an unresolved disposition
in its ingress ledger. Hermes then retries the exclusive Tether owner locally
after callback failure or process restart; Slack redelivery is not the retry
contract. Tether returns `pass`, `rewrite`, `consume(accepted_id)`, or
`defer(reason, retry_after, deadline)`. A competing exclusive owner fails
startup. Failure or timeout of the configured owner fails closed and remains
durable; it cannot be swallowed as an ordinary plugin exception.

There is no cross-database transaction. Hermes retries the event key from its
own ledger; Tether's accept is idempotent. A `pass` or `rewrite` transfers full
ownership to ordinary Hermes dispatch. Tether does not inspect that later
dispatch lifecycle.

### Platform egress and delivery ledger v2

One canonical interface covers `send`, `update`, `upload`, and supported
private/interaction responses. Requests include platform, workspace, channel,
thread, immutable payload hash, idempotency key, and a safe content handle.
Results are `rejected`, `failed_before_io`, `accepted`, `delivered`,
`retryable`, or `uncertain`, with stable receipts and external IDs. Hermes
provides `get`, `watch`, and `reconcile` by receipt ID or idempotency key. Reuse
of a key with a different destination or payload hash is a hard conflict.
Content handles remain valid through the delivery and rollback horizon.

Hermes extends its delivery ledger to all these operations and fails closed
before network I/O when a durable request cannot be recorded. Tether owns no
second generic Slack outbox or pacing/reconciliation implementation after
cutover.

### Platform read, lifecycle, and plugin service v1

Hermes exposes bounded normalized thread history where needed, transport state
(`connecting`, `connected`, `reconnecting`, `disconnected`, `unhealthy`), and a
public status query. Plugin lifecycle provides start, supervised tasks,
readiness contribution, quiesce/stop with deadline, and configuration reload.
Capabilities are semantically versioned and negotiated; a plugin checks a
supported range and named capabilities, never repository cleanliness.

## Data model and invariants

`Endpoint` stores kind, stable native session reference, pinned working and
process identity, incarnation, adapter capabilities, and one immutable
`security_domain_id`. It is the single source of endpoint truth. The security
domain identifies the instance UID, Slack workspace, persona, authorized-owner
set, and authorization-policy generation whose conversation context may be
shared. Sibling bindings must match it. Sharing one live session across
workspaces, personas, or owner trust domains is rejected; use another endpoint.

`ThreadBinding` stores workspace, channel, thread, endpoint ID, security domain,
generation, owner, and state. Creating a new Slack root from an existing
endpoint inserts a new compatible binding; it never replaces a sibling. This
one-to-many feature intentionally shares conversational memory only inside the
declared security domain.

`QueuedTurn` stores the Hermes event key, binding ID and generation, order,
mutation state, and inline or durable payload reference.

`EndpointLease` is the only open execution lease for an endpoint. The
scheduler chooses the oldest ready turn across sibling bindings by durable
`ordered_at` and event key. A blocked binding may be skipped only by a recorded,
bounded fairness rule; no ready sibling may starve indefinitely. The lease
stores endpoint incarnation, attempt ID, monotonically increasing fence, and
expiry. Expiry never proves a previously accepted execution safe to retry; it
starts driver reconciliation and, if proof remains unavailable, `uncertain`.

`NativeAttempt` binds one attempt ID, endpoint lease, binding generation,
endpoint incarnation, ordered event keys, reply token, driver request ID,
receipt cursor, and execution state.
Every admitted turn must reach exactly one execution terminal:
`completed_with_response`, `no_reply`, `cancelled`, `failed_before_start`,
`failed`, or an operator-resolved terminal. `failed_before_start` means no
driver mutation occurred; `failed` means execution started and a definitive
failed exit was observed. `uncertain` is explicitly nonterminal and visible;
it blocks only the affected endpoint until resolved.

Execution completion and Slack delivery are separate. A response record stores
the immutable payload reference and the Hermes egress receipt ID. The endpoint
lease is released once the execution terminal and response intent are durable;
Hermes delivery may finish later and cannot block the next endpoint turn.
Tether does not duplicate the Hermes egress state machine.

Small text is stored inline. Larger bodies and files are copied before
admission into owner-private content-addressed storage, referenced by digest and
length, and retained through the terminal/rollback horizon. No queued turn
depends on a temporary path owned by another process.

`OperatorResolution` records the evidence and action for an uncertain attempt.
One `BlockingCondition` projection powers readiness, health, and `blocked`.
Every condition has a typed reason, scope, age, `operator_resolvable` flag, and
allowed actions. `resolve` exposes only conditions marked operator-resolvable;
transport, provider, and fleet-owner failures remain visible but cannot pretend
to have an operator action.

The database enforces one active binding per workspace/channel/thread, at most
one open lease per endpoint, and unique driver request and egress idempotency
keys. Rebinding changes exactly one binding generation and never mutates a
sibling. An endpoint incarnation change marks all of its bindings
`rebind_required`; queued turns remain durable, but no new submit occurs until
the endpoint is recaptured and the affected binding is explicitly rebound.

Capability grants and security audit records are intentionally absent from
this model. They belong to the root-owned authority service.

### Local control interface

The owner-only, framed and bounded control protocol has one versioned client
and these idempotent operations:

- `new_root(endpoint_ref, workspace, channel, owner, security_domain,
  idempotency_key, payload_ref)` creates a pending binding intent, requests one
  Hermes root egress receipt, and activates a new sibling binding only when the
  accepted Slack thread identity is durable;
- `attach(endpoint_ref, workspace, channel, thread, owner, security_domain,
  idempotency_key)` binds an existing eligible thread without posting;
- `rebind(binding_id, expected_generation, endpoint_ref)` and
  `close(binding_id, expected_generation)` are generation fenced;
- `status` and `blocked` share the authoritative `BlockingCondition` query;
  `resolve` accepts only an operator-resolvable condition and allowed action;
  and
- driver receipts use the adapter channel, not a model-invoked `reply` command.

A crash between root acceptance and binding activation recovers from the same
Hermes egress receipt ID and pending intent. Reusing an idempotency key with a
changed endpoint, destination, owner, domain, or payload fails closed. There is
no generic post operation that can masquerade as attempt completion. Explicit
progress messages, if retained, carry their own Hermes idempotency key and have
no effect on native execution state.

Agent-callable operations and operator operations use distinct authorization.
An agent client may capture its endpoint and request `new_root`, `attach`,
`status`, or `blocked` within its security domain. `resolve`, forced rebind or
close, and ambiguous-execution decisions require a separate operator socket or
authority-issued admin handle whose peer is OS-distinguishable from the endpoint
process. Until the state-writer boundary above is proven, `resolve` and
privileged readiness remain disabled; an API distinction alone does not stop a
same-UID model from forging database state.

## Primary flows

### Slack to Hermes or native

1. Slack sends an event to the one active Hermes owner for that app/workspace.
2. Hermes durably identifies and normalizes the event, then invokes the
   exclusive ingress router.
3. Tether evaluates workspace, principal, mention, binding, generation, and
   participation policy.
4. `SILENT` is durably consumed; `HERMES` passes to ordinary dispatch;
   `NATIVE` atomically inserts a queued turn and returns its accepted ID.
5. The endpoint scheduler selects the oldest ready sibling binding and creates
   one fenced endpoint lease and attempt.

### Native completion

1. Tether loads the persisted endpoint incarnation and verifies durable payload
   availability.
2. The adapter conditionally submits the request ID against that exact
   incarnation and lease fence, then proves `not_started`, completes
   synchronously, or obtains an authenticated exact-turn `accepted` receipt.
3. Tether resumes the durable receipt stream from its stored cursor and
   reconciles by attempt ID and fence after either side restarts. Model text may
   stream, but is not the completion authority.
4. Normal driver completion produces either an immutable response payload
   reference or a driver `NO_REPLY` receipt.
5. Tether atomically stores the execution terminal and any egress intent,
   releases the endpoint lease, and wakes the next endpoint turn.
6. The immutable response is submitted through Hermes egress for the
   originating binding generation. Tether stores only the Hermes receipt ID;
   platform delivery finishes independently from endpoint execution.
7. Ambiguous execution becomes `uncertain`, emits one actionable control
   notice, and appears in the same health/list/resolve surface.

### Local new root and binding

1. The local client captures the caller's exact endpoint and sends `new_root`
   with workspace, channel, owner, security domain, payload reference, and one
   idempotency key.
2. Tether validates endpoint incarnation and domain compatibility, then stores
   one pending binding intent before asking Hermes egress to create the root.
3. Hermes durably records and sends the root. Its accepted receipt contains the
   exact workspace/channel/thread identity.
4. Tether idempotently activates a new `ThreadBinding`; sibling bindings are
   unchanged. A restart reconciles the pending intent by receipt ID.
5. `attach` skips root egress but requires an explicit eligible thread;
   `rebind` and `close` require the current binding generation and reject active
   or ambiguous work.

### Privileged operation

1. A tightly authorized Slack event is admitted. Tether registers its immutable
   provenance over the pre-established, non-inherited authority channel.
2. Authorityd stores that record and returns an opaque single-use handle, which
   Tether attaches to the exact attempt.
3. The agent presents only that handle plus the requested named operation and
   target through the unprivileged authority client.
4. Authorityd looks up protected provenance and verifies principal,
   event/thread scope, policy, target, expiry, and replay nonce; caller-supplied
   identity metadata is ignored.
5. It executes one brokered operation or returns the policy-approved minimum
   result. Bearer values are treated as copyable until expiry or rotation.
6. The root-owned audit ledger records request hash, decision, operation,
   outcome, duration, and non-sensitive target metadata. No secret or message
   body is logged.

### Fleet convergence

The private reconciler deploys an immutable tuple: stock Hermes release, Tether
plugin ref, machine-neutral config, persona/skill bundle, systemd policy, and
credential/authority provider configuration. Each Slack app/workspace identity
has one active owner enforced by a shared lease and monotonic generation across
Greppys. Hermes verifies the lease before connecting and renews it while
connected; lease loss makes readiness false and disconnects transport. A
two-host overlap and takeover test is a rollout gate. Rollout is canary, small
cohort, then fleet. Fleet health
contains versions, capabilities, transport state, queue age/depth, uncertain
counts, last receipt times, and binding/endpoint counts—never bodies, tokens,
paths, or session references.

## Migration

1. Freeze black-box behavior and production incident reproducers.
2. Prove the schema-17 to schema-18 replacement offline: explicit security
   descriptor, fail-closed quarantine, SQLite-native private backup, logical
   before/after manifests, atomic failure injection, and a lossless schema-17
   projection after synthetic post-migration admissions.
   This L1a proof does not enable schema 18 in the runtime. The durable
   `new_root` intent/Hermes receipt saga, one `BlockingCondition` operator
   surface, service-writer isolation, and backup/cutover orchestration are L1b
   gates; `pending_root` is not enabled before those records and crash tests
   exist.
3. Cut over the first-class endpoint/binding core and driver-owned lifecycle
   behind one explicitly temporary Hermes compatibility adapter. The schema-18
   release is its own rollback target; rollback to schema 17 is allowed only
   through the tested projection after quiescing and resolving every open
   lease. Do not restore an old database or mutate live SQLite by hand.
4. Land and release the public Hermes contracts. Shadow normalized decisions
   and payload hashes before transferring ingress ownership.
5. Move one egress operation at a time to the Hermes ledger: send, update,
   upload, then supported private/interaction responses.
6. Package Tether as one stock plugin and remove copied runtime, Node CLI,
   poller authority, private monkeypatches, direct Slack access, and exact SHA
   gate only after their replacements pass crash tests and a rollback window.
7. Add the root authority plane, exercise it on one named canary under separate
   approval, then cut a successor fleet rollout chain.

## Observability and operating contract

Readiness is false when ingress ownership, endpoint scheduling, Hermes egress,
or configured capability providers cannot honor their contracts. Every log and
metric carries content-free correlation IDs: workspace alias, event key,
binding ID, endpoint ID, attempt ID, generation, state transition, and typed
reason.

Minimum service indicators are admission latency, oldest queued age, terminal
attempt ratio, uncertain attempt count/age, endpoint lease age, delivery
receipt latency, duplicate/misroute count, driver receipt loss, authorization
denials, and capability-provider readiness. A blocked attempt creates one
operator-visible notice and alert; repeated user replies do not masquerade as a
retry mechanism.

## Rollback and deletion gates

Each migration release uses either an additive predecessor-readable schema or
a quiesce/fence/drain rollback: stop admission, reconcile native attempts,
drain egress, restore the predecessor, replay preserved Hermes ingress ledger
entries, and retain their egress idempotency keys. A pre-migration backup alone
is not a rollback because it loses post-cutover admissions. Rehearsal includes
rollback after live synthetic admissions. Binary/schema incompatibility fails
clearly; rollback never guesses. Shadow modes compare only normalized decisions
and payload hashes, not message bodies.

Legacy code is deleted only when all are true:

1. Tether imports or invokes no Hermes API outside the documented public
   capability surface.
2. Tether imports neither Slack SDK nor Slack credentials.
3. No executable code loads from `$XDG_DATA_HOME/tether`; Node is not required.
4. Polling is not authoritative ingress recovery.
5. Hermes owns the only platform ingress and egress ledgers.
6. One endpoint binds N threads while exactly one endpoint lease may be open.
7. A released stock Hermes within the declared capability range passes the
   complete corpus without a Tether source change.
8. Readiness, health, and `blocked` share one `BlockingCondition` projection;
   `resolve` exposes exactly its operator-resolvable subset.
9. Capability grants are root-owned and event/thread scoped, never endpoint
   scoped or agent-writable.
10. Fleet reconciliation changes packages, config, and service policy only; it
    never routes messages or reads message data.
11. Exact-turn driver receipts, endpoint-incarnation conditioning, bounded
    sibling fairness, and root-activation receipt recovery pass restart tests.
12. The same-UID isolation canary prevents environment, FD, and ptrace access;
    failure disables privileged capability readiness.
13. A two-host Slack-owner takeover and a rollback after live admissions
    preserve every admitted event and egress idempotency key.

## Consequences

The target deletes more concepts than it adds: one endpoint record replaces N
snapshots; one endpoint scheduler replaces bridge-local locks plus derived
endpoint arbitration; one Hermes delivery ledger replaces Tether's platform
outbox family; one Python plugin replaces copied runtime plus Node CLI; one
root authority service replaces ambient same-UID secret and host access.

The cost is coordinated upstream Hermes work and an explicit migration. This
is preferable to preserving private adapter patches, model-dependent completion,
and broad ambient machine privilege as permanent operating assumptions.
