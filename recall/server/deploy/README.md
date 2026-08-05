# Tailnet-private pilot deployment

## Public MCP deployment profile

`RenderPublicMcpAdapter` creates one digest-pinned Render `web_service` with managed HTTPS,
`/readyz` health checks, bearer authentication, and `RECALL_HTTP_PROFILE=public-mcp`. By
default the application exposes only `/mcp`, its OAuth protected-resource metadata,
`/healthz`, and `/readyz`; no Tailscale gateway,
REST ingest route, metrics route, or doctor route is part of this profile. The separately
gated `RECALL_ADMIN_WEB_ENABLED=1` setting adds only `/admin` assets, authenticated
`/admin/api/v1` routes, and the one-time OAuth callback described below.

PlanetScale IP restrictions require stable egress. `RenderDedicatedIpAdapter` models Render's
separate dedicated-IP resource and refuses to create it unless `purchase_approved` is explicitly
true. The resource is asynchronous and is not ready for database allowlisting until Render
reports `RUNNING` with exactly three IPv4 addresses. It is workspace-scoped to one region, so
database credentials remain the second independent boundary. As of July 2026, Render requires
a Pro-or-higher workspace and bills one dedicated IP set at $100/month:

- <https://render.com/docs/dedicated-ips>
- <https://api-docs.render.com/reference/create-dedicated-ip>

Keep the prior database egress allowlist in place during cutover. Add all three dedicated
addresses as `/32` entries, prove the hosted service can reach PlanetScale, then remove obsolete
egress entries. Dedicated outbound IPs do not restrict inbound MCP traffic; bearer capabilities
and the MCP-only application surface remain mandatory.

`RECALL_HTTP_PROFILE=public-edge` is the opt-in superset for custom incoming evidence. It adds only
`POST /webhooks/v1/events` to the public-MCP route set. The endpoint requires a separate
webhook-capability bearer bound to one source, principal, and `scrub` or `drop` policy; it never
exposes the generic batch-ingest, credential, migration, metrics, doctor, debug, or administrative
surfaces. Use `public-mcp` when no incoming webhook is required.

Create one webhook-only credential through an administrative process and write the one-time value
directly to a new private file:

```bash
recall-server token-create source-webhook \
  --source webhook:service:instance \
  --principal owner \
  --scopes webhook \
  --webhook-privacy-mode scrub \
  --output /approved/private/webhook-credential.json
```

The sending service loads that value through its secret manager. It sends only the closed
`WebhookEventV1` body from `openapi.yaml`; source, principal, visibility, provenance, and privacy
mode are not request fields. Rotate by creating a replacement credential, updating the sender's
secret reference, proving one synthetic event, and revoking the prior credential.

## Managed Core preview

`recall-core` is the existing API, projection, and retrieval runtime packaged as one
non-root container. The deployment preview is intentionally offline and non-mutating:

```bash
python -m recall_server.cli deployment-preview \
  --manifest server/deploy/recall-core.plan.example.json
```

It emits only a content-free plan hash, resource kinds, and the five approvals still
required. It does not contact a provider, read a source, render a reference, or apply
infrastructure. The example is synthetic; a live manifest belongs in a private mode-0600
location and contains references, never credential values.

The production database gate requires a standard PostgreSQL URL with
`sslmode=verify-full` and an explicit trust root, schema migrations 1 through 46,
pgvector 0.8.0 or newer, and a runtime role without superuser, database/role creation,
replication, or RLS-bypass privilege:

```bash
python -m recall_server.cli capability-check
```

`--profile local-fixture` is a visibly non-production exception restricted to a
loopback PostgreSQL fixture. It never reports production readiness.

The separate approval document is owner-only, bound to the exact preview hash, and
contains only explicit booleans plus the approved billing and region slugs. Validate it
without applying anything:

```bash
python -m recall_server.cli deployment-approval-check \
  --manifest server/deploy/recall-core.plan.example.json \
  --approvals /private/approvals.json
```

Infrastructure reconciliation remains impossible until billing, region, provider
authorization, and the Tailnet route are all approved. Writer cutover is a separate
approval and is never inferred from infrastructure approval. Provider adapters receive
only the closed desired state and return content-free receipts; repeated reconciliation
must converge to `unchanged` without duplicate resources.

The managed pilot profile is deliberately one stack:

```text
Grep/Codex/Claude/Mac collectors
              |
       Tailnet HTTPS :9443
              |
  Render private Tailscale gateway
              |
      Render private network
              |
      Recall Core :8788 -------- HTTPS --------> managed embeddings
              |
 PlanetScale Postgres (Virginia, HA, bounded autoscaling)
```

There is no Render public web service and no Tailscale Funnel. Core requires a
revocable bearer credential even after the Tailnet boundary. Grep agents use the
stable MCP Streamable HTTP endpoint at `https://<tailnet-host>:9443/mcp`; the
same bearer token and source scope apply to both MCP tools and REST reads.
`recall_search`, `recall_related`, and `recall_show` are read-only. Browser
clients must match `RECALL_MCP_ALLOWED_ORIGINS`; server-side agents omit
`Origin`.

The live adapter profile pins PlanetScale `PS_80` with two replicas, 50 GiB
initial storage, a 1 TiB autoscaling ceiling, PostgreSQL 17, and the current
PlanetScale Virginia slug `us-east`. It creates only two digest-pinned Render
private services: Starter Core and a Starter Tailscale gateway with a 1 GiB
identity disk. The manifest selects a managed `voyage` or OpenAI-compatible
embedding endpoint over exact-match HTTPS; no dedicated embedding service is
created. Existing service environment variables, secret files, image digests,
commands, plans, disks, and regions must match exactly or reconciliation fails
without mutation.

Hosted embeddings receive the redacted text projection selected for semantic
indexing. The example uses `voyage-4` at 512 dimensions; operators who cannot
send that projection to a provider should use the self-hosted TEI profile in
the existing-host section instead. The managed profile uses the validated
2,000ms database-work ceiling because a remote database round trip cannot meet
the 300ms loopback default reliably at multi-million-item scale.

Inject credentials from the approved 1Password Environment at runtime. Never
put their values in arguments, a manifest, an approval file, shell history, or
the repository:

```text
PLANETSCALE_SERVICE_TOKEN_ID
PLANETSCALE_SERVICE_TOKEN
RENDER_API_KEY
RECALL_DATABASE_URL
RECALL_EMBEDDING_API_KEY
RECALL_ARCHIVE_ACCESS_KEY_ID
RECALL_ARCHIVE_SECRET_ACCESS_KEY
RECALL_ARCHIVE_NAMESPACE_KEY
TAILSCALE_OAUTH_CLIENT_ID
TAILSCALE_OAUTH_CLIENT_SECRET
```

For a managed Cloudflare R2 raw archive, also set the non-secret configuration:

```text
RECALL_ARCHIVE_BACKEND=r2
RECALL_ARCHIVE_BUCKET=recall-raw-owner
RECALL_ARCHIVE_ENDPOINT_URL=https://ACCOUNT_ID.r2.cloudflarestorage.com
RECALL_ARCHIVE_REGION=auto
```

The R2 credential must have Object Read & Write permission for that bucket only.
`RECALL_ARCHIVE_NAMESPACE_KEY` is a separate base64-encoded 32-byte random key;
do not derive it from either R2 credential. Recall uses immutable opaque object
keys and conditional writes because R2 does not expose S3 object version IDs.
R2 rejects S3 SSE headers and applies provider-managed encryption automatically.
Recall does not read a Cloudflare management API token. Validate the configured
archive with `python -m recall_server.cli archive-check`; the probe writes,
replays, reads, deletes, and verifies absence using synthetic bytes, then emits
only a content-free status.

Operational raw-object and bulk-ingest manifests intentionally contain digests,
counts, cursors, and status only. They prove replay and integrity without
copying transcripts, secrets, or PII into logs and deployment evidence. Full
deep-search content lives in a different projection: the canonical
privacy-processed chunk text plus exact Recall receipts, stored in a separate
private evidence bucket. Never mount the raw archive into an external compute
provider.

Deep inspection is hard-bound to exactly one brain tenant per service instance,
evidence bucket credential, and Archil disk:

```text
RECALL_EVIDENCE_ENABLED=1
RECALL_EVIDENCE_TENANT_ID=tenant:company:example
RECALL_EVIDENCE_ARCHIVE_BACKEND=r2
RECALL_EVIDENCE_ARCHIVE_BUCKET=recall-evidence-company-example
RECALL_EVIDENCE_ARCHIVE_ENDPOINT_URL=https://ACCOUNT_ID.r2.cloudflarestorage.com
RECALL_EVIDENCE_ARCHIVE_REGION=auto
RECALL_EVIDENCE_ARCHIVE_ACCESS_KEY_ID=<injected secret>
RECALL_EVIDENCE_ARCHIVE_SECRET_ACCESS_KEY=<injected secret>
RECALL_EVIDENCE_ARCHIVE_NAMESPACE_KEY=<independent base64 32-byte secret>
RECALL_DEEP_INSPECTOR=archil
ARCHIL_API_KEY=<injected secret>
RECALL_ARCHIL_DISK_ID=dsk_replace_me
RECALL_ARCHIL_REGION=aws-us-west-2
```

Give the evidence R2 credential Object Read & Write access to that evidence
bucket only, then attach only that bucket to the tenant's read-only Archil disk.
Do not reuse a bucket or disk across personal and company brains. Validate and
populate it before enabling the MCP tool:

AWS S3 is also supported. Use `RECALL_EVIDENCE_ARCHIVE_BACKEND=s3` for a
versioned bucket or `s3-unversioned` for a bucket without versioning, an exact
regional endpoint such as `https://s3.us-west-2.amazonaws.com`, and the same
bucket-scoped access-key variables. The unversioned profile preserves
immutability with content-addressed keys and conditional writes. Archil
serverless execution requires an Archil disk in a supported AWS region; that
disk may mount the same S3 bucket.

```bash
python -m recall_server.cli evidence-archive-check
python -m recall_server.cli backfill-canonical-evidence \
  --tenant tenant:company:example --batch-size 100 --max-batches 10
python -m recall_server.cli canonical-evidence-worker \
  --tenant tenant:company:example
```

The source-native evidence projection stores one logical session, thread, or
document revision rather than one object per retrieval chunk. Each JSONL record
contains the complete privacy-processed source content, structural role and
time metadata, and every canonical receipt for that source record. Documents
larger than one object are represented by ordered immutable parts behind one
opaque manifest. Oversized collector records are restored from the separately
credentialed raw archive and verified against their declared size and digest
before projection.

Migration 39 adds the logical-document catalog, dirty-session queue, and
durable object-cleanup queue. Migration 40 materializes source-record order so
large-session projection does not repeatedly parse archived JSON in PostgreSQL.
Populate it without removing the prior projection:

```bash
python -m recall_server.cli backfill-logical-evidence \
  --tenant tenant:company:example --source source:google.gmail:example \
  --batch-size 100 --max-batches 100 \
  --upload-concurrency 2
python -m recall_server.cli logical-evidence-worker \
  --tenant tenant:company:example --upload-concurrency 2
```

After a retention-policy or projection-format change, intentionally replace
every current logical document through the same tenant-scoped path:

```bash
python -m recall_server.cli backfill-logical-evidence \
  --tenant tenant:company:example --rebuild-existing \
  --batch-size 2000 --max-batches 100 --upload-concurrency 16
```

Ingest and forget transactions enqueue only affected logical documents.
Revision replacement queues superseded S3 references in the same database
transaction; transient delete failures remain durable and are retried by the
worker. The operational default of 25 documents over two upload streams keeps
memory and database load boring. One-time backfills may use up to 2,000
documents per batch and raise `--upload-concurrency` as high as 32 only when
`RECALL_DATABASE_POOL_MAX_SIZE` is at least the same value and a live
throughput/memory measurement justifies it. The worker pre-warms that bounded
pool before starting the measured projection. Keep the prior
`canonical-evidence-worker` running until the
logical-document integrity, rollback, and consumer cutover gates pass.

Migration 41 adds the disposable, document-linked lossless passage index. It
packs exact visible user and assistant message bytes into overlapping passages
without crossing logical documents. Tool text remains available through the
canonical sparse index; no completion model summarizes, classifies, or segments
ingestion.

```bash
python -m recall_server.cli backfill-lossless-passages \
  --tenant tenant:company:example --target-tokens 1024 \
  --overlap-tokens 128 --batch-size 100 --max-batches 100 \
  --concurrency 8
python -m recall_server.cli lossless-passage-worker \
  --tenant tenant:company:example --target-tokens 1024 \
  --overlap-tokens 128
```

The passage policy is versioned by fingerprint. Evaluate alternative target and
overlap values on a shadow database; production keeps one policy and deletes
losing variants after cutover.

Migration 42 adds fingerprinted shadow passage representations for retrieval
experiments. Each arm remains tenant- and source-scoped, points back to the
same lossless canonical passage, and can be removed by fingerprint. Contextual
text and provider vectors are derived projections; they do not replace or
truncate canonical evidence. Provider inputs use a fingerprinted 7,000-byte
head/tail excerpt so one unusual code token cannot exceed a managed model's
context window; a retrieval hit still resolves to the complete canonical
passage and S3 document. Do not route production retrieval to a shadow arm
until its private optimize and validation gates pass.
Large resumable backfills may be split into deterministic, non-overlapping
passage-ID shards without changing representation fingerprints.

Migration 45 adds actor attribution independently of authentication principals.
It links stable actors and explicit relations to sources, events, logical
documents, and passages. Existing raw artifacts do not change: after binding an
employee's enrolled sources, run the existing logical-document rebuild, passage
projection, and actor-aware contextual-representation backfill. Person filters
remain inside the tenant and authorized-source boundary. See
[`../../docs/actor-attribution.md`](../../docs/actor-attribution.md) for relation
semantics, the retrofit sequence, and the cutover gate.

Migration 46 adds the employee display name to invitations. Acceptance now
creates and links the employee actor, and source-local route enrollment binds
that actor as the source contributor. Apply it before inviting employees; mixed
schema/application deployment is intentionally unsupported.

`recall_deep_search` first uses canonical retrieval to select authorized
candidates, passes only opaque evidence keys and exact allowed receipts to a
fixed server-owned inspection program, mounts the Archil disk read-only, and
revalidates every returned receipt. The question is encoded as data and can
never become a shell command or object path. Interactive executions are bounded
by file, match, output-byte, and time limits. Use
`RECALL_DEEP_INSPECTOR=local` for the same contract without third-party compute.

Enable the canonical v2 write plane only after the archive probe and database
migrations pass:

```text
RECALL_CANONICAL_V2_ENABLED=1
RECALL_CANONICAL_INGEST_PUBLIC=1
RECALL_TENANT_ID=tenant:personal
RECALL_PRINCIPAL_ID=principal:owner
```

`RECALL_CANONICAL_INGEST_PUBLIC=1` adds only the authenticated
`POST /v2/archive/objects` and `POST /v2/ingest/canonical` routes to a public
profile. Both require a write credential bound to the exact tenant, principal,
and source. Create one such credential per source with `token-create --tenant
TENANT --principal PRINCIPAL --source SOURCE --scopes write`. The connector
runner archives raw bytes through the fenced archive route, applies privacy,
writes only the redacted envelope to its private spool, and advances its cursor
only after the canonical ACK. Do not enable this flag on an MCP-only service
that has no collector ingress.

After canonical ingestion is live, provision each personal or company brain and
mint a separate audience-bound MCP credential:

```bash
python -m recall_server.cli brain-provision \
  --organization org:owner --kind personal --display-name "Personal" \
  --tenant tenant:personal --slug personal --owner-principal principal:owner
python -m recall_server.cli mcp-token-create owner-personal \
  --tenant tenant:personal --principal principal:owner \
  --principal-kind workload \
  --scopes read,forget \
  --output /approved/private/owner-personal-mcp.json
```

Set `RECALL_CANONICAL_MCP_ENABLED=1` only after migration 28 and canonical
embedding backfill pass. In this mode, public MCP accepts only unexpired
`recall-mcp` credentials bound to one tenant. Retrieval intersects brain access
with explicit canonical source grants and never reads the legacy evidence
projection. A single principal can hold separate personal and company
credentials without gaining an implicit cross-brain view. Omit the optional
`forget` scope for read-only agents; canonical forget also requires an owner
grant on the exact source.

The outcome-oriented answer façade is disabled by default. Enabling the direct
Pi runner exposes `use_recall` through MCP and
`POST /v1/agent/brains/{tenant_id}/use-recall`. The request cannot carry tenant,
principal, role, source grants, credentials, budgets, or trace policy.

Schema 38 adds durable agent runs. `POST /v1/agent/brains/{tenant}/runs` starts
detached work; the corresponding run, result, and cancel routes remain bound to
the authenticated tenant, principal, and current source grants. MCP exposes the
same compatibility lifecycle as `recall_agent_start`, `recall_agent_status`,
`recall_agent_result`, and `recall_agent_cancel`. Under MCP `2026-06-30`, Recall
also advertises `io.modelcontextprotocol/tasks` and returns a native task only
when the individual `tools/call` opts into that extension. Older or
non-negotiating clients keep the synchronous `use_recall` result.

For semantic synthesis, enable the direct open-source Pi worker included in the
image and configure one explicit OpenAI-compatible endpoint:

```text
RECALL_AGENT_RUNNER=pi
RECALL_AGENT_MODEL_BASE_URL=https://api.cerebras.ai/v1
RECALL_AGENT_MODEL_ALIAS=gpt-oss-120b
RECALL_AGENT_MODEL_KEY_FILE=/etc/secrets/cerebras-api-key
```

The Render secret file contains only the Cerebras API key. It must be a
nonsymlinked regular file owned by the service user or root, with no write or
execute permission for its group and no permissions for other users. Recall
reloads it immediately before each child process. The child receives only that
key, the explicit endpoint, and a minimal process environment. The worker and
Pi dependencies are built from the checked-in lockfile; no opaque runtime
artifact is vendored.

On a Greppy host, use the dedicated credential-owning local broker instead.
Recall passes the literal `not-a-secret` placeholder to Pi; no model bearer
credential enters Recall or the child:

```text
RECALL_AGENT_MODEL_BASE_URL=http://<private-greppy-llm-proxy-host>:<port>
RECALL_AGENT_MODEL_ALIAS=gemma-4-31b
```

This mode fails closed unless the exact approved URL is loopback, link-local,
RFC1918, carrier-grade NAT, or the private Docker host gateway. Do not expose
the broker publicly. A deployment outside that private network supplies an
HTTPS endpoint and a private model-key file.

Recall and Archil credentials remain in the host.

The child can call only authorized read-only evidence tools. Semantic search is
a hint; only receipts returned by deep inspection, exact show, or session
context are citable. The model must finish through `finish`, and Recall
rejects citations that were not opened in the same turn. Raw reasoning,
questions, answers, tool arguments, source bodies, and credentials are excluded
from durable traces.

Lifecycle rows contain authority identifiers, a request hash, opaque handles,
state, bounded redacted trace events, and terminal results. They never contain
the original question, credentials, or source bodies. Expired worker leases become
`worker_lost_retryable` terminal failures and are never silently rerun. Tune
only within the validated bounds using `RECALL_AGENT_WORKERS`,
`RECALL_AGENT_MAX_ACTIVE_PER_PRINCIPAL`, `RECALL_AGENT_LEASE_SECONDS`, and
`RECALL_AGENT_RETENTION_SECONDS`.

### Human OAuth and company-brain invitations

Static MCP tokens remain the simplest machine-to-machine option. For people,
configure one OAuth resource instead of issuing long-lived bearer tokens:

```text
RECALL_MCP_RESOURCE_URI=https://<public-host>/mcp
RECALL_AUTHORIZATION_SERVERS=https://<authorization-server>
RECALL_MCP_AUTH_PROVIDER=oidc
RECALL_OIDC_ISSUER=https://<authorization-server>
RECALL_OIDC_JWKS_URI=https://<authorization-server>/.well-known/jwks.json
```

Set the provider to `descope` for Descope and use the issuer and JWKS URLs shown
for its MCP resource. Recall needs no Descope management key: the provider owns
login, MFA, and account recovery; Recall owns invitations, tenant mapping,
roles, source grants, and revocation. The access token must have the exact
`RECALL_MCP_RESOURCE_URI` audience, a `read` scope, and a provider-verified email
for first-time invitation acceptance.

To turn Descope's standard MCP consent flow into the branded Recall experience,
name the flow `recall-mcp-user-consent` and run the setup-only operator script:

```bash
RECALL_DESCOPE_PROJECT_ID=P_replace_me \
RECALL_DESCOPE_MGMT_KEY='<setup-only management key>' \
node server/scripts/configure_descope_brand.js
```

The script discovers the welcome, OTP, verified-consent, and unverified-consent
screens by their component contracts rather than project-specific screen IDs. It
preserves every flow interaction, applies the Recall palette and copy, and updates
inbound apps already assigned to that flow to use the managed dark host. It uses
only Node's built-in APIs and is safe to rerun. The management key is not an
application runtime dependency; remove it from the operator environment after
setup unless invitation email delivery below also uses Descope.

Recall follows the company-wide Grep visual contract: JetBrains Mono, square
corners, and literal product copy. The operator script pins both Descope font
slots to JetBrains Mono and every global and component border-radius token to
`0px`. Do not add slogans, trust strips, or rounded surfaces.

For the OAuth-first browser experience, create one confidential Descope Inbound
App with the callback below and the `openid email recall.identity` scopes. The
app-local `recall.identity` permission has no role mapping and authorizes only
identity confirmation; Recall remains the authorization system for brains:

```text
RECALL_IDENTITY_OAUTH_CLIENT_ID=<confidential Inbound App client ID>
RECALL_IDENTITY_OAUTH_CLIENT_SECRET=<server-side Inbound App secret>
RECALL_IDENTITY_OAUTH_REDIRECT_URI=https://<public-host>/admin/oauth/callback/identity
RECALL_BOOTSTRAP_OWNER_EMAILS=owner@example.com
```

PKCE and one-use, ten-minute state are enforced even for the confidential app.
Recall calls UserInfo, requires a verified email, discards the provider tokens,
and creates only a 12-hour Recall browser session. On a fresh installation the
allow-listed email may bind the single unclaimed owner principal once. Afterwards
the durable Descope subject—not email—is authoritative. If multiple owner
principals exist or another identity is already bound, claiming fails closed.

In `/admin`, create a company invitation. By default Recall displays the
brain-specific MCP URL for manual sharing. To send an onboarding email with a
brain-specific setup page, enable one server-side delivery provider:

```text
# Use the same isolated Descope project that authenticates Recall users.
RECALL_INVITATION_EMAIL_PROVIDER=descope
RECALL_DESCOPE_PROJECT_ID=P_replace_me
RECALL_DESCOPE_MGMT_KEY=<server-side management key>

# Or use a provider-neutral transactional email account.
RECALL_INVITATION_EMAIL_PROVIDER=resend
RECALL_INVITATION_EMAIL_FROM=Recall <recall@example.com>
RECALL_INVITATION_EMAIL_API_KEY=<server-side API key>
```

Configuration is explicit and all-or-none. Recall never renders or logs either
secret. A delivery failure leaves the email-bound authorization pending and is
shown in the admin UI; re-inviting the same address safely replaces the pending
invitation and retries delivery. The setup page presents the current Codex and
Claude Code setup blocks immediately. The client's Descope login activates the
exact email-bound invitation on its first MCP request, so the primary path has
only one login. Browser identity also offers an optional pre-activation path. See
[`docs/authorization-v1.md`](../../docs/authorization-v1.md) for the policy,
generic OIDC contract, and revocation semantics.

If the authorization server does not support dynamic client registration,
pre-register a public PKCE client for Codex and configure both values below.
The callback URL must match Codex's localhost callback for the brain-specific
MCP URL. Recall then renders a single `codex mcp add` command with the public
client ID, resource indicator, and fixed callback port; no client secret or
bearer token is distributed.

```text
RECALL_CODEX_OAUTH_CLIENT_ID=<public OAuth client ID>
RECALL_CODEX_OAUTH_CALLBACK_PORT=8765
```

## Unified connector administration

Schemas 029–031 add one tenant-aware connector control plane for the web
switchboard and native utilities. A connector installation is always bound to one principal,
one destination brain, and one opaque source ID. Connecting the same provider to
personal and company memory creates separate installations; it never implies a
cross-brain grant.

Enable the browser and native control surface only after injecting the required
owner boundary through the runtime secret manager:

```text
RECALL_ADMIN_WEB_ENABLED=1
RECALL_CONTROL_ENCRYPTION_KEY=<base64url-encoded random 32 bytes>
```

Google Workspace is optional. It becomes available only when all three values
below are configured; a partial set fails startup closed:

```text
RECALL_GOOGLE_CLIENT_ID=<Google web application client ID>
RECALL_GOOGLE_CLIENT_SECRET=<Google web application client secret>
RECALL_GOOGLE_REDIRECT_URI=https://<public-host>/admin/oauth/callback/google
```

Composio is the hosted connection option. It is also optional and can coexist
with direct Google OAuth; its API key and callback are an all-or-none pair:

```text
RECALL_COMPOSIO_API_KEY=<scoped server-side project key>
RECALL_COMPOSIO_REDIRECT_URI=https://<public-host>/admin/oauth/callback/composio
```

Use a stable Recall principal ID as the Composio user ID. The switchboard opens
one Google toolkit per hosted authorization trip and binds the returned exact
`ca_...` account to that principal, connector, and brain route. Recall re-reads
the account, verifies active status/owner/toolkit/scopes, and executes a minimal
read capability probe before enabling ingestion. Only the opaque user, toolkit,
and connected-account references are encrypted in Recall; provider tokens stay
in Composio. Set the optional `RECALL_COMPOSIO_AUTH_CONFIG_*` variables from
`service.env.example` for production-owned OAuth branding and scopes. Omit them
only for Composio-managed development auth.

The encryption key is independent of database, archive, Google, and MCP
credentials. Keep it stable for the lifetime of encrypted provider connections;
rotate it with an explicit decrypt/re-encrypt migration, never by silently
replacing the variable. Google must register the redirect URI exactly. Recall
requests offline access, incremental authorization, PKCE, one-time server-side
state, `openid`, and only the read-only scopes for the source toggles selected by
the owner. Workspace administrators may still need to trust the OAuth client,
and public distributions must complete Google's applicable sensitive-scope
verification.

Mint an audience-specific bootstrap key into a new owner-private file:

```bash
python -m recall_server.cli admin-token-create owner-web \
  --principal principal:owner --expires-in-days 30 \
  --output /approved/private/recall-admin.json
```

Open `/admin`, paste the one-time key into the access dialog, then choose a brain
for each Google service before authorization. The browser exchanges the key for
a twelve-hour Secure, HttpOnly, SameSite session and a CSRF-bound companion
cookie. OAuth refresh and access tokens are encrypted with AES-256-GCM in the
database, never returned by the state API, and cryptographically wiped after
provider disconnection. Pause preserves the installation checkpoint; revoking
one installation disables only that routed source, while disconnecting Google
revokes provider authority for every dependent route. Uninstall removes the
route from the active map.

If the provider reports an expired or forbidden connected account, the managed
worker marks the provider `degraded`, returns each enabled dependent route to
`configured`, and stops retrying. `/admin` then reports that Google must be
authorized again. A successful authorization binds the replacement connection,
re-enables the selected route, and resumes its retained ACK checkpoint. Generic
transport failures remain bounded retries and never masquerade as a reconnect.

Native clients use the same versioned `/admin/api/v1` session, state, OAuth, and
lifecycle contract. They must store the bootstrap or browser-session authority
in the operating-system credential store and must not copy provider tokens out
of Recall.

Run one or more managed workers from the same immutable Recall Core image:

```bash
python -m recall_server.cli managed-worker \
  --state-root /var/lib/recall --interval-seconds 60
```

The worker claims only due, enabled `remote_worker` installations with a
database lease. It decrypts one provider capability in memory, materializes any
short-lived CLI authority beneath an owner-private worker directory, archives
raw records before projection, commits through the tenant-scoped canonical
plane, advances the connector cursor only after acknowledgement, and updates
the same installation row shown in the web UI. Pause, revoke, tenant selection,
and provider disconnect therefore affect execution without a second config
surface. Mount `/var/lib/recall` on persistent encrypted storage so ACK-gated
spools survive image restarts. Every worker replica must receive the same
database, R2, embedding, and control-encryption settings as the API service; it
does not listen on a network port.

The same process can own the existing full-document and lossless-passage queues
without adding another service. Set `RECALL_MANAGED_PROJECTIONS_ENABLED=1` and
give the worker the evidence-archive settings already used by deep inspection.
Each cycle drains at most 20 logical documents and 20 passage documents across
four streams, then embeds at most 100 passages. Keep the worker database pool at
eight or higher so those fixed bounds retain headroom. Leave the flag unset when
an operator runs dedicated logical-evidence and passage workers instead; never
run both owners for steady-state scheduling.

Treat a one-time high-concurrency backfill as maintenance, not free background
capacity. Do not run it beside managed projections unless the database session
budget leaves both services headroom and health/readiness stay green for a
complete canary batch. Stop on the first readiness failure; the durable queues
make resumption safe. On a 50-session database with an eight-connection API pool
and eight-connection managed-worker pool, prefer the managed worker itself over
a sustained external drain.

Run canonical embeddings as a separate worker from that same immutable image:

```bash
python -m recall_server.cli canonical-embedding-worker \
  --tenant tenant:company:example \
  --batch-size 2000 --max-batches-per-cycle 10 --interval-seconds 5
```

Canonical ingest commits do not call an embedding provider. The worker drains
unembedded current chunks in bounded, idempotent cycles, so provider latency or
an outage cannot delay or fail source ingestion. Restarting the worker is safe:
the durable canonical chunk table is the queue, the unique embedding key is the
acknowledgement, and a runtime-scoped keyset watermark avoids rescanning the
already-embedded prefix. The watermark wraps at the end so late-arriving chunks
that sort before it are still repaired. Give this worker the same database and
embedding settings as the API service, but no collector or MCP credentials.

Managed providers can parallelize document batches with
`RECALL_EMBEDDING_WORKERS=2` through `8`; the default is `1`, and the local TEI
sidecar remains deliberately single-request. Results are reassembled in input
order before their idempotent database write. Keep query embedding on the
ordinary bounded path and raise worker concurrency only within the provider's
document rate limits.

`RECALL_DATABASE_URL` must be a PlanetScale application role URL with
`sslmode=verify-full` and an explicit trust root. Prefer
`sslrootcert=/etc/ssl/certs/ca-certificates.crt` in the pinned Linux container;
`sslrootcert=system` is also accepted where the runtime's libpq/OpenSSL build
resolves the OS trust store correctly. Bootstrap and migrate the database with
a separate administrative credential, then retain only this least-privilege
runtime URL in the injected environment. The provider token needs only database
read/create permissions; the Tailscale OAuth client must be restricted to the
dedicated gateway tag.

When migration and runtime use separate PostgreSQL roles, refresh runtime grants
after every migration and before deleting a short-lived migration role. Replace
`recall_runtime` with the actual runtime role identifier:

```sql
GRANT USAGE ON SCHEMA public TO recall_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE
  ON ALL TABLES IN SCHEMA public TO recall_runtime;
GRANT USAGE, SELECT
  ON ALL SEQUENCES IN SCHEMA public TO recall_runtime;
REVOKE ALL PRIVILEGES
  ON TABLE public.schema_migrations FROM recall_runtime;
GRANT SELECT
  ON TABLE public.schema_migrations TO recall_runtime;
```

The final two statements are mandatory after the broad table refresh: the runtime
capability gate requires migration history to remain read-only. Apply only the grants
the enabled runtime operations need. Reassign objects to the durable owner before
deleting a temporary migration role.

After reviewing the zero-network preview and mode-0600 approval document, run
the exact approved apply under 1Password injection:

```bash
OP_CACHE=false op run --environment "$APPROVED_ENV_ID" -- \
  python -m recall_server.cli deployment-apply \
  --manifest /private/recall-core.plan.json \
  --approvals /private/approvals.json \
  --planetscale-organization ORGANIZATION \
  --database-name DATABASE \
  --render-owner-id WORKSPACE_ID \
  --core-name RECALL_CORE \
  --gateway-name RECALL_GATEWAY \
  --tailnet-hostname RECALL \
  --tailnet-tag tag:recall
```

The command checks infrastructure approvals before reading any credential. Its
stdout is content-free: actions, plan hash, and non-reversible resource
receipts only. Writer cutover remains a separate approval.

## Existing host pilot

The existing host pilot listens on a Unix socket, not TCP. Tailscale Serve is its only network proxy.
On Linux, the server verifies `SO_PEERCRED` and trusts Tailscale identity headers only when the
Unix-socket peer UID is explicitly allowlisted. Ubuntu's sandboxed `tailscaled` uses UID 65534;
root is UID 0. Neither identity is assumable by the interactive user, so a same-user process that
connects directly and forges `Tailscale-User-Login` is rejected. Narrow
`RECALL_TRUSTED_PROXY_UIDS` to the UIDs observed for the local `tailscaled` service.

Install from an immutable, reviewed checkout at `~/services/recall-brain`. The service unit never
points at a contributor's active checkout.

```bash
git worktree add --detach ~/services/recall-brain <reviewed-merged-sha>
python3 -m venv ~/.config/recall-brain/venv
~/.config/recall-brain/venv/bin/pip install -r ~/services/recall-brain/recall/server/requirements.txt
docker pull ghcr.io/huggingface/text-embeddings-inference@sha256:ad950d30878eceb72aaf32024d26fa2b1d04a75304fa0b4776b49aa1941fea07
install -m 0600 ~/services/recall-brain/recall/server/deploy/service.env.example ~/.config/recall-brain/service.env
install -m 0644 ~/services/recall-brain/recall/server/deploy/recall-brain.service ~/.config/systemd/user/
install -m 0644 ~/services/recall-brain/recall/server/deploy/recall-brain-backup.service ~/.config/systemd/user/
install -m 0644 ~/services/recall-brain/recall/server/deploy/recall-brain-backup.timer ~/.config/systemd/user/
install -m 0644 ~/services/recall-brain/recall/server/deploy/recall-embedding.service ~/.config/systemd/user/
install -m 0644 ~/services/recall-brain/recall/server/deploy/recall-embedding-backfill-sidecar.service ~/.config/systemd/user/
install -m 0644 ~/services/recall-brain/recall/server/deploy/recall-embedding-backfill.service ~/.config/systemd/user/
install -m 0644 ~/services/recall-brain/recall/server/deploy/recall-embedding-backfill.timer ~/.config/systemd/user/
# Fill in service.env, then apply every schema before starting services or timers.
set -a; source ~/.config/recall-brain/service.env; set +a
cd ~/services/recall-brain/recall/server
~/.config/recall-brain/venv/bin/python -m recall_server.cli migrate
systemctl --user daemon-reload
systemctl --user enable --now recall-embedding
systemctl --user enable --now recall-brain
systemctl --user enable --now recall-brain-backup.timer
systemctl --user enable --now recall-embedding-backfill.timer
tailscale serve --bg --https=9443 unix:/run/user/$(id -u)/recall-brain.sock
```

Do not use Funnel. Preserve unrelated Serve listeners by configuring only the dedicated 9443
listener. Collectors receive revocable tokens from `recall_server.cli token-create`; plaintext
is emitted once and only its SHA-256 is stored. Use `--output /secure/mode-0600-file.json` so the
plaintext never enters terminal or session logs; the command refuses to overwrite an existing file.
A read token may use `--principal OWNER` to read exactly the sources granted to that principal.
Any token with `write` scope must also use `--source SOURCE_ID`; write authority is never
principal-wide. Add `--capture-origin ORIGIN` to a principal-aware read/write token to expose
`recall_capture` and `recall_forget` over MCP. The host binds that origin and source; tool
arguments cannot override either. Hosted capture structurally scrubs title and body before the
canonical event is stored.

The pilot timer starts its first logical backup after 15 minutes and schedules another six hours
after the previous run finishes. This deliberately prevents overlapping full dumps. The interval
is a conservative example, not an RPO guarantee: measure a complete backup on the real corpus and
set the schedule from its duration, available disk, and provider-native recovery guarantees.
Before multi-user scale or C10 production cutover, use provider point-in-time recovery or
continuous WAL archival plus daily base backups; the same blank-database restore/fingerprint
contract remains the gate.

Searches have a 300ms database-work budget by default. Override it only within the validated
10–5000ms range with `RECALL_SEARCH_DEADLINE_MS`; the response and service log expose only
content-free per-leg timings, result counts, and the deadline outcome.

Semantic retrieval requires PostgreSQL with pgvector and one explicitly selected embedding
profile. Cosine score distributions vary by model, so
`RECALL_SEMANTIC_MINIMUM_SIMILARITY` is an explicit validated deployment setting in the
0–1 range and defaults to `0.35`. Calibrate it with a private retrieval eval when changing
models; the bounded top-K candidate pool prevents a lower floor from creating an unbounded
scan. Recall supports three protocols:

- `voyage` is the recommended hosted profile. `voyage-4` supports 512-dimensional output and
  distinct `document`/`query` retrieval modes.
- `openai` calls the standard `/v1/embeddings` contract and works with OpenAI-compatible hosted or
  local services. Optional document/query prefixes support asymmetric open models such as Nomic.
- `tei` preserves the pinned local Qwen profile below for operators who prioritize private
  inference and accept its compute footprint.

Leaving `RECALL_EMBEDDING_URL` unset is a supported zero-dependency lexical-only profile. This is
degraded retrieval, not failed startup. Hosted profiles send projected memory text to the selected
provider; operators must make that privacy choice deliberately.

Every non-loopback endpoint must use HTTPS, exactly match
`RECALL_EMBEDDING_APPROVED_URL`, and read its bearer from exactly one protected source:
an owner-only, non-symlink `RECALL_EMBEDDING_KEY_FILE`, or the deployment secret variable named
by `RECALL_EMBEDDING_KEY_ENV`. The latter is the normal container pattern; the variable's value
must be injected by the secret manager and never written in the config file. Recall rejects
redirects, rereads the selected key source, validates response
indices, dimensions, and finite values, and fingerprints the protocol, model version, dimensions,
and query/document transformation. A profile change therefore makes old vectors stale until the
online backfill converges.

The optional packaged self-hosted unit pins TEI 1.9 and Qwen3-Embedding-0.6B, binds only
`127.0.0.1:8089`, and never exposes an embedding route through Tailscale Serve. Keep
`RECALL_EMBEDDING_BATCH_SIZE=1`: the derivation
fingerprint and backfill deliberately trade background throughput for reproducible Qwen document
vectors. The sidecar admits only one single-input request at a time because this Qwen/TEI CPU path is
not reproducible across batched requests. An overlapping request is rejected and retrieval safely
falls back to exact and lexical legs; a backfill retry converges later. Oversized
documents use a fingerprinted 4,096-character
head-and-tail projection so a giant tool result cannot stall a complete backfill batch. The runtime
verifies the exact model commit and float32 dtype against TEI `/info` before sending text. Search
ignores stale fingerprints, dimensions, projector versions, and content hashes.

Schema 021 adds a second, rebuildable semantic projection for conversational sources. It embeds a
user request together with every assistant message before the next user turn, preserving the
request at the head and the final response at the tail. Search still returns the final canonical
assistant item and its normal `recall://` receipt; the combined turn is only a retrieval vector.
Every contributing item is linked so a soft deletion excludes the vector immediately, and search
also verifies that the cited response is still the current final response for the turn. This
closes the common failure where a short answer is meaningless without its preceding question
without creating uncited synthetic memory.

Production backfill uses an identical second sidecar on `127.0.0.1:8090`. Keeping historical CPU
inference separate prevents convergence work from rejecting live query embeddings. Neither port is
served through Tailscale, and both runtimes enforce the same pinned single-input contract. The worker
sets its endpoint at `ExecStart`, after `EnvironmentFile` loading, so the live-query URL cannot override
the dedicated backfill route through systemd environment precedence. Its 120-second transport timeout
allows long CPU inference to finish under contention without relaxing the live-query timeout.

Query planning is separate from embeddings and optional. When enabled, it must use the staging
LiteLLM HTTPS router plus a
short-lived model-scoped virtual key in a non-symlink owner-only file. A separate secret-manager
timer must atomically replace that file before expiry. Never place a LiteLLM master key in
`service.env`, pass it to Recall, or call a model provider directly. Recall rereads the key on every
uncached plan and keeps only bounded hash-keyed in-memory caches; it does not persist query text or
planner output. Set `RECALL_LITELLM_APPROVED_URL` to the exact same approved staging-router base URL;
startup fails if the planner points anywhere else.

After schema 011, converge the derived embedding projection online. The timer holds a dedicated
advisory lock, processes bounded batches, and is safe to replay. It never rewrites canonical events,
items, or receipts:

```bash
RECALL_DATABASE_URL=... RECALL_EMBEDDING_URL=http://127.0.0.1:8089 \
  python -m recall_server.cli backfill-embeddings --batch-size 128
RECALL_DATABASE_URL=... RECALL_EMBEDDING_URL=http://127.0.0.1:8089 \
  python -m recall_server.cli backfill-turn-embeddings --batch-size 128
```

On a large existing brain, converge high-value sources first without changing global correctness:

```bash
python -m recall_server.cli backfill-embeddings --source-id SOURCE_ID --batch-size 128
python -m recall_server.cli backfill-embeddings --source-id SOURCE_ID --surface user --batch-size 128
python -m recall_server.cli backfill-turn-embeddings --source-id SOURCE_ID --batch-size 128
```

The optional source and surface selectors change scheduling only. Metrics and search compatibility remain global, and an
unscoped replay finishes every remaining source. The packaged oneshot has no start timeout because a
bounded batch on CPU can legitimately exceed the systemd manager default; batch and timer bounds still
provide resumable checkpoints.

`recall_embedding_lag` must reach zero before semantic retrieval is considered ready. The service
continues exact and lexical retrieval if the local sidecar or scoped planner is unavailable; stale
vectors are never searched.

Federated ranking uses explicit host-owned source profiles. Ingest envelopes and model tools
cannot set family, quality, or freshness policy. After a source has ingested at least one event,
an operator may configure it through the database-local admin CLI:

```bash
RECALL_DATABASE_URL=... python -m recall_server.cli source-profile-set SOURCE_ID \
  --family coding_history --quality trusted --freshness-half-life-days 180
RECALL_DATABASE_URL=... python -m recall_server.cli federation-scoreboard
```

Families and quality levels are closed enums. Search results add a content-free source profile
receipt plus bounded lexical, freshness, quality, and cross-family corroboration components.
The scoreboard reports aggregates only: it never returns source IDs, query text, or item text.
Unprofiled sources remain explicitly `unclassified`/`unrated`; the server never guesses a profile
from source-name patterns.

After applying schema 005 to an existing brain, backfill the rebuildable entity projection
online. The command commits bounded batches, holds a dedicated advisory lock, resumes from its
watermark after interruption, and does not rewrite canonical events or items:

```bash
RECALL_DATABASE_URL=... python -m recall_server.cli backfill-entities --batch-size 5000
```

Do not substitute the full `rebuild` command for this live migration; rebuild intentionally
truncates all derived projections inside one transaction and is reserved for offline recovery.

After upgrading to projector version 3, converge existing derived text on the current privacy
contract with the resumable redaction backfill. It snapshots the current item high-water mark,
rewrites only derived items/chunks/entities whose redacted form changes, and never mutates canonical
source events or receipts:

```bash
RECALL_DATABASE_URL=... python -m recall_server.cli backfill-redaction --batch-size 5000
```

The default is single-process. On a dedicated multi-core maintenance host, `--workers N` (maximum
32) parallelizes only the pure redaction computation; database reads, writes, watermarks, and the
advisory lock remain single-owner and ordered.

After schema 009, repair legacy Cowork messages that were projected as one session per message.
This migration only moves derived item/session relationships; canonical events, revisions, content
digests, and item receipts remain unchanged. It is high-water bounded, resumable, and idempotent:

```bash
RECALL_DATABASE_URL=... python -m recall_server.cli backfill-cowork-sessions --batch-size 5000
```

Configure owner-controlled aliases only after their exact source exists. Search routing by source
ID, source family, or alias always intersects with a source-scoped credential:

```bash
RECALL_DATABASE_URL=... python -m recall_server.cli source-alias-set cowork cowork:mac:owner
```

Linux history collectors use `recall-collector@.service` with separate `claude` and `codex`
environment/token files. Issue one source-scoped credential per unit, install the two example
environment files with mode 0600 after replacing every value, then enable the instances:

```bash
install -m 0644 ~/services/recall-brain/recall/server/deploy/recall-collector@.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now recall-collector@claude recall-collector@codex
```

Back up and run a blank-database restore proof:

```bash
RECALL_DATABASE_URL=... recall/server/scripts/backup_restore.sh backup /secure/backup/dir
RECALL_RESTORE_DATABASE_URL=... recall/server/scripts/backup_restore.sh restore-test /secure/backup/dir
```

The database fingerprint covers both legacy events and every v2 canonical truth, projection,
redirect, and forget-fence table. The laptop OSS profile backs up its exact raw archive separately
and proves it into an empty root:

```bash
python -m recall_server.archive_snapshot backup /private/archive /secure/archive-snapshot
python -m recall_server.archive_snapshot restore-test /secure/archive-snapshot /private/empty-restore
```

Both commands emit aggregate counts and fingerprints only. The restore refuses a symlink, a
non-owner-only tree, tampered bytes or metadata, and any nonempty destination.
