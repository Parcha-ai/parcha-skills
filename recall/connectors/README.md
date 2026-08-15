# Recall connector SDK

A connector fetches one bounded page. `ConnectorRunner` owns everything after
that boundary: closed-schema validation, pre-ingest privacy, a mode-0600 SQLite
outbox, Brain acknowledgement recovery, cursor commits, canonical tombstones, and
content-free doctor state.

## Generated integration catalog

Run `recall-brain integration-catalog` to print the closed inventory grouped by
local, remote, portable/import, and deliberate-capture placement. This preview
reads no credential, source, selector, cursor, path, spool, or network state and
performs no writes. The connector registry remains the single source of truth;
tests fail if a registered connector is omitted or duplicated. Each row includes
its concrete activation surface: shipped package, runtime, entrypoint, config
contract, and lifecycle. `recall-brain integration-activation-catalog` prints
that activation matrix alone.

The generated matrix currently covers:

- Mac-local sources through `recall-brain-macos`: iMessage, selected WhatsApp
  export, selected text, Safari, Chrome, Apple Notes, and Hermes sessions.
- Remote-worker APIs through `recall-remote-worker`: Gmail, Google Calendar,
  Google Contacts, Google Drive, GitHub, Linear, Slack, Notion, and X.
- Explicit portable inputs through `recall-brain`: mail, calendar, contacts,
  Slack, Notion, X, RSS/Atom, and closed JSONL.
- Deliberate capture through an MCP host, the selected ChatGPT export inbox,
  Grep AI completed research, and the authenticated custom webhook on Recall
  Core's `public-edge` profile.

These names are documentation only; the command output is authoritative and is
generated from the same immutable registry used by runners and tests.

## Unified activation intent

`integration-activation-preview` validates one private, closed activation intent.
The intent can contain only connector/source/principal identity, a permitted
privacy mode, declared selector values, and file/Keychain authority references.
It cannot contain a command, endpoint, module, import string, executable, plugin,
or arbitrary provider request recipe. The file and its parent must be private;
links and oversized or unknown shapes fail closed.

Preview returns only the connector's static activation surface, privacy mode,
selector count, and authority-reference counts. It does not read authority
values, source content, or network state and never prints identities, selector
values, references, paths, or credentials. Webhook preview additionally confirms
the required `webhook` capability and fixed source/principal binding while
explicitly reporting that no credential was issued.

```bash
recall-brain integration-activation-preview --config <private-intent>
recall-brain integration-activation-configure \
  --config <private-intent> --state <private-state>
recall-brain integration-activation-transition \
  --state <private-state> --connector-id custom.webhook --action enable
recall-brain integration-activation-status \
  --state <private-state> --connector-id custom.webhook
```

The shared lifecycle is `configured → enabled → paused → enabled → revoked →
uninstalled`. Replaying the same action is idempotent; invalid transitions do
not mutate state. The mode-0600 lifecycle database stores only connector ID,
state, revision, last action, and a non-reversible config digest. Provider
configuration remains in the private intent, while source runners continue to
own privacy-before-spool, ACK-before-cursor, bounded retry, and deduplication.

## Connector Kit v3

`connectors.kit` is the versioned publisher surface around the same proven page
runner. A v3 definition is composed from closed placement, auth, sync, policy,
record-kind, and selection facets. It declares what a connector can do without
containing executable paths, import strings, credentials, endpoint templates, or
arbitrary request recipes.

Placements are `source_local`, `remote_worker`, or `either`. Acquisition modes
are explicit (`poll`, `watch`, `snapshot`, `import`, or `webhook`). Provider
scopes remain code-owned strings on the bundled definition, which permits
non-Google APIs without turning configuration into an operation language.

Third-party workers do not load into Recall Core. A source plugin implements
`SourcePluginManifest` plus one `pull(cursor)` operation and exchanges a single
closed `recall.connector-page.v1` document with a source-scoped host:

```python
from connectors.kit import (
    SourcePluginManifest, WireSourceConnector, pull_source_plugin_wire,
)

payload = pull_source_plugin_wire(plugin, cursor)
hosted = WireSourceConnector(
    manifest=plugin.manifest,
    source_id="operator-bound-source-id",
    transport=plugin_process.pull_wire,
)
page = hosted.pull(cursor)
```

The wire contains only typed records, an opaque next cursor, and `has_more`.
Source identity and Brain authority stay outside the payload and are bound by
the host. Unknown versions, fields, record schemas, invalid cursors, non-finite
JSON, and oversized payloads fail before the runner. The schema is published at
`contracts/connector_page_v1.json`.

The host never imports a plugin, discovers Python entry points, accepts provider
request recipes, or gives the plugin Brain credentials. The plugin owns its
provider token; the operator explicitly pins and runs it outside Core. Recall
owns privacy-before-spool, ACK-before-cursor, replay, and source authority.
Bundled connectors use the same contract, so a contributed plugin can graduate
without gaining a second ingestion path.

### Conformance fixtures

`run_connector_conformance(factory)` runs the same aggregate-only matrix for
remote polling, local snapshots, imports, and future acquisition shapes. A
fixture factory exposes a v3 manifest, one synthetic source ID, and a
`build(scenario)` method. The fixed scenarios pin first-page ACK, pagination,
empty terminal pages, lost-ACK recovery, revisions, explicit tombstones,
acknowledged replay, privacy-before-spool, rate limits, malformed pages, source
identity, and wire parity.

The report contains only the connector ID, fixed cell names, and counts. It
never includes source IDs, cursors, paths, exception text, records, or provider
payloads. A publisher should make this matrix part of its own test suite before
asking for a connector to be bundled.

External publishers call `run_source_plugin_conformance(factory)`. Its factory
uses the same named scenarios but exposes a `SourcePluginManifest`; the adapter
then runs the identical Core conformance matrix rather than a plugin-specific
lookalike.

### Remote API rail

`BoundedJsonRail` is the shared transport for bundled remote connectors. Each
connector supplies an immutable in-code map of `RemoteOperation` values and one
exact HTTPS origin. Runtime configuration can select a bundled connector and
its bounded selectors, but it cannot provide an origin, method, path, operation
map, executable, or recipe.

Authority bytes are loaded only when an operation runs, from a private regular
file under a private directory. Redirects, unknown operations or parameters,
non-JSON responses, oversized bodies, revoked authority, and rate limits fail
with stable content-free conditions. The rail never discovers connector code.

### Google Workspace

The bundled Gmail, Calendar, Contacts, and Drive connectors use the
checksum-pinned Workspace CLI rail. They project closed v2 record kinds and
keep every provider cursor behind the runner's Brain acknowledgement.

- Gmail backfills messages, then uses history IDs. An expired history ID
  (`404`) restarts a full reconciliation without inferring deletion.
- Calendar and Contacts use terminal sync tokens. A stale token (`410`)
  restarts reconciliation; only explicit cancelled/deleted resources become
  tombstones.
- Drive captures a start-page token before its file backfill, then consumes
  changes. Only `removed` or explicitly trashed files become tombstones.

Gmail file attachments remain off by default. Setting the closed
`include_attachments` selector to `true` turns each supported attachment into a
child `document.v1`: exact bytes go only to the private content-addressed raw
archive, while bounded extracted text goes through the same privacy policy and
search projection as every other record. The initial allowlist is UTF-8 text,
HTML, PDF, DOCX, and PPTX, with explicit omissions for unsupported, encrypted,
malformed, or bounded-out content and deterministic duplicate suppression. A
parent forget cascades through the child lineage and fences both identities
against resurrection. Google Drive
document bodies remain a separate opt-in: Docs can be exported as bounded plain
text; other Drive file types remain metadata-only. Preview and registration do
not read credentials or sources.

Hosted operators may instead use `ComposioWorkspaceRail` as the authenticated
transport. It creates a current Composio session for one stable Recall principal,
one connector toolkit, and one explicitly pinned connected-account ID. Only the
same immutable Workspace read-operation map can reach Composio proxy execution;
the Google connectors continue to own normalization, incremental cursors,
reconciliation, privacy, acknowledgements, and tombstones. Recall never exposes
Composio tools or a generic proxy to an answering agent. Direct Google OAuth and
the pinned CLI remain available for self-hosted deployments.

Connectors are explicit Python objects. Recall does not discover or execute
plugins, recipes, entry points, or code from the current directory.

### Content fidelity invariant

Recall is a brain, not a link catalog. A content-bearing connector must acquire
the fullest authoritative text the provider exposes within the versioned record
and transport bounds. It must not silently substitute a title, metadata, preview,
or snippet for an available body. When a provider, format, policy, or hard safety
bound prevents complete acquisition, the record must remain useful while declaring
`content_fidelity=partial` and a non-empty, sorted, stable `content_omissions` list;
`complete` is required on every other live typed record and cannot carry omissions.
Snippet-formatted records can never claim complete fidelity. Tombstones carry only
their closed record kind. File attachments are separate content surfaces and can
claim complete only when their exact bytes are privately archived and their
allowlisted content is fully extracted into a linked child document.

Gmail follows this rule by collecting every non-attachment body section, preferring
plain text within each MIME alternative, converting HTML-only bodies to text, and
fetching body sections that Gmail externalizes behind an attachment ID. The Gmail
snippet is last-resort fallback only and is always marked partial. Attachment
metadata is always retained; bytes are fetched only when the owner enables the
attachment selector, and raw bytes never enter the model-facing projection.

### Work APIs

GitHub Issues and pull requests, Linear issues, Slack channel messages, and
Notion page metadata use the same bounded JSON rail. Exact provider origins,
operations, API-version headers, and GraphQL documents are immutable package
code. Runtime values are limited to declared selectors such as one repository,
team, channel, or integration-visible Notion workspace.

Each adapter owns its provider cursor and keeps the acknowledged watermark
inclusive so equal-timestamp revisions are replay-safe. Slack retries an expired
opaque page cursor from the last acknowledged timestamp. Notion's search API is
reconciled from its ordered pages. No adapter deletes by list absence:
only a Slack `message_deleted` event or a Notion object explicitly marked
`in_trash` becomes a tombstone. GitHub and Linear currently emit revisions only.

Factories such as `github_rail(authority_path=...)` read a token lazily from the
same strict mode-0600 authority file contract. Provider tokens, selectors, raw
payloads, cursors, and exception details never enter previews or health output.

The hosted Slack connector is public-workspace-only. Its generated app manifest
requests a bot token for live public-channel events, membership, and workspace
identity, plus an installing-user token for the complete public history rail.
That user rail can read active, archived, joined, and unjoined public channels;
private channels, group messages, and direct messages are outside the connector
contract. Generate the exact manifest from the `recall` directory with:

```bash
python -m connectors.slack_manifest --events https://recall.example.com
```

After adding the user scopes to an existing Slack app, reconnect it once so the
host receives both authorities. The connector then resets its coverage cursor
to the epoch and replays public history. Stable Slack message IDs make the
broader replay idempotent, and only the bot token is used to join active public
channels or receive live events.

### X activity

The X adapter can poll bookmarks, mentions, authored posts, and the reverse
chronological home timeline using OAuth user context. Stream selection is
explicit and closed; the home timeline is never enabled as a default. Authored,
mention, and home streams have independent inclusive `since_id` watermarks.
Bookmarks, whose API has no `since_id`, reconcile by bounded pagination and the
acknowledged-record ledger. Every stream keeps its own opaque page token, and
the adapter requests only immutable code-owned post fields.

Posts become `social_post.v1` revisions with stream-qualified identities.
Removing a bookmark, deleting a post, or losing visibility is not inferred from
list absence and therefore does not create a tombstone. Partial provider error
responses fail the page without advancing any stream watermark.

### Remote worker profile

The remote worker runs all nine bundled API connectors under the existing
durable supervisor. Its one private config contains only source IDs, schedules,
Brain endpoints, mode-0600 authority-file references, provider selectors, and
private spool paths. It cannot name modules, entry points, executables, origins,
HTTP methods, headers, GraphQL, or request recipes.

Preview validates the complete profile without reading an authority or source,
opening the network, or writing state:

```bash
python -m client.cli remote-worker-config-preview \
  --config /var/lib/recall/private/worker.json
```

The packaged `Dockerfile.remote-worker` is a non-root, listener-free worker
image. It includes the checksum-pinned Google Workspace CLI and defaults to the
continuous supervisor:

```bash
python -m client.cli remote-worker-run \
  --config /var/lib/recall/private/worker.json \
  --state /var/lib/recall/supervisor.db
```

Each job has separate source and Brain authorities and its own ACK-gated spool.
A provider failure changes only that job's backoff. Supervisor state and spools
survive image restarts; an unchanged provider cycle advances a bounded private
cycle generation without creating another acknowledged content version.

For canonical v2, set `RECALL_CANONICAL_V2_ENABLED=1`,
`RECALL_TENANT_ID`, and `RECALL_PRINCIPAL_ID` in the worker environment.
The host then replaces the legacy Brain client with the tenant-scoped canonical
archive client and writer. The existing per-job Brain credential must have been
created with the matching tenant, principal, source, and `write` scope.

### Portable mail, calendar, and contacts

The first portable-import pack reads one explicitly selected file: EML or
[RFC 4155 MBOX](https://www.rfc-editor.org/info/rfc4155/),
[RFC 5545 iCalendar](https://www.rfc-editor.org/info/rfc5545/), or
[RFC 6350 vCard](https://www.rfc-editor.org/info/rfc6350/). It uses only the
Python standard library, never discovers an export directory, and never follows
links. Files, lines, records, decoded text, pages, and total output are bounded.
Attachments and binary/encoded properties remain off.

Every import requires a stable owner-chosen `--archive-id`, so moving the same
export does not change native IDs. Re-importing identical content is suppressed
by the acknowledged-record ledger; a changed message, event, or contact becomes
one content revision. File or record absence never means deletion. A tombstone
requires an explicit bounded `--remove-native-id` that matches that connector's
native-ID namespace.

```bash
recall-brain mail-import-sync ... \
  --input /explicit/export/mail.mbox --archive-id personal-mail \
  --owner-identifier owner@example.com --spool /private/mail.db
recall-brain calendar-import-sync ... \
  --input /explicit/export/calendar.ics --archive-id personal-calendar \
  --spool /private/calendar.db
recall-brain contact-import-sync ... \
  --input /explicit/export/contacts.vcf --archive-id personal-contacts \
  --spool /private/contacts.db
```

### Slack, Notion, and X archives

`slack-archive-sync`, `notion-archive-sync`, and `x-archive-sync` read one
explicitly selected ZIP export. They never discover export folders, extract
archive members to disk, infer deletion from a missing member, or ingest
attachments. ZIP paths, member types, counts, sizes, expansion, and compression
ratios are bounded before projection.

- Slack follows the official JSON export layout and projects dated channel
  message files plus user identities. Its `archive_id` must be the real Slack
  workspace/team ID and the export must contain `channels.json`; this makes an
  archive message converge exactly with later API and Events API ingestion.
- Notion projects UTF-8 Markdown and CSV members from the official workspace
  export.
- X projects own posts from the account archive `data/tweet*.js` files.

Every import requires a stable owner-selected `archive_id`. Re-importing the
same export converges; an edited source record becomes one new revision.
Deletion is accepted only through an explicit `--remove-native-id` tombstone.
See the official [Slack export
format](https://slack.com/help/articles/220556107-How-to-read-Slack-data-exports),
[Notion export guide](https://www.notion.com/help/export-your-content), and [X
archive
guide](https://help.x.com/en/managing-your-account/how-to-download-your-x-archive).

### RSS/Atom and closed JSONL

`feed-sync` polls one explicit public HTTPS RSS 2.0 or Atom URL. It sends
`If-None-Match` and `If-Modified-Since` only after the previous response cursor
is Brain-acknowledged. Redirects, credentials in URLs, non-HTTPS/local
destinations, DTD/entities, unsupported content types, oversized XML, and more
than 500 entries fail closed. A 304 produces no records; a 200 response keeps
stable entry identity across edits. Feed absence never means deletion.

`jsonl-import-sync` reads `.jsonl` files below one explicitly selected local
root. Each non-empty line must be the closed object
`{"id","text","title"?,"occurred_at"?}`; unknown fields, aliases, links,
duplicate identities, invalid timestamps, and file/tree/line/record bound
violations fail closed. Stable identity is the selected-root-relative file plus
record ID. Missing files never delete; explicit native IDs may be tombstoned.

These sources implement the official [RSS 2.0
specification](https://www.rssboard.org/rss-specification), [Atom
RFC 4287](https://www.rfc-editor.org/info/rfc4287), and HTTP conditional
request semantics from [RFC 7232](https://www.rfc-editor.org/info/rfc7232).

### Optional managed-auth adapter

`connectors.managed_auth` is a provider-neutral, disabled-by-default adapter
contract for a managed auth/sync service. It is exported by the connector kit,
but no managed provider, endpoint, credential, webhook listener, registry
entry, or background job is installed or enabled.

The contract follows Nango's sync shape without coupling Recall to Nango:
HMAC-SHA256-signed change wakeups are closed and bounded; an explicitly
constructed transport is bound to exactly one Recall source; record pages use
opaque ACK-gated cursors and explicit `ADDED`, `UPDATED`, or `DELETED` actions;
and a bundled code-owned mapper must produce a typed connector-v2 projection.
The adapter owns stable native identity and deletion tombstones. Revocation
parks later reads. Provider payloads cannot define endpoints, credentials,
commands, connector code, or capture origins.

This reflects Nango's documented
[webhook-then-record-fetch](https://docs.nango.dev/guides/infrastructure)
architecture while keeping the default Recall installation at zero managed
third-party transfers. Deliberate MCP/web capture remains the separate
`recall.capture` push capability with Brain-only authority; bulk connectors
cannot gain shared capture visibility or choose the capture source/origin.

### iMessage local snapshot

`IMessageConnector` reads one explicitly selected Messages `chat.db` through a
query-only SQLite transaction. It does not discover a home directory, attach to
Messages, parse attachments or attributed-body archives, open a send surface,
or modify the source database. The constructor probes the exact required table
and column subset; an unsupported schema fails closed before any record is
projected.

Messages use hashed stable native and conversation IDs and become typed
`communication_message.v1` records. Text, direction, sender, edit time, and
service are selected explicitly. Only an upstream `is_deleted` marker becomes
a tombstone; row or database absence never does. Each bounded sweep starts
again after its ACK-gated terminal cursor, so edits to older rows converge
without treating a local snapshot as an authoritative deletion list.

The CLI requires an explicit database, private spool, source-scoped Brain
authority, and `scrub` or `drop` privacy:

```bash
recall-brain imessage-sync \
  --endpoint https://brain.example.invalid \
  --source-id imessage:mac:host \
  --keychain-service ai.parcha.recall \
  --keychain-account imessage:mac:host \
  --database "$HOME/Library/Messages/chat.db" \
  --spool "$HOME/Library/Application Support/RecallBrain/state/imessage.db"
```

### WhatsApp export and selected text

`WhatsAppExportConnector` watches one user-selected UTF-8 text export. It has no
network client, linked-device credential, QR/session surface, attachment
reader, or send operation. The user supplies the stable conversation ID,
owner-name set, date order, and IANA timezone. Bounded iOS and Android export
lines become typed messages; multiline text is retained. Identity is based on
conversation, timestamp, author, and same-timestamp ordinal, so content changes
become revisions while file disappearance never becomes deletion.

`SelectedTextConnector` reads one explicit Markdown/text or Obsidian root. Its
closed extensions are `.md`, `.markdown`, and `.txt`; recursion depth, file
count, per-file bytes, and total bytes are bounded. Hidden trees such as
`.obsidian`, non-UTF-8 input, NUL data, symlinks, hard links, replacement races,
and paths outside the selected root are never ingested. Stable relative-path
identities make edits revisions. Rename or absence is deliberately not a
tombstone.

Both commands are private-only and require pre-spool `scrub` or `drop`:

```bash
recall-brain whatsapp-export-sync --export /selected/chat.txt \
  --conversation-id selected-chat --date-order mdy --timezone UTC ...
recall-brain selected-text-sync --root /selected/notes --max-depth 8 ...
```

### Browsers, Apple Notes, and Hermes

`BrowserActivityConnector` reads only explicitly selected Safari or Chrome
history/bookmark files. History uses a query-only SQLite snapshot; Safari
bookmarks use the selected plist and Chrome bookmarks use the selected JSON
file. Only bounded HTTP(S) URLs are projected. Profile discovery, cookies,
passwords, form data, downloads, browser automation, and non-web local paths are
out of scope. Snapshot absence never becomes a tombstone.

`AppleNotesConnector` supports one pinned Notes schema and projects title and
snippet only. It skips password-protected notes and never decodes proprietary
note bodies, walks attachments, or infers deletion from an absent row. Any
missing or changed required column fails closed before projection.

`HermesSessionConnector` supports the upstream canonical `state.db` schema v22.
At least one Hermes source tag must be selected. Only selected user/assistant
turns are read; system prompts, tools, reasoning, attachments, billing data, and
other tables are excluded. An explicit inactive message becomes a tombstone;
database or row absence does not.

All four connectors require private visibility plus `scrub` or `drop`, use
stable hashed identities, cap snapshot rows, and commit their cursor only after
Brain acknowledgement:

```bash
recall-brain browser-sync --browser safari --history /selected/History.db ...
recall-brain apple-notes-sync --database /selected/NoteStore.sqlite ...
recall-brain hermes-session-sync --database /selected/state.db --source cli ...
```

```python
from connectors.sdk import ConnectorPage, ConnectorRecord, ConnectorRunner

class MyConnector:
    connector_id = "example.pull"
    source_id = "example:account:mine"

    def pull(self, cursor: str | None) -> ConnectorPage:
        # Fetch with connector-owned credentials. Never put those credentials in
        # content, provenance, cursors, exceptions, or the Brain client.
        return ConnectorPage(
            records=(ConnectorRecord.from_mapping({
                "schema_version": 1,
                "native_id": "stable-upstream-id",
                "occurred_at": "2026-07-14T00:00:00Z",
                "content": {"text": "source value"},
                "provenance": {"uri": "https://source.example/item/stable-upstream-id"},
                "deleted": False,
            }),),
            next_cursor="stable-upstream-checkpoint",
            has_more=False,
        )
```

The cursor is opaque private coordination state. It is persisted only in the
private spool and is never returned by `doctor`. A page cursor commits only after
all retained records and tombstones receive a valid Brain acknowledgement. When
privacy drops every record, the runner can commit the cursor without making a
Brain request.

`ConnectorRateLimited(retry_after_seconds=...)` returns a bounded, jittered retry
hint without sleeping. All other upstream and Brain failures surface only a
stable `ConnectorRunError.error_code`; payload-bearing exception text is not
propagated. The caller owns scheduling and may log only the code and doctor
counts.

External source credentials stay inside the connector. The Brain credential is
source-scoped and stays inside its Brain client. Revoking either side leaves the
pending page and cursor recoverable. Deletion records set `deleted: true`; their
tombstones bypass contextual privacy-judge availability.

## Explicit ChatGPT/Cowork export inbox

`ExportInboxConnector` is the bundled, network-free adapter for user-supplied
OpenAI-style `conversations.json`, JSONL, and ZIP exports. It never discovers
Downloads, Desktop, application support, browser state, or private databases.
The selected directory is flat: nested roots, symlinks, hard links, archive
traversal, and malformed records fail closed.

The catalog stores only hashes, stable native IDs, timestamps, and content-free
provenance. Message bodies cross the shared privacy boundary before the durable
runner spool or Brain network. Removing a file from the inbox does not delete
central history. List its opaque export ID and explicitly queue removal instead:

```bash
recall-brain export-inbox-dry-run --inbox "$HOME/Recall Inbox" --catalog "$STATE/catalog.db"
recall-brain export-inbox-list --inbox "$HOME/Recall Inbox" --catalog "$STATE/catalog.db"
recall-brain export-inbox-remove --inbox "$HOME/Recall Inbox" --catalog "$STATE/catalog.db" exp_...
```

The next scheduled `export-inbox-sync` ACKs reference-safe tombstones. If another
active export still owns the same upstream message, that message remains live.

## Read-only Grep AI completed research

`GrepAIConnector` imports completed work from the official Grep API v2. It uses
only authenticated `GET /api/v2/research` list and exact job-detail requests.
It cannot create research, spend credits, mutate sharing, use public-capability
reads, upload attachments, or download signed files.

Grep authority and Brain authority are separate. Supply the Grep
`research:read` key through Keychain or a regular mode-0600 file; its value never
belongs in argv, config previews, cursors, receipts, errors, or the Brain. The
preview reads neither credential and performs no writes or network calls:

```bash
recall-brain grep-ai-config-preview \
  --endpoint https://brain.example.ts.net \
  --source-id grep-ai:tenant:mine \
  --keychain-service ai.parcha.recall.grep-ai \
  --keychain-account grep-ai:tenant:mine \
  --grep-keychain-service ai.parcha.grep.research-read \
  --grep-keychain-account research-read \
  --spool "$HOME/.local/state/recall/grep-ai.db"
```

Each invocation fetches one bounded keyset page. The private cursor resets to
the list head after a historical sweep, so a job inserted above an in-progress
page walk is found on the next pass. Brain acknowledgement gates cursor commit;
a lost acknowledgement replays the staged batch without refetching Grep.
Completed reports cross the shared privacy boundary before SQLite or Brain.
Failed, blocked, and cancelled jobs advance the settled boundary without becoming
memories. Queued, moderation, planning, running, paused, and the exact
`in progress`/`in_progress` variants also remain out of memory, but hold that
boundary behind them so a later status transition is revisited.
List absence, 404, retention, and authorization failure never infer deletion—forget
an imported result explicitly with its Brain receipt.

After a valid Brain acknowledgement, the runner keeps only the lowercase
SHA-256 pair for the source-qualified native ID and canonical content. Exact
versions encountered again on a reordered upstream page advance the cursor
without another Brain request. Changed content and tombstones remain distinct
versions and still ingest. A lost acknowledgement deliberately replays because
the pair is not recorded until the acknowledgement transaction commits.

When upgrading a spool whose records already exist centrally, stop its service
and create a private hash-only seed manifest from canonical Brain events:

```json
{"schema_version":1,"records":[{"native_sha256":"<64 lowercase hex>","content_sha256":"<64 lowercase hex>"}]}
```

The manifest and existing spool must be absolute, regular non-symlink mode-0600
files in mode-0700 non-symlink directories. The spool must have pinned identity
and no pending work. The command validates the entire closed schema before one
atomic, idempotent insert and emits counts only:

```bash
recall-brain connector-spool-seed-acknowledged \
  --spool "$STATE/grep-ai.db" --input "$PRIVATE/grep-ai-ack-seed.json"
```

## Declarative registry and content-free status

`connectors.registry` is the closed inventory for Recall's installed input
surfaces. It declares command, push/pull mode, exact authority slots, allowed
visibility/privacy modes, checkpoint behavior, and explicit deletion semantics
for deliberate capture, the ChatGPT/Cowork export inbox, and Grep AI. It is an
immutable tuple in the package—not an entry-point or working-directory plugin
loader.

Preview is static and performs zero credential/source reads, writes, imports,
or network calls:

```bash
recall-brain connector-registry-preview
```

Status accepts only authority-presence flags and an optional local spool. The
spool is opened read-only and immutable; output is limited to health, counts,
checkpoint presence, privacy policy, and a whitelisted stable error code:

```bash
recall-brain connector-registry-status \
  --connector-id grep.ai --enabled --privacy-mode drop \
  --authority brain --authority source --spool "$STATE/grep-ai.db"
```

Status never syncs, repairs, advances a cursor, opens source material, or emits
the spool path, source identity, cursor, authority reference, or private content.

## Resumable connector supervision

`connectors.supervisor` decides only when an explicitly constructed pull
connector runner may execute. Schedules use an opaque random job key, injected
clock and jitter, bounded cadence/backoff/rate-limit/lease values, atomic leases,
and an explicit repair generation. Connector credentials, source IDs, command
arguments, paths, cursors, and content are not schedule fields and never enter
the supervisor database.

Authority and contract failures park a job until its generation changes or the
running process receives an explicit wake. Transient failures back off; rate
limit hints are capped; success resets failures. A due job still runs when
another job fails. Expired leases are recoverable after process death, while an
active lease cannot be acquired by another supervisor.

Preview is static. Status opens the mode-0600 state database read-only and emits
only aggregate counts and stable outcome classes:

```bash
recall-brain connector-supervisor-preview
recall-brain connector-supervisor-status --state "$STATE/supervisor.db"
```

The scheduler core never reads the wall clock or sleeps: hosts inject time and
wait/wake events. Private schedule configuration and service launch wiring are
separate deployment concerns; the supervisor does not discover plugins or
execute arbitrary commands.

## Private connector host

`connectors.host` is the closed deployment layer for the bundled export-inbox
and Grep AI pull connectors. It reads one explicitly selected regular mode-0600
JSON file from a mode-0700 directory. The file contains source-specific paths
and authority *references*; inline tokens/API keys, command arrays, import
strings, plugins, relative paths, shared visibility, duplicate source/job/state
ownership, and Brain/Grep authority aliasing are rejected.

Preview validates only the config schema and renders aggregate counts. It does
not dereference authority, open an inbox/report, construct a connector, create
state, or call a network:

```bash
recall-brain connector-supervisor-config-preview --config "$PRIVATE/host.json"
recall-brain connector-supervisor-run --config "$PRIVATE/host.json" \
  --state "$STATE/connector-supervisor.db" --once
```

Without `--once`, the host runs bounded supervisor cycles. TERM/INT wake and
stop it; HUP wakes the current cycle and reloads the private config between
cycles. Only the closed factory may construct `ExportInboxConnector`,
`GrepAIConnector`, the selected legacy or canonical Brain writer,
`PrivacyPolicy`, and `ConnectorRunner`.

The macOS installer accepts `--connector-supervisor-config FILE` to install the
fixed `ai.parcha.recall.connector-supervisor` LaunchAgent. The plist passes the
config path, content-free supervisor state path, bundled interpreter, and fixed
module/subcommand—never authority values or source content. Use
`--disable-connector-supervisor` to unload/remove only that agent while keeping
recoverable config, connector spools, and scheduler state.
