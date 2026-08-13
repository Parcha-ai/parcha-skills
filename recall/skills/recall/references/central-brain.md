# Recall Brain — central memory upgrade

The Recall Brain is a private, self-hosted central memory service: deliberate
memory writes, cross-device search, consented ChatGPT-export import, a Cowork
collector, pull connectors, and MCP capture — company or person memory beyond
this machine's local session index.

**Standing rule:** read this file whenever `RECALL_URL` is set, `RECALL_MODE`
is `remote` or `shadow`, or `~/.config/recall-brain/client.json` exists. None
of the commands below apply to a plain local installation; the local engine in
`SKILL.md` needs nothing from this file.

All `python3 scripts/recall.py` commands run relative to the recall skill
directory. `recall-brain` is the separately packaged Mac client utility.

## Setup choice (only when configuring the Brain)

This applies only when the user is deliberately setting up or reconfiguring a
Brain connection — never on plain skill install, where local is the default.

When configuring Recall for a Brain and no valid client profile exists, do not infer a mode.
Use the harness's native structured question tool—`AskUserQuestion` in Claude Code or
`request_user_input` in Codex—to ask one blocking question with exactly these choices:

- **Hosted brain (Recommended):** configure the supplied HTTPS `/mcp` endpoint and private
  token-file reference.
- **Local-only:** use the disposable on-device SQLite index without a network service.

Ask: **Where should Recall search?** If the structured question tool is unavailable, ask the
same blocking question in the conversation. Do not ask again when a valid profile already
exists unless the user requests reconfiguration.

After applying the choice, run `python3 scripts/recall.py doctor`. Accept hosted setup only when
it reports `OK remote`; accept local-only setup only when it reports local index health. A failed
hosted check stays remote and fails closed—it never falls back to SQLite. If hosted was selected
but the endpoint or token-file reference is unavailable, ask for the missing reference without
searching unrelated credential stores or rendering secret values.

## Mode resolution and routing

With no central configuration, every command behaves exactly as the local engine in `SKILL.md`.
Setting `RECALL_URL` selects the tailnet central service for read commands (`search`, `show`,
`related`, and `doctor`) and enables deliberate writes (`put` and `delete`). The same flags and output shapes remain valid, and every displayed remote
hit includes its resolvable receipt on the `WHY` line.

Use an explicit `/mcp` suffix for a public or managed MCP endpoint, for example
`RECALL_URL=https://recall.example.com/mcp`. The skill then calls the scoped
`recall_search`, `recall_show`, `recall_related`, `recall_capture`, and
`recall_forget` tools directly; `doctor` uses MCP ping. `session-export` has no
MCP tool and fails closed. A URL without `/mcp` preserves the legacy REST
transport.

### Direct MCP retrieval

When the MCP is installed in an agentic client, the client agent is the retrieval
agent. Recall does not call another model or return a synthesized answer:

1. Interpret the user's question and call `recall_search` with useful person,
   source, and UTC time boundaries. Reformulate or split the search when that
   improves recall; do not treat rank one as the answer.
2. For exact facts, open a returned `recall://` receipt with `recall_show` or
   `recall_session_context`.
3. For synthesis over full or large documents, pass only returned
   `logical_document_id` values to `recall_exec`. Use the read-only shell,
   `recall-scan`, Python, `jq`, and ordinary text tools freely inside its bounded,
   networkless sandbox.
4. Cite only receipts returned in `opened_receipts`. State a gap when the opened
   evidence is insufficient; search snippets alone are pointers, not proof.

This keeps planning and semantic judgment in the capable agent the user already
chose while Recall remains a small authorization, retrieval, execution, and
evidence-verification service.

For a persistent per-device read profile, use a mode-0600 regular file at
`~/.config/recall-brain/client.json` with the exact shape
`{"schema_version":1,"url":"https://brain.example.com/mcp",`
`"token_file":"/absolute/private/read-token.json"}`. The referenced token file
must also be a non-symlink mode-0600 regular file. Environment variables override
the profile field by field; `RECALL_MODE=local` remains the instant rollback.
Neither config validation nor transport errors render either private path.

Before searching, run `doctor` once when the active mode is uncertain. If it reports
`OK remote`, use the central service. Never set `RECALL_MODE=local` or run `index` to
repair a stale or slow central query; diagnose the remote service and collectors instead.
Use local rollback only when the user explicitly requests it.

Remote search can route explicitly without weakening credential scope:

```bash
python3 scripts/recall.py search "budget decision" --source-id cowork:mac:owner
python3 scripts/recall.py search "budget decision" --source-family coding_history
python3 scripts/recall.py search "budget decision" --source-alias cowork
```

Aliases are configured by the Brain owner and resolve to one exact source. Requested source ID,
family, and alias filters are intersected with any source-scoped bearer; they can narrow results but
never broaden authorization. The remote response includes content-free routing diagnostics. Source
routing fails closed in local mode because the local index has no central source authority.

Use `RECALL_MODE=local|remote|shadow` when the mode must be explicit:

- `local` is the config-only rollback switch and never calls the central service.
- `remote` fails closed on transport/auth errors; it never silently returns stale local results.
- `shadow` returns the local result while recording a receipt-level local/central comparison under
  `~/.recall/shadow.jsonl` (override with `RECALL_SHADOW_LOG`).

Interactive tailnet access uses the Tailscale identity boundary. If a scoped bearer is required,
set `RECALL_TOKEN_FILE` to a mode-`0600` JSON file containing `{"token":"..."}`. Never put a token
in `RECALL_URL`, shell history, a repository, or evidence. `index` is a local-only maintenance
command and never refreshes central data. On a device with a central profile it fails closed unless
the operator adds `--allow-local-index`; switching read modes cannot rewrite central canonical events.

## Deliberate memory writes

When the user explicitly asks to remember durable information, write it through
the central evidence protocol rather than editing an opaque memory file:

```bash
export RECALL_WRITE_SOURCE_ID="memory:mac:$(hostname -s)"  # credential must be scoped to this exact source
python3 scripts/recall.py put "the durable fact or work receipt" \
  --visibility private --provenance-uri "manual://current-task"
python3 scripts/recall.py delete 'recall://memory:mac:host/memory-…?rev=1'
```

`put` returns the canonical receipt. Preserve it when reporting the write;
`delete` requires that receipt and emits a tombstone under the same source and
native ID. REST writes require the exact source ID. MCP writes are instead
bound to the source and origin of the scoped host credential, so the client
does not transmit either authority. All writes require remote mode and fail
closed if the endpoint or scoped credential is unavailable. Never infer a
shared visibility choice: default to `private`, and use `shared` only when the
user deliberately selects it. Secret-shaped lines are redacted before ingest.

**Remember / forget playbook** — only on an explicit request, `put` the durable
text with a provenance URI and return its receipt; `delete` that exact receipt
when asked to forget it.

## Connectors

Completed Grep AI research can be imported through the packaged read-only v2
connector. Use `grep-ai-config-preview` to inspect the private one-shot command,
then `grep-ai-sync`; Grep `research:read` authority and Brain source authority
must use separate Keychain or mode-0600 references. The connector never creates
jobs or infers deletion from list absence. Use the returned Brain receipt for an
explicit `delete`.

Use `connector-registry-preview` for the static, content-free inventory of
capture, export inbox, and Grep AI trust boundaries. Use
`connector-registry-status` with authority-presence flags and an optional spool
to inspect only bounded health/count/checkpoint facts. Neither command reads
credential values or source content, and status never syncs or repairs state.

Use `connector-supervisor-preview` to inspect the static cadence/lease/backoff
contract and `connector-supervisor-status --state <private-db>` for aggregate
ready/due/leased/parked/outcome counts. Status is immutable and never renders a
job key, connector/source identity, path, cursor, command, credential, exception,
or content. The supervisor schedules only explicitly constructed registered pull
connectors; it does not discover plugins or own connector configuration.

For a deliberately configured Mac service, keep the closed two-source host JSON
in a mode-0700 directory as a mode-0600 regular file. Validate it with
`connector-supervisor-config-preview --config <file>`; this reads no authority
or source content. Install it with `--connector-supervisor-config <file>` or run
one bounded cycle with `connector-supervisor-run --config <file> --state
<private-db> --once`. Config may contain only file/Keychain references—never
credential values—and Brain/Grep authority references must be distinct. Use
`--disable-connector-supervisor` to unload the agent without deleting its
recoverable private state.

## Pre-ingest privacy policy

When the packaged Brain client is available, offer its opt-in pre-ingest privacy
policy for transcript/export/memory writes: `off` preserves compatibility,
`scrub` retains safe context, and `drop` omits the classified record before spool
or network. Run `privacy-preview` for a content-free category/action receipt.
Explain that this does not delete evidence already committed or alter the original
transcript; deletion still requires the canonical receipt. Never enable the
optional contextual-PII judge without consent, and route it only through staging
LiteLLM with a short-lived scoped virtual key—never a master key or direct provider.

## Consented ChatGPT exports and Cowork local project logs

When the packaged Brain client is installed, use its explicit export inbox for
ChatGPT exports. Never scrape application databases, caches, browser storage,
Desktop, or Downloads. Inventory only the directory the user selected:

```bash
recall-brain export-inbox-dry-run --inbox "$HOME/Recall Inbox" \
  --catalog "$HOME/Library/Application Support/RecallBrain/state/chatgpt-export-catalog.db" \
  --privacy-mode scrub
```

`export-inbox-list` returns opaque export IDs. `export-inbox-remove ... exp_...`
queues reference-safe tombstones; deleting a local file alone deliberately does
not delete central memory. Use `--export-inbox` during Mac package installation
to opt into scheduled sync, and `--disable-export-inbox` to unload that agent
without destroying its recoverable catalog/spool.

For Claude Cowork, the user may separately opt into the packaged `cowork`
collector. This is a narrow exception for Cowork's local project-log surface
beneath an explicitly selected `local-agent-mode-sessions` root; it is
not permission to inspect a Claude application database, cache, audit log,
attachment store, browser store, session metadata file, Desktop, or Downloads.
Only user/assistant natural-language records under the nested
`.claude/projects` logs are eligible. Privacy must be `scrub` or `drop` and is
applied before spool or network writes. Local absence and archive state never
imply deletion.

Install the unified utility with explicit selections such as
`--sources claude-code,codex,cowork` and `--export-inbox <selected-directory>`.
Use `recall-brain mac-status` for a content-free enabled/health/lag/checkpoint
view. Use `recall-brain mac-disable --source <class>` to unload one source while
retaining its recoverable state; uninstall also retains state unless the user
explicitly selects `--delete-state`.

## Deliberate capture from any MCP host

Prefer the packaged `recall_capture` MCP tool when the user wants an agent to
remember a selected decision, result, or external finding. Capture one concise
evidence object with a timestamp, title/body, tags, and a non-secret provenance
URI; the host configuration supplies the truthful, fixed origin. Return its
canonical receipt. Do not capture whole
transcripts, hidden reasoning, ambient context, secrets, or third-party results
the user did not select. The MCP process—not the model—owns origin, the
source-scoped credential, and privacy policy. Use one source profile per host;
never reuse another host's authority or try to send `origin` in the tool call.
Use `recall_forget` only with the exact receipt; never approximate identity from
search text. ChatGPT needs a remote MCP or Secure MCP Tunnel adapter rather than
the local stdio configuration.

## Central session-export parity

Central `session-export` cursors are random, source-authorized server state;
like local cursors, they never encode transcript text or a path. For
local/central evidence-ID parity, collectors set the source ID; a standalone
local export uses `RECALL_EXPORT_SOURCE_ID` when configured. `session-relations`
stays local-only until the central Recall service implements the same graph
contract.

## Index health on a centrally configured device

If `doctor` reports `OK remote`, do not run `index`: it operates only the disposable
SQLite fallback and cannot improve central freshness. Inspect collector/service health.
On a centrally configured device, intentional fallback maintenance additionally requires
`--allow-local-index`.
