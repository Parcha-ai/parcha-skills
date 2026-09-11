# Storage runbook: legacy v1 plane retirement (H1-T6)

Decision (Miguel, 2026-09-11): the canonical v2 plane is the only authority.
The legacy v1 plane stops receiving writes immediately, its read routes answer
`410 Gone`, and its tables are dropped thirty days after the retirement ships.
This runbook covers the flags, the write-path change, the read-path change,
and the drop.

## What changed

Every v1 write entry point now lands on the canonical plane:

| Entry point | Before | After (default) |
| --- | --- | --- |
| `POST /webhooks/v1/events` | `BrainStore.ingest` → `source_events`, `items`, `chunks`, ... | raw body → `raw_artifacts` via the fenced archive gateway, envelope → `CanonicalPlane.ingest_document` |
| `POST /webhooks/v1/slack` | same, one legacy batch per installation route | same canonical path, one artifact per installation route (tenant from `connector_installations.tenant_id`) |
| `POST /v1/ingest/batches` | same, keyed by `Idempotency-Key` in `ingest_batches` | one artifact per envelope (its canonical JSON), then `ingest_document`; dedupe is content-addressed |
| MCP `recall_capture` / `recall_forget` (`BrainStore.capture`, `forget_capture`) | `BrainStore.ingest` | same bridge, connector `mcp.capture`; `forget_capture` looks the captured revision up in `canonical_events` first |

The bridge lives in `server/recall_server/legacy_plane.py`
(`LegacyIngestBridge`). `BrainStore.ingest` itself stays callable as a library
function: the rollback path and the legacy e2e fixtures use it directly, but no
HTTP or MCP entry point reaches it while `RECALL_LEGACY_WRITES=0`. Acknowledgements keep the v1 shape
(`status`, `inserted`, `duplicate_events`, `receipts`, `replay`); a replay of
the same event returns `duplicate_events=1`, `replay=true`, and the same
receipt. Webhook responses now carry `duplicate_events` too. `batch_id` is no
longer returned in the default mode because nothing is written to
`ingest_batches`.

Receipts minted by the canonical writer look like
`recall://<source>/<native>?rev=<n>#item=<ordinal>`.
`GET /v1/receipts/resolve` resolves them from `canonical_events` and
`canonical_chunks` first and falls back to `source_events` for rows written
before the cut. It keeps the v1 response shape.

The four legacy read routes answer `410 Gone` before authentication:

```json
{"error": "gone", "code": "legacy_plane_retired", "replacement": "recall_search"}
```

| Route | Replacement |
| --- | --- |
| `POST /v1/search` | `recall_search` (MCP) |
| `POST /v1/show` | `recall_show` (MCP) |
| `POST /v1/related` | `recall_related` (MCP) |
| `POST /v1/session-export` | `recall_session_context` (MCP) |

`GET /v1/receipts/resolve` and `GET /v1/doctor` stay.

## Flags

| Variable | Default | Meaning |
| --- | --- | --- |
| `RECALL_LEGACY_WRITES` | `0` | `1` restores the old dual-write path for rollback: the canonical write still happens when a canonical plane is configured, then `BrainStore.ingest` writes the v1 tables and its acknowledgement is what callers see. |
| `RECALL_LEGACY_READS` | `0` | `1` restores `/v1/search`, `/v1/show`, `/v1/related`, `/v1/session-export` exactly as before. |
| `RECALL_LEGACY_INGEST_TENANT_ID` | `tenant:personal` | Tenant that owns canonical rows written for v1 callers whose credential is not tenant-bound (existing webhook tokens, development mode). A credential created with `token-create --tenant ...` wins over this value; webhook-scoped credentials may now carry a tenant. |

Accepted spellings are `0/1`, `true/false`, `yes/no`, `on/off`. Anything
else fails startup (`validate_http_profile`) so a typo cannot silently flip the
plane.

The canonical write path needs `RECALL_CANONICAL_V2_ENABLED=1` and a
configured archive (`RECALL_ARCHIVE_*`). A deployment that receives webhooks
or v1 batches without a canonical plane returns
`503 {"error": "canonical plane unavailable"}` and writes nothing. Either
enable the canonical plane there or set `RECALL_LEGACY_WRITES=1` until it is.

### Rollback

1. Set `RECALL_LEGACY_WRITES=1` and `RECALL_LEGACY_READS=1`, restart.
2. Rows written while the flags were `0` exist only on the canonical plane;
   the legacy projection does not backfill from canonical. If the rollback
   window matters, replay the affected webhooks or batches: canonical dedupe
   makes the replay a no-op on the canonical side while the v1 tables fill in.
3. Reverse the flags to `0` when done. Do not drop tables while either flag is `1`.

## Thirty-day drop plan

Target: 2026-10-11 or later, thirty days after this change ships with both
flags at `0` in production. The drop is a separate change; nothing in H1-T6
removes a table.

Tables to drop, in dependency order (children first):

1. `turn_embedding_items`
2. `turn_embeddings`
3. `turn_embedding_projection_watermarks`
4. `item_embeddings`
5. `embedding_projection_watermarks`
6. `entities`
7. `chunks`
8. `items`
9. `sessions`
10. `projection_backfills`
11. `projection_watermarks`
12. `source_events`
13. `ingest_batches`
14. `source_grants`
15. `sources`

The same list is `LEGACY_DROPPABLE_TABLES` in
`server/recall_server/legacy_plane.py`; the e2e prints it as evidence.

Pre-drop checklist:

- Both flags have been `0` in production for the full thirty days
  (deploy history, not memory).
- `recall_ingest_commits_total` kept increasing and `source_events` row count
  did not change during that window (`SELECT count(*) FROM source_events`).
- `storage-discard-covered-legacy` (see `recall_server.cli`) reports every
  legacy identity as canonical-covered or archived to the legacy gap archive.
- No collector or MCP client still calls the four retired routes: the
  `recall_http_errors_total` counter split by status shows 410s trending to
  zero.
- `GET /v1/receipts/resolve` fallback to `source_events` is removed in the
  same change that drops the tables.
- Take a database snapshot first.

Code that still references the legacy tables and goes away with the drop:
`BrainStore.ingest`, `_project_one`/`_project_batch`, `search`, `show`,
`related`, `session_export`, the embedding and turn-embedding projection
workers in `db.py`, the legacy fallback in `BrainStore.resolve`, and the
`RECALL_LEGACY_*` flags themselves.

## Evidence

`server/tests/e2e_webhook_ingest.py` on a fresh PostgreSQL proves, in one run:
canonical rows and raw artifacts for the generic webhook, the Slack webhook,
and `/v1/ingest/batches`; zero rows in `source_events`, `items`, `chunks`,
`ingest_batches`, `sources`; receipts resolving; replays returning
`duplicate_events=1`; tombstoned receipts answering 404; the four read routes
answering 410 by default and 200 with `RECALL_LEGACY_READS=1`; and
`RECALL_LEGACY_WRITES=1` dual-writing. Unit coverage is
`tests/central_brain/test_legacy_plane.py`.
