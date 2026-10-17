# Tool string source units

This contract lets the existing selected-passage reader resolve exact tool text
inside an authorized logical evidence record. It does not publish passages or
change which records the production projector selects.

## Coordinates and origin

Reuse `PassageSpan.source_unit`, `record_ordinal`, `record_count`, and the existing
source/passage byte ranges. No database column or receipt format changes.

Claude's adapter supports these provider-typed message envelopes:

- `claude.tool-input.string.v1#<pointer>`: a string leaf under a `tool_use.input`
  block in an assistant message.
- `claude.tool-result.string.v1#<pointer>`: the literal `tool_result.content`
  string in a user message, or a declared `text` string of a provider `type=text`
  block in its content array. Block metadata (`type`, IDs, image payloads) is not
  a text source. Nonstandard bare object results are unsupported; arbitrary
  business JSON within a result string remains literal and is not re-parsed.

`<pointer>` is a canonical RFC 6901 JSON pointer into the complete reassembled
source record. Array indices have no leading zero; object keys escape `~` as
`~0` and `/` as `~1`. Empty and Unicode keys retain their exact identity.
Coordinates count UTF-8 bytes of the **decoded string leaf**, not bytes of the
serialized JSON. A string containing JSON remains literal; it is not parsed a
second time. Identical text at two paths has two identities. Tool names have no
effect on resolution. Input/result origin describes the provider container;
it does not establish execution, success, attribution, quality or truth.

The shared direct-pointer resolver lives in `contracts/source_units.py`. It
resolves one requested path without enumerating unrelated leaves. Duplicate JSON keys,
wrong roles/envelopes, unknown versions, forged pointers, non-string values and
unsupported provider formats cannot resolve a new unit. Provider non-text
result blocks (including image and reasoning blocks) are not text sources.
Tool arguments remain literal data, including arguments named `type` or
`channel`. No new record, leaf-count, depth or pointer-length limits are added.
The existing caller, program and output bounds remain authoritative.

Codex additionally supports four literal string fields inside a `response_item`
envelope:

| Provider item type | Source unit | Pointer | Provider role |
| --- | --- | --- | --- |
| `function_call` | `codex.tool-input.string.v1` | `/payload/arguments` | assistant |
| `custom_tool_call` | `codex.tool-input.string.v1` | `/payload/input` | assistant |
| `function_call_output` | `codex.tool-result.string.v1` | `/payload/output` | tool |
| `custom_tool_call_output` | `codex.tool-result.string.v1` | `/payload/output` | tool |

The discriminator and pointer are joined with `#`. These fields are literal
strings, including JSON-looking arguments; arrays/objects and non-text payloads
remain unsupported by this adapter. Tool names do not select a parser. Optional
`name` and `call_id` strings are preserved with the item type and provider role
in `source_tool` provenance. They do not establish completion, success, human
approval, or a link to another event. Input means an authored request; result
means reported tool output.

Archived Codex tool events often have no explicit role. Empty archived roles are
accepted only for a supported source unit after the exact native envelope,
field and provider type validate. If a role is present in either provider
envelope or archive metadata, it must agree with the declared input/result
role. Legacy visible text still requires its original visible role. Analysis
containers, duplicate keys, wrong provider/type/role, arbitrary metadata paths
and unknown unit versions are refused. The raw archive is unchanged.

Legacy spans retain historical visible-message coordinates. No changes are
made to `visible_message_text`, passage identity generation, canonical span
serialization, complete-report markers, indexing policy or append checkpoints.
New units are not complete-report markers and must never receive that ranking
or completeness treatment implicitly.

## Existing authority remains authoritative

`CandidateCapture` retains the new discriminator during exact metadata recovery.
Its existing reader verifies the pinned logical document, parent, revision,
manifest and part hashes, reassembles the exact source event, checks source
receipts, and then resolves the selected leaf. It rejects out-of-range bytes
and UTF-8 splits. Input/result role must agree with the source record. Reader
fragments carry the discriminator and deterministic origin, which assembly
checks again; source receipt checks still require `opened_receipts`.

The resolver is embedded unchanged in the existing `recall_exec` program only
when the request contains a new unit; legacy requests omit it.
Its fixed local resolver and reader source use one gzip/base64 payload, decoded
inside the reader. Tests compare the decoded bytes with the exact shared
resolver, `_read_page` and `_page_stdout` functions. Source records and unit references never become executable code.
There is no new endpoint, grant check, archive reader, evidence store, model
call or alternative verifier. Live authorization, revisions, retirement and
Forget remain owned by the existing metadata/source-opening/execution path.
A resolver result alone is not a receipt or authorization decision.

The full captured record/parent remains in the same immutable archive and
available through the existing authorized reader. Resolving one leaf does not
replace that context, infer completeness, drop siblings or delete raw history.
The capture result continues to distinguish complete selected passages from
a complete document.

## Review and tests

Independent test fixtures author expected paths and text directly, rather than
using the resolver to generate its own expectations. They exercise both origins,
segmented source events, escaped/Unicode paths, literal JSON versus nested
objects, duplicate keys, wrong role/kind/version, forged non-tool pointers,
manifest/part changes, exact receipts, UTF-8 splits and origin tampering.
Retained metadata/capture/span tests cover the legacy path and original size
limits. The embedded program remains bounded by the unchanged 16,000-byte cap;
the reader's page cap remains unchanged.
The retained two-passage fixtures exercise 128 spans per passage for legacy,
Claude and Codex units, including 256 distinct Claude argument paths. Extending
the plain reader exceeded the existing cap for a retained Claude fixture;
bundling the trusted reader with the already-compressed resolver preserves
capacity. This is an application API guard enforced by MCP, canonical execution
and deep inspection, not an externally established 16 KB transport limit.
Reviewing that policy is separate from this compatible source contract.

The native PostgreSQL fixture uses the original metadata and source-opening
owners, archived files, and generated reader. It verifies both origins and
refuses revoked grants, an advanced revision, and pending Forget. The fixture
inserts canonical Codex tool spans only for this test because Codex production
emission is still disabled. Local execution does not claim remote sandbox verification.

Codex extraction and publication require a separate reviewed change. This
contract does not enable them, alter requested extraction versions, create a
new passage policy, modify the schema or change the collector. That later
publisher must retain provider attribution and explicitly handle tool roles;
it must not label output as a user decision or invent visible-message roles.
The existing Claude publisher remains unchanged. This reader contract does
not establish model selection quality or a retrieval gain.
