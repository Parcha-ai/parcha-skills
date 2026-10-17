# Tool string source units

This contract lets the existing selected-passage reader resolve exact tool text
inside an authorized logical evidence record. It does not publish passages or
change which records the production projector selects.

## Coordinates and origin

Reuse `PassageSpan.source_unit`, `record_ordinal`, `record_count`, and the existing
source/passage byte ranges. No database column or receipt format changes.

The first adapter supports Claude's provider-typed message envelope only:

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
The existing caller, program and output bounds remain authoritative. Codex is
deliberately outside this first adapter.

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
Its fixed local source is gzip/base64 encoded for transport and decoded inside
the reader. Tests compare the decoded source byte for byte with the shared
function. Source records and unit references never become executable code.
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
The retained two-passage fixture with 128 spans per passage uses at most 13,928
program bytes for legacy spans and 15,828 for new units. The new fixture has
172 bytes of headroom. Longer variable pointers still obey the existing cap
and may be refused; this measurement does not promise every string length fits.
The separate 256-distinct-input-key fixture reads back every leaf and uses at
most 14,900 program bytes, leaving 1,100 bytes of headroom.

The native PostgreSQL fixture uses the original metadata and source-opening
owners, archived files, and generated reader. It verifies both origins and
refuses revoked grants, an advanced revision, and pending Forget. The fixture
inserts canonical tool spans only for this test because production emission is
still disabled. Local execution does not claim remote sandbox verification.

Only a subsequent separately reviewed extractor/selector change may emit these
new spans into canonical storage. It must use this contract and prove native
source readback before any publication. This contract does not establish model
selection quality or a retrieval gain. Today `PassageMessage.validate` accepts
only the existing complete-report markers, and the builders do not propagate
generic tool units. Append checkpoints, copy folding, audit grouping and any
selection/ranking treatment must be reviewed when that later change is made;
none is implicitly enabled by this reader contract.
