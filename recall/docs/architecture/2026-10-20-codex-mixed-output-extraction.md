# Mixed Codex output text

The typed Codex extractor first tries the unchanged whole-output v1 resolver.
Successful string and all-text-list outputs retain their existing text, source
units and passage IDs. If a custom-tool output list is refused, the extractor
resolves each exact indexed text leaf through the separately shipped block-text
contract. It keeps one tool-event group per provider item.

Images and unknown blocks remain in the original raw source. They are not
searchable text. Mixed events count their refused siblings as
`unsupported_output_block`; an event with no resolvable leaf retains the original
`source_unit_refused` disposition. Empty and whitespace-only strings retain their
existing dispositions. This coverage does not claim to understand image content.

## Dependencies and historical repair

All source readers must support the block-text contract, and the database must
have migration 093 before this producer is released. This feature does not alter
either contract, authority, Forget handling or the native publication transaction.

Codex checkpoint epoch 5 makes previously completed typed projections eligible
for a full rebuild through the existing ordinary or bounded seed owner. Old epoch
4 prefixes cannot be reused; after the rebuild, the original append proof applies.
Successful old passages remain identical, and unsupported-only parents terminate
without an invented reusable prefix. Claude and other source epochs are unchanged.

Epoch 5 alone does not enroll a legacy document in typed extraction. The ordinary
projection worker now requests those upgrades through the same locked transaction
as `request_extraction_version`. Its internal scope is tenant-bound and requires
the source's current owner grant; the public API still requires its caller's
authority and exact document pins. Revision/hash drift or pending Forget refuses
the request. Requested/applied versions and the existing queue preserve progress
across restarts.

At most once per 30 seconds, admission gets a three-second metadata allowance.
It rotates source turns and fills only the observed headroom below the configured
passage batch size, counting existing non-certification queue entries. Concurrent
external producers can change that depth. Admission never reads source bodies or
excludes a parent by byte size; the existing part reader and disk-backed publisher
own preparation. Large parents remain owed, and a parent-count bound is not a
bound on downstream archive or passage bytes.

Admission failure yields until a later turn without stopping ordinary publication.
A lost ACK stops that admission attempt; durable requested mode prevents the next
turn from blindly enqueueing it again. Shipping this code alone does not establish
corpus repair, vendor publication, source completeness or net queue drain.
