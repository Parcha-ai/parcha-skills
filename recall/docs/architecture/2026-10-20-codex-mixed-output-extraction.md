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

Epoch 5 does not enroll a legacy document in typed extraction. Such documents
still need the existing guarded `request_extraction_version` operation. Campaigns
must retain that distinction and use finite admission/ACK batches; shipping this
code alone does not prove corpus repair or publication.
