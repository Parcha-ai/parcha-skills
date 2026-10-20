# Codex output text blocks

Codex `response_item` records can carry `custom_tool_call_output.output` as a
list of `{type: "input_text", text: string}` blocks. Previously these records
remained raw-only. They use the existing source unit
`codex.tool-result.string.v1#/payload/output`.

For this exact provider form, the shared resolver accepts a nonempty list only
when every block contains exactly `type` and `text`, with `type=input_text` and
a string `text`. It joins the decoded strings in order with one newline between
blocks. Empty blocks and repeated strings remain in their original positions.
Source byte coordinates refer to that normalized UTF-8 string. The versioned
unit describes the resolved string; it does not permit arbitrary JSON traversal.
Existing string-valued inputs and outputs retain their exact bytes and roles.

Extraction and mounted source readback use the same resolver. A mixed, unknown,
malformed or analysis-bearing block refuses the complete output; the existing
`source_unit_refused` coverage disposition records it and the raw event remains
available. No partial extraction is presented as complete output. Source
currentness, grants, Forget, receipts, archive proof and passage commit retain
their existing owners.

Codex checkpoint epoch 4 prevents old append checkpoints from retaining a prefix
that omitted these blocks. The existing backfill owner recognizes the stale
epoch, and completed tool-only or empty projections receive a full-rebuild-only
marker so that seeding terminates. Claude epoch 3 and other source compatibility
remain unchanged. No global backfill is implicit in deployment.

Release compatible MCP readers and refresh client-embedded mounted readers
before publishing these spans from the worker. Older readers safely refuse
list-valued output. Then use the existing bounded native backfill and verify
current receipt authorization, canonical publication and authorized vendor
publication. This change does not establish a production latency or retrieval
quality improvement; the frozen evaluation remains unchanged.
