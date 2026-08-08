# Direct MCP retrieval

Recall is an evidence service for an agent, not an agent hidden behind an agent.
The Codex, Claude Code, or other MCP client already interpreting the user's
question owns planning, query reformulation, evidence gathering, and synthesis.
The hosted Recall service owns authorization and access to private data.

## Interface

- `recall_search` returns authorized high-recall pointers, exact receipts, and
  logical document IDs. Embeddings help the caller find where an answer could be;
  a hit is not proof.
- `recall_show` and `recall_session_context` open exact receipt-backed context.
- `recall_exec` mounts only explicitly selected full documents into a bounded,
  read-only, networkless sandbox. The caller can use `recall-scan`, shell, Python,
  `jq`, and ordinary text tools to inspect them.
- `opened_receipts` is the citation boundary. Recall reauthorizes every requested
  document and verifies every receipt returned by execution.

The caller can search narrowly, broaden, decompose a time range, sample, or inspect
whole documents according to the question. Recall does not impose a deterministic
retrieval plan and does not need a model-provider credential.

## Trust boundary

The MCP credential determines the tenant, principal, roles, and allowed sources.
Tool arguments can only narrow that authority. Storage credentials never cross the
service boundary; the execution sandbox has no network and cannot mutate evidence.
Search snippets remain hints until the caller opens supporting records.

## Why the nested agent was removed

A hosted Pi worker duplicated the reasoning agent already present in every target
client. It added another prompt, model failure mode, queue, lifecycle API, model
credential, and roughly half the container image while making the outer agent wait
for an opaque investigation. The direct interface keeps one semantic agent and one
small evidence plane.
