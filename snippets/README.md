# Instruction Snippets

Drop-in blocks for root agent instruction files. Use these when a behavior must
apply to every session and cannot depend on skill discovery.

Each folder contains an `AGENTS.md` payload and a README describing its purpose,
design boundaries, and installation. The same payload can be used in
`CLAUDE.md` when the target harness reads that file.

| Snippet | Purpose |
|---|---|
| [`effective-comms`](effective-comms/) | Makes responses relevant, findable, understandable, and usable without shortening away necessary detail. |
| [`evidence`](evidence/) | Ties claims to witnessed evidence and keeps sensitive evidence out of public repositories. |
| [`control`](control/) | Preserves user control over consequential actions and gives errors a bounded recovery path. |
| [`systems-thinking`](systems-thinking/) | Prevents local fixes from making the larger system worse without blocking bounded work. |
