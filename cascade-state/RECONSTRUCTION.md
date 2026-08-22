# Cascade state reconstruction — 2026-08-22

Between 2026-08-20 and 2026-08-22, a machine cleanup removed the uncommitted worktree
`~/worktrees/parcha-skills-tether-rewrite-cascade` (along with the untouched
`tether-rewrite-l1a-20260818` and `tether-rewrite-l1b-20260818` worktrees). The cascade chain
and every evidence receipt lived only there. Root cause of the loss: the authoritative chain was
never committed to version control — the exact failure mode the machine docs warn about.

**Fundamental fix:** cascade state now lives on the `cascade/tether-rewrite-2026-08-17` branch of
Parcha-ai/parcha-skills and is pushed after every state change. Recurrence watch: any future
cascade must start by committing its chain file.

Fidelity per file:

- `LOOP_CHAIN_2026-08-17_TETHER_REWRITE.md` — reconstructed from the acting session's full context
  (the file was read in full at takeover and every subsequent edit was authored in-session). The
  completed L0/L1 loop prompts and repair-loop definitions are summarized rather than reproduced
  verbatim; their operative content survives in the receipts. Amendments A1, the A1 data point,
  authority, invariants, and current state are verbatim-equivalent.
- `evidence/L1-durable-domain-core/PLAN-L1C.md, CHECKPOINT-L1C.md, PLAN-L1D.md, CHECKPOINT-L1D.md,
  EXIT.md` and `evidence/L2-stock-hermes-plugin/PLAN-L2.md` — authored by the acting session;
  rewritten verbatim from context (PLAN-L2 additionally carries the L2c status update of
  2026-08-22).
- Transcript recovery (2026-08-22, see `evidence/RECOVERY-NOTES.md` for hashes and sources):
  **verbatim with independent in-transcript hash verification** — `L0 EXIT.md`,
  `CHECKPOINT-L1A.md`, `CHECKPOINT-L1B-CONTROL.md`, `PLAN-L1C.md` (the recovered original,
  `status: READY`, replaces this session's reconstruction), `review-findings.md`.
  **Partial, marked with `// RECOVERY:` headers** — `baseline-metrics.json` (complete predecessor
  version plus final-version fragments in the sidecar), `failure-corpus.json` (truncated
  predecessor), `cross-repo-contracts.json` (first 1800 chars of the final file).
  **Not recovered** — `provenance.json` (final sha `6794dd43…` recorded; regenerable from the
  committed `evals/capture_provenance.py`, though not byte-identically), `github-pr.json`,
  `blueprint-validation.json`, and the full local test log (shas recorded in RECOVERY-NOTES).

Original content-address digests (e.g. blueprint.sha256) are lost with the originals; digests of
the reconstructed files are NOT substitutes for them. The published blueprint at
https://docs.greppy3.parcha.dev/2026-08-18-tether-system-blueprint.html survives independently, as
do all git-committed artifacts (the entire PR stack 392–399 and the upstream patch branches).
