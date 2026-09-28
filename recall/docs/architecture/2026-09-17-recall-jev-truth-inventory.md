# Jev W5: delegated source review and validation expansion

Updated September 18, 2026. The owner explicitly assigned review and approval to
Codex. Decisions are recorded as **owner-delegated agent review**, not human review
and not approval by Jev. The original 60-case benchmark remains byte-for-byte unchanged.
The expansion is private and separately versioned; production retrieval is unchanged.

## Review decisions

The original 28-card packet required substantive review, not a bulk acceptance:

- Eighteen proposals are supported within their reviewed scope: nine retained as
  written and nine corrected. Corrections separate recorded findings from current
  behavior, proposed work from completed work, and primary evidence from copied
  conversations or compaction summaries.
- Ten historical proposals are excluded from validation. Their child documents had
  distinct IDs, but their actual parent sessions overlap the frozen optimize/test
  splits. Native parent IDs, not child-document uniqueness, determine this exclusion.
- One evidence row incorrectly copied a passage spanning two chunks under both
  receipts. The reviewed evidence now attributes each portion to its actual chunk;
  one fact's unsupported second receipt was removed.
- Ten replacement questions were independently reviewed and approved. Their evidence
  comes from ten distinct native parents outside the frozen set and the eighteen
  retained additions. All thirty facts have exact source witnesses; no exact semantic
  question duplicate was found.

The lead independently checked 113 quotation witnesses against exact source receipts
and file hashes: eighty for the retained eighteen cases and thirty-three for the ten
replacements. Two cases are accepted only as
explicitly attributed accounts of secondary records; their underlying implementation
claims are not independently established.

The parent audit also found three existing native parents spanning splits inside the
frozen benchmark. Those historical rows are preserved for comparison, and the limit
is disclosed. This expansion does not retroactively certify the old test split as
independent. Related workstreams are recorded separately because unique session IDs
alone cannot establish semantic or statistical independence.

## Private artifacts

The freeze now contains 28 approved additions: **43 validation cases, 40 answerable
and three insufficient**. The full versioned corpus contains 88 cases; optimize and
test remain the original twenty-five and twenty cases. No Jev probability became an
approval.

Owner-private directory:
`~/.recall/evals/jev-w5-20260917/delegated-review-20260917/`.

- `review-a.json`, `review-b.json`, and `review-c.json` contain per-fact source
  witnesses, corrections, scope limits, and reviewer provenance.
- `review-decisions-v1.jsonl` records all approvals/corrections and ten evaluation
  exclusions; `review-decisions-v1.html` makes approved questions, facts, source text
  and scope limits readable. `replacement-review-v2.json` holds the independent
  replacement audit.
- `freeze-v1.json` pins the base, additions, complete family mapping, approval ledger,
  source reviews, and readable packet. `approved-additions-v1.jsonl` is the expansion;
  `expanded-truth-v1.jsonl` includes the unchanged original sixty cases.
- `native-parent-audit-v2.json` and `parent-resolution.json` retain metadata-only
  lineage evidence. All previously unresolved frozen receipts were resolved using
  the configured central Recall service; held-out question text was not inspected.
- `replacement-source-independence-v1.json` independently verifies replacement
  lineage against the complete base plus retained additions, and records related
  workstream cautions.

Source files, previous proposal packets and model outputs remain unchanged. Files
are mode 0600, directories 0700, outside Git. The raw frozen benchmark hash remains
`a186b261deb9e23abd5d74b0f9fb801c16b9f3715ffb8b8abf358edae4e5c4bc`.

## Verifier boundary

The verifier change is isolated in [#615](https://github.com/Parcha-ai/parcha-skills/pull/615),
separate from the Jev client/candidate work. The legacy v2 API still requires exactly
60 cases, its original balanced splits and strata, approved labels, and strict result
contracts. An explicit expansion API requires the pinned base plus approved,
validation-only answerable additions and a complete reviewed family mapping. An
addition cannot share a family with any frozen case or another addition.

The APIs share the existing metric calculations. The lead compared the complete real
15-query baseline report against the pre-change scorer and found exact equality.
The initial eighteen additions and 78 mapped boundaries also passed the new validator.
Twenty-seven truth tests and fifty systems-card tests passed; full CI and secret
scanning passed on `f243976`. No candidate code, acceptance threshold, dependency, or
production behavior changed in that PR.

The family map is a reviewed lineage assertion supplied by the caller. The validator
checks its completeness and consistency; it cannot infer whether a caller supplied
truthful lineage. Raw artifact hashes, canonical base hashes, source witnesses, and
reviewer decisions therefore travel together in the private freeze receipt.

## Expanded baseline measurement

Two sequential passes ran the same frozen forty-three validation questions through
existing production search, with fifty candidates and 256-character snippets. Every
case and all three negatives were retained, with zero backend errors. Both passes
measured recall@20 **0.9625**, recall@5 **0.8000**, and MRR **0.68423**. Client latency
p50 was **1,371 / 1,355 ms** and p95 **1,859 / 1,634 ms**.

The original fifteen-case panel still measures MRR **0.54246** in both passes; the
added twenty-eight measure **0.74498**. Independent direct-rank recalculation confirms
the weighted overall result. These are baseline measurements on a different question
mix, not retrieval improvement. The forty answerable cases form thirty-nine native-parent
components. Per-case reciprocal ranks were identical across these two passes; paired
cluster resampling therefore reports a zero observed difference and a degenerate
zero interval. That does not establish future run stability, low sampling uncertainty,
three accepted nightlies, or sensitivity to a small candidate improvement.

The adapter marks returned candidates as authorized with valid pointers; those fields
are assumptions in this run, not separate authorization/pointer audits. All three
insufficient cases returned candidates, so the reported false-hit rate is 1.0; it means
any search hit, not a judged wrong final answer. Search remains an evidence-hint tool.
Exact captured responses, score rows, freeze identity, and accounting remain private
under `baseline-v1/`. A separate manifest binds ninety-four files, including all
eighty-six responses, score rows, reports, interpretation, capture script and freeze.

## Historical Noul pass and limits

Before delegated review, one bounded pass produced 56 valid Noul answers for the
original 28 proposals, with no retries or transport/schema errors. Request p95 was
734 ms; observed input usage was 52,125 tokens, about $0.00219 at the configured rate.
This was an input-only estimate, not reconciled provider billing. Broker passthrough
billing remains unmetered.

Relevance probabilities were 0.90–0.99 and joint fact-support probabilities 0.65–0.96.
Every answer leaned yes. The pass did not establish calibration or discrimination of
unsupported claims. Those probabilities were not used to approve the corrected
questions, and they must not be carried forward as if they describe revised inputs.
The old `owner-review-noul-v1.html` is a preserved proposal artifact, not the current
approval ledger.

The larger 785-candidate pool remains a separate incomplete preparation artifact:
757 candidates lack attached source excerpts and 527 candidate boundaries lack resolved
families. It is not an approved relevance set. Expanded per-candidate relevance labels,
negative-class calibration, and repeated systems-card evidence remain W5 work.

## Phase check

**Simplicity:** one shared scorer, an explicit expansion boundary, and private review
artifacts; no production service or dependency. **Power:** approvals now have exact
source witnesses, corrected facts and parent-family checks. **Breakthrough:** better
measurement foundations, with no claim of improved retrieval. **Speed:** production
latency is unchanged. The current Jev search prototype still fails the measured W0
quality/latency gates, so no ranker replacement or semantic deletion is earned.
