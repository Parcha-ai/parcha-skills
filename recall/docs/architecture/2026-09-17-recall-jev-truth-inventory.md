# Jev W5: private truth and review readiness

2026-09-17. The owner can now review 28 source-backed question proposals. Together
with 12 approved answerable validation questions, they give 40 distinct question and
known native-family groups. **This is preparation, not an approved truth expansion,
proof of semantic independence, or a retrieval-quality result.** The frozen truth,
strict boundary scorer, and systems-card accuracy probe are unchanged.

## Review the questions first

The owner-private directory is `~/.recall/evals/jev-w5-20260917/`. The current focused
packet is **`owner-review-noul-v1.html`**: the same 28 pending question cards and source
excerpts, with unapproved Jev relevance proposals and an escaped appendix showing both
raw relevance and source-support probabilities in the original question order. The
15-case frozen reference panel remains collapsed. The original
`owner-review-questions-v2.html` is preserved byte-for-byte.

Ten questions come from a historical curator pool; eighteen were drafted by this
assistant from centrally opened evidence. Jev supplied probability proposals, not the
questions or human approvals.

Current private inputs and audit receipts:

- `question-proposals-v3.jsonl`, `candidate-evidence-v4.jsonl`, and
  `native-families-v3.jsonl` reproduce the original question packet. The new
  `candidate-evidence-v5.jsonl` preserves every source byte and receipt association;
  only the previously empty probability proposal field changes.
- `w5-noul-requests-v1.jsonl`, `w5-noul-answers-v1.jsonl`, and
  `w5-noul-proposals-v1.jsonl` retain the exact independent-question contract, typed
  answers, model, input/source digests, question provenance, and pending status.
  `w5-noul-run-report-v1.json` and `w5-noul-receipt-v1.json` record counts and usage
  without source text. `python3 w5_noul.py render --output-dir <new-private-directory>`
  reproduces the new packet offline; it does not repeat inference.
- `assistant-question-provenance-v2.jsonl` records the eighteen assistant drafts and
  source digests. Two rely on a copied conversation or compaction summary; their
  underlying claims need explicit confirmation during review.
- `semantic-question-audit.json` records the removed semantic duplicate and three
  related-work groups that still need owner independence review.
- `preparation-v4-receipt.json` records counts, input hashes, permission checks, and
  unchanged-truth/protected-evidence checks. Source responses remain private in
  `source-evidence/` and `new-question-evidence/`.

`candidate-pool-review-v2.html` is a separate preparation artifact for later candidate
label review. It contains 785 pooled candidates, 757 without attached source excerpts,
and 527 distinct candidate boundaries with unresolved native families. Its family
audit correctly reports **incomplete**. Twelve protected-family candidate appearances
were withheld. Those unfinished rows are absent from the focused question packet.

## Inventory and coverage limits

The approved manifest hash remains
`a186b261deb9e23abd5d74b0f9fb801c16b9f3715ffb8b8abf358edae4e5c4bc`.

| Split/status | Questions | Answerable | Insufficient |
| --- | ---: | ---: | ---: |
| Approved optimize | 25 | 20 | 5 |
| Approved validation | 15 | 12 | 3 |
| Approved test | 20 | 16 | 4 |
| Pending validation proposals | 28 | 28 | 0 |

There are 77 saved systems-card candidate files: 74 validation and three optimize.
They hold candidate identities and scores, without source passages. Repeated retrievals
and July reader-smoke subsets do not increase independent question coverage. Three
older 50-question receipt holdouts use another schema and lack explicit split/family
assignments; their raw counts are not added to validation.

Two historical 180-row curation pools initially yielded eleven unused accepted
proposals after excluding frozen truth documents. The second pool added no further
accepted unused document. Central Recall opened their seventeen unique source
receipts; curator acceptance was never treated as owner approval.

A manual comparison with the approved validation answers then found one semantic
duplicate despite distinct native session IDs. That proposal was removed and replaced
with a new, centrally opened two-turn decision history. The audit compared the twelve
approved answerable validation cases with the pending proposals; it did not inspect
held-out question text. No other exact answer duplicate was observed. Related AML
investigations, skill-hydration work, and QA-hub work remain explicit grouping questions
for the owner. Native-session uniqueness cannot certify semantic independence.

The eighteen new assistant drafts use source records dated August 13–September 17,
from seven source IDs associated with all six configured contributor groups. They span
fifteen Claude and three Codex sessions, five intent categories, seventeen exact-document
questions and one bounded timeline. The final 28 proposals comprise thirteen decision
rationale, eight incident root cause, three project status, two change history, and two
ownership/next-step questions. They still cover only coding history: explicit searches
for other source families returned no candidates, and no new cross-source question
was fabricated. The ten retained historical proposals remain concentrated in May–July.

All 88 frozen/proposed gold document boundaries have centrally resolved native session
metadata. No proposed boundary overlaps a frozen family or another proposed family.
The focused packet has 28 cards with evidence and 36 excerpt associations. The fifteen
frozen reference candidates have no newly attached excerpts. There are now 28 unapproved
model relevance proposals and 28 unapproved joint-support proposals, with zero new owner
approvals. Source availability does not establish that every proposed answer is correct;
two secondary-record sources need extra care.

**The approved gap is still 28 answerable questions.** If all proposals pass source,
usefulness, and independence review, the nominal forty-question target is covered.
If related questions must be grouped or any proposal is rejected, collect replacements.
Forty is a minimum sampling target, not a promise that MRR noise will fall below 0.02.

## Bounded Noul proposal pass

One pass asked two independent Nouls per pending card: whether the supplied excerpts
contain a specific fact relevant to the question, and whether they support every
material proposed fact as an account of what the source records. The latter judges
63 proposed facts jointly within their 28 cards; it neither locates an individual
unsupported assertion nor verifies external truth. Only these already sourced pending
cards went to Jev. No retrieval, frozen reference questions, or held-out content was sent.

All 28 sequential requests returned valid answers: 56 Nouls, no transport/schema errors,
no retries, and a 10-second deadline per request. Model: `jev-1.13.0`, through the local
broker. Request p50 was 508.451 ms and p95 734.308 ms. This is one bounded review pass,
not production latency evidence or a repeated quality measurement.

Observed usage was 52,125 input and 1,260 output tokens, with zero unknown-usage requests.
The conservative input reservation was 338,016 tokens against a 500,000-token cap.
At the configured $0.042 per million input tokens, observed input usage estimates
$0.00218925. Output charges and actual billing are not established by this estimate;
the LiteLLM pass-through remains unmetered and provider billing needs reconciliation.

Relevance probabilities ranged from **0.90–0.99**; support for all proposed facts ranged
from **0.65–0.96**. Every answer leaned yes. This pass produced no clear negative signal
and does not demonstrate calibration, correctness, or useful negative discrimination.
The weaker support cases provide a place for human inspection, not an automatic verdict.
A manual source comparison identified one draft whose cancellation wording should
distinguish a recorded row state from its error message. The private
`w5-noul-review-observations-v1.json` records that concern as an assistant source audit,
separately from Jev's probabilities; no fact or label was corrected or approved
automatically. Two of the weakest support scores concern excerpts that
largely repeat their proposed claims, which also limits what the score spread proves.

No thresholds, question/candidate sorting, or approval actions were applied. The new
packet and its three generated JSON/JSONL artifacts regenerated byte-identically from
the saved answers, without another model call. All 36 source associations, original
question/proposal inputs, original packet, approved truth, and strict scorer stayed
unchanged. Four private fake-client checks cover capture identity, no retry on failure,
unavailable answers, source preservation, and escaped deterministic output. The 31
existing tool/truth tests still pass.

## Small offline boundary

`evals/judgments_review.py` unions saved candidates with known gold, preserves source
receipts, and renders a private static packet. It reuses existing case/boundary
validation, receipt identity, and private-file guards. There are no model/network calls,
approval action, new service, or scorer fork. The existing `build_owner_review_packet`
requires the complete 60-case truth schema and does not accept these evidence/label
proposals, so that established function remains unchanged.

Optimize/test normalized receipt identities and known families are withheld. Known
frozen receipt/document associations are checked even when revisions or fragments
change. New drafts and attached evidence fail closed when required native families
are unresolved. Completeness covers all frozen reference and rendered candidate
boundaries; unknown candidate identities can be inventoried but are not vetted source
material. Novel receipt associations are explicitly marked `supplied-unverified`:
an offline renderer cannot prove that supplied text belongs to a newly supplied receipt.

All inputs and outputs must be owner-private and outside Git. Files are mode 0600,
directories 0700, and symlink inputs are rejected. Summaries contain counts and hashes,
never question/source text. Real packet checks found no protected document or receipt
identity leakage, and the original approved manifest bytes are unchanged.

From the repository root, create a fresh focused packet with:

```sh
PYTHONPATH=recall python3 -m evals.judgments_review \
  --truth "$HOME/.recall-cascade/agentic-map-reduce-20260727/employee-truth-v2-approved.jsonl" \
  --questions "$HOME/.recall/evals/jev-w5-20260917/question-proposals-v3.jsonl" \
  --labels "$HOME/.recall/evals/jev-w5-20260917/candidate-evidence-v4.jsonl" \
  --families "$HOME/.recall/evals/jev-w5-20260917/native-families-v3.jsonl" \
  --output "$HOME/.recall/evals/jev-w5-20260917/new-question-review.html"
```

The output must be a new path. To inspect the full candidate pool, pass the saved
September 16 09:30 and September 17 06:14 validation files through `--results`.
Model probability proposals must match pooled identities and
revisions; they remain proposals. Record owner corrections and decisions separately.

## Validation, remaining work, and phase check

Test-first development began with the absent module. Seventeen tool tests and fourteen
existing truth tests pass (31 total). Independent review reproduced two held-out receipt
bypasses and an overclaimed family audit before the fixes; regression tests now cover
label and proposed-gold laundering, revision/fragment variations, wrong document
associations, unresolved families, private paths, HTML escaping, candidate unioning,
nonfinite labels, pending status, duplicate grouping, and unchanged truth bytes. The
independent reviewer reran the exploits and cleared the offline preparation code 5/5.

The owner must review the 28 cards and related-work groups before anything becomes
gold. The three existing insufficient validation cases remain; additional reviewed hard
negatives and broader source/timeline coverage are still needed. Freeze a new manifest
with split and family assignments before measuring a candidate. Expanded-manifest
validation belongs in a separate verifier PR; this change does not relax the strict
60-case scorer. W5 also needs paired repeatability/uncertainty evidence before exit.

**Simplicity:** one offline tool and a focused review task; no permanent workflow or
production dependency. **Power:** actual source evidence, gold outside retrieved pools,
explicit uncertainty about independence, and two reusable typed probabilities per card.
**Breakthrough:** a concrete measurement base for replacing brittle semantic heuristics;
the Noul pass makes review inspectable but has not demonstrated better labels or retrieval.
**Company-brain usefulness and speed:** the proposals exercise decisions, causes,
changes, and next steps; production behavior and latency remain unchanged.
