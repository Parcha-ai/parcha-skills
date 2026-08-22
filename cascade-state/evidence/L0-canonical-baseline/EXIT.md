---
cascade_version: 2
episode_id: tether-rewrite-2026-08-17
loop: L0
status: COMPLETE
target_head: e37126b0a9adcef1b851102878bc1ca71d90d0c5
next: L1
---

# L0 boundary receipt — canonical baseline and executable contract

## Bound accounting

- Valid evidence attempts: 1/2. Final verdict: all five acceptance criteria PASS.
- Review/fix rounds: 3/3. Architecture/security contract, evidence/provenance
  harness, then PR portability/integration.
- Instrumentation failures: 2, not charged as evidence attempts. The first local
  full-suite command named a nonexistent Node 22 directory and therefore fell
  through to Node 20; two installer tests correctly rejected it. The first HTML
  validation compared package-relative `docs/...` provenance to a repository-
  relative `tether/docs/...` key. Both mechanisms were diagnosed and corrected
  before the final clean-tree proof.

## Accept criteria → evidence

| ID | Verdict | Evidence |
|---|---|---|
| L0.1 | PASS | `provenance.json` proves clean candidate `e37126b0a9ad`, tree `007a99830356`, behavior commit ancestry, merge base, and 10/10 installed managed artifacts matching current plus both independently identified deployed source commits. |
| L0.2 | PASS | `local-node22-python311-full.log` records 505 tests, release-install lifecycle, and release-tarball lifecycle at exit 0. `github-pr.json` records SUCCESS for Python/Node 3.11/22, 3.12/24, 3.13/22, 3.14/24, repository portability, and native Linux arm64 at the exact head; gitleaks, Desloppify, and Recall are also SUCCESS. |
| L0.3 | PASS | `failure-corpus.json` is valid at the exact clean head/tree/content digest and classifies 4 baseline defects observed, 2 cross-repository defects observed, and 7 legacy controls preserved. `cross-repo-contracts.json` binds the wrapper/hook/signup facts to clean detached Parcha main `9b3acf4c3955`. |
| L0.4 | PASS | Repository ADR plus the reviewed `review-findings.md` resolve all P0/P1 contradictions. `blueprint-validation.json` reports schema 17, 9 SVGs, zero scripts, zero external assets, zero stale schema docs, evidence-linked 505 tests, and `valid=true`. The published page returns HTTP 200 and SHA-256 `8ec51c116cfc908711a79324b1601a8cbfa9edcfbeca266a3b7aac10c4b5d1b9`. |
| L0.5 | PASS | `baseline-metrics.json` is valid at the exact candidate and records component file/line measures, 13 tables, explicit states, six distribution roots, 23 validated Hermes adapter methods, two private hook-registry references, 25 health-query samples, operator visibility, and the sanitized >72-hour production lower bound. |

## Evidence manifest

| Criterion | Command/action | Runtime/environment | Target | Timestamp | Artifact/digest | Falsifier / negative | Cleanup / rollback |
|---|---|---|---|---|---|---|---|
| L0.1 | `python3 evals/capture_provenance.py` | Python 3.11.9; clean isolated worktree; deployed files read-only | `e37126b0a9ad` | 2026-08-18T03:10Z | `provenance.json` / `6794dd4392ab…` | Dirty tree, wrong merge base, observed-version mismatch, or any installed artifact hash mismatch makes the report invalid. | No live mutation. Close PR/delete isolated worktree to abandon candidate. |
| L0.2 | `npm test`; GitHub `tether-ci` matrix | Local Node 22.23.2/Python 3.11.9; GitHub pinned matrix and native arm64 | `e37126b0a9ad` | 2026-08-18T03:18Z | `local-node22-python311-full.log` / `80035a876262…`; `github-pr.json` / `585641ac3e0c…` | Any skip counted as pass, different head, failed lifecycle, non-aarch64 runner, or pending/failing matrix job fails the criterion. | Test temp roots self-clean. No deploy or service action. |
| L0.3 | `run_cross_repo_contracts.py`; `run_incident_corpus.py` | Python 3.11.9; Node 22.23.2; clean candidate and clean detached fleet source | candidate `e37126b0a9ad`; fleet `9b3acf4c3955` | 2026-08-18T03:10Z | `failure-corpus.json` / `c014c5afabb5…`; `cross-repo-contracts.json` / `b752a5e62e3d…` | Missing external receipt, selector failure, source drift, dirty target, or inability to distinguish blocked delivery invalidates the report. | Synthetic local sockets/files only; no Slack or credential access. |
| L0.4 | Three-track review; validator; HTTP and browser render checks | Self-contained HTML, inline CSS/SVG, tailnet-only docs server | ADR at `e37126b0a9ad`; HTML digest above | 2026-08-18T03:19Z | `review-findings.md` / `1e188a3dc618…`; `blueprint-validation.json` / `d5ee761fd0dc…` | Endpoint-scoped privilege, two ledgers, unproved same-UID boundary, stale schema, external asset, evidence mismatch, or HTTP/render failure fails the criterion. | Published page is documentation only; no runtime dependency. |
| L0.5 | `python3 evals/capture_baseline_metrics.py` | Python 3.11.9; SQLite 3.40.1; Linux x86_64 | `e37126b0a9ad` | 2026-08-18T03:10Z | `baseline-metrics.json` / `704ca8bafb8e…` | Dirty target, unlabeled query latency, hand count, or claimed alert time without measurement fails the criterion. | Temporary synthetic SQLite store self-cleans. |

## POST-ZEN

The target now has one purpose, one endpoint truth, one endpoint scheduler, one
Hermes ingress owner, one Hermes platform delivery ledger, one plugin package,
one typed blocker projection, and one separate root authority. The design
removed contradictory one-endpoint/one-thread documentation, model callback as
completion authority, endpoint-scoped privilege, duplicate delivery ownership,
and backup-only rollback from the accepted target. The L0 source adds reusable
evidence rather than a second runtime path. Current compatibility machinery is
explicitly temporary, owned by L2, and may be removed only at the ADR's
mechanical deletion gates. No deployed path was changed, so runtime rollback is
N/A; candidate rollback is closing PR 391 and discarding its isolated worktree.

The merged Cascade skill also now states the stock-pi foreground limitation its
own portability contract required, keeping autonomous execution honest across
harnesses.

## Transition

`COMPLETE`: follow normal `exit -> L1`. L1 must start from this receipt and
candidate in a fresh isolated worktree. It may implement and test the durable
domain core locally, but it inherits every deployment, Slack, credential,
host-policy, canary, fleet, and destructive-action gate.
