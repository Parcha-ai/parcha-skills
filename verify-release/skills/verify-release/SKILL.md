---
name: verify-release
description: Verify a specific release in its target environment, follow an in-progress rollout, and resume agreed checks when it is ready. Use for “I deployed it, verify”, “monitor the deployment and continue QA”, or “is this release working in staging/production?”. Discovers the stack and uses live evidence. Not a deployment tool or a full QA campaign by default.
---

# Verify release

Establish that the intended change is serving in the intended environment and that its agreed behavior works there. Keep the verification proportional to the change. Use the repository's existing deployment tools, tests, and observability; no particular provider, framework, browser, or telemetry service is required.

## Carry forward the request

Recover the target environment, candidate release, expected behavior, selected checks, and existing authorization from the request and any tracker or handoff. Inspect deployment configuration and the relevant diff only as needed to fill gaps. Ask only for a consequential choice you cannot establish; do not repeat settled scope or permission questions.

Identify the components and dependencies that actually serve the selected path. Do not make unrelated or intentionally disabled services gates. Keep environments and test identities separate. A preview result proves only what that preview exercised.

## Establish the live candidate

Map the intended change to an immutable deployed identity: commit, image digest, build ID, package version, or equivalent. Use the release manifest when components have different versions; do not require every component to share one SHA. A merge commit may differ from the PR head: establish the mapping instead of assuming a mismatch or equivalence.

Check the deployment's state and its running artifact, not merely a green CI job or a completed deploy label. Where available, corroborate with a live version endpoint or fresh runtime evidence. A missing version endpoint is not itself a defect if another trustworthy source proves the running artifact. If identity cannot be established, report that gap instead of claiming the candidate passed.

If rollout is still progressing, use bounded waits appropriate to its normal duration and continue independent checks. Resume dependent checks as soon as their prerequisites pass. Stop unchanged retries when evidence shows failure, unavailable access, or an exhausted execution limit. State the next diagnostic or resumption condition. Claim ongoing monitoring only when a real monitor is running, with an owner and a way to retrieve its result; otherwise save a resumable checkpoint.

## Verify the changed path

- **Readiness:** inspect applicable health/readiness checks and effective configuration. When relevant, include traffic destinations, build-time versus runtime settings, TLS, migrations, and queue routing. A removed probe or a process saying “ready” does not prove the configured readiness contract.
- **Prerequisites:** before a stateful probe, check the chosen identity, permissions, effective flags, fixture state, and capacity needed for that case. Mark blocked cases individually and continue runnable ones. Do not repeatedly swap accounts without checking what prerequisite would change.
- **Behavior:** run the smallest live probe that exercises the changed boundary through its actual entry point. Use AutoQA when available for an agreed test matrix or substantial browser/stateful coverage; pass the existing scope and candidate identity. For a narrow check, use the project's existing smoke command or a focused API, CLI, or browser probe.
- **Outcome:** correlate the request with its actual result. HTTP acceptance is insufficient for an asynchronous workflow. Verify the relevant terminal state or side effect. A browser error can occur after the server accepted a request: establish whether it executed before retrying a mutation.
- **Observability:** inspect fresh, relevant signals in the target environment and rollout window. Check environment/release attribution when applicable. Old events, another environment's traffic, or no traffic at all do not establish success. Separate observed failures from unproven causes and explain whether baseline issues affect acceptance.

Use existing authorization for tests and fixes. Verification alone does not authorize deployment, rollback, flag changes, credential changes, or new spending. If a fix is authorized, follow it through the applicable release and recheck the affected cases. Otherwise report the concrete repair and continue independent verification. Restore temporary test state that this run owns, respecting concurrent changes, and verify cleanup.

## Close or resume

For longer work, update the existing tracker or one compact receipt in private session scratch space; do not build a second task system. Record:

- Target, intended-to-observed release mapping, and observation time/window.
- Required checks with `PASS`, `FAIL`, `BLOCKED`, or `NOT RUN`, plus evidence pointers.
- Cleanup, remaining gaps, and the exact next action or resumption condition.

On resume and before the final verdict, recheck the deployed identity and relevant configuration. Label evidence from a superseded candidate and repeat only checks whose applicability changed. Never silently combine results from different environments or releases.

Lead the answer with `VERIFIED`, `FAILED`, or `INCOMPLETE`, qualified by environment, candidate, and tested scope. Name both intended and observed identities when they differ. A required failed check means `FAILED`; missing required evidence means `INCOMPLETE`. Use `VERIFIED` only when all required checks pass and required cleanup is confirmed. Distinguish rollout failure, application failure, and test-tool failure. State material coverage limits without implying full release or production approval. Link concise evidence; keep secrets and private payloads out of the receipt and reply.
