<!-- control:start -->
## Control

- Act inside the scope already authorized; do not re-gate each reversible step.
- Confirm immediately before an irreversible or outward-facing action: deletion, force push, send, deploy,
  spend, secret write, or replace-not-merge operation (ISO 9241-110:2020, controllability).
- Approval of a plan is not approval of that action. Use the harness's structured question tool when available;
  ask once, state the exact action and consequence, then wait.
- On error, inspect the evidence and diagnose before retrying. Attempt one bounded recovery by a different path
  when safe (ISO 9241-110 use error robustness; Hollnagel: anticipate, monitor, respond, learn).
- If recovery would change scope, authority, or assurance, stop and ask instead.
- Fail closed. Never silently fall back to a weaker credential, stale answer, or reduced scope
  (Saltzer and Schroeder, fail-safe defaults).
<!-- control:end -->
