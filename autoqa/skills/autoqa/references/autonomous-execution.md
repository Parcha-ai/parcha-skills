# Autonomous slot execution

Read this reference when the selected scope includes stateful behavior or `AUTOQA.md`
declares a disposable development/test slot.

## Authority boundary

Repo-declared synthetic-write authority permits the ordinary product operations needed to
exercise selected rows in that slot: create/update/delete synthetic users, agents, jobs,
continuations, files, shares, and dev-service resources already used by the application.
Namespace every object with a run ID.

It does not permit production or customer writes, shared snapshot mutation, credential
retrieval or injection outside the documented runtime, new network/relay infrastructure, or
changes to external systems not named by the repo contract. Stop for new authority if a real
flow crosses that boundary.

## Fixture synthesis

Build a reusable dependency graph instead of isolated one-off fixtures:

1. establish health and synthetic authentication;
2. create the smallest parent resources through the real entry point;
3. run the primary job/action and poll it to the documented terminal state;
4. use its IDs for continuation, export, share, or lifecycle rows;
5. record IDs in a content-free manifest for cleanup.

If creation fails, capture the request shape with secrets removed, response/status, relevant
logs, and elapsed time. Only then may the dependent row be UNTESTED. A fixture assumed
missing without an attempt is a QA failure.

## Live backend evidence

Use the repo-defined slot E2E command against the externally reachable backend URL. A valid
live run crosses the HTTP boundary, uses the slot database and workers, and reaches the
configured dev dependencies. Poll asynchronous work for the full repo timeout. Useful
witnesses include terminal events, DB row IDs/counts, content hashes, artifact listings,
worker logs, and API responses with sensitive content removed.

## Chrome DevTools evidence

When required, capture at least:

- a screenshot or DOM snapshot showing the visible result; and
- a network or console witness showing the browser request/runtime state.

Recover a busy profile or start a dedicated DevTools session. Do not substitute Playwright
or source inspection and call the row covered.

## Cleanup

Cleanup runs in reverse dependency order and is attempted even after a failed row. Delete
only IDs from the run manifest. Capture API responses/listings proving absence, preserve
audit/provenance objects when the repo contract requires them, and re-check instance health.
The report records `Cleanup: PASS` and links the cleanup witness. A cleanup failure or an
unknown leaked resource blocks shipping.
