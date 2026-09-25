# Render projection-worker cutover

Stop the old namespace writer before starting its replacement. Deploy an inert
worker first, verify that only that worker remains, then deploy the candidate.
Only the projection worker pauses; MCP and managed ingestion remain running.
Queued projection work remains in PostgreSQL.

The search projector coordinates writes within one process. Its outbox generation
checks do not exclude another process. An old writer can retain a tombstone,
delete a passage after another writer has republished and acknowledged it, then
exit before restoring it. Read authority rejects invalid hits but cannot restore
an absent vendor row. Restart replay therefore assumes non-overlapping writers,
including foreground `search-plane-project` and `search-plane-reconcile --apply`.

[Render rolling deploys](https://render.com/docs/deploys#zero-downtime-deploys)
start the replacement before terminating the old instance. A deploy marked live
is not evidence that the old process stopped. Use this procedure for worker
upgrades, command changes, restarts and rollbacks; do not use a direct rolling
restart as a shortcut.

## Prepare and record

Use the Render API under `https://api.render.com/v1` with existing authorized
credentials. Keep credentials and private state out of the repository and logs.
Let `SERVICE` denote the projection worker service ID. One operator/controller
owns the whole transition; exclude concurrent manual deploys and other writers to the search namespace.

1. Read `GET /services/SERVICE`, `/deploys`, `/instances`, `/jobs` and `/env-vars`.
   Handle pagination rather than assuming the first page is complete. Require
   one configured instance, no autoscaling, no active deploy/job, and no
   pre-deploy command that could start a writer. Set `autoDeploy` to `no` with
   `PATCH /services/SERVICE` if necessary, then verify it. Keep it disabled for
   future controlled cutovers. Freeze linked environment groups too.
2. Save the current live commit, deploy ID, all old instance IDs and the exact
   `serviceDetails.envSpecificDetails.dockerCommand` string. Preserve command
   bytes; do not split/rejoin it or expand its environment references. Pin the
   remaining service configuration and environment using hashes without logging
   secret values. Pin the candidate's full commit SHA and its passing CI/review.
3. Keep a private durable state directory (0700, files 0600). Before every PATCH
   or POST, exclusively create an intent file, flush and fsync it and the
   directory. Record each response/deploy ID. On an ambiguous response, reconcile
   provider state; never blindly repeat a mutation. Changed configuration or an
   unexpected deployment stops the transition for inspection.

## Stage 1: deploy an inert worker on the current commit

Use an unquoted standard-library command that imports no Recall code and binds
only inside the container:

```sh
python -u -m http.server 8789 --bind 127.0.0.1 --directory /nonexistent-recall-cutover
```

PATCH only the command via this request shape, encoding the command as a JSON
string with a JSON serializer:

```json
{"serviceDetails":{"envSpecificDetails":{"dockerCommand":"<inert command>"}}}
```

Read back and verify the command and pinned configuration. Then
`POST /services/SERVICE/deploys` with
`{"commitId":"<CURRENT_LIVE_FULL_SHA>","clearCache":"do_not_clear"}`.
Deploy the **same currently live commit**, not the candidate. Use an actual
deploy: [Render restart reuses the deployed configuration](https://render.com/docs/deploys#restarting-a-service)
and does not pick up pending configuration changes.

Wait for this exact deploy to become live. Read the full instance list until
only one new instance is RUNNING and ready, with every old writer ID absent.
Use `GET /logs` filtered by owner, service and this deploy's start time; require
the startup marker `Serving HTTP on 127.0.0.1 port 8789` on that exact
instance label. The deploy ID, start time and new instance ID identify the
transition; the message itself is not unique. This proves the command override
actually ran the inert process, including with a Docker ENTRYPOINT. Avoid inline `python -c` quoting that the
platform may parse differently. Configuration alone is insufficient. Record two matching instance/marker observations at least
10 seconds apart. Do not advance while any old writer remains.

## Stage 2: restore the command and deploy the candidate

Recheck the same inert-only instance, deploy, disabled auto-deploy, and pinned
configuration/environment. Restore the saved command byte-for-byte with PATCH;
read it back. POST one deploy with the **reviewed candidate's full SHA** and
`clearCache: do_not_clear`. Save the returned deploy ID and observe that exact
deploy. Never substitute the current branch head or replay an ambiguous POST.

Verify the candidate commit, restored command, sole running writer instance and
publication progress. Check a pinned current passage's vendor presence and public
search/open behavior. Record queue trend and elapsed freshness separately; a
successful deploy alone does not prove that the backlog is shrinking or that the
freshness target holds.

## Failure and rollback

If stage 1 fails, the old writer may still be active: inspect state and do not
start the candidate. If the controller stops after changing configuration, use
the persisted intent, exact command and deploy IDs to reconcile before resuming.

If stage 2 fails before any candidate writer starts, verify the inert-only barrier
again, restore the original command and deploy the saved known-good commit. If a
candidate writer started, **first repeat stage 1 to establish a new inert-only
barrier**, then restore the command and deploy known-good code. Direct rolling
rollback over a partially running candidate recreates the overlap.

This procedure excludes overlapping worker processes; it does not fence an
external request already accepted before the old process stopped. Such a request
could finish later. Preserve that limitation and verify publication after the
handoff rather than claiming unconditional distributed fencing or restart safety.
