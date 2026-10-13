# Finite archive retirement activation

The finite maintenance command captures existing catalog identities, publishes missing locators through the existing owner, and enrolls absent retirement markers. It does not change the clear verifier, enable restored parents, enroll future catalogs, or prove physical disk reclamation.

The host owns the private journal. Render runs bounded jobs with its existing database and R2 access. No database allowlist change, new service, or persistent Render disk is needed. One enrollment job considers at most 25 parents from one source in 260 seconds. Subsequent invocations rotate sources when a combined sealed inventory contains more than one source.

## Code and evidence

- `run_finite_retirement_job.py` connects the host journal to real Render jobs. Its `capture` operation returns at most 100 considered identities and a cursor. Each completed page survives separately; a later timeout loses only that page's read attempt.
- `retirement_campaign.py` persists exact plans before any launch, exclusive process locks, hash-linked results and complete deferred identities. Unknown POST, active job timeout, truncated logs or unknown owner commit stops the campaign until that exact job is reconciled. There is no automatic retry.
- `retirement_enrollment_owners` moves the reviewed October 5 metadata/operator helpers into the repository. The operator accepts an outer deadline shorter than its existing 260-second limit and emits a metadata-start event so a failed read can be attributed to the correct parent. Its archive and publication budgets remain shared across the entire quantum. Python formatting and split imports account for the rest. `parent_chunk_proof.py`, `chunk_retirement.py`, `retirement_runner.py`, `streaming_locators.py` and their authority rules remain unchanged.
- `e2e_finite_retirement.py` exercises real PostgreSQL, a local archive, metadata publication avoidance, original locator publication, clear proof, cooldown, disabled markers and changed catalog identity. The existing retirement runner integration tests remain separate and unchanged.

## Prepare a canary

Run with a fresh Render operator environment using the existing named 1Password wrapper. Do not print credentials. The host CLI reads `TASK_RECALL_RENDER_KEY`; the TypeSafe/model broker is unrelated.

Create a private config file with these fields:

```json
{
  "service_id": "srv-d9o4vf6417fc73ei24ag",
  "commit": "56630a8898238c6fec011965f1fbb8e2eb7690b2",
  "owner_modules": {
    "recall_server.retirement_runner": "SHA256_OF_DEPLOYED_FILE",
    "recall_server.chunk_retirement": "SHA256_OF_DEPLOYED_FILE",
    "recall_server.parent_chunk_proof": "SHA256_OF_DEPLOYED_FILE",
    "recall_server.chunk_bodies": "SHA256_OF_DEPLOYED_FILE",
    "recall_server.streaming_locators": "SHA256_OF_DEPLOYED_FILE",
    "recall_server.locator_backfill_plan": "SHA256_OF_DEPLOYED_FILE"
  },
  "checkpoint_file": "/PRIVATE/PATH/checkpoint.json",
  "checkpoint_sha256": "SHA256_OF_CHECKPOINT_FILE",
  "expected_fleet": {},
  "capacity_bytes": 348966092800,
  "minimum_free_bytes": 32212254720
}
```

Replace every placeholder and freeze the current fleet. The checkpoint uses the existing supervisor's shape: `observed_epoch`, `ready`, `health_ok`, `receipt_ok`, `search_ok`, `replicas`, `lag_seconds`, `fleet`, and `nodes` with `capacity_bytes` and `free_bytes`. All serving checks must pass, the cluster must be writable, all three nodes must have the configured capacity and free space, replica lag must be at most 10 seconds, and the observation must be less than 120 seconds old immediately before launch. Re-measure actual free space and provider capacity; the values above describe the recovery expansion, not permission to change it. Reconcile old running maintenance jobs first. Auto-deploy must be off; any active Render job prevents a launch.

With that config and a fresh empty private output directory, capture a small actual page:

```bash
python recall/scripts/run_finite_retirement_job.py capture \
  --config /PRIVATE/PATH/config.json \
  --state-dir /PRIVATE/PATH/capture-001 \
  --inventory /PRIVATE/PATH/inventory-001.json \
  --tenant-id tenant:company:parcha --principal-id principal:owner \
  --source-id claude:linux:greppy3 --limit 5
```

Review the page and returned inventory hash. A page can contain zero eligible parents: represented and queued identities still advance its cursor and remain in `page.json`. To continue, pass `--after-page /PRIVATE/PATH/capture-001/page.json` and its reported `--after-page-sha256`, with a new capture directory and inventory filename. Stop when `more` is false. This is a bounded capture window, not an MVCC snapshot. No later page can silently add itself to an already sealed inventory.

Refresh the checkpoint, then enroll the reviewed hash:

```bash
python recall/scripts/run_finite_retirement_job.py enroll --apply \
  --config /PRIVATE/PATH/config.json \
  --state-dir /PRIVATE/PATH/enrollment-001 \
  --inventory /PRIVATE/PATH/inventory-001.json \
  --inventory-sha256 EXACT_REPORTED_HASH --limit 5
```

The host first fsyncs the selected plans, then launch intent, then the returned job ID. It reads logs only after the job terminates. Compressed result fragments must be complete and match the package hash and result hash before any cursor advances. A preflight refusal before launch leaves the parents unattempted. An unknown launch or execution keeps the intent unresolved. Do not remove that intent to make a retry possible.

## Clear and measure

After enrollment and the existing 60-second grace, invoke the unchanged `run_chunk_retirement.py run --apply` through the existing bounded Render maintenance launcher for the explicit enrolled source. Its schema069 ledger controls which existing enabled parents can be claimed. It may include previously enrolled parents; this is distinct from the finite enrollment inventory. Keep its full archive proof, owner locks, scope epoch and commit classification. Do not run it concurrently with the dropped-column sweep. A cooldown report or `no_ready_parents` is not completion.

After each quantum, recheck readiness, the same receipt hashes, representative search, replica lag, disk usage and publication backlog. Stop on a failing check, unknown commit, less than the reviewed free-space bound, more than 1 GiB positive provider allocation from the activation baseline, or a deployment change. These between-job guards belong to the supervisor; this command deliberately runs one quantum and exits. Reuse the existing supervisor's lifecycle instead of launching an unattended shell retry loop.

A finite pass is complete only when its considered and deferred identities are accounted for and the unchanged clear runner has verified all enrolled eligible bodies. Logical cleared bytes, physical provider usage and billed capacity are three separate measurements. Future-parent enrollment requires a separate explicit policy and tests; this finite command does not authorize it.
