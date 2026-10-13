# Retirement in the projection worker

This adds a bounded retirement round to the existing projection worker. The
command defaults to `--retirement off`; enabling it does not enroll sources or
parents. It processes only sources whose explicit policy inventory is sealed,
and clearing still requires each parent's existing enabled consent, a current
owner/grant, current catalog identity and the unchanged full-parent proof.

## One loop, existing authority

After the existing projection and thinning phases, the worker runs maintenance
at the next reachable boundary after 30 seconds. Each round services one source,
selected by the oldest durable maintenance timestamp. A source that has lost its
owner grant advances in scheduling without archive reads or clearing, so it
cannot starve the other sources. There is no new service or work queue.

Migration 078 adds four small operational fields to the existing source policy:
reconciliation cursor, last scheduled time, invocation epoch and current parent
intent. Parent authorization and cumulative clearing counters stay exclusively
in the existing retirement progress ledger.

A bounded catalog page retries publication enrollment missed because of a lock
or deadline. The cursor survives restart, advances past deferred parents and
wraps after the source tail. Current policy/owner checks run again at every
positive enrollment. Disabling the policy stops new enrollment and rebinds;
previous finite parent approvals remain independent. Per-parent disable/restore
stops that parent's clearing. `--retirement off` stops this worker's maintenance.

## Crash recovery

The existing runner claims a parent by incrementing its scope epoch under its
progress-row lock. Before reading any archive or clearing, a small callback saves
that exact parent, epoch, catalog identity and starting counters in the source
policy. A failed intent write stops the invocation before that parent clears.

A crash or lost clear response leaves that intent intact. The next round reads
the exact catalog and progress under shared locks. It reports observed counter
deltas separately from this round's cleared work and preserves uncertainty about
the old response. It refuses a still-locked row, invalid intent or unexplained
epoch/counter regression. Catalog replacement or revoked consent is observed
without reenabling it.

Once current state is readable, a fresh normal claim and whole-parent proof can
resume automatically. If the old clear already holds the progress lock, recovery
waits for a later round. If the new claim happens first, its epoch prevents the
old proof from committing. No operator approval is needed for ordinary worker
crash recovery. Source invocation epochs also prevent a stale worker from
replacing a newer worker's intent or cursor.

## Budgets, metrics and rollout

The default invocation uses at most 10 parents, 64 MiB of clearing and 256 MiB of
archive reads. The default deadline is 10 seconds, at most 5 while freshness
work is pending; `--retirement-seconds` configures the idle deadline. Reconciliation
gets at most one second and retains a margin to commit its cursor. Full-parent
proof and transport deadlines remain active, SDK retries remain disabled for
maintenance archive reads. The existing SQL pool has a separate five-second
connection wait, so saturation can extend wall time beyond the round deadline.
Archive transport also retains its existing cooperative deadline limitations
around DNS and a continuously streaming response. Late results are rejected;
these bounds are not a strict wall-clock interrupt. Clearing stops when its invocation byte budget
cannot fit another whole document. Limits bound a scheduling round, not stored
evidence or which parents may eventually be proved.

Cycle logs expose status, cleared documents/UTF8 bytes, archive bytes, enrolled
parents, reconciled intents, unknown outcomes and elapsed time. Durable ledger
counters are the authoritative cumulative measure. They measure logical body
removal, not physical SQL disk reclamation; restore/revision writes do not
subtract from them.

Deploy optional-078-compatible binaries before applying 078, which requires 076
(and therefore 075). Keep retirement off while running the existing nonoverlap
worker cutover. Apply the migration in a bounded window, then activate only after
checking the selected source policies and a known parent. Turning the command
back off is the scheduling rollback; do not remove schema markers or reset
parent holds. Existing approved finite jobs must not overlap activation.

Local proofs cover both clear/claim lock orders, crashes before proof and after
commit, policy-off versus finite consent, revoked owners, catalog recreation,
source/cursor fairness, CLI deadline transport and projection-loop cadence.
Production acceptance is still required: cleared bytes must exceed incoming plus
historically restored bytes over sustained windows, with ingest lag, search
latency, database headroom/replication lag and archive read costs within bounds.
Long projection phases still delay the next reachable maintenance boundary.
Historical revision hydration and physical database downsizing remain separate
work; this change makes no claim that either is complete.
