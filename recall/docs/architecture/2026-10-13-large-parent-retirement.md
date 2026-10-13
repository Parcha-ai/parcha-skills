# Large-parent retirement without blocking projection

The existing worker gives retirement five seconds while projection is busy and
256 MiB of archive reads. A 500–590 MB parent cannot finish its mandatory full
proof within the byte budget. Repeated attempts leave its SQL bodies in place.

The continuous worker now supervises one short-lived retirement process. The
main loop polls it without waiting for proof. The child owns at most one SQL
connection and releases every SQL lease before archive reads. It uses the same
policy, parent ledger, claim epoch, proof and clear owners. There is no new queue,
service, table or source of permission. `--once` retains the synchronous path.

For each claimed parent, a read-only snapshot checks the exact catalogue identity
and sums its committed part sizes. That sum supplies the proof's archive budget.
The existing verifier's 64 GiB maximum remains; larger parents remain unhandled,
with their bodies intact. Changing this existing resource boundary requires a
separate verifier review, not an ingestion rejection or silent truncation.

The child has its own wall-clock alarm, including when its supervisor dies.
SIGTERM shuts down and reaps the child. A failed kill/wait retains ownership, so
another invocation cannot overlap accidentally. Missing or malformed output is
an unknown outcome. Existing durable intent and epoch reconciliation observe
committed progress before a fresh proof; a process exit never proves commitment.

## Witnessed local measurements

A PostgreSQL fixture published 128 four-MiB records through the real logical
publisher. SQL text was deliberately compressible; archive reads and verifier
work covered the actual bytes. The test generated one record at a time.

| Measurement | Result |
| --- | ---: |
| Parent body bytes cleared | 512 MiB |
| Bounded rounds | 9 |
| Active retirement wall time | 98.308 seconds |
| Child CPU time across rounds | 70.395 seconds |
| Peak child memory | 82,160 KiB |
| Archive bytes read across rounds | 4,849,883,912 |
| Maximum retirement SQL connections | 1 |
| Concurrent foreground SQL probes | 1,470 |
| Worst round's foreground query p95 | 4.013 ms |
| Foreground source publication during proof | 102.8 ms |
| Failed parents / unknown outcomes | 0 / 0 |

Two small foreground publications account for 38 additional cleared bytes and
caused the ninth round. Each invocation retained the existing 64 MiB clear
budget. Every round proved the complete parent again; this explains the read
amplification. Fixture setup took 97.2 seconds and the full test took 201.2
seconds. Scheduling cooldowns were advanced in the fixture, so these are active
work measurements, not production throughput.

An initial memory assertion used `getrusage().ru_maxrss`, which retained the
publisher parent's pre-exec high-water mark. A separate control measured a
175,852 KiB inherited value against the fresh child's 10,520 KiB `VmHWM`.
Child metrics now read `/proc/self/status`, without changing the memory threshold
or any verifier code.

## Verification and rollout boundary

Real PostgreSQL tests cover owner revocation and catalogue replacement during
proof, no SQL lease during archive reads, foreground publication, killed-child
recovery, and the retained owner concurrency/unknown-commit cases. Subprocess
tests cover SIGTERM, SIGKILL of the supervisor, failed reaping, configuration
transport failure and malformed results. The existing parent proof and chunk
retirement verifier files are unchanged.

Run the explicit large fixture against disposable PostgreSQL with:

```sh
RECALL_LARGE_RETIREMENT_TEST=1 PYTHONPATH=recall:recall/server \
  python recall/server/tests/e2e_retirement_maintenance.py \
  MaintenanceTests.test_512mib_parent_makes_bounded_progress_until_complete
```

The worker's existing default budget is still ten seconds. This is insufficient
for the measured large parent and is not a production convergence claim. Before
activation, use an exact hosted read-only proof to measure R2/SQL timing, choose
an explicit work budget, and check foreground latency and replication lag while
clearing. The separately reviewed worker cutover procedure still applies. The
existing one-minute parent cooldown and worker polling cadence also affect the
achievable drain rate.
