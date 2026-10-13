# Large-parent retirement without blocking projection

The existing worker gives retirement five seconds while projection is busy and
256 MiB of archive reads. A 500–590 MB parent cannot finish its mandatory full
proof within the byte budget. Repeated attempts leave its SQL bodies in place.

The continuous worker now supervises one short-lived retirement process. The
main loop polls it without waiting for proof. The child owns at most one SQL
connection and releases every SQL lease before archive reads. It uses the same
policy, parent ledger, claim epoch, proof and clear owners. There is no new queue,
service, table or source of permission. `--once` retains the synchronous path.
Existing logical publication heartbeats and the end-of-cycle boundary share a
30-second retirement cadence. A long logical batch therefore launches and reaps
the child before returning. Completed-child counters accumulate across heartbeats
and survive a later failed publication cycle until they can be reported.

For each claimed parent, a read-only snapshot checks the exact catalogue identity
and sums its committed part sizes. That sum supplies the proof's archive budget.
The existing verifier's 64 GiB maximum remains; larger parents remain unhandled,
with their bodies intact. Changing this existing resource boundary requires a
separate verifier review, not an ingestion rejection or silent truncation.

The child has its own wall-clock alarm, including when its supervisor dies.
SIGTERM shuts down and reaps the child. A failed kill/wait retains ownership, so
another invocation cannot overlap accidentally. Missing or malformed output is
an unknown outcome. Child failures expose only a bounded error class and phase,
never raw exception text or connection credentials. Existing durable intent and epoch reconciliation observe
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

An exact production read-only proof of a committed 590,450,307-byte parent
(138 parts, 46,602 records) completed in 132.474 seconds: 47.051 seconds of
archive reads, 71.981 seconds across 29 SQL leases, and 21.048 CPU seconds.
Peak child memory was 252,052 KiB; at most one SQL connection was leased, and no
SQL lease overlapped archive body reads. The unchanged verifier found 46,567
eligible documents containing 341,760,838 UTF-8 bytes. It retained 35 oversized
and 7,537 structural exclusions. The probe cleared nothing.

The continuous worker defaults to a 180-second child work budget, leaving about
47 seconds beyond that witnessed proof for bounded clearing. This is evidence
for one committed parent, not a universal completion guarantee. The invocation
still clears at most 64 MiB; its existing 8 MiB/5-second commit bounds remain.
Repeated full proofs and one-minute parent cooldown affect throughput.

Before activation, check the worker's memory allowance against the measured
246 MiB child plus its foreground publisher, and allow one additional database
connection. Use the separately reviewed worker cutover procedure, then witness
actual clearing, foreground latency and replication lag. No production drain
rate or physical storage reduction is claimed from the read-only measurement.
