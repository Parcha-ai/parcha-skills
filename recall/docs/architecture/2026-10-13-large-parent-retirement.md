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

The continuous worker defaults to a 180-second child work budget. It stops
admitting clear batches at 170 seconds, gives the owner until 175 seconds, and
retains the last five seconds for maintenance-intent bookkeeping. That leaves
about 37 seconds beyond the witnessed proof for admitting bounded clears.
Short synchronous rounds reserve one quarter of their budget for each finishing
phase. The existing cooperative stop hook leaves uncommitted rows intact; slow
or failed commits still retain their original unknown-outcome classification. This is evidence
for one committed parent, not a universal completion guarantee. The invocation
now clears at most 512 MiB; its existing 8 MiB/5-second commit bounds remain.
The prior 64 MiB measurements above remain the comparison baseline.
Repeated full proofs and one-minute parent cooldown affect throughput.

Before activation, check the worker's memory allowance against the measured
246 MiB child plus its foreground publisher, and allow one additional database
connection. Use the separately reviewed worker cutover procedure, then witness
actual clearing, foreground latency and replication lag. No production drain
rate or physical storage reduction is claimed from the read-only measurement.

## Clearing more after the same proof

Changing only the invocation clear budget from 64 to 512 MiB let the same local
512 MiB parent finish in one proof and 64 original eight-MiB transactions:

| Measurement | 512 MiB invocation budget |
| --- | ---: |
| Active retirement wall time | 15.813 seconds |
| Archive bytes read | 538,875,922 |
| Child CPU time | 6.962 seconds |
| Peak child memory | 81,796 KiB |
| Foreground SQL probes / p95 | 245 / 3.408 ms |
| Foreground publication | 88.7 ms |
| Maximum SQL connections | 1 |
| Failed parents / unknown outcomes | 0 / 0 |

Setup took 97.6 seconds; the full measurement took 115.0 seconds. This was a local
synthetic run, not a production throughput result. A retained 72 MiB integration
case first stopped at the old 64 MiB limit, then cleared the parent in one proof
through the actual child after the constructor change.

The production canary remains limited to the existing enabled source policy,
one child, the 180-second deadline and the existing commit/proof owners. Before
activation, record the worker memory, public read/search health and replication
lag. Observe the exact durable parent intent and cleared/archive counters over
complete rounds; inspect unknown outcomes, foreground latency and memory during
proof and clearing. Admit the larger budget only with replication lag at most
10 seconds and the reviewed headroom checks satisfied. Stop further rounds if
those gates fail, child memory exhausts the measured margin, or unknown outcomes
repeat. A partial round is valid progress, not proof of convergence.

Rollback restores the 64 MiB child constructor via the existing worker cutover.
It does not reset consent, epochs, archive locators or committed progress. The
synchronous maintenance default remains 64 MiB; no global operator limit changes.
