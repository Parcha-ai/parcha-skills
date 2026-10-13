# Stop obsolete logical input streams early

On October 13, the live worker spent 327.9 seconds preparing a 517.9 MB parent
before discarding it as stale. The existing queue checks ran before preparation,
after SQL input spooling, during archive recovery/encoding, and at commit. None
ran while PostgreSQL streamed the parent into the private input spool.

The worker now checks the existing queue generation and timestamp at the existing
five-second progress interval while consuming SQL rows. It reuses the cursor's
connection. Under READ COMMITTED, the queue SELECT sees committed changes while
the named cursor retains its original source snapshot. It does not borrow a
second connection when other workers also own streams.

Append or forget supersession closes the cursor and private spool and follows
the existing retryable-stale path. It performs no archive upload, does not mark
the source empty, and does not penalize the newer queue generation. Direct
multi-parent preparation retains its existing behavior. Archive verification
and the final publication CAS are unchanged.

Three PostgreSQL 17 regressions failed before the change and pass afterward:
append and forget stop after the mutation becomes visible instead of reading
the remaining parent; unchanged input retains every record and rearms the
interval. The tests refuse an extra connection checkout and check spool closure.

The interval is checked between returned rows; a slow FETCH can still exceed
five seconds. This reduces wasted obsolete reads. It does not make an actively
changing large parent converge or establish a production latency improvement.
Incremental reuse of verified archive parts remains a separate architecture task.
