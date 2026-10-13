# Repair retained passage receipts through their publisher

The October 13 read-only inventory sampled two passages from each of 205
nominated or previously selected documents. Four rows across two `codex:linux:m`
parents contained ten stale receipt references despite matching revision 2,
matching source content, and no passage queue:

- `ldoc_07f5119639c7648a6c026f412885aa80`: seven stale references in two rows.
- `ldoc_33f39a3400ccf31a2717e05f7e90bd43`: three in two rows.

Inventory SHA256:
`6be05133619e27457ba95d9cfab73d4a8cfb28d73e259585adabef683c21595d`.
This sample is not an exhaustive corpus audit. Neither parent is a nominated
answer boundary in the optimize-25 truth set. Repair is a coverage correction,
not a demonstrated ranking improvement.

## Existing owner, explicit admission

`seed_backfill()` skips already-current projection revisions. Logical
same-manifest repair updates an existing passage notification but does not insert
a missing queue item. Neither path repairs this inactive receipt defect.

`CanonicalPassageProjector.requeue_receipt_repair()` accepts one frozen stale
witness and queues the existing passage publisher. It requires the exact source
owner and owner grant, or an explicit source grant plus the existing company-admin
predicate from `ControlPlane._brain_admin`. That predicate requires both the
organization membership and the tenant permission to be owner/admin. A source
read grant alone never permits maintenance. Shared locks preserve those grants
through queue commit. The method also checks catalog creation identity, revision, content and manifest,
current passage policy, passage text hash, and receipt-array digest. Shared row
locks keep the selected source/grant/catalog stable during admission. A pending
logical or passage projection, changed catalog, clean witness, or policy mismatch
refuses admission. The insert never replaces an existing queue generation.

The method writes no receipts or bodies and makes no archive or model calls.
Normal `_prepare`/`_commit`, archive verification, generation fences, and search
receipt liveness remain the authorities. Retirement markers are untouched.
The retained PostgreSQL regression follows same-revision queue admission through
the actual publisher and checks retained passage identity, refreshed receipts,
zero passage inserts/deletes, and zero additional vendor writes. Additional cases
cover admin admission, role/access/source revocation before admission, and
concurrent revocation blocked until the queue transaction ends.

This is one maintenance method, not a background repair policy or new runtime
operator. Private one-off plan/observation packets read exact scoped metadata;
the existing `RenderJob` transport and durable journal helpers own remote
execution. No permanent repair CLI or separate receipt-writing owner is added.

## Supervised two-parent lifecycle

1. Resolve the acting principal through the existing credential owner, then
   verify its current source/admin authority. A read-scoped credential establishes
   identity; the supervised operator is separately authorized to do maintenance.
   Never substitute the source owner's identity. Read and freeze one positive
   witness per parent, with the original inventory
   hash and complete current catalog proof. Review those plans before admission.
2. Bind the exact package body, invocation nonce, deployment, and fleet checkpoint
   to create-once durable intent. Launch once through the existing guarded
   transport; an ambiguous response never triggers another admission attempt.
3. Process the ordinary passage queue. Record actual publisher counters; the
   fixture's zero-vendor-write result is not a blanket promise for all historical
   passages under a parent.
4. If the original job result is unavailable, verify that exact terminal job's
   body and nonce with GET only, then observe current state separately. Preserve
   the original unknown outcome. A live witness proves current witness state,
   not original execution or correctness of every passage.
5. Recheck all four sampled rows and ordinary source opens before running the
   frozen default-10 search trace. Report data repair separately from algorithm
   changes, preserving all earlier scores.

No production admission has been launched from this change.
