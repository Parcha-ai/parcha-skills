# Reuse committed evidence prefixes for append-only publication

The logical publisher currently reads and rebuilds a whole session after each new
turn. This optional reader uses schema079's exact queue-generation and manifest
binding to read only newly appended SQL bodies. It keeps the existing archive
builder, publication transaction, citations and stale-generation checks.

## Behavior and ownership

`projection-worker --logical-append` opts in; the default is off. Missing schema079,
missing frontier, a late/tied/NULL ordinal, an edit, a tombstone, an authority
change or an unknown writer takes the existing full reader. A source's frontier
covers all canonical inputs, including excluded structural records. Publication
with custom structural exclusions withholds the frontier, so a later default
publisher must rebuild the full session.

The existing worker already prepares one parent per invocation. For an eligible
parent, its original SQL query receives a strict ordinal-greater-than-frontier
predicate. All original source, document, chunk and actor joins remain. Old
NULL locator pairs are read separately as metadata so the existing locator
repair/reset race check survives. This query does not load old body, chunk or
raw-event payloads.

The existing archive builder reads and verifies the old manifest and every old
part, feeds those exact bytes into the full-document hash, borrows complete
parts, and resumes its normal buffer with the old last part. Only new records
are encoded. Part boundaries, receipts, times, document hash and manifest bytes
must equal a full build. Archive body reads hold no database lease. Existing
periodic supersession checks can stop prefix processing; archive reads retain
the existing provider timeout behavior.

The final transaction keeps the original queue generation/changed-at check and
also requires the exact base manifest used by the append attempt. Forget, newer
writes and catalog replacement cannot publish stale output. Lost COMMIT
acknowledgment remains unknown; shared catalog-aware cleanup protects objects
that did commit. No retirement verifier changes are included.

## Local evidence

* Real PostgreSQL: eight prefix inputs plus two new inputs returned only the two
  new bodies. Actual chunk lookup loops fell from ten to two. Full and append
  prepared metadata, manifest, part references and locator intents matched.
* Twenty-six PostgreSQL cases cover the capability fallback matrix, custom
  exclusions, schema-marker removal, structural-only appends, multi-segment Unicode bodies, cold retired chunks, missing archive
  objects, lease-free archive body reads, catalog replacement, concurrent append,
  forget and lost acknowledgment.
* A 100,000-record prefix (97,266,670 bytes in 24 parts) plus ten new records
  produced identical bytes and references. The local builder took 3.025 seconds
  for the full path and 0.322 seconds for append, with process peak RSS 245,060 KiB.
  This uses a local synthetic archive, not production database or network timing.
* Opt-in real PostgreSQL measurement: 10,000 old records plus ten new records
  returned 10,010 rows/20,745,250 body bytes on full preparation versus ten
  rows/15,250 bytes on append, with zero old bodies. Actual chunk lookup loops
  fell from 10,010 to ten. Full preparation took 2.683 seconds versus 0.220 seconds;
  outputs matched and the unchanged commit succeeded. The archive document was
  23,506,830 bytes, process peak RSS 208,960 KiB, and fixture seeding took 78.773
  seconds. This is local PostgreSQL/filesystem evidence, not a hosted result.
* Retained full-publication, locator, supersession, archive-admission and original
  retirement-runner gates pass. The frozen fallback comparison passes in its
  isolated PostgreSQL fixture (148 shapes, 264 ordered rows, unchanged legacy
  hash and buffer threshold). An accumulated fixture database missed that
  performance threshold; its failure receipt remains retained.

## Activation and rollback

Do not apply schema079 or enable the flag solely because phase1 code is deployed.
Every publisher must first run this compatible implementation. Older publishers
can complete a same-manifest authority change without renewing the binding, so
mixed publishers are not safe for append activation.

After assembled CI and fleet compatibility, use one private synthetic native
session: full publication, append, exact full/append output and read comparison,
then forget. Observe a fixed production cohort before broader use: old SQL body
rows/bytes, prepare/commit latency, RSS, archive bytes and GET count, staleness,
queue lag and foreground health. Disable `--logical-append` before rolling any
publisher back. Schema079 may remain; the default reader still rebuilds fully.

This removes prefix SQL body hydration, decoding, encoding and re-chunking.
It still reads and hashes all prefix archive bytes, and still reads old NULL
locator metadata. Passage preparation is unchanged. Continually arriving turns
can still invalidate an attempt; no stale-generation check is bypassed and no
live convergence or latency improvement is claimed yet.
