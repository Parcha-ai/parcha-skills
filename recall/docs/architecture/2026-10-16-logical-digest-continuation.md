# Reuse committed SHA state during logical append

Appending a small suffix previously reread every archived prefix part to rebuild the whole-document SHA256. The logical worker now retains at most 64 process-local hash continuations on its existing projection store. A continuation is admitted only after acknowledged native publication. Each append copies the state; concurrent attempts never mutate a shared digest.

The key includes tenant, source, parent, exact manifest, ordered artifact references, part geometry and packing size. Each use rechecks the manifest and verifies the final part. Missing, evicted, changed or inconsistent state uses the original full-prefix verification. Restart loses the cache. No state is serialized, no body is cached, and no schema or service is added. Existing publication generation, Forget and source authority checks remain authoritative.

The whole-document hash, immutable part bytes, manifest and publication identities match the full builder. Repeated native append tests use that full builder as an independent byte oracle, including a 64-record parent with more than 20 prefix parts. Warm append reads only the final part.

There is one deliberate integrity tradeoff: warm append does not re-audit untouched interior objects. If an old object is corrupted or removed after publication, the unchanged public reader rejects it when accessed; cold append also detects it. Manifest and final-part corruption still reject warm append immediately. The native test preserves this distinction by corrupting an interior object, completing a warm append, and requiring public `show` to fail closed.

Stale, failed, adopted, repaired and unknown-commit attempts do not admit new state. This optimization removes repeated prefix archive reads; it does not eliminate the separate full-parent actor derivation or establish a production freshness percentile.
