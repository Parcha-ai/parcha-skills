# Shared archive reference cleanup

A failed revision-bundle publisher previously deleted every object it had
uploaded. Objects are deterministic: another publisher can commit the same
object before that failure. Separately, logical cleanup checked only logical
manifests, parts and Parquet shards, so it could delete objects still owned by
revision bundles or raw artifacts.

Failed bundle publication now adds returned references to the existing durable
cleanup queue. Cleanup takes the existing artifact advisory lock without waiting
and then checks all five current reference tables in a fresh statement snapshot.
Contended artifacts stay queued. Protected artifacts are acknowledged without
object deletion; unreferenced objects retain the existing bounded delete/retry
path. The nonblocking lock avoids a queue-row/artifact-lock order deadlock with
admission owners. No schema, archive representation or ingestion HEAD call is
added.

Real PostgreSQL tests cover both missing reference owners, a losing publisher
whose concurrent winner already committed, artifact-lock contention, an owner
committing before cleanup acquires its lock, and failed unowned upload cleanup.
The retained skewed catalogue test still compares exact decisions with and
without the three existing artifact indexes and preserves its index-validity
and cross-tenant/source assertions.

This patch does not close the interval between upload and catalogue admission
for every publisher. Several existing writers do not yet use the artifact lock;
therefore this is not sufficient authority to enable historical archive writes.
That next change needs coordinated admission/cleanup ownership and measured
metadata-verification cost. Existing direct revision-bundle forget deletion and
uploads whose response is lost also require that separate lifecycle review.
If cleanup enqueue itself fails, the object is retained and the operation fails;
no successful cleanup receipt is claimed.

Before deployment, check the revision-bundle catalogue size and scoped artifact
lookup plan: schema035 lacks the artifact index supplied by074 to the other
projection catalogues. Do not enable large-scale historical bundle publication
without resolving that lookup cost.
