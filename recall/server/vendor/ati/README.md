# ATI brain-turn artifact

`grep_ati_brain_turn.mjs` is a proprietary agent bundle. It is **not
committed** to this repository and is ignored by `.gitignore`.

The runtime never assumes the bundle is present. A deployment opts in by
setting the environment contract (see `server/deploy/service.env.example`):

```text
RECALL_ATI_COMMAND_JSON=["node","/opt/ati/grep_ati_brain_turn.mjs"]
RECALL_ATI_ARTIFACT_PATH=/opt/ati/grep_ati_brain_turn.mjs
RECALL_ATI_ARTIFACT_SHA256=<lowercase-sha256>
```

Recall verifies the artifact bytes against `RECALL_ATI_ARTIFACT_SHA256`
before every agent turn, so a wrong or tampered bundle fails closed.

## How the bundle reaches an image

`scripts/fetch_ati_bundle.sh` builds the bundle from its private source and
installs it at this path. It is driven entirely by environment variables
(`ATI_BUNDLE_REPO`, `ATI_BUNDLE_REF`, `ATI_BUNDLE_SHA256`,
`ATI_BUNDLE_TOKEN`) so no private source location is recorded in this
repository. The `recall-image` publish workflow runs it when those secrets
are configured; images built without them (for example CI pull-request
builds) simply omit the bundle and the ATI agent stays disabled.

A deployment may instead mount the artifact into the container at
`/opt/ati/` — the runtime digest check applies either way.
