# ATI brain-turn artifact

`grep_ati_brain_turn.mjs` is the standalone `brain-turn` bundle built from
`Parcha-ai/ati-harness` commit
`a0a86b7f9acd618e63773a9352e22dc209aa7f36`.

Build command:

```text
npm ci
npm run build:brain-turn
```

SHA-256:

```text
58b75f00c0abd120bd2cc2d2b6b291b239f060d3487c442b5c0648fbbd0d7041
```

Recall verifies these bytes again before every agent turn. Update the source
commit, bundle, digest, tests, and deployment configuration together.
