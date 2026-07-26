# ATI brain-turn artifact

`grep_ati_brain_turn.mjs` is the standalone `brain-turn` bundle built from
`Parcha-ai/ati-harness` commit
`b6a05916b2e2833da5d8ae81b2cbd8fb845336a7`.

Build command:

```text
npm ci
npm run build:brain-turn
```

SHA-256:

```text
a63283ab81a75d48afc89a513b7c5469750fb7ac476c2a8aa218883fe0b8e713
```

Recall verifies these bytes again before every agent turn. Update the source
commit, bundle, digest, tests, and deployment configuration together.
