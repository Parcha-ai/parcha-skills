---
name: before-after
description: Capture paired base-versus-branch screenshots of a page or element and emit a "Before / After" markdown section for a PR body, with the images committed to a pr-assets branch in the same repository. Use when the user says "before and after", "PR screenshots", "screenshot comparison", "visual diff", or "show the change" for a UI change. Accepts two URLs (file://, http://, https://) or existing image pairs.
license: PolyForm-Shield-1.0.0
---

# Before / after

This skill has one job: produce paired "before" and "after" captures of the same page or
element, publish them where the PR can show them, and emit the markdown section for the PR
body. It presents a change without judging it. It does not decide whether the change is
correct or whether anything passed or failed; that is `autoqa`'s job. It does not analyze what
the change could break; that is `blast-radius`'s job. It never fabricates a "before" by
switching branches, stashing, or starting a second dev server.

## Rules

- The visual is required. For any change with a UI surface (a page, a component, or a rendered
  template), a real image pair is mandatory; a text-only "Before / After" table does not satisfy
  this skill, and a `## Before / After` section a UI change ships without images is incomplete.
- Drive the browser through the repo's existing Stagehand-based browser automation (the
  `agent-browser` integration that `scripts/capture.sh` calls, such as the sandbox `browser-mcp`
  facade). Do not hand-roll a separate headless browser or a local static server to take the shot.
- "Before" is a real running deployment of the base branch: the main preview, staging, or
  production. "After" is the branch: its preview deployment or the local run of the current
  checkout. Never switch git branches, stash changes, or start a dev server to manufacture
  either side.
- If the user gives one URL, or says "PR screenshots" without URLs, ask for the other side:
  "What URL should I use for the 'before' state? (main preview, staging, or production)". Do not
  guess.
- Assume the current checkout is "after". Record its commit SHA.
- Do not use `--full` (full-scroll capture) unless the user asks for it.
- Use `--mobile` or `--tablet` when the user mentions phones, tablets, or responsive layout.
- Never upload a capture to a public host by default. The default adapter commits the PNGs to
  the same repository; the `gist` and `blob` adapters are opt-in and the user must name one.
- The section lists the two source URLs and the commit SHA of "after" so a reader can
  reproduce the pair.

## Steps

1. Pre-flight. `which before-and-after || npm install -g @vercel/before-and-after`. The
   package name is `@vercel/before-and-after`; the bare `before-and-after` npm package is a
   different project. If the CLI is unavailable, `scripts/capture.sh` takes one URL and one
   output path and captures with `agent-browser`.
2. Protection check. For a `.vercel.app` or otherwise gated URL, run
   `curl -s -o /dev/null -w "%{http_code}" "<url>"`. A 401 or 403 means the deployment is
   protected; see "Protected deployments".
3. Capture one pair per view. Name files `<label>-before.png` and `<label>-after.png` so
   the emitter can label the rows.

   ```bash
   before-and-after "<before-url>" "<after-url>" -o ./captures            # page
   before-and-after "<before-url>" "<after-url>" ".hero" -o ./captures    # element
   before-and-after "<before-url>" "<after-url>" --mobile -o ./captures   # 375x812
   ```

4. Publish and emit. Run the emitter with both source URLs. It uploads every pair through the
   adapter, prints the markdown section, and copies it to the clipboard when one exists.

   ```bash
   scripts/upload-and-copy.sh --markdown \
     --before-url "<before-url>" --after-url "<after-url>" \
     captures/home-before.png captures/home-after.png \
     --label "Settings page" captures/settings-before.png captures/settings-after.png
   ```

   Pass `--after-sha <sha>` when the checkout is not the "after" commit. `--label` applies to
   the next pair; without it the label is derived from the "before" file name.
5. Put the section in the PR body. If a `## Before / After` section already exists, replace
   it; otherwise append it. With `gh`:

   ```bash
   gh pr view <number> --json body -q .body > /tmp/pr-body.md
   # edit /tmp/pr-body.md so the section appears exactly once
   gh pr edit <number> --body-file /tmp/pr-body.md
   ```

   Without `gh`, print the section and ask the user to paste it.

## Output

The emitter writes this shape, one row per pair:

```markdown
## Before / After

| | Before | After |
|:--|:------:|:-----:|
| home | ![home before](<before-url>) | ![home after](<after-url>) |

Before: https://main.example.dev | After: https://pr-42.example.dev at `<after-sha>`
```

## Adapters

`IMAGE_ADAPTER` selects where the PNGs go. Each adapter takes one file and prints one URL.

| Adapter | Default | Where the image lives | Who can see it |
|---|---|---|---|
| `github-branch` | yes | Orphan branch `pr-assets/<pr-number>` (or `pr-assets/<branch-slug>` before a PR exists) in the same repository, pushed to `origin` with git plumbing; no worktree, no checkout, the caller's index is untouched | Anyone with access to the repository. The URL is `https://github.com/<owner>/<repo>/blob/pr-assets/<n>/<file>.png?raw=true`, which renders in PR bodies |
| `gist` | opt-in | A secret gist per file via `gh gist create` | Anyone who has the raw URL |
| `blob` | opt-in | A custom endpoint from `BLOB_UPLOAD_URL` | Whatever that endpoint allows |

`github-branch` reads `PR_NUMBER` or asks `gh pr view` for it, and accepts `PR_ASSETS_BRANCH`
and `PR_ASSETS_REMOTE` overrides. It needs push access to the repository, which the caller's
existing git credentials supply; the skill never mints tokens.

## Protected deployments

For a Vercel deployment that returns 401 or 403:

1. `which vercel && vercel whoami` to see whether the Vercel CLI is signed in.
2. If it is, `vercel inspect <url>` to obtain a protection bypass token and pass it to the
   capture.
3. If it is not, ask the user for a bypass token, or for screenshots taken by hand, or to
   disable protection for the preview.

For any other gated URL, ask the user how to authenticate; do not guess credentials.

## Errors

| Symptom | Fix |
|---|---|
| `command not found` | `npm install -g @vercel/before-and-after` |
| `could not determine executable` | Run `npx @vercel/before-and-after` with the full package name |
| 401 or 403 on the URL | See "Protected deployments" |
| Element not found | Confirm the selector exists on both pages |
| `remote 'origin' is not a github.com repository` | Set `PR_ASSETS_REMOTE` to the GitHub remote, or pick another adapter |
| Push rejected on the pr-assets branch | Someone pushed to the branch between fetch and push; rerun the emitter |
