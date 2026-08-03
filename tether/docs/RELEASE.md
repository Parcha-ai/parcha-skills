# Tether Release Integrity

This is the release control for `@parcha/tether`. One approved commit produces
one inspected tarball. The same SHA-256-identified tarball is attested and
published unchanged to npm and GitHub.

## Trust boundaries

- The workflow runs only by manual dispatch from protected `main`.
- The operator supplies a signed annotated tag and the reviewed 40-character
  release commit.
- The dispatch SHA, current remote `main`, approved input, tag target, tested
  checkout, and packaged checkout must all be the same commit.
- A valid signature is insufficient by itself. The signing key must currently
  be registered to an account in `AUTHORIZED_RELEASE_SIGNERS` in
  `tether-release.yml`. The current explicit allowlist is `miguelrios`.
- The protected `npm-release` environment supplies the independent human
  approval before publication.
- npm receives only the tested workflow artifact. No workstation publishes.

Changes to the release workflow, signer allowlist, protected environment, npm
trusted publisher, or package manifest require normal review.

## Required repository controls

Do not release until all controls are active:

1. Protect `main`: require pull requests, review, all `tether-ci` checks,
   blocked force pushes, and blocked deletion.
2. Protect `tether-v*` tags. Restrict tag creation, update, and deletion to
   release maintainers. Tags must be signed and annotated; never move one.
3. Configure `npm-release` with a required reviewer, no self-review, no
   administrator bypass, and a deployment branch rule allowing only protected
   `main`.
4. Configure npm trusted publishing for package `@parcha/tether`, repository
   `Parcha-ai/parcha-skills`, workflow `tether-release.yml`, and environment
   `npm-release`. Remove `NPM_TOKEN` and disallow token-based publishing after
   OIDC is verified.
5. Enable GitHub private vulnerability reporting and route notifications to
   repository administrators or security managers.
6. Allow manual workflow dispatch only to release maintainers.

GitHub Actions are pinned by commit. Release validation uses Node `24.18.0` and
Python `3.14.0`; the publish job installs only the exact npm `12.0.1` client.
The identity job compares both JavaScript toolchain pins with the official
Node release index and npm registry, and fails when either pin is stale. The
artifact-producing job has a fresh exact commit checkout and installs no lint
or test tooling.

## Release procedure

1. Merge the version, lockfile, plugin metadata, and release notes to protected
   `main`. Keep the version identical everywhere it is represented. Do not
   update the Agent Plugins marketplace pin to the new commit yet; that would
   be a self-referential commit hash.
2. Confirm every `tether-ci` check is green on the exact `main` commit,
   including native `ubuntu-24.04-arm`. Run locally from `tether/`:

   ```bash
   npm ci
   npm test
   npm run pack:check
   ```

3. Fetch protected `main`. Confirm the signing key is registered to an
   allowlisted GitHub account, then create and push a signed annotated tag on
   that exact commit:

   ```bash
   git fetch origin main
   version="$(node -p 'require("./tether/package.json").version')"
   commit="$(git rev-parse origin/main)"
   git tag -s -a "tether-v$version" "$commit" -m "Tether $version"
   git push origin "refs/tags/tether-v$version"
   ```

4. Dispatch `tether-release.yml` from `main`, not from the tag or another
   branch:

   ```bash
   gh workflow run tether-release.yml \
     --ref main \
     -f tag="tether-v$version" \
     -f approved_commit="$commit"
   ```

5. Review the identity and test jobs. A different maintainer then approves the
   `npm-release` environment. The workflow attests and publishes the tested
   tarball to npm, then creates the matching immutable GitHub release.
6. Verify both registries and the attestation:

   ```bash
   gh release download "tether-v$version" --pattern '*.tgz'
   gh attestation verify "parcha-tether-$version.tgz" \
     --repo Parcha-ai/parcha-skills
   npm view "@parcha/tether@$version" version dist.integrity dist.tarball
   ```

A green read-only validation is not authority to approve publication.

## Workflow guarantees

The identity job fails unless:

- the selected workflow ref and dispatch SHA are protected `main`;
- `approved_commit` is a full SHA and still equals current remote `main`;
- the tag is annotated, matches the package version, and targets that SHA;
- GitHub verifies the signature;
- local Git verification succeeds using only current GitHub-registered GPG or
  SSH signing keys from the explicit account allowlist;
- the npm version does not already exist; and
- the marketplace source is a 40-character commit that is an ancestor of the
  release.

Tests run from an exact-SHA checkout. Packaging waits for those tests, starts
from another fresh exact-SHA checkout, and installs no third-party test tools.
`package.json#files` is an exact path manifest: no globs, exclusions, or
directory entries. The tarball test independently compares every archive
member with that manifest, rejects links and path traversal, rejects
secret/artifact filenames, scans contents for credentials, and runs the
install, upgrade, and uninstall lifecycle from the packed artifact.

The publish and GitHub release jobs download the one retained artifact,
recompute SHA-256, and reject extra files. npm publication uses OIDC and
provenance; GitHub receives the same tarball and checksum.

## Partial failure and recovery

The workflow rejects an npm version or GitHub release that already exists. It
never treats existing mutable state as success and never rebuilds a published
version.

- If publication fails before npm succeeds, inspect the failed run and rerun
  the same manual dispatch only after the cause is fixed. The identity checks
  will reject a moved tag or changed `main`.
- If npm succeeds but GitHub release creation fails, recover the exact tarball
  from the workflow artifact or npm. Verify its SHA-256 and attestation before
  creating the GitHub release manually. Do not rebuild or republish npm.
- If registry, artifact, checksum, or attestation values disagree, stop. Treat
  the mismatch as a security incident.

Record the failed run, checksums, reviewer, and recovery action in the release
notes.

## Rollback and yank policy

Published versions and release tags are immutable. Never overwrite an asset,
move a tag, or reuse a semantic version.

For an ordinary defect, deprecate the affected npm version with a specific
message, publish a corrected version, and document operator rollback steps.
Use npm unpublish only for a confirmed security, privacy, legal, or malicious
package event when npm policy permits it. Preserve a private incident record.

## Advance the marketplace pin

The marketplace must use a full commit SHA, never a branch or tag. A commit
cannot contain its own SHA, so the release commit retains the previous reviewed
release pin. After the release is verified:

```bash
git fetch origin "refs/tags/tether-v$version"
pin="$(git rev-parse "tether-v$version^{commit}")"
git merge-base --is-ancestor "$pin" origin/main
printf '%s\n' "$pin"
```

In a reviewed follow-up commit, replace
`tether/.agents/plugins/marketplace.json` `source.ref` with `pin`. Confirm that
commit contains `tether/package.json`. The next release requires this immutable
pin to remain an ancestor of its approved release commit.
