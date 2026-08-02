#!/usr/bin/env bash
# Build the proprietary ATI brain-turn bundle from its private source and
# install it at the target path. Configuration is environment-only so the
# private source location is never recorded in this repository:
#
#   ATI_BUNDLE_REPO    owner/name of the private source repository
#   ATI_BUNDLE_REF     commit SHA or ref to build
#   ATI_BUNDLE_SHA256  expected lowercase sha256 of the built bundle
#   ATI_BUNDLE_TOKEN   token with read access to ATI_BUNDLE_REPO
#
# Usage: fetch_ati_bundle.sh <target-path>
set -euo pipefail

target="${1:?usage: fetch_ati_bundle.sh <target-path>}"

for var in ATI_BUNDLE_REPO ATI_BUNDLE_REF ATI_BUNDLE_SHA256 ATI_BUNDLE_TOKEN; do
  if [[ -z "${!var:-}" ]]; then
    echo "fetch_ati_bundle: $var is not set" >&2
    exit 1
  fi
done

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

git -c credential.helper= -c "http.https://github.com/.extraheader=Authorization: basic $(printf 'x-access-token:%s' "$ATI_BUNDLE_TOKEN" | base64 | tr -d '\n')" \
  clone --quiet --no-tags "https://github.com/$ATI_BUNDLE_REPO" "$workdir/src"
git -C "$workdir/src" checkout --quiet "$ATI_BUNDLE_REF"

(cd "$workdir/src" && npm ci --silent && npm run --silent build:brain-turn)

bundle="$workdir/src/dist/grep_ati_brain_turn.mjs"
if [[ ! -f "$bundle" ]]; then
  bundle="$(find "$workdir/src" -name grep_ati_brain_turn.mjs -not -path '*/node_modules/*' | head -1)"
fi
if [[ -z "$bundle" || ! -f "$bundle" ]]; then
  echo "fetch_ati_bundle: build produced no grep_ati_brain_turn.mjs" >&2
  exit 1
fi

digest="$(shasum -a 256 "$bundle" 2>/dev/null | cut -d' ' -f1 || sha256sum "$bundle" | cut -d' ' -f1)"
if [[ "$digest" != "$ATI_BUNDLE_SHA256" ]]; then
  echo "fetch_ati_bundle: digest mismatch (built $digest)" >&2
  exit 1
fi

install -m 0444 "$bundle" "$target"
echo "fetch_ati_bundle: installed bundle at $target ($digest)"
