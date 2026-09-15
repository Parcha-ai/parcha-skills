#!/bin/bash
# github-branch adapter (default) - commit an image to an orphan pr-assets branch
# in the same repository and print a URL that renders in a PR body for anyone
# with repository access. Nothing leaves the repository's own GitHub remote.
#
# Usage: ./github-branch.sh <file>
# Output: https://github.com/<owner>/<repo>/blob/<branch>/<file>?raw=true (stdout)
#
# Environment (all optional):
#   PR_ASSETS_BRANCH   Branch to commit to. Default: pr-assets/<pr-number> when the
#                      current branch has an open PR (via gh), else pr-assets/<branch-slug>.
#   PR_NUMBER          PR number to use for the default branch name (skips the gh lookup).
#   PR_ASSETS_REMOTE   Remote to push to. Default: origin.
#
# Implementation: git plumbing only, no worktree and no checkout. A temporary index is
# populated from the existing branch tree (if any), the blob is added, a commit is created
# with git commit-tree, and the commit is pushed to refs/heads/<branch>. The working tree
# and the caller's index are never touched.

set -euo pipefail

FILE="${1:-}"

if [[ -z "$FILE" ]]; then
    echo "Usage: $0 <file>" >&2
    exit 1
fi

if [[ ! -f "$FILE" ]]; then
    echo "Error: File not found: $FILE" >&2
    exit 1
fi

FILE="$(cd "$(dirname "$FILE")" && pwd)/$(basename "$FILE")"
NAME="$(basename "$FILE")"
REMOTE="${PR_ASSETS_REMOTE:-origin}"

if ! git rev-parse --show-toplevel >/dev/null 2>&1; then
    echo "Error: run from inside the git repository the PR belongs to" >&2
    exit 1
fi

REMOTE_URL="$(git config --get "remote.$REMOTE.url" || true)"
OWNER_REPO="$(printf '%s' "$REMOTE_URL" \
    | sed -E 's#^(https://github\.com/|git@github\.com:|ssh://git@github\.com/)##; s#\.git$##; s#/$##')"

if [[ ! "$OWNER_REPO" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]]; then
    echo "Error: remote '$REMOTE' is not a github.com repository: $REMOTE_URL" >&2
    exit 1
fi

if [[ -n "${PR_ASSETS_BRANCH:-}" ]]; then
    BRANCH="$PR_ASSETS_BRANCH"
else
    PR_NUMBER="${PR_NUMBER:-}"
    if [[ -z "$PR_NUMBER" ]] && command -v gh >/dev/null 2>&1; then
        PR_NUMBER="$(gh pr view --json number -q .number 2>/dev/null || true)"
    fi
    if [[ -n "$PR_NUMBER" ]]; then
        BRANCH="pr-assets/$PR_NUMBER"
    else
        SLUG="$(git rev-parse --abbrev-ref HEAD | tr -c 'A-Za-z0-9\n' '-' | sed -E 's/-{2,}/-/g; s/^-//; s/-$//')"
        if [[ -z "$SLUG" || "$SLUG" == "HEAD" ]]; then
            echo "Error: detached HEAD and no PR_NUMBER; set PR_ASSETS_BRANCH explicitly" >&2
            exit 1
        fi
        BRANCH="pr-assets/$SLUG"
    fi
fi

# Existing branch tip, if the branch already exists on the remote.
PARENT=""
if git fetch -q "$REMOTE" "refs/heads/$BRANCH:refs/remotes/$REMOTE/$BRANCH" 2>/dev/null; then
    PARENT="$(git rev-parse "refs/remotes/$REMOTE/$BRANCH")"
fi

# Temporary index: git needs the path to be absent or a valid index, not an empty file.
TMP_INDEX="$(mktemp)"
rm -f "$TMP_INDEX"
trap 'rm -f "$TMP_INDEX"' EXIT
export GIT_INDEX_FILE="$TMP_INDEX"

if [[ -n "$PARENT" ]]; then
    git read-tree "$PARENT"
else
    git read-tree --empty
fi

BLOB="$(git hash-object -w -- "$FILE")"
git update-index --add --cacheinfo "100644,$BLOB,$NAME"
TREE="$(git write-tree)"
unset GIT_INDEX_FILE

if [[ -n "$PARENT" ]]; then
    COMMIT="$(git commit-tree "$TREE" -p "$PARENT" -m "Add $NAME")"
else
    COMMIT="$(git commit-tree "$TREE" -m "PR assets: $NAME")"
fi

echo "Pushing $NAME to $REMOTE $BRANCH" >&2
git push -q "$REMOTE" "$COMMIT:refs/heads/$BRANCH"

echo "https://github.com/$OWNER_REPO/blob/$BRANCH/$NAME?raw=true"
