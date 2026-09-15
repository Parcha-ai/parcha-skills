#!/bin/bash
# upload-and-copy.sh - Publish capture pairs through a storage adapter and emit PR markdown
#
# Usage: ./upload-and-copy.sh [options] <before.png> <after.png> [<before2.png> <after2.png> ...]
#
# Options:
#   --markdown            Emit a "## Before / After" section (table, one row per pair,
#                         source URLs, and the "after" commit SHA) and copy it to the clipboard
#   --before-url <url>    URL the "before" captures came from (a running base deployment)
#   --after-url <url>     URL the "after" captures came from (the branch preview or local run)
#   --after-sha <sha>     Commit SHA of the "after" state (default: git rev-parse HEAD)
#   --label <text>        Label for the next pair (repeatable; default: derived from the file name)
#
# Environment:
#   IMAGE_ADAPTER    Storage adapter (default: github-branch)
#                    Available: github-branch, gist, blob
#
# Adapter-specific environment variables:
#   github-branch:  PR_ASSETS_BRANCH, PR_NUMBER, PR_ASSETS_REMOTE (see adapters/github-branch.sh)
#   blob:           BLOB_UPLOAD_URL - Custom upload endpoint
#
# Examples:
#   ./upload-and-copy.sh --markdown --before-url https://main.example.dev --after-url https://pr-42.example.dev \
#       home-before.png home-after.png
#   IMAGE_ADAPTER=gist ./upload-and-copy.sh --markdown home-before.png home-after.png

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADAPTERS_DIR="$SCRIPT_DIR/adapters"

IMAGE_ADAPTER="${IMAGE_ADAPTER:-github-branch}"
MARKDOWN_MODE=false
BEFORE_URL=""
AFTER_URL=""
AFTER_SHA=""
FILES=()
LABELS=()
PENDING_LABEL=""

usage() {
    echo "Usage: $0 [--markdown] [--before-url <url>] [--after-url <url>] [--after-sha <sha>] [--label <text>] <before.png> <after.png> [...]"
    echo ""
    echo "Environment:"
    echo "  IMAGE_ADAPTER    Storage adapter (default: github-branch)"
    echo "                   Available: github-branch, gist, blob"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --markdown)
            MARKDOWN_MODE=true
            shift
            ;;
        --before-url)
            BEFORE_URL="${2:-}"
            shift 2
            ;;
        --after-url)
            AFTER_URL="${2:-}"
            shift 2
            ;;
        --after-sha)
            AFTER_SHA="${2:-}"
            shift 2
            ;;
        --label)
            PENDING_LABEL="${2:-}"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        -*)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 1
            ;;
        *)
            FILES+=("$1")
            if (( ${#FILES[@]} % 2 == 1 )); then
                LABELS+=("$PENDING_LABEL")
                PENDING_LABEL=""
            fi
            shift
            ;;
    esac
done

if (( ${#FILES[@]} < 2 || ${#FILES[@]} % 2 != 0 )); then
    echo "Error: pass files in before/after pairs" >&2
    usage >&2
    exit 1
fi

for file in "${FILES[@]}"; do
    if [[ ! -f "$file" ]]; then
        echo "Error: file not found: $file" >&2
        exit 1
    fi
done

ADAPTER_SCRIPT="$ADAPTERS_DIR/$IMAGE_ADAPTER.sh"
if [[ ! -f "$ADAPTER_SCRIPT" ]]; then
    echo "Error: Unknown adapter: $IMAGE_ADAPTER" >&2
    echo "" >&2
    echo "Available adapters:" >&2
    for adapter in "$ADAPTERS_DIR"/*.sh; do
        echo "  - $(basename "$adapter" .sh)" >&2
    done
    exit 1
fi

if [[ -z "$AFTER_SHA" ]]; then
    AFTER_SHA="$(git rev-parse HEAD 2>/dev/null || true)"
fi

upload_file() {
    local file="$1"
    echo "Uploading: $(basename "$file") (via $IMAGE_ADAPTER)" >&2
    "$ADAPTER_SCRIPT" "$file"
}

# Derive a label from the "before" file name: strip the extension and a trailing
# "before" token, so "home-before.png" and "before-home.png" both become "home".
derive_label() {
    local base
    base="$(basename "$1")"
    base="${base%.*}"
    base="$(printf '%s' "$base" | sed -E 's/([-_ .]?before[-_ .]?)//I')"
    if [[ -z "$base" ]]; then
        base="Capture"
    fi
    printf '%s' "$base"
}

ROWS=""
PLAIN=""
PAIR_COUNT=$(( ${#FILES[@]} / 2 ))
echo "=== Uploading $PAIR_COUNT capture pair(s) ===" >&2
for (( i = 0; i < PAIR_COUNT; i++ )); do
    before_file="${FILES[$((i * 2))]}"
    after_file="${FILES[$((i * 2 + 1))]}"
    label="${LABELS[$i]}"
    if [[ -z "$label" ]]; then
        label="$(derive_label "$before_file")"
    fi
    before_url="$(upload_file "$before_file")"
    after_url="$(upload_file "$after_file")"
    echo "" >&2
    echo "$label" >&2
    echo "  Before URL: $before_url" >&2
    echo "  After URL:  $after_url" >&2
    ROWS+="| $label | ![$label before]($before_url) | ![$label after]($after_url) |"$'\n'
    PLAIN+="$label"$'\n'"  Before: $before_url"$'\n'"  After: $after_url"$'\n'
done

copy_to_clipboard() {
    if command -v pbcopy >/dev/null 2>&1; then
        pbcopy
        echo "Copied to clipboard." >&2
    elif command -v xclip >/dev/null 2>&1; then
        xclip -selection clipboard
        echo "Copied to clipboard." >&2
    else
        cat >/dev/null
        echo "(clipboard copy not available; paste the text above)" >&2
    fi
}

if [[ "$MARKDOWN_MODE" == "true" ]]; then
    SOURCES="Before: ${BEFORE_URL:-unknown source} | After: ${AFTER_URL:-unknown source}"
    if [[ -n "$AFTER_SHA" ]]; then
        SOURCES+=" at \`${AFTER_SHA:0:12}\`"
    fi
    MARKDOWN="## Before / After

| | Before | After |
|:--|:------:|:-----:|
${ROWS}
${SOURCES}"
    echo "" >&2
    echo "=== PR markdown ===" >&2
    printf '%s\n' "$MARKDOWN"
    printf '%s\n' "$MARKDOWN" | copy_to_clipboard
else
    printf '%s' "$PLAIN" | copy_to_clipboard
fi
