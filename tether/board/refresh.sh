#!/usr/bin/env bash
# Collect -> render -> publish the Agent Hub Board.
#
# Publishes only on success: the page in ~/docs is replaced from a temp file
# by an atomic rename, so a failed collection leaves the last good board up
# rather than a blank or half-written one.
set -euo pipefail

BOARD_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCS_DIR="${DOCS_DIR:-$HOME/docs}"
PAGE="${PAGE:-2026-09-09-agent-hub-board.html}"
PYTHON="${PYTHON:-python3}"

DATA="$BOARD_DIR/board.json"
STAGE="$(mktemp -t agent-hub-board.XXXXXX.html)"
trap 'rm -f "$STAGE"' EXIT

"$PYTHON" "$BOARD_DIR/collect.py" -o "$DATA"
"$PYTHON" "$BOARD_DIR/render.py" -i "$DATA" -o "$STAGE"

# Never publish a page still carrying an unsubstituted template token.
if grep -q '{{[A-Z_]*}}' "$STAGE"; then
  echo "refresh: rendered page has unsubstituted tokens; not publishing" >&2
  exit 1
fi

mkdir -p "$DOCS_DIR"
install -m 0644 "$STAGE" "$DOCS_DIR/$PAGE"
echo "refresh: published $DOCS_DIR/$PAGE"
