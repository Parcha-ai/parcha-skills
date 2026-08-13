#!/bin/sh
set -eu

PREFIX="$HOME/Library/Application Support/RecallBrain"
LAUNCH_AGENTS="$HOME/Library/LaunchAgents"
ENDPOINT=""
HOST_ID=""
KEYCHAIN_SERVICE=""
VISIBILITY=""
SOURCES=""
CLAUDE_ROOT="$HOME/.claude/projects"
CODEX_ROOT="$HOME/.codex/sessions"
CODEX_ARCHIVE_ROOT="$HOME/.codex/archived_sessions"
COWORK_ROOT="$HOME/Library/Application Support/Claude/local-agent-mode-sessions"
IMESSAGE_DATABASE=""
WHATSAPP_EXPORT=""
WHATSAPP_CONVERSATION_ID=""
WHATSAPP_OWNER_NAME=""
WHATSAPP_DATE_ORDER=""
WHATSAPP_TIMEZONE=""
SELECTED_TEXT_ROOT=""
SAFARI_HISTORY=""
SAFARI_BOOKMARKS=""
CHROME_HISTORY=""
CHROME_BOOKMARKS=""
APPLE_NOTES_DATABASE=""
HERMES_DATABASE=""
HERMES_SOURCES=""
HERMES_ROLES=""
NO_LOAD=0
PRIVACY_MODE="scrub"
EXPORT_INBOX=""
DISABLE_EXPORT_INBOX=0
DISABLE_SOURCES=""
SUPERVISOR_CONFIG=""
DISABLE_SUPERVISOR=0
ROLLBACK=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --prefix) PREFIX=$2; shift 2 ;;
    --launch-agents) LAUNCH_AGENTS=$2; shift 2 ;;
    --endpoint) ENDPOINT=$2; shift 2 ;;
    --host-id) HOST_ID=$2; shift 2 ;;
    --keychain-service) KEYCHAIN_SERVICE=$2; shift 2 ;;
    --visibility) VISIBILITY=$2; shift 2 ;;
    --sources) SOURCES=$2; shift 2 ;;
    --claude-root) CLAUDE_ROOT=$2; shift 2 ;;
    --codex-root) CODEX_ROOT=$2; shift 2 ;;
    --codex-archive-root) CODEX_ARCHIVE_ROOT=$2; shift 2 ;;
    --cowork-root) COWORK_ROOT=$2; shift 2 ;;
    --imessage-database) IMESSAGE_DATABASE=$2; shift 2 ;;
    --whatsapp-export) WHATSAPP_EXPORT=$2; shift 2 ;;
    --whatsapp-conversation-id) WHATSAPP_CONVERSATION_ID=$2; shift 2 ;;
    --whatsapp-owner-name) WHATSAPP_OWNER_NAME=$2; shift 2 ;;
    --whatsapp-date-order) WHATSAPP_DATE_ORDER=$2; shift 2 ;;
    --whatsapp-timezone) WHATSAPP_TIMEZONE=$2; shift 2 ;;
    --selected-text-root) SELECTED_TEXT_ROOT=$2; shift 2 ;;
    --safari-history) SAFARI_HISTORY=$2; shift 2 ;;
    --safari-bookmarks) SAFARI_BOOKMARKS=$2; shift 2 ;;
    --chrome-history) CHROME_HISTORY=$2; shift 2 ;;
    --chrome-bookmarks) CHROME_BOOKMARKS=$2; shift 2 ;;
    --apple-notes-database) APPLE_NOTES_DATABASE=$2; shift 2 ;;
    --hermes-database) HERMES_DATABASE=$2; shift 2 ;;
    --hermes-sources) HERMES_SOURCES=$2; shift 2 ;;
    --hermes-roles) HERMES_ROLES=$2; shift 2 ;;
    --privacy-mode) PRIVACY_MODE=$2; shift 2 ;;
    --export-inbox) EXPORT_INBOX=$2; shift 2 ;;
    --disable-export-inbox) DISABLE_EXPORT_INBOX=1; shift ;;
    --disable-source) DISABLE_SOURCES="$DISABLE_SOURCES $2"; shift 2 ;;
    --connector-supervisor-config) SUPERVISOR_CONFIG=$2; shift 2 ;;
    --disable-connector-supervisor) DISABLE_SUPERVISOR=1; shift ;;
    --rollback) ROLLBACK=1; shift ;;
    --no-load) NO_LOAD=1; shift ;;
    *) echo "usage: install.sh [connection] [--sources explicit,comma,separated,sources] [explicit source paths and selectors] [disable/lifecycle options]" >&2; exit 2 ;;
  esac
done

if [ "$ROLLBACK" -eq 1 ]; then
  ROLLBACK_ROOT="$PREFIX/.rollback"
  [ -d "$ROLLBACK_ROOT/code" ] && [ ! -L "$ROLLBACK_ROOT" ] || {
    echo "rollback_unavailable" >&2
    exit 1
  }
  mkdir -p "$PREFIX" "$LAUNCH_AGENTS"
  CURRENT="$PREFIX/.rollback-current.$$"
  rm -rf "$CURRENT"
  mkdir -p "$CURRENT/code" "$CURRENT/plists"
  for NAME in bin lib runtime; do
    if [ -e "$PREFIX/$NAME" ] || [ -L "$PREFIX/$NAME" ]; then
      mv "$PREFIX/$NAME" "$CURRENT/code/$NAME"
    fi
  done
  for NAME in RUNTIME_LOCK.json MANIFEST.json; do
    if [ -e "$PREFIX/$NAME" ] || [ -L "$PREFIX/$NAME" ]; then
      mv "$PREFIX/$NAME" "$CURRENT/code/$NAME"
    fi
  done
  for PLIST in "$LAUNCH_AGENTS"/ai.parcha.recall.*.plist; do
    [ -e "$PLIST" ] || [ -L "$PLIST" ] || continue
    cp -p "$PLIST" "$CURRENT/plists/"
  done
  rollback_stop_agents() {
    [ "$NO_LOAD" -eq 0 ] || return 0
    command -v launchctl >/dev/null 2>&1 || return 0
    for LABEL in claude codex cowork chatgpt-export imessage whatsapp selected-text safari chrome apple-notes hermes connector-supervisor; do
      TARGET="gui/$(id -u)/ai.parcha.recall.$LABEL"
      launchctl bootout "$TARGET" >/dev/null 2>&1 || true
      ATTEMPTS=0
      while launchctl print "$TARGET" >/dev/null 2>&1; do
        ATTEMPTS=$((ATTEMPTS + 1))
        [ "$ATTEMPTS" -lt 100 ] || return 1
        sleep 0.1
      done
    done
  }
  ROLLBACK_COMMITTED=0
  restore_current() {
    STATUS=$?
    trap - EXIT HUP INT TERM
    if [ "$ROLLBACK_COMMITTED" -eq 1 ]; then
      exit "$STATUS"
    fi
    rm -rf "$PREFIX/bin" "$PREFIX/lib" "$PREFIX/runtime"
    rm -f "$PREFIX/RUNTIME_LOCK.json" "$PREFIX/MANIFEST.json"
    for NAME in bin lib runtime RUNTIME_LOCK.json MANIFEST.json; do
      if [ -e "$CURRENT/code/$NAME" ] || [ -L "$CURRENT/code/$NAME" ]; then
        mv "$CURRENT/code/$NAME" "$PREFIX/$NAME"
      fi
    done
    rm -f "$LAUNCH_AGENTS"/ai.parcha.recall.*.plist
    for PLIST in "$CURRENT/plists"/*.plist; do
      [ -e "$PLIST" ] || continue
      cp -p "$PLIST" "$LAUNCH_AGENTS/"
      if [ "$NO_LOAD" -eq 0 ] && command -v launchctl >/dev/null 2>&1; then
        launchctl bootstrap "gui/$(id -u)" "$LAUNCH_AGENTS/$(basename "$PLIST")" || true
      fi
    done
    rm -rf "$CURRENT"
    exit "$STATUS"
  }
  trap 'restore_current' EXIT HUP INT TERM
  rollback_stop_agents
  rm -f "$LAUNCH_AGENTS"/ai.parcha.recall.*.plist
  for NAME in bin lib runtime RUNTIME_LOCK.json MANIFEST.json; do
    if [ -e "$ROLLBACK_ROOT/code/$NAME" ] || [ -L "$ROLLBACK_ROOT/code/$NAME" ]; then
      cp -Rp "$ROLLBACK_ROOT/code/$NAME" "$PREFIX/$NAME"
    fi
  done
  for PLIST in "$ROLLBACK_ROOT/plists"/*.plist; do
    [ -e "$PLIST" ] || continue
    cp -p "$PLIST" "$LAUNCH_AGENTS/"
  done
  if [ "$NO_LOAD" -eq 0 ] && command -v launchctl >/dev/null 2>&1; then
    for PLIST in "$LAUNCH_AGENTS"/ai.parcha.recall.*.plist; do
      [ -e "$PLIST" ] || continue
      launchctl bootstrap "gui/$(id -u)" "$PLIST"
    done
  fi
  ROLLBACK_COMMITTED=1
  trap - EXIT HUP INT TERM
  rm -rf "$ROLLBACK_ROOT" "$CURRENT"
  echo '{"schema_version":1,"mode":"mac-rollback","restored":true,"state_retained":true}'
  exit 0
fi

case "$PRIVACY_MODE" in off|scrub|drop) ;; *) echo "privacy mode must be off, scrub, or drop" >&2; exit 2 ;; esac
if [ -n "$EXPORT_INBOX" ] && [ "$DISABLE_EXPORT_INBOX" -eq 1 ]; then
  echo "export inbox enable and disable options are mutually exclusive" >&2; exit 2
fi
if [ -n "$SUPERVISOR_CONFIG" ] && [ "$DISABLE_SUPERVISOR" -eq 1 ]; then
  echo "connector supervisor enable and disable options are mutually exclusive" >&2; exit 2
fi
if [ -z "$SOURCES" ] && [ -z "$EXPORT_INBOX" ] && [ "$DISABLE_EXPORT_INBOX" -eq 0 ] && [ -z "$DISABLE_SOURCES" ] && [ -z "$SUPERVISOR_CONFIG" ] && [ "$DISABLE_SUPERVISOR" -eq 0 ]; then
  echo "select at least one coding source, export inbox, or connector supervisor action" >&2; exit 2
fi
if [ -n "$SOURCES" ] || [ -n "$EXPORT_INBOX" ]; then
  case "$ENDPOINT" in https://*) ;; *) echo "endpoint must use https" >&2; exit 2 ;; esac
  case "$HOST_ID" in ""|*[!A-Za-z0-9_.-]*) echo "invalid host id" >&2; exit 2 ;; esac
  [ -n "$KEYCHAIN_SERVICE" ] || { echo "keychain service is required" >&2; exit 2; }
  case "$VISIBILITY" in private|shared) ;; *) echo "visibility must be private or shared" >&2; exit 2 ;; esac
fi
NORMALIZED_SOURCES=""
if [ -n "$SOURCES" ]; then
  case ",$SOURCES," in *,,*) echo "invalid duplicate or empty source selection" >&2; exit 2 ;; esac
fi
for SOURCE_NAME in $(echo "$SOURCES" | tr ',' ' '); do
  case "$SOURCE_NAME" in
    claude|claude-code) NORMALIZED=claude ;;
    codex|chatgpt-codex-desktop) NORMALIZED=codex ;;
    cowork) NORMALIZED=cowork ;;
    imessage) NORMALIZED=imessage ;;
    whatsapp|whatsapp-export) NORMALIZED=whatsapp ;;
    selected-text|obsidian) NORMALIZED=selected-text ;;
    safari) NORMALIZED=safari ;;
    chrome) NORMALIZED=chrome ;;
    apple-notes|notes) NORMALIZED=apple-notes ;;
    hermes) NORMALIZED=hermes ;;
    *) echo "unsupported source: $SOURCE_NAME" >&2; exit 2 ;;
  esac
  case ",$NORMALIZED_SOURCES," in *,$NORMALIZED,*) echo "invalid duplicate or empty source selection" >&2; exit 2 ;; esac
  NORMALIZED_SOURCES="${NORMALIZED_SOURCES:+$NORMALIZED_SOURCES,}$NORMALIZED"
done
SOURCES=$NORMALIZED_SOURCES
case ",$SOURCES," in *,cowork,*)
  [ "$PRIVACY_MODE" != "off" ] || { echo "Cowork requires scrub or drop privacy" >&2; exit 2; }
  [ -d "$COWORK_ROOT" ] && [ ! -L "$COWORK_ROOT" ] || { echo "Cowork root must be an explicit non-symlink directory" >&2; exit 2; }
;; esac
case ",$SOURCES," in *,imessage,*)
  [ "$PRIVACY_MODE" != "off" ] || { echo "iMessage requires scrub or drop privacy" >&2; exit 2; }
  [ "$VISIBILITY" = "private" ] || { echo "iMessage visibility must be private" >&2; exit 2; }
  [ -f "$IMESSAGE_DATABASE" ] && [ ! -L "$IMESSAGE_DATABASE" ] || { echo "iMessage database must be an explicit non-symlink file" >&2; exit 2; }
;; esac
case ",$SOURCES," in *,whatsapp,*)
  [ "$PRIVACY_MODE" != "off" ] || { echo "WhatsApp requires scrub or drop privacy" >&2; exit 2; }
  [ "$VISIBILITY" = "private" ] || { echo "WhatsApp visibility must be private" >&2; exit 2; }
  [ -f "$WHATSAPP_EXPORT" ] && [ ! -L "$WHATSAPP_EXPORT" ] || { echo "WhatsApp export must be an explicit non-symlink file" >&2; exit 2; }
  [ -n "$WHATSAPP_CONVERSATION_ID" ] || { echo "WhatsApp conversation id is required" >&2; exit 2; }
  case "$WHATSAPP_DATE_ORDER" in dmy|mdy) ;; *) echo "WhatsApp date order must be dmy or mdy" >&2; exit 2 ;; esac
  [ -n "$WHATSAPP_TIMEZONE" ] || { echo "WhatsApp timezone is required" >&2; exit 2; }
;; esac
case ",$SOURCES," in *,selected-text,*)
  [ "$PRIVACY_MODE" != "off" ] || { echo "selected text requires scrub or drop privacy" >&2; exit 2; }
  [ "$VISIBILITY" = "private" ] || { echo "selected text visibility must be private" >&2; exit 2; }
  [ -d "$SELECTED_TEXT_ROOT" ] && [ ! -L "$SELECTED_TEXT_ROOT" ] || { echo "selected text root must be an explicit non-symlink directory" >&2; exit 2; }
;; esac
case ",$SOURCES," in *,safari,*)
  [ "$PRIVACY_MODE" != "off" ] || { echo "Safari requires scrub or drop privacy" >&2; exit 2; }
  [ "$VISIBILITY" = "private" ] || { echo "Safari visibility must be private" >&2; exit 2; }
  [ -n "$SAFARI_HISTORY" ] || [ -n "$SAFARI_BOOKMARKS" ] || { echo "Safari requires an explicit history or bookmarks file" >&2; exit 2; }
  if [ -n "$SAFARI_HISTORY" ]; then [ -f "$SAFARI_HISTORY" ] && [ ! -L "$SAFARI_HISTORY" ] || { echo "Safari history must be an explicit non-symlink file" >&2; exit 2; }; fi
  if [ -n "$SAFARI_BOOKMARKS" ]; then [ -f "$SAFARI_BOOKMARKS" ] && [ ! -L "$SAFARI_BOOKMARKS" ] || { echo "Safari bookmarks must be an explicit non-symlink file" >&2; exit 2; }; fi
;; esac
case ",$SOURCES," in *,chrome,*)
  [ "$PRIVACY_MODE" != "off" ] || { echo "Chrome requires scrub or drop privacy" >&2; exit 2; }
  [ "$VISIBILITY" = "private" ] || { echo "Chrome visibility must be private" >&2; exit 2; }
  [ -n "$CHROME_HISTORY" ] || [ -n "$CHROME_BOOKMARKS" ] || { echo "Chrome requires an explicit history or bookmarks file" >&2; exit 2; }
  if [ -n "$CHROME_HISTORY" ]; then [ -f "$CHROME_HISTORY" ] && [ ! -L "$CHROME_HISTORY" ] || { echo "Chrome history must be an explicit non-symlink file" >&2; exit 2; }; fi
  if [ -n "$CHROME_BOOKMARKS" ]; then [ -f "$CHROME_BOOKMARKS" ] && [ ! -L "$CHROME_BOOKMARKS" ] || { echo "Chrome bookmarks must be an explicit non-symlink file" >&2; exit 2; }; fi
;; esac
case ",$SOURCES," in *,apple-notes,*)
  [ "$PRIVACY_MODE" != "off" ] || { echo "Apple Notes requires scrub or drop privacy" >&2; exit 2; }
  [ "$VISIBILITY" = "private" ] || { echo "Apple Notes visibility must be private" >&2; exit 2; }
  [ -f "$APPLE_NOTES_DATABASE" ] && [ ! -L "$APPLE_NOTES_DATABASE" ] || { echo "Apple Notes database must be an explicit non-symlink file" >&2; exit 2; }
;; esac
case ",$SOURCES," in *,hermes,*)
  [ "$PRIVACY_MODE" != "off" ] || { echo "Hermes requires scrub or drop privacy" >&2; exit 2; }
  [ "$VISIBILITY" = "private" ] || { echo "Hermes visibility must be private" >&2; exit 2; }
  [ -f "$HERMES_DATABASE" ] && [ ! -L "$HERMES_DATABASE" ] || { echo "Hermes database must be an explicit non-symlink file" >&2; exit 2; }
  [ -n "$HERMES_SOURCES" ] || { echo "Hermes requires at least one explicit source selector" >&2; exit 2; }
  case "$HERMES_SOURCES" in ,*|*,|*,,*) echo "Hermes source selectors are invalid" >&2; exit 2 ;; esac
  SEEN_HERMES_SOURCES=""
  for HERMES_SOURCE in $(echo "$HERMES_SOURCES" | tr ',' ' '); do
    case "$HERMES_SOURCE" in ""|*[!A-Za-z0-9_.:@-]*) echo "Hermes source selectors are invalid" >&2; exit 2 ;; esac
    case ",$SEEN_HERMES_SOURCES," in *,$HERMES_SOURCE,*) echo "Hermes source selectors are duplicated" >&2; exit 2 ;; esac
    SEEN_HERMES_SOURCES="${SEEN_HERMES_SOURCES:+$SEEN_HERMES_SOURCES,}$HERMES_SOURCE"
  done
  if [ -n "$HERMES_ROLES" ]; then
    case "$HERMES_ROLES" in ,*|*,|*,,*) echo "Hermes role selectors are invalid" >&2; exit 2 ;; esac
    SEEN_HERMES_ROLES=""
    for HERMES_ROLE in $(echo "$HERMES_ROLES" | tr ',' ' '); do
      case "$HERMES_ROLE" in assistant|user) ;; *) echo "Hermes role selectors are invalid" >&2; exit 2 ;; esac
      case ",$SEEN_HERMES_ROLES," in *,$HERMES_ROLE,*) echo "Hermes role selectors are duplicated" >&2; exit 2 ;; esac
      SEEN_HERMES_ROLES="${SEEN_HERMES_ROLES:+$SEEN_HERMES_ROLES,}$HERMES_ROLE"
    done
  fi
;; esac
for SOURCE_NAME in $DISABLE_SOURCES; do
  case "$SOURCE_NAME" in claude|claude-code|codex|chatgpt-codex-desktop|cowork|chatgpt-export|imessage|whatsapp|whatsapp-export|selected-text|obsidian|safari|chrome|apple-notes|notes|hermes) ;; *) echo "unsupported disable source: $SOURCE_NAME" >&2; exit 2 ;; esac
done
if [ -n "$EXPORT_INBOX" ]; then
  [ -d "$EXPORT_INBOX" ] && [ ! -L "$EXPORT_INBOX" ] || { echo "export inbox must be an explicit non-symlink directory" >&2; exit 2; }
fi

stop_launch_agent() {
  TARGET="gui/$(id -u)/$1"
  launchctl bootout "$TARGET" >/dev/null 2>&1 || true
  ATTEMPTS=0
  while launchctl print "$TARGET" >/dev/null 2>&1; do
    ATTEMPTS=$((ATTEMPTS + 1))
    if [ "$ATTEMPTS" -ge 100 ]; then
      echo "launch agent stop did not converge" >&2
      return 1
    fi
    sleep 0.1
  done
}

SOURCE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PACKAGE_RUNTIME="$SOURCE/runtime/bin/python3"
[ -x "$PACKAGE_RUNTIME" ] || { echo "package_integrity_failed" >&2; exit 1; }
"$PACKAGE_RUNTIME" -B - "$SOURCE" <<'PY'
import hashlib
import json
import os
import stat
import sys
from pathlib import Path, PurePosixPath

try:
    root = Path(sys.argv[1])
    manifest_path = root / "MANIFEST.json"
    if manifest_path.stat().st_size > 10_000_000:
        raise ValueError
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("format") != "recall-macos-v2":
        raise ValueError
    entries = manifest["files"]
    if not isinstance(entries, list) or len(entries) > 100_000:
        raise ValueError
    expected = set()
    for entry in entries:
        relative = PurePosixPath(entry["path"])
        if relative.is_absolute() or ".." in relative.parts or str(relative) == "MANIFEST.json":
            raise ValueError
        if str(relative) in expected:
            raise ValueError
        expected.add(str(relative))
        path = root.joinpath(*relative.parts)
        for parent in path.parents:
            if parent == root:
                break
            if stat.S_ISLNK(parent.lstat().st_mode):
                raise ValueError
        details = path.lstat()
        if entry.get("type") == "symlink":
            if not stat.S_ISLNK(details.st_mode) or os.readlink(path) != entry.get("target"):
                raise ValueError
        elif entry.get("type") == "file":
            if (
                not stat.S_ISREG(details.st_mode)
                or details.st_size != entry.get("bytes")
                or details.st_size > 1_000_000_000
            ):
                raise ValueError
            digest = hashlib.sha256()
            with path.open("rb") as source:
                while chunk := source.read(1024 * 1024):
                    digest.update(chunk)
            if digest.hexdigest() != entry.get("sha256"):
                raise ValueError
        else:
            raise ValueError
    actual = set()
    for directory, directories, files in os.walk(root, followlinks=False):
        base = Path(directory)
        for name in list(directories):
            path = base / name
            if path.is_symlink():
                actual.add(path.relative_to(root).as_posix())
        for name in files:
            path = base / name
            relative = path.relative_to(root).as_posix()
            if relative != "MANIFEST.json":
                actual.add(relative)
    if actual != expected:
        raise ValueError
except (KeyError, OSError, TypeError, UnicodeError, ValueError):
    raise SystemExit("package_integrity_failed") from None
PY

case ",$SOURCES," in *,codex,*)
  mkdir -p "$CODEX_ARCHIVE_ROOT"
  [ -d "$CODEX_ARCHIVE_ROOT" ] && [ ! -L "$CODEX_ARCHIVE_ROOT" ] || {
    echo "Codex archive root must be a non-symlink directory" >&2
    exit 2
  }
;; esac

if [ -L "$PREFIX" ] || [ -L "$LAUNCH_AGENTS" ]; then
  echo "install_location_unsafe" >&2
  exit 1
fi
mkdir -p "$PREFIX" "$PREFIX/state" "$LAUNCH_AGENTS"
chmod 700 "$PREFIX/state"
TRANSACTION="$PREFIX/.transaction.$$"
rm -rf "$TRANSACTION"
mkdir -p "$TRANSACTION/stage/bin" "$TRANSACTION/stage/lib" \
  "$TRANSACTION/old/code" "$TRANSACTION/old/plists"
cp -R "$SOURCE/lib/client" "$TRANSACTION/stage/lib/client"
cp -R "$SOURCE/lib/collector" "$TRANSACTION/stage/lib/collector"
cp -R "$SOURCE/lib/skills" "$TRANSACTION/stage/lib/skills"
cp -R "$SOURCE/lib/connectors" "$TRANSACTION/stage/lib/connectors"
cp -R "$SOURCE/lib/contracts" "$TRANSACTION/stage/lib/contracts"
cp -R "$SOURCE/lib/privacy" "$TRANSACTION/stage/lib/privacy"
cp -R "$SOURCE/runtime" "$TRANSACTION/stage/runtime"
cp "$SOURCE/bin/recall-brain" "$TRANSACTION/stage/bin/recall-brain"
cp "$SOURCE/RUNTIME_LOCK.json" "$TRANSACTION/stage/RUNTIME_LOCK.json"
cp "$SOURCE/MANIFEST.json" "$TRANSACTION/stage/MANIFEST.json"
chmod 755 "$TRANSACTION/stage/bin/recall-brain"
for NAME in bin lib runtime RUNTIME_LOCK.json MANIFEST.json; do
  if [ -e "$PREFIX/$NAME" ] || [ -L "$PREFIX/$NAME" ]; then
    cp -Rp "$PREFIX/$NAME" "$TRANSACTION/old/code/$NAME"
  fi
done
for PLIST in "$LAUNCH_AGENTS"/ai.parcha.recall.*.plist; do
  [ -e "$PLIST" ] || [ -L "$PLIST" ] || continue
  [ ! -L "$PLIST" ] || { echo "install_location_unsafe" >&2; exit 1; }
  cp -p "$PLIST" "$TRANSACTION/old/plists/"
done

TRANSACTION_COMMITTED=0
restore_transaction() {
  STATUS=$?
  trap - EXIT HUP INT TERM
  if [ "$TRANSACTION_COMMITTED" -eq 0 ]; then
    if [ "$NO_LOAD" -eq 0 ] && command -v launchctl >/dev/null 2>&1; then
      for LABEL in claude codex cowork chatgpt-export imessage whatsapp selected-text safari chrome apple-notes hermes connector-supervisor; do
        stop_launch_agent "ai.parcha.recall.$LABEL" || true
      done
    fi
    rm -rf "$PREFIX/bin" "$PREFIX/lib" "$PREFIX/runtime"
    rm -f "$PREFIX/RUNTIME_LOCK.json" "$PREFIX/MANIFEST.json"
    for NAME in bin lib runtime RUNTIME_LOCK.json MANIFEST.json; do
      if [ -e "$TRANSACTION/old/code/$NAME" ] || [ -L "$TRANSACTION/old/code/$NAME" ]; then
        cp -Rp "$TRANSACTION/old/code/$NAME" "$PREFIX/$NAME"
      fi
    done
    rm -f "$LAUNCH_AGENTS"/ai.parcha.recall.*.plist
    for PLIST in "$TRANSACTION/old/plists"/*.plist; do
      [ -e "$PLIST" ] || continue
      cp -p "$PLIST" "$LAUNCH_AGENTS/"
      if [ "$NO_LOAD" -eq 0 ] && command -v launchctl >/dev/null 2>&1; then
        launchctl bootstrap "gui/$(id -u)" "$LAUNCH_AGENTS/$(basename "$PLIST")" || true
      fi
    done
  fi
  rm -rf "$TRANSACTION"
  exit "$STATUS"
}
trap 'restore_transaction' EXIT HUP INT TERM

RUNTIME="$TRANSACTION/stage/runtime/bin/python3"
[ -x "$RUNTIME" ] || { echo "bundled runtime is not executable" >&2; exit 1; }
"$RUNTIME" -B - "$TRANSACTION/stage/RUNTIME_LOCK.json" <<'PY'
import ctypes
import json
import os
import platform
import sqlite3
import ssl
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    lock = json.load(source)
capabilities = lock["capabilities"]
assert sys.version.split()[0] == lock["version"], sys.version
assert platform.python_implementation() == capabilities["implementation"]
assert platform.system() == capabilities["system"], platform.system()
assert platform.machine() == capabilities["machine"], platform.machine()
if capabilities["language"]["zip_strict"]:
    assert list(zip([1], [2], strict=True)) == [(1, 2)]
ctypes.CDLL(None)
verify_paths = ssl.get_default_verify_paths()
assert any(path and os.path.exists(path) for path in (verify_paths.cafile, verify_paths.capath))
assert ssl.create_default_context().get_ca_certs()
connection = sqlite3.connect(":memory:")
try:
    if capabilities["sqlite"]["fts5"]:
        connection.execute("CREATE VIRTUAL TABLE exact_runtime_fts USING fts5(body)")
finally:
    connection.close()
PY

preserve_canonical_route() {
  PLIST=$1
  SOURCE_ID=$2
  PREVIOUS="$TRANSACTION/old/plists/$(basename "$PLIST")"
  [ -f "$PREVIOUS" ] && [ ! -L "$PREVIOUS" ] || return 0
  "$RUNTIME" - "$PREVIOUS" "$PLIST" "$SOURCE_ID" <<'PY'
import os
import plistlib
import re
import sys
import tempfile
from pathlib import Path

previous_path = Path(sys.argv[1])
current_path = Path(sys.argv[2])
expected_source_id = sys.argv[3]
identity = re.compile(r"[A-Za-z0-9][A-Za-z0-9:._/@+-]{1,255}\Z")

try:
    with previous_path.open("rb") as source:
        previous = plistlib.load(source)
    with current_path.open("rb") as source:
        current = plistlib.load(source)
    previous_environment = previous.get("EnvironmentVariables", {})
    current_environment = current.get("EnvironmentVariables", {})
    route_keys = {
        "RECALL_CANONICAL_V2_ENABLED",
        "RECALL_TENANT_ID",
        "RECALL_PRINCIPAL_ID",
    }
    if not route_keys.intersection(previous_environment):
        raise SystemExit(0)
    tenant_id = previous_environment.get("RECALL_TENANT_ID")
    principal_id = previous_environment.get("RECALL_PRINCIPAL_ID")
    if (
        previous_environment.get("RECALL_CANONICAL_V2_ENABLED") != "1"
        or not isinstance(tenant_id, str)
        or not identity.fullmatch(tenant_id)
        or not isinstance(principal_id, str)
        or not identity.fullmatch(principal_id)
        or not isinstance(current_environment, dict)
    ):
        raise ValueError

    def option(arguments, name):
        index = arguments.index(name) + 1
        if index >= len(arguments) or not isinstance(arguments[index], str):
            raise ValueError
        return index, arguments[index]

    previous_arguments = previous["ProgramArguments"]
    current_arguments = list(current["ProgramArguments"])
    _, previous_source_id = option(previous_arguments, "--source-id")
    _, previous_principal_id = option(previous_arguments, "--principal-id")
    _, current_source_id = option(current_arguments, "--source-id")
    current_principal_index, _ = option(current_arguments, "--principal-id")
    if (
        previous_source_id != expected_source_id
        or current_source_id != expected_source_id
        or previous_principal_id != principal_id
    ):
        raise ValueError

    current_arguments[current_principal_index] = principal_id
    current_environment = dict(current_environment)
    current_environment.update({
        "RECALL_CANONICAL_V2_ENABLED": "1",
        "RECALL_TENANT_ID": tenant_id,
        "RECALL_PRINCIPAL_ID": principal_id,
    })
    current["ProgramArguments"] = current_arguments
    current["EnvironmentVariables"] = current_environment
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".recall-install-", suffix=".plist", dir=current_path.parent,
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as output:
            plistlib.dump(current, output, sort_keys=True)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, current_path)
    finally:
        temporary.unlink(missing_ok=True)
except (KeyError, OSError, TypeError, ValueError, plistlib.InvalidFileException):
    raise SystemExit("canonical_route_preservation_failed") from None
PY
}

rm -rf "$PREFIX/bin" "$PREFIX/lib" "$PREFIX/runtime"
rm -f "$PREFIX/RUNTIME_LOCK.json" "$PREFIX/MANIFEST.json"
for NAME in bin lib runtime RUNTIME_LOCK.json MANIFEST.json; do
  mv "$TRANSACTION/stage/$NAME" "$PREFIX/$NAME"
done
RUNTIME="$PREFIX/runtime/bin/python3"

write_plist() {
  HARNESS=$1
  ROOT=$2
  ARCHIVE_ROOT=$3
  SOURCE_ID="$HARNESS:mac:$HOST_ID"
  LABEL="ai.parcha.recall.$HARNESS"
  PLIST="$LAUNCH_AGENTS/$LABEL.plist"
  if [ "$NO_LOAD" -eq 0 ] && command -v launchctl >/dev/null 2>&1; then
    stop_launch_agent "$LABEL"
  fi
  "$RUNTIME" - "$PLIST" "$LABEL" "$RUNTIME" "$PREFIX/lib" "$ENDPOINT" "$SOURCE_ID" "$HARNESS" "$ROOT" "$ARCHIVE_ROOT" "$PREFIX/state/$HARNESS.db" "$KEYCHAIN_SERVICE" "$VISIBILITY" "$PRIVACY_MODE" <<'PY'
import plistlib
import sys

path, label, program, pythonpath, endpoint, source_id, harness, root, archive_root, spool, service, visibility, privacy_mode = sys.argv[1:]
arguments = [
    program, "-m", "client.cli", "collect", "--endpoint", endpoint,
    "--source-id", source_id, "--principal-id", "owner",
    "--visibility", visibility, "--harness", harness,
    "--root", root,
]
if harness == "codex":
    if not archive_root:
        raise SystemExit("codex_archive_root_required")
    arguments.extend(["--archive-root", archive_root])
arguments.extend([
    "--spool", spool,
    "--max-scan-records", "1000", "--max-scan-seconds", "20",
    "--keychain-service", service, "--keychain-account", source_id,
    "--privacy-mode", privacy_mode,
])
value = {
    "Label": label,
    "ProgramArguments": arguments,
    "EnvironmentVariables": {
        "PYTHONPATH": pythonpath,
        "RECALL_KEYCHAIN_REFERENCE": "Keychain service/account only",
    },
    "RunAtLoad": True,
    "StartInterval": 30,
    "ProcessType": "Background",
    "Umask": 0o077,
    "StandardOutPath": spool + ".stdout.log",
    "StandardErrorPath": spool + ".stderr.log",
}
with open(path, "wb") as output:
    plistlib.dump(value, output, sort_keys=True)
PY
  preserve_canonical_route "$PLIST" "$SOURCE_ID"
  chmod 600 "$PLIST"
  if [ "$NO_LOAD" -eq 0 ] && command -v launchctl >/dev/null 2>&1; then
    launchctl bootstrap "gui/$(id -u)" "$PLIST"
  fi
}

write_export_inbox_plist() {
  SOURCE_ID="chatgpt-export:mac:$HOST_ID"
  LABEL="ai.parcha.recall.chatgpt-export"
  PLIST="$LAUNCH_AGENTS/$LABEL.plist"
  SPOOL="$PREFIX/state/chatgpt-export-runner.db"
  CATALOG="$PREFIX/state/chatgpt-export-catalog.db"
  if [ "$NO_LOAD" -eq 0 ] && command -v launchctl >/dev/null 2>&1; then
    stop_launch_agent "$LABEL"
  fi
  "$RUNTIME" - "$PLIST" "$LABEL" "$RUNTIME" "$PREFIX/lib" "$ENDPOINT" "$SOURCE_ID" "$EXPORT_INBOX" "$CATALOG" "$SPOOL" "$KEYCHAIN_SERVICE" "$PRIVACY_MODE" <<'PY'
import plistlib
import sys

path, label, program, pythonpath, endpoint, source_id, inbox, catalog, spool, service, privacy_mode = sys.argv[1:]
value = {
    "Label": label,
    "ProgramArguments": [
        program, "-m", "client.cli", "export-inbox-sync", "--endpoint", endpoint,
        "--source-id", source_id, "--principal-id", "owner", "--visibility", "private",
        "--inbox", inbox, "--catalog", catalog, "--spool", spool,
        "--keychain-service", service, "--keychain-account", source_id,
        "--privacy-mode", privacy_mode,
    ],
    "EnvironmentVariables": {
        "PYTHONPATH": pythonpath,
        "RECALL_KEYCHAIN_REFERENCE": "Keychain service/account only",
    },
    "RunAtLoad": True,
    "StartInterval": 30,
    "ProcessType": "Background",
    "Umask": 0o077,
    "StandardOutPath": spool + ".stdout.log",
    "StandardErrorPath": spool + ".stderr.log",
}

with open(path, "wb") as output:
    plistlib.dump(value, output, sort_keys=True)
PY
  preserve_canonical_route "$PLIST" "$SOURCE_ID"
  chmod 600 "$PLIST"
  if [ "$NO_LOAD" -eq 0 ] && command -v launchctl >/dev/null 2>&1; then
    launchctl bootstrap "gui/$(id -u)" "$PLIST"
  fi
}

write_cowork_plist() {
  SOURCE_ID="cowork:mac:$HOST_ID"
  LABEL="ai.parcha.recall.cowork"
  PLIST="$LAUNCH_AGENTS/$LABEL.plist"
  SPOOL="$PREFIX/state/cowork.db"
  if [ "$NO_LOAD" -eq 0 ] && command -v launchctl >/dev/null 2>&1; then
    stop_launch_agent "$LABEL"
  fi
  "$RUNTIME" - "$PLIST" "$LABEL" "$RUNTIME" "$PREFIX/lib" "$ENDPOINT" "$SOURCE_ID" "$COWORK_ROOT" "$SPOOL" "$KEYCHAIN_SERVICE" "$PRIVACY_MODE" <<'PY'
import plistlib
import sys

path, label, program, pythonpath, endpoint, source_id, root, spool, service, privacy_mode = sys.argv[1:]
value = {
    "Label": label,
    "ProgramArguments": [
        program, "-m", "client.cli", "cowork-local-sync", "--endpoint", endpoint,
        "--source-id", source_id, "--principal-id", "owner",
        "--root", root, "--spool", spool,
        "--keychain-service", service, "--keychain-account", source_id,
        "--privacy-mode", privacy_mode,
    ],
    "EnvironmentVariables": {
        "PYTHONPATH": pythonpath,
        "RECALL_KEYCHAIN_REFERENCE": "Keychain service/account only",
    },
    "RunAtLoad": True,
    "StartInterval": 30,
    "ProcessType": "Background",
    "Umask": 0o077,
    "StandardOutPath": spool + ".stdout.log",
    "StandardErrorPath": spool + ".stderr.log",
}
with open(path, "wb") as output:
    plistlib.dump(value, output, sort_keys=True)
PY
  preserve_canonical_route "$PLIST" "$SOURCE_ID"
  chmod 600 "$PLIST"
  if [ "$NO_LOAD" -eq 0 ] && command -v launchctl >/dev/null 2>&1; then
    launchctl bootstrap "gui/$(id -u)" "$PLIST"
  fi
}

write_local_connector_plist() {
  TYPE=$1
  case "$TYPE" in
    imessage) SOURCE_ID="imessage:mac:$HOST_ID"; LABEL="ai.parcha.recall.imessage"; SPOOL="$PREFIX/state/imessage.db" ;;
    whatsapp) SOURCE_ID="whatsapp:mac:$HOST_ID"; LABEL="ai.parcha.recall.whatsapp"; SPOOL="$PREFIX/state/whatsapp.db" ;;
    selected-text) SOURCE_ID="selected-text:mac:$HOST_ID"; LABEL="ai.parcha.recall.selected-text"; SPOOL="$PREFIX/state/selected-text.db" ;;
    safari) SOURCE_ID="safari:mac:$HOST_ID"; LABEL="ai.parcha.recall.safari"; SPOOL="$PREFIX/state/safari.db" ;;
    chrome) SOURCE_ID="chrome:mac:$HOST_ID"; LABEL="ai.parcha.recall.chrome"; SPOOL="$PREFIX/state/chrome.db" ;;
    apple-notes) SOURCE_ID="notes:mac:$HOST_ID"; LABEL="ai.parcha.recall.apple-notes"; SPOOL="$PREFIX/state/apple-notes.db" ;;
    hermes) SOURCE_ID="hermes:mac:$HOST_ID"; LABEL="ai.parcha.recall.hermes"; SPOOL="$PREFIX/state/hermes.db" ;;
    *) echo "unsupported local connector" >&2; exit 2 ;;
  esac
  PLIST="$LAUNCH_AGENTS/$LABEL.plist"
  if [ "$NO_LOAD" -eq 0 ] && command -v launchctl >/dev/null 2>&1; then
    stop_launch_agent "$LABEL"
  fi
  "$RUNTIME" - "$PLIST" "$LABEL" "$RUNTIME" "$PREFIX/lib" "$ENDPOINT" "$SOURCE_ID" "$SPOOL" "$KEYCHAIN_SERVICE" "$PRIVACY_MODE" "$TYPE" "$IMESSAGE_DATABASE" "$WHATSAPP_EXPORT" "$WHATSAPP_CONVERSATION_ID" "$WHATSAPP_OWNER_NAME" "$WHATSAPP_DATE_ORDER" "$WHATSAPP_TIMEZONE" "$SELECTED_TEXT_ROOT" "$SAFARI_HISTORY" "$SAFARI_BOOKMARKS" "$CHROME_HISTORY" "$CHROME_BOOKMARKS" "$APPLE_NOTES_DATABASE" "$HERMES_DATABASE" "$HERMES_SOURCES" "$HERMES_ROLES" <<'PY'
import plistlib
import sys

(path, label, program, pythonpath, endpoint, source_id, spool, service,
 privacy_mode, source_type, imessage_database, whatsapp_export,
 whatsapp_conversation_id, whatsapp_owner_name, whatsapp_date_order,
 whatsapp_timezone, selected_text_root, safari_history, safari_bookmarks,
 chrome_history, chrome_bookmarks, apple_notes_database, hermes_database,
 hermes_sources, hermes_roles) = sys.argv[1:]
arguments = [
    program, "-m", "client.cli", "--placeholder",
]
common = [
    "--endpoint", endpoint, "--source-id", source_id,
    "--principal-id", "owner", "--keychain-service", service,
    "--keychain-account", source_id, "--privacy-mode", privacy_mode,
    "--spool", spool,
]
if source_type == "imessage":
    arguments[3] = "imessage-sync"
    arguments.extend(common + ["--database", imessage_database])
elif source_type == "whatsapp":
    arguments[3] = "whatsapp-export-sync"
    arguments.extend(common + [
        "--export", whatsapp_export,
        "--conversation-id", whatsapp_conversation_id,
        "--date-order", whatsapp_date_order,
        "--timezone", whatsapp_timezone,
    ])
    if whatsapp_owner_name:
        arguments.extend(["--owner-name", whatsapp_owner_name])
elif source_type == "selected-text":
    arguments[3] = "selected-text-sync"
    arguments.extend(common + ["--root", selected_text_root])
elif source_type in {"safari", "chrome"}:
    arguments[3] = "browser-sync"
    arguments.extend(common + ["--browser", source_type])
    history = safari_history if source_type == "safari" else chrome_history
    bookmarks = safari_bookmarks if source_type == "safari" else chrome_bookmarks
    if history:
        arguments.extend(["--history", history])
    if bookmarks:
        arguments.extend(["--bookmarks", bookmarks])
elif source_type == "apple-notes":
    arguments[3] = "apple-notes-sync"
    arguments.extend(common + ["--database", apple_notes_database])
elif source_type == "hermes":
    arguments[3] = "hermes-session-sync"
    arguments.extend(common + ["--database", hermes_database])
    for value in filter(None, hermes_sources.split(",")):
        arguments.extend(["--source", value])
    for value in filter(None, hermes_roles.split(",")):
        arguments.extend(["--role", value])
else:
    raise SystemExit("unsupported local connector")
value = {
    "Label": label,
    "ProgramArguments": arguments,
    "EnvironmentVariables": {
        "PYTHONPATH": pythonpath,
        "RECALL_KEYCHAIN_REFERENCE": "Keychain service/account only",
    },
    "RunAtLoad": True,
    "StartInterval": 60,
    "ProcessType": "Background",
    "Umask": 0o077,
    "StandardOutPath": spool + ".stdout.log",
    "StandardErrorPath": spool + ".stderr.log",
}
with open(path, "wb") as output:
    plistlib.dump(value, output, sort_keys=True)
PY
  preserve_canonical_route "$PLIST" "$SOURCE_ID"
  chmod 600 "$PLIST"
  if [ "$NO_LOAD" -eq 0 ] && command -v launchctl >/dev/null 2>&1; then
    launchctl bootstrap "gui/$(id -u)" "$PLIST"
  fi
}

write_supervisor_plist() {
  LABEL="ai.parcha.recall.connector-supervisor"
  PLIST="$LAUNCH_AGENTS/$LABEL.plist"
  STATE="$PREFIX/state/connector-supervisor.db"
  if [ "$NO_LOAD" -eq 0 ] && command -v launchctl >/dev/null 2>&1; then
    stop_launch_agent "$LABEL"
  fi
  if [ -n "$EXPORT_INBOX" ]; then
    PYTHONPATH="$PREFIX/lib" "$RUNTIME" -m client.cli connector-supervisor-config-preview \
      --config "$SUPERVISOR_CONFIG" --reserved-export-inbox "$EXPORT_INBOX" >/dev/null
  else
    PYTHONPATH="$PREFIX/lib" "$RUNTIME" -m client.cli connector-supervisor-config-preview \
      --config "$SUPERVISOR_CONFIG" >/dev/null
  fi
  "$RUNTIME" - "$PLIST" "$LABEL" "$RUNTIME" "$PREFIX/lib" "$SUPERVISOR_CONFIG" "$STATE" <<'PY'
import plistlib
import sys

path, label, program, pythonpath, config, state = sys.argv[1:]
value = {
    "Label": label,
    "ProgramArguments": [
        program, "-m", "client.cli", "connector-supervisor-run",
        "--config", config, "--state", state,
    ],
    "EnvironmentVariables": {"PYTHONPATH": pythonpath},
    "RunAtLoad": True,
    "KeepAlive": True,
    "ThrottleInterval": 10,
    "ProcessType": "Background",
    "Umask": 0o077,
    "StandardOutPath": state + ".stdout.log",
    "StandardErrorPath": state + ".stderr.log",
}
with open(path, "wb") as output:
    plistlib.dump(value, output, sort_keys=True)
PY
  chmod 600 "$PLIST"
  if [ "$NO_LOAD" -eq 0 ] && command -v launchctl >/dev/null 2>&1; then
    launchctl bootstrap "gui/$(id -u)" "$PLIST"
  fi
}

case ",$SOURCES," in *,claude,*) write_plist claude "$CLAUDE_ROOT" "" ;; esac
case ",$SOURCES," in *,codex,*) write_plist codex "$CODEX_ROOT" "$CODEX_ARCHIVE_ROOT" ;; esac
case ",$SOURCES," in *,cowork,*) write_cowork_plist ;; esac
case ",$SOURCES," in *,imessage,*) write_local_connector_plist imessage ;; esac
case ",$SOURCES," in *,whatsapp,*) write_local_connector_plist whatsapp ;; esac
case ",$SOURCES," in *,selected-text,*) write_local_connector_plist selected-text ;; esac
case ",$SOURCES," in *,safari,*) write_local_connector_plist safari ;; esac
case ",$SOURCES," in *,chrome,*) write_local_connector_plist chrome ;; esac
case ",$SOURCES," in *,apple-notes,*) write_local_connector_plist apple-notes ;; esac
case ",$SOURCES," in *,hermes,*) write_local_connector_plist hermes ;; esac
if [ -n "$EXPORT_INBOX" ]; then write_export_inbox_plist; fi
if [ -n "$SUPERVISOR_CONFIG" ]; then write_supervisor_plist; fi
if [ "$DISABLE_EXPORT_INBOX" -eq 1 ]; then
  LABEL="ai.parcha.recall.chatgpt-export"
  if [ "$NO_LOAD" -eq 0 ] && command -v launchctl >/dev/null 2>&1; then
    stop_launch_agent "$LABEL"
  fi
  rm -f "$LAUNCH_AGENTS/$LABEL.plist"
fi
for SOURCE_NAME in $DISABLE_SOURCES; do
  case "$SOURCE_NAME" in
    claude|claude-code) LABEL="ai.parcha.recall.claude" ;;
    codex|chatgpt-codex-desktop) LABEL="ai.parcha.recall.codex" ;;
    cowork) LABEL="ai.parcha.recall.cowork" ;;
    chatgpt-export) LABEL="ai.parcha.recall.chatgpt-export" ;;
    imessage) LABEL="ai.parcha.recall.imessage" ;;
    whatsapp|whatsapp-export) LABEL="ai.parcha.recall.whatsapp" ;;
    selected-text|obsidian) LABEL="ai.parcha.recall.selected-text" ;;
    safari) LABEL="ai.parcha.recall.safari" ;;
    chrome) LABEL="ai.parcha.recall.chrome" ;;
    apple-notes|notes) LABEL="ai.parcha.recall.apple-notes" ;;
    hermes) LABEL="ai.parcha.recall.hermes" ;;
  esac
  if [ "$NO_LOAD" -eq 0 ] && command -v launchctl >/dev/null 2>&1; then
    stop_launch_agent "$LABEL"
  fi
  rm -f "$LAUNCH_AGENTS/$LABEL.plist"
done
if [ "$DISABLE_SUPERVISOR" -eq 1 ]; then
  LABEL="ai.parcha.recall.connector-supervisor"
  if [ "$NO_LOAD" -eq 0 ] && command -v launchctl >/dev/null 2>&1; then
    stop_launch_agent "$LABEL"
  fi
  rm -f "$LAUNCH_AGENTS/$LABEL.plist"
fi
rm -rf "$PREFIX/.rollback"
mv "$TRANSACTION/old" "$PREFIX/.rollback"
TRANSACTION_COMMITTED=1
trap - EXIT HUP INT TERM
rm -rf "$TRANSACTION"
echo "installed Recall Brain Mac client"
echo "selected sources: $SOURCES; visibility: $VISIBILITY; privacy: $PRIVACY_MODE"
if [ -n "$SOURCES" ] || [ -n "$EXPORT_INBOX" ]; then
  echo "Keychain accounts use each selected source id as the account"
fi
