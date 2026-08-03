#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

if (( EUID == 0 )); then
  echo "Tether must be installed as the unprivileged user who will run Hermes and the coding agents." >&2
  exit 2
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ACTION="install"
HARNESS="auto"
DRY_RUN=0
RESTART=0

usage() {
  cat <<'EOF'
usage: install.sh [install|upgrade|rollback|uninstall] [options]

Options:
  --harness=auto|codex|claude-code|both
  --codex | --claude-code | --both
  --dry-run       Validate and show managed targets without changing files.
  --restart       Restart the Hermes gateway after a successful operation.
  -h, --help

Uninstall removes managed code but preserves Tether config, bridge state, and
rollback snapshots. See docs/OPERATIONS.md before deleting retained state.
EOF
}

if [[ $# -gt 0 ]]; then
  case "$1" in
    install|upgrade|rollback|uninstall) ACTION="$1"; shift ;;
  esac
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --harness=*) HARNESS="${1#--harness=}" ;;
    --codex) HARNESS="codex" ;;
    --claude-code) HARNESS="claude-code" ;;
    --both) HARNESS="both" ;;
    --dry-run) DRY_RUN=1 ;;
    --restart) RESTART=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

DATA_HOME="${XDG_DATA_HOME:-$HOME/.local/share}"
CONFIG_HOME="${XDG_CONFIG_HOME:-$HOME/.config}"
STATE_HOME="${XDG_STATE_HOME:-$HOME/.local/state}/tether-installer"
HERMES_HOME="${HERMES_HOME:-$HOME/.hermes}"
RUNTIME_HOME="$DATA_HOME/tether"
CONFIG_DIR="$CONFIG_HOME/tether"
PLUGIN_HOME="$HERMES_HOME/plugins/tether"
SKILL_SOURCE="$ROOT_DIR/skills/tether"
LOCAL_BIN="$HOME/.local/bin"
CODEX_ROOT="${CODEX_HOME:-$HOME/.codex}"
CLAUDE_ROOT="${CLAUDE_HOME:-${CLAUDE_CONFIG_DIR:-$HOME/.claude}}"
CURRENT_MANIFEST="$STATE_HOME/current.tsv"
LAST_BACKUP="$STATE_HOME/last-backup"
LOCK_FILE="$STATE_HOME/install.lock"
JOURNAL_FILE="$STATE_HOME/transaction.tsv"
MANIFEST_HEADER="# tether-manifest-v2"
declare -a LEGACY_HARNESSES=()

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 2
  }
}

preflight_platform() {
  [[ "$(uname -s)" == "Linux" ]] || {
    echo "Tether 0.2 supports Linux only; refusing an untested installation." >&2
    exit 2
  }
  local command
  for command in awk bash chmod cp date dirname flock install mkdir mktemp mv rm sha256sum stat sync uname; do
    require_command "$command"
  done
  if [[ "$ACTION" == "install" || "$ACTION" == "upgrade" ]]; then
    require_command node
    require_command python3
    node -e '
      const major = Number(process.versions.node.split(".")[0]);
      if (![22, 24].includes(major)) process.exit(1);
    ' || {
      echo "Supported Node.js LTS releases are 22 and 24." >&2
      exit 2
    }
    python3 - <<'PY' || {
import sys
raise SystemExit(0 if (3, 11) <= sys.version_info[:2] < (3, 15) else 1)
PY
      echo "Supported Python releases are 3.11 through 3.14." >&2
      exit 2
    }
  fi
}

package_version() {
  TETHER_PACKAGE_ROOT="$ROOT_DIR" node -e '
    const path = require("node:path");
    const packagePath = path.join(process.env.TETHER_PACKAGE_ROOT, "package.json");
    process.stdout.write(require(packagePath).version);
  '
}

validate_managed_root() {
  local label="$1" path="$2"
  local uid component owner mode permissions deepest_owner=""
  local -a parts=()

  [[ "$path" == /* ]] || {
    echo "$label must be an absolute path: $path" >&2
    exit 2
  }
  case "/${path#/}/" in
    *"/../"*|*"/./"*|*"//"*)
      echo "$label contains a non-canonical path component: $path" >&2
      exit 2
      ;;
  esac

  uid="$EUID"
  IFS='/' read -r -a parts <<<"${path#/}"
  component=""
  local part
  for part in "${parts[@]}"; do
    [[ -n "$part" ]] || continue
    component="$component/$part"
    if [[ -L "$component" ]]; then
      echo "refusing symlinked $label path component: $component" >&2
      exit 2
    fi
    if [[ -e "$component" ]]; then
      [[ -d "$component" ]] || {
        echo "refusing non-directory $label path component: $component" >&2
        exit 2
      }
      owner="$(stat -c '%u' -- "$component")"
      if [[ "$owner" != "0" && "$owner" != "$uid" ]]; then
        echo "refusing $label path component not owned by root or the current user: $component" >&2
        exit 2
      fi
      mode="$(stat -c '%a' -- "$component")"
      permissions=$((8#$mode))
      if [[ "$owner" == "$uid" ]] && (( permissions & 8#022 )); then
        echo "refusing group/world-writable $label path component: $component" >&2
        exit 2
      fi
      if [[ "$owner" == "0" ]] &&
         (( permissions & 8#022 )) &&
         ! (( permissions & 8#1000 )); then
        echo "refusing unsafe root-owned writable $label path component: $component" >&2
        exit 2
      fi
      deepest_owner="$owner"
    fi
  done

  if [[ -n "$deepest_owner" && "$deepest_owner" != "$uid" ]]; then
    echo "refusing $label whose nearest existing directory is not owned by the current user: $path" >&2
    exit 2
  fi
}

validate_managed_target() {
  local label="$1" target="$2" parent owner mode permissions
  [[ "$target" == /* && "$target" != *$'\n'* && "$target" != *$'\t'* ]] || {
    echo "$label must be an absolute single-line path: $target" >&2
    exit 2
  }
  parent="$(dirname -- "$target")"
  validate_managed_root "$label parent" "$parent"
  if [[ -L "$target" ]]; then
    echo "refusing symlinked $label: $target" >&2
    exit 2
  fi
  if [[ -e "$target" ]]; then
    [[ -f "$target" ]] || {
      echo "refusing non-file $label: $target" >&2
      exit 2
    }
    owner="$(stat -c '%u' -- "$target")"
    [[ "$owner" == "$EUID" ]] || {
      echo "refusing $label not owned by the current user: $target" >&2
      exit 2
    }
    mode="$(stat -c '%a' -- "$target")"
    permissions=$((8#$mode))
    if (( permissions & 8#022 )); then
      echo "refusing group/world-writable $label: $target" >&2
      exit 2
    fi
  fi
}

validate_managed_roots() {
  validate_managed_root "runtime root" "$RUNTIME_HOME"
  validate_managed_root "state root" "$STATE_HOME"
  validate_managed_root "config root" "$CONFIG_DIR"
  validate_managed_root "Hermes root" "$HERMES_HOME"
  validate_managed_root "Hermes plugin root" "$PLUGIN_HOME"
  validate_managed_root "local executable root" "$LOCAL_BIN"
}

validate_harness_roots() {
  if [[ "$HARNESS" == "codex" || "$HARNESS" == "both" ]]; then
    validate_managed_root "Codex root" "$CODEX_ROOT"
  fi
  if [[ "$HARNESS" == "claude-code" || "$HARNESS" == "both" ]]; then
    validate_managed_root "Claude Code root" "$CLAUDE_ROOT"
  fi
}

resolve_harness() {
  if [[ "$HARNESS" == "auto" ]]; then
    if [[ -d "$CODEX_ROOT" && -d "$CLAUDE_ROOT" ]]; then
      HARNESS="both"
    elif [[ -d "$CODEX_ROOT" ]]; then
      HARNESS="codex"
    elif [[ -d "$CLAUDE_ROOT" ]]; then
      HARNESS="claude-code"
    else
      echo "No Codex or Claude Code home was detected; pass --harness explicitly." >&2
      exit 2
    fi
  fi
  case "$HARNESS" in
    codex|claude-code|both) ;;
    *) echo "unknown harness: $HARNESS" >&2; exit 2 ;;
  esac
  validate_harness_roots
}

find_hermes() {
  local candidate
  for candidate in \
    "${HERMES_BIN:-}" \
    "$(command -v hermes 2>/dev/null || true)" \
    "$HOME/.local/bin/hermes" \
    "$HERMES_HOME/hermes-agent/venv/bin/hermes"
  do
    if [[ -n "$candidate" && -x "$candidate" ]]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  return 1
}

restart_gateway() {
  [[ "$RESTART" -eq 1 ]] || return 0
  local hermes rc
  if ! hermes="$(find_hermes)"; then
    echo "Hermes was not found; requested gateway restart cannot be completed." >&2
    return 74
  fi
  if "$hermes" gateway restart; then
    return 0
  else
    rc=$?
    echo "Hermes gateway restart failed; restoring the previous installation." >&2
    return "$rc"
  fi
}

LOCK_HELD=0
LOCK_FD=""
STAGE_ROOT=""
COMMITTING=0
ACTIVE_BACKUP=""
ACTIVE_BACKUP_ID=""
HANDLING_FAILURE=0
REMOVE_CONFIG_ON_ROLLBACK=0

cleanup() {
  if [[ -n "$STAGE_ROOT" && -d "$STAGE_ROOT" ]]; then
    rm -rf -- "$STAGE_ROOT"
  fi
  if [[ "$LOCK_HELD" -eq 1 && -n "$LOCK_FD" ]]; then
    flock -u "$LOCK_FD" 2>/dev/null || true
    exec {LOCK_FD}>&-
    LOCK_HELD=0
  fi
}
trap cleanup EXIT

valid_backup_id() {
  [[ "$1" =~ ^[0-9]{8}T[0-9]{6}Z-[0-9]+(-rollback-recovery)?$ ]]
}

validate_journal_file() {
  [[ -e "$JOURNAL_FILE" || -L "$JOURNAL_FILE" ]] || return 0
  if [[ -L "$JOURNAL_FILE" || ! -f "$JOURNAL_FILE" ||
        "$(stat -c '%u' -- "$JOURNAL_FILE")" != "$EUID" ||
        "$(stat -c '%h' -- "$JOURNAL_FILE")" != "1" ||
        "$(stat -c '%a' -- "$JOURNAL_FILE")" != "600" ]]; then
    echo "refusing unsafe Tether installer transaction journal: $JOURNAL_FILE" >&2
    return 1
  fi
}

write_transaction_journal() {
  local phase="$1" action="$2" backup_id="$3" remove_config="$4"
  local temp="$STATE_HOME/.transaction-${$}.tmp"
  case "$phase" in
    rollback|committed) ;;
    *) echo "invalid transaction phase: $phase" >&2; return 2 ;;
  esac
  case "$action" in
    install|upgrade|rollback|uninstall) ;;
    *) echo "invalid transaction action: $action" >&2; return 2 ;;
  esac
  valid_backup_id "$backup_id" || {
    echo "invalid transaction backup identifier: $backup_id" >&2
    return 2
  }
  [[ "$remove_config" == "0" || "$remove_config" == "1" ]] || {
    echo "invalid transaction config state" >&2
    return 2
  }
  printf 'version\t1\nphase\t%s\naction\t%s\nbackup\t%s\nremove_config\t%s\n' \
    "$phase" "$action" "$backup_id" "$remove_config" >"$temp"
  chmod 600 "$temp"
  sync -f "$temp"
  mv -f -- "$temp" "$JOURNAL_FILE"
  sync -f "$STATE_HOME"
}

clear_transaction_journal() {
  rm -f -- "$JOURNAL_FILE"
  sync -f "$STATE_HOME"
}

sync_target_state() {
  local target="$1" parent
  parent="$(dirname -- "$target")"
  if [[ -f "$target" && ! -L "$target" ]]; then
    sync -f "$target"
  fi
  if [[ -d "$parent" && ! -L "$parent" ]]; then
    sync -f "$parent"
  fi
  return 0
}

sync_backup_state() {
  local backup="$1"
  local records="$backup/records.tsv"
  local index existed mode target
  if [[ -f "$records" ]]; then
    while IFS=$'\t' read -r index existed mode target; do
      [[ -n "$target" ]] || continue
      sync_target_state "$target"
    done <"$records"
  fi
  sync_target_state "$CURRENT_MANIFEST"
  sync_target_state "$LAST_BACKUP"
  sync_target_state "$CONFIG_DIR/config.toml"
}

read_transaction_journal() {
  local key value extra
  local version="" phase="" action="" backup="" remove_config=""
  validate_journal_file || return 1
  [[ -f "$JOURNAL_FILE" ]] || return 1
  while IFS=$'\t' read -r key value extra; do
    [[ -z "$extra" && -n "$key" ]] || {
      echo "invalid Tether installer transaction journal record" >&2
      return 1
    }
    case "$key" in
      version) [[ -z "$version" ]] || return 1; version="$value" ;;
      phase) [[ -z "$phase" ]] || return 1; phase="$value" ;;
      action) [[ -z "$action" ]] || return 1; action="$value" ;;
      backup) [[ -z "$backup" ]] || return 1; backup="$value" ;;
      remove_config) [[ -z "$remove_config" ]] || return 1; remove_config="$value" ;;
      *) echo "unknown Tether installer transaction journal field: $key" >&2; return 1 ;;
    esac
  done <"$JOURNAL_FILE"
  [[ "$version" == "1" ]] || {
    echo "unsupported Tether installer transaction journal version" >&2
    return 1
  }
  case "$phase" in rollback|committed) ;; *) return 1 ;; esac
  case "$action" in install|upgrade|rollback|uninstall) ;; *) return 1 ;; esac
  valid_backup_id "$backup" || return 1
  [[ "$remove_config" == "0" || "$remove_config" == "1" ]] || return 1
  printf '%s\t%s\t%s\t%s\n' "$phase" "$action" "$backup" "$remove_config"
}

cleanup_stale_stages() {
  local stale owner
  for stale in "$STATE_HOME"/stage.*; do
    [[ -e "$stale" || -L "$stale" ]] || continue
    if [[ -L "$stale" || ! -d "$stale" ]]; then
      echo "refusing unsafe stale installer stage: $stale" >&2
      return 1
    fi
    owner="$(stat -c '%u' -- "$stale")"
    [[ "$owner" == "$EUID" ]] || {
      echo "refusing stale installer stage not owned by the current user: $stale" >&2
      return 1
    }
    rm -rf -- "$stale"
  done
}

recover_incomplete_transaction() {
  local record phase interrupted_action backup_id remove_config backup rollback_failed=0
  if [[ ! -e "$JOURNAL_FILE" && ! -L "$JOURNAL_FILE" ]]; then
    cleanup_stale_stages
    return
  fi
  record="$(read_transaction_journal)" || {
    echo "Tether found an invalid transaction journal and will not continue." >&2
    exit 74
  }
  IFS=$'\t' read -r phase interrupted_action backup_id remove_config <<<"$record"
  if [[ "$phase" == "committed" ]]; then
    clear_transaction_journal
    cleanup_stale_stages
    return
  fi

  backup="$STATE_HOME/backups/$backup_id"
  echo "Recovering interrupted Tether $interrupted_action transaction $backup_id." >&2
  if ! restore_backup "$backup"; then
    rollback_failed=1
  fi
  if ! restore_plugin_state "$backup"; then
    rollback_failed=1
  fi
  if [[ "$remove_config" == "1" ]] && ! rm -f -- "$CONFIG_DIR/config.toml"; then
    rollback_failed=1
  fi
  if [[ "$rollback_failed" -eq 0 ]] && ! sync_backup_state "$backup"; then
    rollback_failed=1
  fi
  if [[ "$rollback_failed" -ne 0 ]]; then
    echo "Interrupted transaction recovery failed; journal retained at $JOURNAL_FILE." >&2
    exit 74
  fi
  clear_transaction_journal
  cleanup_stale_stages
  echo "Recovered the previous Tether state." >&2
}

acquire_lock() {
  local uid path_identity fd_identity fd_owner fd_links fd_mode
  validate_managed_root "state root" "$STATE_HOME"
  install -d -m 700 "$STATE_HOME"
  validate_managed_root "state root" "$STATE_HOME"
  validate_managed_root "backup root" "$STATE_HOME/backups"
  install -d -m 700 "$STATE_HOME/backups"
  validate_managed_root "backup root" "$STATE_HOME/backups"

  if [[ ! -e "$LOCK_FILE" && ! -L "$LOCK_FILE" ]]; then
    (
      set -o noclobber
      : >"$LOCK_FILE"
    ) 2>/dev/null || true
  fi
  if [[ -L "$LOCK_FILE" || ! -f "$LOCK_FILE" ]]; then
    echo "refusing unsafe Tether installer lock file: $LOCK_FILE" >&2
    exit 3
  fi
  uid="$(id -u)"
  if [[ "$(stat -c '%u' -- "$LOCK_FILE")" != "$uid" ||
        "$(stat -c '%h' -- "$LOCK_FILE")" != "1" ||
        "$(stat -c '%a' -- "$LOCK_FILE")" != "600" ]]; then
    echo "refusing Tether installer lock with unsafe ownership, mode, or link count: $LOCK_FILE" >&2
    exit 3
  fi

  exec {LOCK_FD}<>"$LOCK_FILE"
  path_identity="$(stat -c '%d:%i' -- "$LOCK_FILE")"
  fd_identity="$(stat -Lc '%d:%i' -- "/proc/self/fd/$LOCK_FD")"
  fd_owner="$(stat -Lc '%u' -- "/proc/self/fd/$LOCK_FD")"
  fd_links="$(stat -Lc '%h' -- "/proc/self/fd/$LOCK_FD")"
  fd_mode="$(stat -Lc '%a' -- "/proc/self/fd/$LOCK_FD")"
  if [[ "$path_identity" != "$fd_identity" || "$fd_owner" != "$uid" ||
        "$fd_links" != "1" || "$fd_mode" != "600" ]]; then
    exec {LOCK_FD}>&-
    LOCK_FD=""
    echo "Tether installer lock changed or became unsafe while opening it: $LOCK_FILE" >&2
    exit 3
  fi
  if ! flock -n "$LOCK_FD"; then
    exec {LOCK_FD}>&-
    LOCK_FD=""
    echo "another Tether install operation holds $LOCK_FILE" >&2
    exit 3
  fi
  if [[ -L "$LOCK_FILE" ||
        "$(stat -c '%d:%i' -- "$LOCK_FILE")" != "$fd_identity" ]]; then
    flock -u "$LOCK_FD" 2>/dev/null || true
    exec {LOCK_FD}>&-
    LOCK_FD=""
    echo "Tether installer lock changed while acquiring it: $LOCK_FILE" >&2
    exit 3
  fi
  LOCK_HELD=1
  recover_incomplete_transaction
}

declare -a SOURCES=()
declare -a TARGETS=()
declare -a MODES=()

add_file() {
  local source="$1" target="$2" mode="$3"
  [[ -f "$source" && ! -L "$source" ]] || {
    echo "required package file is missing or unsafe: $source" >&2
    exit 2
  }
  validate_managed_target "managed target" "$target"
  SOURCES+=("$source")
  TARGETS+=("$target")
  MODES+=("$mode")
}

add_skill() {
  local harness_home="$1" harness_name="$2"
  local destination="$harness_home/skills/tether"
  add_file "$SKILL_SOURCE/SKILL.md" "$destination/SKILL.md" 644
  add_file "$SKILL_SOURCE/agents/openai.yaml" "$destination/agents/openai.yaml" 644
  add_file "$SKILL_SOURCE/references/setup.md" "$destination/references/setup.md" 644
  add_file "$SKILL_SOURCE/references/contract.md" "$destination/references/contract.md" 644
  add_file "$SKILL_SOURCE/scripts/tether_notify.py" "$destination/scripts/tether_notify.py" 700

  local legacy="$harness_home/skills/hermes-slack-bridge"
  if [[ -d "$legacy" ]]; then
    LEGACY_HARNESSES+=("$harness_name")
    add_file \
      "$ROOT_DIR/runtime/compat/hermes-slack-bridge-SKILL.md" \
      "$legacy/SKILL.md" \
      644
    add_file \
      "$SKILL_SOURCE/scripts/tether_notify.py" \
      "$legacy/scripts/hermes_notify.py" \
      700
  fi
}

build_install_plan() {
  [[ -f "$ROOT_DIR/package.json" && -d "$SKILL_SOURCE" ]] || {
    echo "Install and upgrade must run from the complete Tether package." >&2
    exit 2
  }
  resolve_harness
  add_file "$ROOT_DIR/runtime/bridge_runtime.py" "$RUNTIME_HOME/bridge_runtime.py" 600
  add_file "$ROOT_DIR/runtime/hermes_compat.py" "$RUNTIME_HOME/hermes_compat.py" 600
  add_file "$ROOT_DIR/runtime/routing.py" "$RUNTIME_HOME/routing.py" 600
  add_file "$ROOT_DIR/runtime/security.py" "$RUNTIME_HOME/security.py" 600
  add_file "$ROOT_DIR/runtime/slack_protocol.py" "$RUNTIME_HOME/slack_protocol.py" 600
  add_file "$SKILL_SOURCE/scripts/tether_notify.py" "$RUNTIME_HOME/tether_notify.py" 700
  add_file "$ROOT_DIR/install.sh" "$RUNTIME_HOME/install.sh" 700
  add_file "$ROOT_DIR/package.json" "$RUNTIME_HOME/package.json" 600
  add_file "$ROOT_DIR/runtime/plugin/__init__.py" "$PLUGIN_HOME/__init__.py" 600
  add_file "$ROOT_DIR/runtime/plugin/plugin.yaml" "$PLUGIN_HOME/plugin.yaml" 644
  add_file "$ROOT_DIR/bin/tether.js" "$LOCAL_BIN/tether" 700
  if [[ "$HARNESS" == "codex" || "$HARNESS" == "both" ]]; then
    add_skill "$CODEX_ROOT" codex
  fi
  if [[ "$HARNESS" == "claude-code" || "$HARNESS" == "both" ]]; then
    add_skill "$CLAUDE_ROOT" claude-code
  fi
}

restore_backup() {
  local backup="$1"
  local records="$backup/records.tsv"
  [[ -f "$records" ]] || {
    echo "rollback snapshot is incomplete: $backup" >&2
    return 1
  }
  while IFS=$'\t' read -r index existed mode target; do
    [[ -n "$target" ]] || continue
    local parent temp
    validate_managed_target "rollback target" "$target"
    parent="$(dirname "$target")"
    install -d -m 700 "$parent"
    temp="$parent/.tether-restore-${$}-${index}"
    if [[ "$existed" == "1" ]]; then
      install -m "$mode" "$backup/files/$index" "$temp"
      mv -f -- "$temp" "$target"
    else
      rm -f -- "$target"
    fi
  done <"$records"

  if [[ -f "$backup/previous-current.tsv" ]]; then
    cp -p -- "$backup/previous-current.tsv" "$CURRENT_MANIFEST.tmp"
    mv -f -- "$CURRENT_MANIFEST.tmp" "$CURRENT_MANIFEST"
  else
    rm -f -- "$CURRENT_MANIFEST"
  fi
  if [[ -f "$backup/previous-backup" ]]; then
    cp -p -- "$backup/previous-backup" "$LAST_BACKUP.tmp"
    mv -f -- "$LAST_BACKUP.tmp" "$LAST_BACKUP"
  else
    rm -f -- "$LAST_BACKUP"
  fi
}

current_plugin_state() {
  local hermes="$1" output state
  output="$("$hermes" plugins list --plain)" || {
    echo "could not read Hermes plugin state" >&2
    return 1
  }
  state="$(
    awk '
      $NF == "tether" {
        count += 1
        if ($1 == "enabled") {
          enabled += 1
        }
      }
      END {
        if (count > 1) exit 2
        print enabled == 1 ? "enabled" : "disabled"
      }
    ' <<<"$output"
  )" || {
    echo "Hermes reported an ambiguous Tether plugin state" >&2
    return 1
  }
  printf '%s\n' "$state"
}

snapshot_plugin_state() {
  local backup="$1" hermes="" state="disabled"
  if hermes="$(find_hermes)"; then
    state="$(current_plugin_state "$hermes")" || return 1
  fi
  printf '%s\n' "$state" >"$backup/plugin-state"
  chmod 600 "$backup/plugin-state"
}

restore_plugin_state() {
  local backup="$1"
  [[ -f "$backup/plugin-state" ]] || return 0
  local desired current hermes=""
  desired="$(<"$backup/plugin-state")"
  case "$desired" in
    enabled|disabled) ;;
    *)
      echo "rollback snapshot has invalid Hermes plugin state: $desired" >&2
      return 1
      ;;
  esac
  if ! hermes="$(find_hermes)"; then
    if [[ "$desired" == "enabled" ]]; then
      echo "Hermes is unavailable; cannot restore enabled Tether plugin state" >&2
      return 1
    fi
    return 0
  fi
  current="$(current_plugin_state "$hermes")" || return 1
  [[ "$current" != "$desired" ]] || return 0
  if [[ "$desired" == "enabled" ]]; then
    "$hermes" plugins enable tether
  else
    "$hermes" plugins disable tether
  fi
}

handle_error() {
  local code="$1"
  local reason="${2:-error}" rollback_failed=0
  if [[ "$HANDLING_FAILURE" -eq 1 ]]; then
    exit "$code"
  fi
  HANDLING_FAILURE=1
  trap - ERR HUP INT TERM
  set +e
  if [[ "$COMMITTING" -eq 1 && -n "$ACTIVE_BACKUP" ]]; then
    echo "Tether $ACTION was interrupted by $reason; restoring the previous state." >&2
    restore_backup "$ACTIVE_BACKUP" || rollback_failed=1
    restore_plugin_state "$ACTIVE_BACKUP" || rollback_failed=1
    if [[ "$REMOVE_CONFIG_ON_ROLLBACK" -eq 1 ]]; then
      rm -f -- "$CONFIG_DIR/config.toml" || rollback_failed=1
    fi
    if [[ "$rollback_failed" -ne 0 ]]; then
      echo "Automatic rollback failed. Snapshot: $ACTIVE_BACKUP" >&2
    else
      sync_backup_state "$ACTIVE_BACKUP" || rollback_failed=1
      if [[ "$rollback_failed" -eq 0 ]]; then
        clear_transaction_journal || rollback_failed=1
      fi
      if [[ "$rollback_failed" -ne 0 ]]; then
        echo "Automatic rollback could not be made durable. Snapshot: $ACTIVE_BACKUP" >&2
      fi
    fi
  fi
  exit "$code"
}
trap 'handle_error $?' ERR
trap 'handle_error 129 HUP' HUP
trap 'handle_error 130 INT' INT
trap 'handle_error 143 TERM' TERM

snapshot_plan() {
  local backup="$1"
  install -d -m 700 "$backup" "$backup/files"
  : >"$backup/records.tsv"
  if [[ -f "$CURRENT_MANIFEST" ]]; then
    cp -p -- "$CURRENT_MANIFEST" "$backup/previous-current.tsv"
  fi
  if [[ -f "$LAST_BACKUP" ]]; then
    cp -p -- "$LAST_BACKUP" "$backup/previous-backup"
  fi
  snapshot_plugin_state "$backup"
  local index target mode
  for index in "${!TARGETS[@]}"; do
    target="${TARGETS[$index]}"
    validate_managed_target "snapshot target" "$target"
    if [[ -f "$target" ]]; then
      mode="$(stat -c '%a' "$target")"
      cp -p -- "$target" "$backup/files/$index"
      printf '%s\t1\t%s\t%s\n' "$index" "$mode" "$target" >>"$backup/records.tsv"
    else
      printf '%s\t0\t%s\t%s\n' "$index" "${MODES[$index]}" "$target" >>"$backup/records.tsv"
    fi
  done
}

begin_transaction() {
  local backup="$1" remove_config="${2:-0}"
  local backup_id
  backup_id="$(basename -- "$backup")"
  valid_backup_id "$backup_id" || {
    echo "invalid transaction backup identifier: $backup_id" >&2
    return 2
  }
  sync -f "$backup"
  ACTIVE_BACKUP="$backup"
  ACTIVE_BACKUP_ID="$backup_id"
  REMOVE_CONFIG_ON_ROLLBACK="$remove_config"
  COMMITTING=1
  write_transaction_journal rollback "$ACTION" "$backup_id" "$remove_config"
}

complete_transaction() {
  sync_backup_state "$ACTIVE_BACKUP"
  write_transaction_journal \
    committed "$ACTION" "$ACTIVE_BACKUP_ID" "$REMOVE_CONFIG_ON_ROLLBACK"
  clear_transaction_journal
  COMMITTING=0
  ACTIVE_BACKUP=""
  ACTIVE_BACKUP_ID=""
  REMOVE_CONFIG_ON_ROLLBACK=0
}

stage_plan() {
  STAGE_ROOT="$(mktemp -d "$STATE_HOME/stage.XXXXXX")"
  install -d -m 700 "$STAGE_ROOT/files"
  local index
  for index in "${!SOURCES[@]}"; do
    install -m "${MODES[$index]}" "${SOURCES[$index]}" "$STAGE_ROOT/files/$index"
  done
}

commit_plan() {
  local install_id="$1"
  local index target parent temp hash legacy="none"
  local manifest_tmp="$STATE_HOME/current.tsv.new-$install_id"
  if ((${#LEGACY_HARNESSES[@]})); then
    local IFS=,
    legacy="${LEGACY_HARNESSES[*]}"
  fi
  {
    printf '%s\n' "$MANIFEST_HEADER"
    printf '@harness\t%s\n' "$HARNESS"
    printf '@runtime_home\t%s\n' "$RUNTIME_HOME"
    printf '@plugin_home\t%s\n' "$PLUGIN_HOME"
    printf '@local_bin\t%s\n' "$LOCAL_BIN"
    printf '@codex_root\t%s\n' "$CODEX_ROOT"
    printf '@claude_root\t%s\n' "$CLAUDE_ROOT"
    printf '@legacy\t%s\n' "$legacy"
  } >"$manifest_tmp"
  for index in "${!TARGETS[@]}"; do
    target="${TARGETS[$index]}"
    validate_managed_target "commit target" "$target"
    parent="$(dirname "$target")"
    install -d -m 700 "$parent"
    temp="$parent/.tether-new-$install_id-$index"
    install -m "${MODES[$index]}" "$STAGE_ROOT/files/$index" "$temp"
    mv -f -- "$temp" "$target"
    hash="$(sha256sum "$target" | awk '{print $1}')"
    printf '%s\t%s\t%s\n' "$target" "${MODES[$index]}" "$hash" >>"$manifest_tmp"
  done
  mv -f -- "$manifest_tmp" "$CURRENT_MANIFEST"
  printf '%s\n' "$install_id" >"$LAST_BACKUP.tmp"
  mv -f -- "$LAST_BACKUP.tmp" "$LAST_BACKUP"
}

perform_install() {
  build_install_plan
  if [[ "$DRY_RUN" -eq 1 ]]; then
    printf 'Tether %s %s preflight passed for %s.\n' \
      "$(package_version)" "$ACTION" "$HARNESS"
    printf 'manage %s\n' "${TARGETS[@]}"
    return 0
  fi

  acquire_lock
  if [[ "$ACTION" == "upgrade" && ! -f "$CURRENT_MANIFEST" ]]; then
    echo "No managed manifest was found; adopting and snapshotting the existing files." >&2
  fi
  stage_plan
  local install_id backup remove_config=0
  install_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
  backup="$STATE_HOME/backups/$install_id"
  [[ -f "$CONFIG_DIR/config.toml" ]] || remove_config=1
  snapshot_plan "$backup"
  begin_transaction "$backup" "$remove_config"
  commit_plan "$install_id"

  install -d -m 700 "$CONFIG_DIR"
  if [[ ! -f "$CONFIG_DIR/config.toml" ]]; then
    install -m 600 "$ROOT_DIR/runtime/config.example.toml" "$CONFIG_DIR/config.toml"
  fi
  restart_gateway
  local version
  version="$(package_version)"
  complete_transaction
  echo "Installed Tether $version for $HARNESS."
  echo "Rollback snapshot: $backup"
  if [[ ":$PATH:" != *":$LOCAL_BIN:"* ]]; then
    echo "Add $LOCAL_BIN to PATH, or run $LOCAL_BIN/tether directly."
  fi
  echo "Next: run tether setup. Existing bridge state and operator config were preserved."
}

load_backup_targets() {
  local backup="$1"
  local records="$backup/records.tsv"
  [[ -f "$records" ]] || {
    echo "rollback snapshot is incomplete: $backup" >&2
    exit 2
  }
  SOURCES=()
  TARGETS=()
  MODES=()
  local index existed mode target
  while IFS=$'\t' read -r index existed mode target; do
    [[ -n "$target" ]] || continue
    validate_managed_target "rollback target" "$target"
    SOURCES+=("")
    TARGETS+=("$target")
    MODES+=("$mode")
  done <"$records"
}

perform_rollback() {
  acquire_lock
  [[ -f "$LAST_BACKUP" ]] || {
    echo "No Tether rollback snapshot is available." >&2
    exit 2
  }
  local backup_id backup
  backup_id="$(<"$LAST_BACKUP")"
  [[ "$backup_id" =~ ^[0-9]{8}T[0-9]{6}Z-[0-9]+$ ]] || {
    echo "invalid rollback snapshot identifier" >&2
    exit 2
  }
  backup="$STATE_HOME/backups/$backup_id"
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "Would restore Tether snapshot $backup_id."
    return 0
  fi
  load_backup_targets "$backup"
  local recovery_id recovery_backup
  recovery_id="$(date -u +%Y%m%dT%H%M%SZ)-$$-rollback-recovery"
  recovery_backup="$STATE_HOME/backups/$recovery_id"
  snapshot_plan "$recovery_backup"
  begin_transaction "$recovery_backup" 0
  restore_backup "$backup"
  restore_plugin_state "$backup"
  restart_gateway
  complete_transaction
  echo "Restored Tether snapshot $backup_id."
  echo "Operator config and bridge state were not changed."
  rm -rf -- "$recovery_backup" || {
    echo "WARN could not remove completed rollback recovery snapshot: $recovery_backup" >&2
  }
}

load_current_targets() {
  [[ -f "$CURRENT_MANIFEST" ]] || {
    echo "No managed Tether installation was found." >&2
    exit 2
  }
  local first_line target mode hash extra key value
  local metadata_count=0 row_count=0 line_number=0
  local v2=0
  declare -A seen_metadata=()
  declare -A seen_targets=()
  IFS= read -r first_line <"$CURRENT_MANIFEST" || true
  [[ "$first_line" == "$MANIFEST_HEADER" ]] && v2=1
  while IFS=$'\t' read -r target mode hash extra; do
    line_number=$((line_number + 1))
    [[ -n "$target" ]] || continue
    if [[ "$line_number" -eq 1 && "$v2" -eq 1 ]]; then
      [[ "$target" == "$MANIFEST_HEADER" && -z "$mode$hash$extra" ]] || {
        echo "invalid Tether installer manifest header" >&2
        exit 2
      }
      continue
    fi
    if [[ "$v2" -eq 1 && "$target" == @* ]]; then
      key="${target#@}"
      value="$mode"
      [[ -z "$hash$extra" && -n "$value" && -z "${seen_metadata[$key]:-}" ]] || {
        echo "invalid Tether installer manifest metadata" >&2
        exit 2
      }
      case "$key" in
        harness|runtime_home|plugin_home|local_bin|codex_root|claude_root|legacy) ;;
        *) echo "unknown Tether installer manifest metadata: $key" >&2; exit 2 ;;
      esac
      seen_metadata["$key"]=1
      metadata_count=$((metadata_count + 1))
      continue
    fi
    [[ -z "$extra" && "$mode" =~ ^[0-7]{3,4}$ &&
       "$hash" =~ ^[0-9a-f]{64}$ && -z "${seen_targets[$target]:-}" ]] || {
      echo "invalid Tether installer manifest record" >&2
      exit 2
    }
    validate_managed_target "uninstall target" "$target"
    seen_targets["$target"]=1
    TARGETS+=("$target")
    MODES+=("$mode")
    SOURCES+=("$hash")
    row_count=$((row_count + 1))
  done <"$CURRENT_MANIFEST"
  if [[ "$v2" -eq 1 && "$metadata_count" -ne 7 ]]; then
    echo "incomplete Tether installer manifest metadata" >&2
    exit 2
  fi
  [[ "$row_count" -gt 0 ]] || {
    echo "Tether installer manifest has no managed files" >&2
    exit 2
  }
}

perform_uninstall() {
  acquire_lock
  load_current_targets
  if [[ "$DRY_RUN" -eq 1 ]]; then
    printf 'remove managed file %s\n' "${TARGETS[@]}"
    echo "Preserve $CONFIG_DIR and $HERMES_HOME/bridges.db."
    return 0
  fi
  local hermes="" plugin_state="disabled"
  local uninstall_id backup index target expected actual
  uninstall_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
  backup="$STATE_HOME/backups/$uninstall_id"
  snapshot_plan "$backup"
  begin_transaction "$backup" 0
  if hermes="$(find_hermes)"; then
    plugin_state="$(<"$backup/plugin-state")"
    if [[ "$plugin_state" == "enabled" ]]; then
      "$hermes" plugins disable tether
    fi
  fi
  for index in "${!TARGETS[@]}"; do
    target="${TARGETS[$index]}"
    expected="${SOURCES[$index]}"
    if [[ ! -e "$target" ]]; then
      continue
    fi
    if [[ -L "$target" || ! -f "$target" ]]; then
      echo "WARN preserving unmanaged target type: $target" >&2
      continue
    fi
    actual="$(sha256sum "$target" | awk '{print $1}')"
    if [[ "$actual" != "$expected" ]]; then
      echo "WARN preserving locally modified managed file: $target" >&2
      continue
    fi
    rm -f -- "$target"
  done
  cp -p -- "$CURRENT_MANIFEST" "$backup/previous-current.tsv"
  rm -f -- "$CURRENT_MANIFEST"
  printf '%s\n' "$uninstall_id" >"$LAST_BACKUP.tmp"
  mv -f -- "$LAST_BACKUP.tmp" "$LAST_BACKUP"
  if [[ -n "$hermes" ]]; then
    "$hermes" gateway restart
  fi
  complete_transaction
  echo "Removed Tether managed code."
  echo "Preserved config: $CONFIG_DIR"
  echo "Preserved bridge state: $HERMES_HOME/bridges.db"
  echo "Run the package installer with 'rollback' to restore snapshot $uninstall_id."
}

preflight_platform
validate_managed_roots
case "$ACTION" in
  install|upgrade) perform_install ;;
  rollback) perform_rollback ;;
  uninstall) perform_uninstall ;;
esac
