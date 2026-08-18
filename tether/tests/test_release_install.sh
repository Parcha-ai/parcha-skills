#!/usr/bin/env bash
# This test intentionally installs command wrappers before defining final
# no-runtime stubs near the end of the lifecycle.
# shellcheck disable=SC2218
set -Eeuo pipefail
umask 077

PACKAGE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPOSITORY_ROOT="$(cd "$PACKAGE_ROOT/.." && pwd)"
EXPECTED_VERSION="0.3.0-beta.1"
TEST_ROOT="$(mktemp -d)"
trap 'rm -rf -- "$TEST_ROOT"' EXIT

fail() {
  echo "release-install test failed: $*" >&2
  exit 1
}

assert_file() {
  [[ -f "$1" ]] || fail "missing file: $1"
}

assert_absent() {
  [[ ! -e "$1" ]] || fail "unexpected path: $1"
}

assert_mode() {
  local actual
  actual="$(stat -c '%a' "$1")"
  [[ "$actual" == "$2" ]] || fail "mode for $1 is $actual, expected $2"
}

assert_contains() {
  grep -Fq -- "$2" "$1" || fail "$1 does not contain: $2"
}

assert_equals() {
  [[ "$1" == "$2" ]] || fail "got '$1', expected '$2'"
}

if command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
  ROOT_GUARD="$TEST_ROOT/root-guard"
  mkdir -p "$ROOT_GUARD"
  cat >"$ROOT_GUARD/hermes" <<EOF
#!/usr/bin/env bash
touch "$ROOT_GUARD/hermes-ran"
EOF
  chmod 700 "$ROOT_GUARD/hermes"
  set +e
  # The test user owns the destination; sudo only executes the installer.
  # shellcheck disable=SC2024
  sudo -n env \
    HOME="$ROOT_GUARD/home" \
    XDG_DATA_HOME="$ROOT_GUARD/data" \
    XDG_CONFIG_HOME="$ROOT_GUARD/config" \
    XDG_STATE_HOME="$ROOT_GUARD/state" \
    HERMES_HOME="$ROOT_GUARD/hermes-home" \
    HERMES_BIN="$ROOT_GUARD/hermes" \
    "$PACKAGE_ROOT/install.sh" install --dry-run --harness=codex \
    >"$ROOT_GUARD/output" 2>&1
  root_guard_rc=$?
  set -e
  [[ "$root_guard_rc" -eq 2 ]] ||
    fail "root guard returned $root_guard_rc instead of 2"
  assert_contains "$ROOT_GUARD/output" "must be installed as the unprivileged user"
  assert_absent "$ROOT_GUARD/hermes-ran"
  assert_absent "$ROOT_GUARD/state/tether-installer"
  assert_absent "$ROOT_GUARD/data/tether"
fi

version="$(node -p "require('$PACKAGE_ROOT/package.json').version")"
[[ "$version" == "$EXPECTED_VERSION" ]] || fail "package version is $version"
[[ "$(node "$PACKAGE_ROOT/bin/tether.js" --version)" == "tether $EXPECTED_VERSION" ]] ||
  fail "package CLI version disagrees"

python3 - "$REPOSITORY_ROOT" "$EXPECTED_VERSION" <<'PY'
import json
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
expected = sys.argv[2]
json_paths = [
    root / "tether/package.json",
    root / "tether/package-lock.json",
    root / "tether/.claude-plugin/plugin.json",
    root / "tether/.codex-plugin/plugin.json",
]
versions = {path.as_posix(): json.loads(path.read_text())["version"] for path in json_paths}
marketplace = json.loads((root / ".claude-plugin/marketplace.json").read_text())
versions[".claude-plugin/marketplace.json"] = next(
    plugin["version"] for plugin in marketplace["plugins"] if plugin["name"] == "tether"
)
plugin_yaml = (root / "tether/runtime/plugin/plugin.yaml").read_text()
match = re.search(r"^version:\s*[\"']?([^\"'\s]+)", plugin_yaml, re.MULTILINE)
if not match:
    raise SystemExit("runtime plugin version is missing")
versions["tether/runtime/plugin/plugin.yaml"] = match.group(1)
wrong = {path: value for path, value in versions.items() if value != expected}
if wrong:
    raise SystemExit(f"inconsistent Tether versions: {wrong}")

agent_marketplace = json.loads(
    (root / "tether/.agents/plugins/marketplace.json").read_text()
)
tether = next(row for row in agent_marketplace["plugins"] if row["name"] == "tether")
marketplace_ref = tether["source"]["ref"]
if not re.fullmatch(r"[0-9a-f]{40}", marketplace_ref):
    raise SystemExit(
        "agent marketplace ref must be an immutable 40-character commit SHA"
    )
PY

export HOME="$TEST_ROOT/home"
export XDG_DATA_HOME="$TEST_ROOT/data"
export XDG_CONFIG_HOME="$TEST_ROOT/config"
export XDG_STATE_HOME="$TEST_ROOT/state"
export HERMES_HOME="$TEST_ROOT/hermes"
export CODEX_HOME="$TEST_ROOT/codex"
export CLAUDE_HOME="$TEST_ROOT/claude"
FAKE_BIN="$TEST_ROOT/bin"
mkdir -p "$HOME" "$CODEX_HOME" "$CLAUDE_HOME" "$FAKE_BIN"
REAL_INSTALL="$(command -v install)"
REAL_MV="$(command -v mv)"
REAL_SHA256SUM="$(command -v sha256sum)"
export REAL_INSTALL REAL_MV REAL_SHA256SUM
export PATH="$FAKE_BIN:$PATH"
export HERMES_BIN="$FAKE_BIN/hermes"
export HERMES_TEST_STATE="$TEST_ROOT/hermes.state"
printf 'enabled\n' >"$HERMES_TEST_STATE"

cat >"$HERMES_BIN" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"${HERMES_TEST_LOG:?}"
command_line="$*"
if [[ "${HERMES_TEST_FAIL_ON:-}" == "$command_line" ]]; then
  exit 74
fi
case "$command_line" in
  "plugins list --plain")
    case "$(<"${HERMES_TEST_STATE:?}")" in
      enabled) printf 'enabled      user     0.2.0    tether\n' ;;
      disabled) printf 'disabled     user     0.2.0    tether\n' ;;
      absent) ;;
      *) exit 75 ;;
    esac
    ;;
  "plugins enable tether")
    printf 'enabled\n' >"${HERMES_TEST_STATE:?}"
    ;;
  "plugins disable tether")
    printf 'disabled\n' >"${HERMES_TEST_STATE:?}"
    ;;
esac
EOF
chmod 700 "$HERMES_BIN"
export HERMES_TEST_LOG="$TEST_ROOT/hermes.log"
export TETHER_TEST_SYSTEM_GATEWAY_ACTIVE=0

HERDR_BIN="$FAKE_BIN/herdr"
export HERDR_TEST_LOG="$TEST_ROOT/herdr.log"
cat >"$HERDR_BIN" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"${HERDR_TEST_LOG:?}"
EOF
chmod 700 "$HERDR_BIN"

cat >"$FAKE_BIN/install" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
destination="${!#}"
if [[ -n "${TETHER_TEST_FAIL_INSTALL_DEST:-}" &&
      "$destination" == *"${TETHER_TEST_FAIL_INSTALL_DEST}" ]]; then
  exit 74
fi
"${REAL_INSTALL:?}" "$@"
if [[ -n "${TETHER_TEST_SIGNAL_INSTALL_DEST:-}" &&
      "$destination" == *"${TETHER_TEST_SIGNAL_INSTALL_DEST}" ]]; then
  kill -"${TETHER_TEST_SIGNAL:-TERM}" "$PPID"
fi
EOF
chmod 700 "$FAKE_BIN/install"

cat >"$FAKE_BIN/mv" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
arguments="$*"
if [[ -n "${TETHER_TEST_FAIL_RESTORE_ONCE:-}" &&
      "$arguments" == *".tether-restore-"* &&
      ! -e "${TETHER_TEST_FAIL_RESTORE_ONCE}" ]]; then
  : >"${TETHER_TEST_FAIL_RESTORE_ONCE}"
  exit 74
fi
"${REAL_MV:?}" "$@"
if [[ -n "${TETHER_TEST_SIGNAL_COMMIT_ONCE:-}" &&
      "$arguments" == *".tether-new-"* &&
      ! -e "${TETHER_TEST_SIGNAL_COMMIT_ONCE}" ]]; then
  : >"${TETHER_TEST_SIGNAL_COMMIT_ONCE}"
  kill -"${TETHER_TEST_SIGNAL:-TERM}" "$PPID"
fi
EOF
chmod 700 "$FAKE_BIN/mv"

cat >"$FAKE_BIN/sha256sum" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ -n "${TETHER_TEST_FAIL_SHA_ONCE:-}" &&
      ! -e "${TETHER_TEST_FAIL_SHA_ONCE}" ]]; then
  : >"${TETHER_TEST_FAIL_SHA_ONCE}"
  exit 74
fi
guard="${TETHER_TEST_SHA_GUARD:-}"
if [[ -n "$guard" ]]; then
  if ! mkdir "$guard" 2>/dev/null; then
    echo "concurrent installer entered checksum phase" >&2
    exit 92
  fi
  trap 'rmdir -- "$guard"' EXIT
  sleep 0.4
fi
"${REAL_SHA256SUM:?}" "$@"
EOF
chmod 700 "$FAKE_BIN/sha256sum"

assert_unsafe_root_rejected() {
  local case_name="$1" variable="$2" value="$3" expected="$4"
  local output="$TEST_ROOT/$case_name.out"
  set +e
  env "$variable=$value" \
    "$PACKAGE_ROOT/install.sh" install --dry-run --harness=both >"$output" 2>&1
  local rc=$?
  set -e
  [[ "$rc" -eq 2 ]] || fail "$case_name returned $rc instead of 2"
  assert_contains "$output" "$expected"
}

UNSAFE_ROOT="$TEST_ROOT/unsafe-roots"
mkdir -p "$UNSAFE_ROOT/runtime-victim" "$UNSAFE_ROOT/state-victim" "$UNSAFE_ROOT/config-victim"
ln -s "$UNSAFE_ROOT/runtime-victim" "$UNSAFE_ROOT/runtime-link"
ln -s "$UNSAFE_ROOT/state-victim" "$UNSAFE_ROOT/state-link"
ln -s "$UNSAFE_ROOT/config-victim" "$UNSAFE_ROOT/config-link"
assert_unsafe_root_rejected \
  runtime-symlink XDG_DATA_HOME "$UNSAFE_ROOT/runtime-link" \
  "refusing symlinked runtime root path component"
assert_unsafe_root_rejected \
  state-symlink XDG_STATE_HOME "$UNSAFE_ROOT/state-link" \
  "refusing symlinked state root path component"
assert_unsafe_root_rejected \
  config-symlink XDG_CONFIG_HOME "$UNSAFE_ROOT/config-link" \
  "refusing symlinked config root path component"

mkdir -p "$UNSAFE_ROOT/writable-data" "$UNSAFE_ROOT/writable-parent/data"
chmod 777 "$UNSAFE_ROOT/writable-data" "$UNSAFE_ROOT/writable-parent"
assert_unsafe_root_rejected \
  runtime-writable XDG_DATA_HOME "$UNSAFE_ROOT/writable-data" \
  "refusing group/world-writable runtime root path component"
assert_unsafe_root_rejected \
  runtime-writable-component XDG_DATA_HOME "$UNSAFE_ROOT/writable-parent/data" \
  "refusing group/world-writable runtime root path component"
chmod 700 "$UNSAFE_ROOT/writable-data" "$UNSAFE_ROOT/writable-parent"

mkdir -p \
  "$UNSAFE_ROOT/hermes-victim" \
  "$UNSAFE_ROOT/codex-victim" \
  "$UNSAFE_ROOT/claude-victim" \
  "$UNSAFE_ROOT/bin-victim" \
  "$HOME/.local"
ln -s "$UNSAFE_ROOT/hermes-victim" "$UNSAFE_ROOT/hermes-link"
ln -s "$UNSAFE_ROOT/codex-victim" "$UNSAFE_ROOT/codex-link"
ln -s "$UNSAFE_ROOT/claude-victim" "$UNSAFE_ROOT/claude-link"
assert_unsafe_root_rejected \
  hermes-symlink HERMES_HOME "$UNSAFE_ROOT/hermes-link" \
  "refusing symlinked Hermes root path component"
assert_unsafe_root_rejected \
  codex-symlink CODEX_HOME "$UNSAFE_ROOT/codex-link" \
  "refusing symlinked Codex root path component"
assert_unsafe_root_rejected \
  claude-symlink CLAUDE_HOME "$UNSAFE_ROOT/claude-link" \
  "refusing symlinked Claude Code root path component"
ln -s "$UNSAFE_ROOT/bin-victim" "$HOME/.local/bin"
set +e
"$PACKAGE_ROOT/install.sh" install --dry-run --harness=both \
  >"$TEST_ROOT/local-bin-symlink.out" 2>&1
local_bin_rc=$?
set -e
[[ "$local_bin_rc" -eq 2 ]] || fail "local-bin-symlink returned $local_bin_rc instead of 2"
assert_contains \
  "$TEST_ROOT/local-bin-symlink.out" \
  "refusing symlinked local executable root path component"
rm -f -- "$HOME/.local/bin"

assert_absent "$UNSAFE_ROOT/runtime-victim/tether"
assert_absent "$UNSAFE_ROOT/state-victim/tether-installer"
assert_absent "$UNSAFE_ROOT/config-victim/tether"
assert_absent "$UNSAFE_ROOT/hermes-victim/plugins"
assert_absent "$UNSAFE_ROOT/codex-victim/skills"
assert_absent "$UNSAFE_ROOT/claude-victim/skills"
assert_absent "$UNSAFE_ROOT/bin-victim/tether"

if [[ "$(id -u)" -eq 0 ]]; then
  mkdir -p "$UNSAFE_ROOT/wrong-owner"
  chown 65534 "$UNSAFE_ROOT/wrong-owner"
  WRONG_OWNER_ROOT="$UNSAFE_ROOT/wrong-owner"
else
  WRONG_OWNER_ROOT="/var/lib"
fi
assert_unsafe_root_rejected \
  runtime-owner XDG_DATA_HOME "$WRONG_OWNER_ROOT" \
  "nearest existing directory is not owned by the current user"
assert_unsafe_root_rejected \
  state-owner XDG_STATE_HOME "$WRONG_OWNER_ROOT" \
  "nearest existing directory is not owned by the current user"
assert_unsafe_root_rejected \
  config-owner XDG_CONFIG_HOME "$WRONG_OWNER_ROOT" \
  "nearest existing directory is not owned by the current user"

"$PACKAGE_ROOT/install.sh" install --harness=both

RUNTIME="$XDG_DATA_HOME/tether"
STATE="$XDG_STATE_HOME/tether-installer"
CONFIG="$XDG_CONFIG_HOME/tether/config.toml"
LAUNCHER="$HOME/.local/bin/tether"
BRIDGE="$RUNTIME/bridge_runtime.py"
DOMAIN_SCHEMA="$RUNTIME/domain_schema.py"
HERMES_COMPAT="$RUNTIME/hermes_compat.py"
ROUTING="$RUNTIME/routing.py"
SECURITY="$RUNTIME/security.py"
SLACK_PROTOCOL="$RUNTIME/slack_protocol.py"
HERDR_MANIFEST="$RUNTIME/herdr-plugin/herdr-plugin.toml"
HERDR_PLUGIN="$RUNTIME/herdr-plugin/tether_plugin.py"
HERDR_README="$RUNTIME/herdr-plugin/README.md"
CODEX_SKILL="$CODEX_HOME/skills/tether/SKILL.md"
CLAUDE_SKILL="$CLAUDE_HOME/skills/tether/SKILL.md"

for path in "$BRIDGE" "$DOMAIN_SCHEMA" "$HERMES_COMPAT" "$ROUTING" "$SECURITY" "$SLACK_PROTOCOL" \
  "$RUNTIME/install.sh" "$RUNTIME/package.json" "$LAUNCHER" "$CODEX_SKILL" \
  "$CLAUDE_SKILL" "$HERDR_MANIFEST" "$HERDR_PLUGIN" "$HERDR_README" \
  "$STATE/current.tsv" "$CONFIG"
do
  assert_file "$path"
done
set +e
"$LAUNCHER" doctor >"$TEST_ROOT/installed-doctor.out" 2>&1
doctor_rc=$?
set -e
[[ "$doctor_rc" -eq 1 ]] ||
  fail "installed doctor returned $doctor_rc instead of broker-unready status 1"
assert_contains "$TEST_ROOT/installed-doctor.out" "ok managed install integrity verified"
! grep -Fq "installer manifest metadata" "$TEST_ROOT/installed-doctor.out" ||
  fail "installed doctor rejected the freshly written manifest"
assert_mode "$BRIDGE" 600
assert_mode "$HERMES_COMPAT" 600
assert_mode "$ROUTING" 600
assert_mode "$SECURITY" 600
assert_mode "$SLACK_PROTOCOL" 600
assert_mode "$HERDR_MANIFEST" 644
assert_mode "$HERDR_PLUGIN" 700
assert_mode "$LAUNCHER" 700
assert_mode "$CONFIG" 600
node "$PACKAGE_ROOT/bin/tether.js" upgrade --dry-run --harness=both --herdr
assert_contains "$HERDR_TEST_LOG" "plugin link $RUNTIME/herdr-plugin"
node "$PACKAGE_ROOT/bin/tether.js" rollback --dry-run --herdr
assert_contains "$HERDR_TEST_LOG" "plugin link $RUNTIME/herdr-plugin"
node "$PACKAGE_ROOT/bin/tether.js" uninstall --dry-run --herdr
assert_contains "$HERDR_TEST_LOG" "plugin unlink parcha.tether"
chmod 722 "$LAUNCHER"
set +e
"$PACKAGE_ROOT/install.sh" upgrade --dry-run --harness=both \
  >"$TEST_ROOT/writable-target.out" 2>&1
writable_target_rc=$?
set -e
[[ "$writable_target_rc" -eq 2 ]] ||
  fail "writable managed target returned $writable_target_rc instead of 2"
assert_contains "$TEST_ROOT/writable-target.out" "refusing group/world-writable managed target"
chmod 700 "$LAUNCHER"
python3 -m py_compile "$SLACK_PROTOCOL"
[[ "$("$LAUNCHER" version)" == "tether $EXPECTED_VERSION" ]] ||
  fail "installed CLI version disagrees"
printf '{"version":"99.0.0"}\n' >"$HOME/.local/package.json"
[[ "$("$LAUNCHER" version)" == "tether $EXPECTED_VERSION" ]] ||
  fail "installed CLI trusted unrelated local package metadata"

printf 'home_channel = "C_TEST"\n' >"$CONFIG"
chmod 600 "$CONFIG"

LOCK_FILE="$STATE/install.lock"
assert_file "$LOCK_FILE"
assert_mode "$LOCK_FILE" 600
lock_inode_before="$(stat -c '%d:%i' "$LOCK_FILE")"
exec {TEST_LOCK_FD}<>"$LOCK_FILE"
flock -n "$TEST_LOCK_FD" || fail "test could not acquire persistent installer lock"
set +e
"$PACKAGE_ROOT/install.sh" upgrade --harness=both >"$TEST_ROOT/live-lock.out" 2>&1
lock_rc=$?
set -e
[[ "$lock_rc" -eq 3 ]] || fail "live lock returned $lock_rc instead of 3"
assert_contains "$TEST_ROOT/live-lock.out" "another Tether install operation"

mv -- "$STATE/current.tsv" "$STATE/current.tsv.lock-test"
set +e
"$PACKAGE_ROOT/install.sh" upgrade --harness=both \
  >"$TEST_ROOT/upgrade-lock-before-state.out" 2>&1
upgrade_lock_rc=$?
"$PACKAGE_ROOT/install.sh" uninstall \
  >"$TEST_ROOT/uninstall-lock-before-state.out" 2>&1
uninstall_lock_rc=$?
set -e
mv -- "$STATE/current.tsv.lock-test" "$STATE/current.tsv"
[[ "$upgrade_lock_rc" -eq 3 ]] ||
  fail "upgrade state-order test returned $upgrade_lock_rc instead of 3"
[[ "$uninstall_lock_rc" -eq 3 ]] ||
  fail "uninstall state-order test returned $uninstall_lock_rc instead of 3"
assert_contains "$TEST_ROOT/upgrade-lock-before-state.out" "another Tether install operation"
assert_contains "$TEST_ROOT/uninstall-lock-before-state.out" "another Tether install operation"
! grep -Fq "No managed manifest" "$TEST_ROOT/upgrade-lock-before-state.out" ||
  fail "upgrade read lifecycle state before acquiring the lock"
! grep -Fq "No managed Tether installation" "$TEST_ROOT/uninstall-lock-before-state.out" ||
  fail "uninstall read lifecycle state before acquiring the lock"

flock -u "$TEST_LOCK_FD"
exec {TEST_LOCK_FD}>&-

parallel_guard="$TEST_ROOT/parallel-checksum"
parallel_success=0
parallel_locked=0
parallel_other=0
parallel_pids=()
for index in $(seq 1 12); do
  (
    TETHER_TEST_SHA_GUARD="$parallel_guard" \
      "$PACKAGE_ROOT/install.sh" upgrade --harness=both
  ) >"$TEST_ROOT/parallel-$index.out" 2>&1 &
  parallel_pids+=("$!")
done
set +e
for index in "${!parallel_pids[@]}"; do
  wait "${parallel_pids[$index]}"
  rc=$?
  case "$rc" in
    0) parallel_success=$((parallel_success + 1)) ;;
    3) parallel_locked=$((parallel_locked + 1)) ;;
    *) parallel_other=$((parallel_other + 1)) ;;
  esac
done
set -e
assert_equals "$parallel_success" "1"
assert_equals "$parallel_locked" "11"
assert_equals "$parallel_other" "0"
assert_absent "$parallel_guard"
assert_equals "$(stat -c '%d:%i' "$LOCK_FILE")" "$lock_inode_before"

printf '\n# local-before-upgrade\n' >>"$BRIDGE"
"$PACKAGE_ROOT/install.sh" upgrade --harness=both
! grep -Fq "local-before-upgrade" "$BRIDGE" ||
  fail "upgrade did not replace the managed payload"
assert_contains "$CONFIG" 'home_channel = "C_TEST"'
assert_file "$LOCK_FILE"

"$LAUNCHER" rollback
assert_contains "$BRIDGE" "local-before-upgrade"
assert_equals "$(<"$HERMES_TEST_STATE")" "enabled"

printf '\n# crash-recovery-marker\n' >>"$BRIDGE"
baseline_bridge_hash="$("$REAL_SHA256SUM" "$BRIDGE" | awk '{print $1}')"
baseline_manifest_hash="$("$REAL_SHA256SUM" "$STATE/current.tsv" | awk '{print $1}')"
baseline_last_backup="$(<"$STATE/last-backup")"
cp -p -- "$CONFIG" "$TEST_ROOT/config.saved"

kill_commit_marker="$TEST_ROOT/kill-commit.once"
set +e
TETHER_TEST_SIGNAL=KILL \
TETHER_TEST_SIGNAL_COMMIT_ONCE="$kill_commit_marker" \
  "$PACKAGE_ROOT/install.sh" upgrade --harness=both \
  >"$TEST_ROOT/kill-commit.out" 2>&1
kill_commit_rc=$?
set -e
[[ "$kill_commit_rc" -eq 137 ]] ||
  fail "commit SIGKILL returned $kill_commit_rc instead of 137"
assert_file "$kill_commit_marker"
assert_file "$STATE/transaction.tsv"
assert_mode "$STATE/transaction.tsv" 600
"$PACKAGE_ROOT/install.sh" rollback --dry-run \
  >"$TEST_ROOT/kill-commit-recovery.out" 2>&1
assert_contains "$TEST_ROOT/kill-commit-recovery.out" "Recovering interrupted Tether upgrade transaction"
assert_absent "$STATE/transaction.tsv"
assert_contains "$BRIDGE" "# crash-recovery-marker"
assert_equals "$("$REAL_SHA256SUM" "$BRIDGE" | awk '{print $1}')" "$baseline_bridge_hash"
assert_equals "$("$REAL_SHA256SUM" "$STATE/current.tsv" | awk '{print $1}')" "$baseline_manifest_hash"
assert_equals "$(<"$STATE/last-backup")" "$baseline_last_backup"
if compgen -G "$STATE/stage.*" >/dev/null; then
  fail "crash recovery left stale installer stages"
fi

rm -f -- "$CONFIG"
set +e
TETHER_TEST_SIGNAL=KILL \
TETHER_TEST_SIGNAL_INSTALL_DEST="/config.toml" \
  "$PACKAGE_ROOT/install.sh" upgrade --harness=both \
  >"$TEST_ROOT/kill-config.out" 2>&1
kill_config_rc=$?
set -e
[[ "$kill_config_rc" -eq 137 ]] ||
  fail "config SIGKILL returned $kill_config_rc instead of 137"
assert_file "$STATE/transaction.tsv"
"$PACKAGE_ROOT/install.sh" rollback --dry-run \
  >"$TEST_ROOT/kill-config-recovery.out" 2>&1
assert_contains "$TEST_ROOT/kill-config-recovery.out" "Recovering interrupted Tether upgrade transaction"
assert_absent "$STATE/transaction.tsv"
assert_absent "$CONFIG"
assert_equals "$("$REAL_SHA256SUM" "$BRIDGE" | awk '{print $1}')" "$baseline_bridge_hash"
assert_equals "$("$REAL_SHA256SUM" "$STATE/current.tsv" | awk '{print $1}')" "$baseline_manifest_hash"
assert_equals "$(<"$STATE/last-backup")" "$baseline_last_backup"
"$REAL_INSTALL" -m 600 "$TEST_ROOT/config.saved" "$CONFIG"

set +e
TETHER_TEST_SYSTEM_GATEWAY_ACTIVE=1 \
HERMES_TEST_FAIL_ON="gateway restart --system" \
  "$PACKAGE_ROOT/install.sh" upgrade --harness=both --restart \
  >"$TEST_ROOT/restart-failure.out" 2>&1
restart_failure_rc=$?
set -e
[[ "$restart_failure_rc" -eq 74 ]] ||
  fail "restart failure returned $restart_failure_rc instead of 74"
assert_contains "$TEST_ROOT/restart-failure.out" "restoring the previous state"
assert_contains "$HERMES_TEST_LOG" "gateway restart --system"
! grep -Fq "Installed Tether" "$TEST_ROOT/restart-failure.out" ||
  fail "restart failure reported installation success"
assert_absent "$STATE/transaction.tsv"
assert_equals "$("$REAL_SHA256SUM" "$BRIDGE" | awk '{print $1}')" "$baseline_bridge_hash"
assert_equals "$("$REAL_SHA256SUM" "$STATE/current.tsv" | awk '{print $1}')" "$baseline_manifest_hash"
assert_equals "$(<"$STATE/last-backup")" "$baseline_last_backup"

cp -p -- "$BRIDGE" "$TEST_ROOT/bridge-before-committed-recovery"
printf '\n# committed-journal-marker\n' >>"$BRIDGE"
committed_bridge_hash="$("$REAL_SHA256SUM" "$BRIDGE" | awk '{print $1}')"
printf 'version\t1\nphase\tcommitted\naction\tupgrade\nbackup\t%s\nremove_config\t0\n' \
  "$baseline_last_backup" >"$STATE/transaction.tsv"
chmod 600 "$STATE/transaction.tsv"
"$PACKAGE_ROOT/install.sh" rollback --dry-run \
  >"$TEST_ROOT/committed-recovery.out" 2>&1
assert_absent "$STATE/transaction.tsv"
assert_equals "$("$REAL_SHA256SUM" "$BRIDGE" | awk '{print $1}')" "$committed_bridge_hash"
"$REAL_INSTALL" -m 600 "$TEST_ROOT/bridge-before-committed-recovery" "$BRIDGE"

rm -f -- "$CONFIG"
set +e
TETHER_TEST_FAIL_INSTALL_DEST="/config.toml" \
  "$PACKAGE_ROOT/install.sh" upgrade --harness=both \
  >"$TEST_ROOT/post-commit-error.out" 2>&1
post_commit_rc=$?
set -e
[[ "$post_commit_rc" -eq 74 ]] ||
  fail "post-commit error returned $post_commit_rc instead of 74"
assert_contains "$TEST_ROOT/post-commit-error.out" "restoring the previous state"
assert_equals "$("$REAL_SHA256SUM" "$BRIDGE" | awk '{print $1}')" "$baseline_bridge_hash"
assert_equals "$("$REAL_SHA256SUM" "$STATE/current.tsv" | awk '{print $1}')" "$baseline_manifest_hash"
assert_equals "$(<"$STATE/last-backup")" "$baseline_last_backup"
assert_absent "$CONFIG"
"$REAL_INSTALL" -m 600 "$TEST_ROOT/config.saved" "$CONFIG"

for signal in HUP INT TERM; do
  case "$signal" in
    HUP) expected_rc=129 ;;
    INT) expected_rc=130 ;;
    TERM) expected_rc=143 ;;
  esac
  signal_marker="$TEST_ROOT/signal-$signal.once"
  set +e
  TETHER_TEST_SIGNAL="$signal" \
  TETHER_TEST_SIGNAL_COMMIT_ONCE="$signal_marker" \
    "$PACKAGE_ROOT/install.sh" upgrade --harness=both \
    >"$TEST_ROOT/signal-$signal.out" 2>&1
  signal_rc=$?
  set -e
  [[ "$signal_rc" -eq "$expected_rc" ]] ||
    fail "$signal returned $signal_rc instead of $expected_rc"
  assert_file "$signal_marker"
  assert_contains "$TEST_ROOT/signal-$signal.out" "restoring the previous state"
  assert_equals "$("$REAL_SHA256SUM" "$BRIDGE" | awk '{print $1}')" "$baseline_bridge_hash"
  assert_equals "$("$REAL_SHA256SUM" "$STATE/current.tsv" | awk '{print $1}')" "$baseline_manifest_hash"
  assert_equals "$(<"$STATE/last-backup")" "$baseline_last_backup"
done

rm -f -- "$CONFIG"
set +e
TETHER_TEST_SIGNAL=TERM \
TETHER_TEST_SIGNAL_INSTALL_DEST="/config.toml" \
  "$PACKAGE_ROOT/install.sh" upgrade --harness=both \
  >"$TEST_ROOT/post-commit-signal.out" 2>&1
post_commit_signal_rc=$?
set -e
[[ "$post_commit_signal_rc" -eq 143 ]] ||
  fail "post-commit TERM returned $post_commit_signal_rc instead of 143"
assert_contains "$TEST_ROOT/post-commit-signal.out" "restoring the previous state"
assert_equals "$("$REAL_SHA256SUM" "$BRIDGE" | awk '{print $1}')" "$baseline_bridge_hash"
assert_equals "$("$REAL_SHA256SUM" "$STATE/current.tsv" | awk '{print $1}')" "$baseline_manifest_hash"
assert_equals "$(<"$STATE/last-backup")" "$baseline_last_backup"
assert_absent "$CONFIG"
"$REAL_INSTALL" -m 600 "$TEST_ROOT/config.saved" "$CONFIG"

mkdir -p "$HERMES_HOME"
printf 'synthetic bridge state\n' >"$HERMES_HOME/bridges.db"

set +e
HERMES_TEST_FAIL_ON="plugins disable tether" \
  "$PACKAGE_ROOT/install.sh" uninstall >"$TEST_ROOT/uninstall-disable-failure.out" 2>&1
disable_failure_rc=$?
set -e
[[ "$disable_failure_rc" -eq 74 ]] ||
  fail "Hermes disable failure returned $disable_failure_rc instead of 74"
assert_file "$LAUNCHER"
assert_file "$STATE/current.tsv"
assert_equals "$(<"$HERMES_TEST_STATE")" "enabled"
assert_equals "$(<"$STATE/last-backup")" "$baseline_last_backup"

enabled_failure_marker="$TEST_ROOT/uninstall-enabled-failure.once"
set +e
TETHER_TEST_FAIL_SHA_ONCE="$enabled_failure_marker" \
  "$PACKAGE_ROOT/install.sh" uninstall >"$TEST_ROOT/uninstall-enabled-failure.out" 2>&1
enabled_failure_rc=$?
set -e
[[ "$enabled_failure_rc" -eq 74 ]] ||
  fail "enabled uninstall failure returned $enabled_failure_rc instead of 74"
assert_file "$LAUNCHER"
assert_file "$STATE/current.tsv"
assert_equals "$(<"$HERMES_TEST_STATE")" "enabled"
assert_equals "$(<"$STATE/last-backup")" "$baseline_last_backup"

printf 'disabled\n' >"$HERMES_TEST_STATE"
disabled_failure_marker="$TEST_ROOT/uninstall-disabled-failure.once"
set +e
TETHER_TEST_FAIL_SHA_ONCE="$disabled_failure_marker" \
  "$PACKAGE_ROOT/install.sh" uninstall >"$TEST_ROOT/uninstall-disabled-failure.out" 2>&1
disabled_failure_rc=$?
set -e
[[ "$disabled_failure_rc" -eq 74 ]] ||
  fail "disabled uninstall failure returned $disabled_failure_rc instead of 74"
assert_file "$LAUNCHER"
assert_file "$STATE/current.tsv"
assert_equals "$(<"$HERMES_TEST_STATE")" "disabled"
assert_equals "$(<"$STATE/last-backup")" "$baseline_last_backup"
printf 'enabled\n' >"$HERMES_TEST_STATE"

"$PACKAGE_ROOT/bin/tether.js" uninstall >"$TEST_ROOT/uninstall.out" 2>&1
assert_contains "$TEST_ROOT/uninstall.out" "with 'rollback' to restore snapshot"
! grep -Fq "command not found" "$TEST_ROOT/uninstall.out" ||
  fail "uninstall instructions executed shell markup"
assert_file "$BRIDGE"
assert_absent "$SLACK_PROTOCOL"
assert_contains "$BRIDGE" "local-before-upgrade"
assert_absent "$LAUNCHER"
assert_file "$CONFIG"
assert_file "$HERMES_HOME/bridges.db"
assert_contains "$HERMES_TEST_LOG" "plugins disable tether"
assert_contains "$HERMES_TEST_LOG" "gateway restart"
assert_equals "$(<"$HERMES_TEST_STATE")" "disabled"
uninstall_snapshot="$(<"$STATE/last-backup")"

set +e
HERMES_TEST_FAIL_ON="plugins enable tether" \
  "$PACKAGE_ROOT/install.sh" rollback >"$TEST_ROOT/rollback-enable-failure.out" 2>&1
enable_failure_rc=$?
set -e
[[ "$enable_failure_rc" -eq 74 ]] ||
  fail "Hermes enable failure returned $enable_failure_rc instead of 74"
assert_absent "$LAUNCHER"
assert_absent "$STATE/current.tsv"
assert_equals "$(<"$HERMES_TEST_STATE")" "disabled"
assert_equals "$(<"$STATE/last-backup")" "$uninstall_snapshot"

rollback_failure_marker="$TEST_ROOT/rollback-failure.once"
set +e
TETHER_TEST_FAIL_RESTORE_ONCE="$rollback_failure_marker" \
  "$PACKAGE_ROOT/install.sh" rollback >"$TEST_ROOT/rollback-failure.out" 2>&1
rollback_failure_rc=$?
set -e
[[ "$rollback_failure_rc" -eq 74 ]] ||
  fail "rollback failure returned $rollback_failure_rc instead of 74"
assert_file "$rollback_failure_marker"
assert_absent "$LAUNCHER"
assert_absent "$STATE/current.tsv"
assert_equals "$(<"$HERMES_TEST_STATE")" "disabled"
assert_equals "$(<"$STATE/last-backup")" "$uninstall_snapshot"

"$PACKAGE_ROOT/install.sh" rollback
assert_file "$LAUNCHER"
assert_file "$SLACK_PROTOCOL"
python3 -m py_compile "$SLACK_PROTOCOL"
assert_file "$CODEX_SKILL"
assert_file "$CLAUDE_SKILL"
assert_contains "$BRIDGE" "local-before-upgrade"
assert_file "$STATE/current.tsv"
assert_file "$CONFIG"
assert_file "$HERMES_HOME/bridges.db"
assert_equals "$(<"$HERMES_TEST_STATE")" "enabled"

node() { return 127; }
python3() { return 127; }
export -f node python3
"$PACKAGE_ROOT/install.sh" uninstall >"$TEST_ROOT/no-runtime-uninstall.out" 2>&1
assert_absent "$LAUNCHER"
assert_absent "$SLACK_PROTOCOL"
assert_equals "$(<"$HERMES_TEST_STATE")" "disabled"
"$PACKAGE_ROOT/install.sh" rollback >"$TEST_ROOT/no-runtime-rollback.out" 2>&1
assert_file "$LAUNCHER"
assert_file "$SLACK_PROTOCOL"
assert_file "$STATE/current.tsv"
assert_equals "$(<"$HERMES_TEST_STATE")" "enabled"

echo "release-install lifecycle: ok"
