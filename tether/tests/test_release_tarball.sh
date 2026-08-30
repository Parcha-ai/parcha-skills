#!/usr/bin/env bash
set -Eeuo pipefail

SOURCE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_ROOT="$(mktemp -d)"
trap 'rm -rf -- "$TEST_ROOT"' EXIT

fail() {
  echo "release-tarball test failed: $*" >&2
  exit 1
}

PACK_DIR="$TEST_ROOT/pack"
INSTALL_PREFIX="$TEST_ROOT/npm path'with quote"
EXPECTED_PATHS="$TEST_ROOT/expected-paths.txt"
mkdir -p "$PACK_DIR" "$INSTALL_PREFIX"

# package.json is the authoritative, exact package manifest. Globs, exclusions,
# directory entries, links, duplicate paths, and implicit traversal are refused.
node - "$SOURCE_ROOT/package.json" "$SOURCE_ROOT" >"$EXPECTED_PATHS" <<'NODE'
const fs = require("node:fs");
const path = require("node:path");

const packagePath = process.argv[2];
const sourceRoot = process.argv[3];
const pkg = JSON.parse(fs.readFileSync(packagePath, "utf8"));
const marketplace = JSON.parse(
  fs.readFileSync(path.join(sourceRoot, ".agents/plugins/marketplace.json"), "utf8"),
);
const tether = marketplace.plugins?.find((row) => row.name === "tether");
if (!/^[0-9a-f]{40}$/.test(tether?.source?.ref ?? "")) {
  throw new Error("Agent Plugins marketplace source is not an immutable commit SHA");
}
if (!Array.isArray(pkg.files) || pkg.files.length === 0) {
  throw new Error("package files manifest is empty");
}

const paths = ["package.json"];
const seen = new Set(paths);
for (const value of pkg.files) {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error("package files manifest contains a non-string or empty path");
  }
  if (
    value.startsWith("/") ||
    value.endsWith("/") ||
    value.includes("\\") ||
    /[*?[\]{}!]/.test(value) ||
    path.posix.normalize(value) !== value ||
    value === ".." ||
    value.startsWith("../")
  ) {
    throw new Error(`package files manifest is not exact: ${value}`);
  }
  if (seen.has(value)) throw new Error(`duplicate package path: ${value}`);
  const source = path.join(sourceRoot, ...value.split("/"));
  const stat = fs.lstatSync(source);
  if (!stat.isFile() || stat.isSymbolicLink()) {
    throw new Error(`package path is not a regular file: ${value}`);
  }
  seen.add(value);
  paths.push(value);
}
process.stdout.write(`${paths.sort().join("\n")}\n`);
NODE

if [[ -n "${TETHER_TARBALL_PATH:-}" ]]; then
  [[ -f "$TETHER_TARBALL_PATH" && ! -L "$TETHER_TARBALL_PATH" ]] ||
    fail "provided tarball is not a regular file"
  tarball_path="$(realpath -- "$TETHER_TARBALL_PATH")"
else
  pack_json="$TEST_ROOT/pack.json"
  (
    cd "$SOURCE_ROOT"
    npm pack --json --pack-destination "$PACK_DIR"
  ) >"$pack_json"
  tarball="$(
    node -e '
      const rows = require(process.argv[1]);
      if (!Array.isArray(rows) || rows.length !== 1 || !rows[0].filename) {
        throw new Error("npm pack did not produce exactly one tarball");
      }
      process.stdout.write(rows[0].filename);
    ' "$pack_json"
  )"
  tarball_path="$PACK_DIR/$tarball"
fi
[[ -f "$tarball_path" && ! -L "$tarball_path" ]] ||
  fail "npm tarball was not created as a regular file"

TARBALL_PATH="$tarball_path" EXPECTED_PATHS="$EXPECTED_PATHS" python3 <<'PY'
import os
import pathlib
import tarfile

tarball = pathlib.Path(os.environ["TARBALL_PATH"])
expected = set(pathlib.Path(os.environ["EXPECTED_PATHS"]).read_text().splitlines())
actual: set[str] = set()

with tarfile.open(tarball, "r:gz") as archive:
    for member in archive.getmembers():
        if not member.name.startswith("package/"):
            raise SystemExit(f"tarball path lacks package/ prefix: {member.name}")
        relative = member.name.removeprefix("package/")
        pure = pathlib.PurePosixPath(relative)
        if (
            not relative
            or pure.is_absolute()
            or ".." in pure.parts
            or str(pure) != relative
        ):
            raise SystemExit(f"unsafe tarball path: {member.name}")
        if not member.isfile():
            raise SystemExit(f"tarball member is not a regular file: {member.name}")
        if relative in actual:
            raise SystemExit(f"duplicate tarball path: {relative}")
        actual.add(relative)

missing = sorted(expected - actual)
unexpected = sorted(actual - expected)
if missing or unexpected:
    raise SystemExit(
        "tarball differs from exact package manifest; "
        f"missing={missing!r} unexpected={unexpected!r}"
    )
PY

EXTRACT_ROOT="$TEST_ROOT/extracted"
mkdir -p "$EXTRACT_ROOT"
tar -xzf "$tarball_path" --no-same-owner --no-same-permissions -C "$EXTRACT_ROOT"
PACKAGE_ROOT="$EXTRACT_ROOT/package"

PACKAGE_ROOT="$PACKAGE_ROOT" python3 <<'PY'
import math
import os
import pathlib
import re

root = pathlib.Path(os.environ["PACKAGE_ROOT"])

forbidden_parts = {
    ".cache",
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
    "coverage",
    "dist",
    "node_modules",
    "sessions",
    "transcripts",
}
forbidden_names = {
    ".env",
    ".gitconfig",
    ".netrc",
    ".npmrc",
    ".pypirc",
    "authorized_keys",
    "credentials.json",
    "id_ed25519",
    "id_rsa",
    "known_hosts",
}
forbidden_suffixes = {
    ".7z",
    ".bak",
    ".db",
    ".gz",
    ".jks",
    ".jsonl",
    ".key",
    ".keystore",
    ".log",
    ".p12",
    ".pem",
    ".pfx",
    ".sqlite",
    ".sqlite3",
    ".swp",
    ".tar",
    ".tgz",
    ".tmp",
    ".zip",
}
secret_patterns = {
    "private key": re.compile(
        rb"-----BEGIN (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----"
    ),
    "GitHub token": re.compile(
        rb"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b"
    ),
    "Slack token": re.compile(rb"\bxox[baprs]-[A-Za-z0-9-]{20,}\b"),
    "AWS access key": re.compile(rb"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    "Google API key": re.compile(rb"\bAIza[0-9A-Za-z_-]{30,}\b"),
    "Anthropic key": re.compile(rb"\bsk-ant-[A-Za-z0-9_-]{16,}\b"),
    "OpenAI key": re.compile(rb"\bsk-(?:proj-)?[A-Za-z0-9_-]{20,}\b"),
    "1Password service token": re.compile(rb"\bops_[A-Za-z0-9_-]{20,}\b"),
    "Stripe live key": re.compile(rb"\b(?:sk|rk)_live_[A-Za-z0-9]{16,}\b"),
    "JWT": re.compile(
        rb"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b"
    ),
    "credentialed URL": re.compile(
        rb"\bhttps?://[A-Za-z0-9._~-]+:[^@\s/]{8,}@[A-Za-z0-9.-]+"
    ),
}
quoted_assignment = re.compile(
    rb"""(?ix)
    \b(?:api[_-]?key|access[_-]?token|auth[_-]?token|client[_-]?secret|
        password|private[_-]?key)\b
    \s*[:=]\s*
    (?P<quote>["'])(?P<value>[^"'\r\n]{16,})(?P=quote)
    """
)
environment_assignment = re.compile(
    rb"""(?imx)
    ^[A-Z0-9_]*(?:KEY|TOKEN|SECRET|PASSWORD)[A-Z0-9_]*=
    (?P<value>[^\s#]{16,})\s*$
    """
)
structured_assignment = re.compile(
    rb"""(?imx)
    ^\s*["']?
    (?:api[_-]?key|access[_-]?token|auth[_-]?token|client[_-]?secret|
       password|private[_-]?key)
    ["']?\s*[:=]\s*
    (?P<value>[A-Za-z0-9+/_.:@=-]{16,})
    \s*,?\s*$
    """
)


def placeholder(value: bytes) -> bool:
    lowered = value.lower()
    markers = (
        b"${",
        b"<",
        b"changeme",
        b"example",
        b"not-a-secret",
        b"placeholder",
        b"redacted",
        b"replace-me",
        b"your-",
        b"your_",
    )
    return (
        any(marker in lowered for marker in markers)
        or re.fullmatch(rb"[A-Z][A-Z0-9_]+", value) is not None
    )


def entropy(value: bytes) -> float:
    if not value:
        return 0.0
    return -sum(
        (count / len(value)) * math.log2(count / len(value))
        for count in {byte: value.count(byte) for byte in set(value)}.values()
    )


# Keep these assertions with the scanner so weakening a pattern breaks the test.
for label, sample in {
    "private key": b"-----BEGIN " + b"OPENSSH PRIVATE KEY-----",
    "GitHub token": b"github_pat_AAAAAAAAAAAAAAAAAAAA",
    "Slack token": b"xox" + b"b-1111111111-2222222222-abcdefghijklmnop",
    "AWS access key": b"AKIA" + b"ABCDEFGHIJKLMNOP",
    "Anthropic key": b"sk-" + b"ant-abcdefghijklmnopqrst",
}.items():
    if not secret_patterns[label].search(sample):
        raise SystemExit(f"secret scanner self-test failed for {label}")
high_entropy = b"8uH3" + b"zKp9Vx2nQw7LmR4t"
if not quoted_assignment.search(b'client_secret = "' + high_entropy + b'"'):
    raise SystemExit("generic secret assignment scanner self-test failed")
if not structured_assignment.search(b"api_key: " + high_entropy):
    raise SystemExit("structured secret assignment scanner self-test failed")

for path in sorted(root.rglob("*")):
    relative = path.relative_to(root)
    if path.is_symlink():
        raise SystemExit(f"packaged path is not a regular file: {relative}")
    if path.is_dir():
        continue
    if not path.is_file():
        raise SystemExit(f"packaged path is not a regular file: {relative}")
    lowered_parts = {part.lower() for part in relative.parts}
    lowered_name = relative.name.lower()
    if lowered_parts & forbidden_parts:
        raise SystemExit(f"forbidden artifact directory in package: {relative}")
    if (
        lowered_name in forbidden_names
        or lowered_name.startswith(".env.")
        or lowered_name.endswith(".env")
        or pathlib.PurePosixPath(lowered_name).suffix in forbidden_suffixes
    ):
        raise SystemExit(f"forbidden secret/artifact filename in package: {relative}")

    data = path.read_bytes()
    if b"\x00" in data:
        raise SystemExit(f"binary or NUL-containing package file: {relative}")
    for label, pattern in secret_patterns.items():
        if pattern.search(data):
            raise SystemExit(f"{label} pattern found in package file: {relative}")
    for pattern in (
        quoted_assignment,
        environment_assignment,
        structured_assignment,
    ):
        for match in pattern.finditer(data):
            value = match.group("value").strip()
            if placeholder(value):
                continue
            # Long literal credential assignments are forbidden regardless of
            # entropy. Entropy is retained in the diagnostic category without
            # ever printing the candidate value.
            category = "high-entropy" if entropy(value) >= 3.5 else "literal"
            raise SystemExit(
                f"{category} credential assignment found in package file: {relative}"
            )
PY

npm install \
  --prefix "$INSTALL_PREFIX" \
  --ignore-scripts \
  --no-audit \
  --no-fund \
  "$tarball_path" >/dev/null

INSTALLED_ROOT="$INSTALL_PREFIX/node_modules/@parcha/tether"
[[ -x "$INSTALLED_ROOT/install.sh" ]] || fail "tarball omitted executable install.sh"
[[ -f "$INSTALLED_ROOT/runtime/bridge_runtime.py" ]] ||
  fail "tarball omitted bridge runtime"
[[ -f "$INSTALLED_ROOT/runtime/domain_control.py" ]] ||
  fail "tarball omitted domain control runtime"
[[ -f "$INSTALLED_ROOT/runtime/domain_schema.py" ]] ||
  fail "tarball omitted domain schema runtime"
[[ -f "$INSTALLED_ROOT/runtime/schema_orchestrator.py" ]] ||
  fail "tarball omitted schema orchestrator"
[[ -f "$INSTALLED_ROOT/runtime/slack_protocol.py" ]] ||
  fail "tarball omitted Slack protocol runtime"
python3 -m py_compile "$INSTALLED_ROOT/runtime/slack_protocol.py"
python3 -m py_compile "$INSTALLED_ROOT/runtime/domain_control.py"
python3 -m py_compile "$INSTALLED_ROOT/runtime/domain_schema.py"
python3 -m py_compile "$INSTALLED_ROOT/runtime/schema_orchestrator.py"

export HOME="$TEST_ROOT/home"
export XDG_DATA_HOME="$TEST_ROOT/data"
export XDG_CONFIG_HOME="$TEST_ROOT/config"
export XDG_STATE_HOME="$TEST_ROOT/state"
export HERMES_HOME="$TEST_ROOT/hermes"
export CODEX_HOME="$TEST_ROOT/codex"
export HERMES_BIN="$TEST_ROOT/bin/hermes"
export TETHER_TEST_SYSTEM_GATEWAY_ACTIVE=0
install -d -m 700 "$HOME" "$CODEX_HOME" "$(dirname "$HERMES_BIN")"

cat >"$HERMES_BIN" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  "plugins list --plain") printf 'disabled user 0.2.0 tether\n' ;;
  "plugins disable tether"|"plugins enable tether"|"gateway restart") ;;
  *) exit 73 ;;
esac
EOF
chmod 700 "$HERMES_BIN"

"$INSTALLED_ROOT/install.sh" install --harness=codex >/dev/null
launcher="$HOME/.local/bin/tether"
runtime="$XDG_DATA_HOME/tether/bridge_runtime.py"
slack_protocol="$XDG_DATA_HOME/tether/slack_protocol.py"
config="$XDG_CONFIG_HOME/tether/config.toml"
[[ -x "$launcher" ]] || fail "tarball install did not create the launcher"
[[ -f "$runtime" ]] || fail "tarball install did not create the runtime"
[[ -f "$slack_protocol" ]] || fail "tarball install omitted Slack protocol runtime"
python3 -m py_compile "$slack_protocol"
[[ -f "$config" ]] || fail "tarball install did not create config"
set +e
"$launcher" doctor >"$TEST_ROOT/installed-doctor.out" 2>&1
doctor_rc=$?
set -e
[[ "$doctor_rc" -eq 1 ]] ||
  fail "tarball doctor returned $doctor_rc instead of broker-unready status 1"
grep -Fq "ok managed install integrity verified" "$TEST_ROOT/installed-doctor.out" ||
  fail "tarball doctor rejected the fresh install manifest"

printf '\n# replaced-by-upgrade\n' >>"$runtime"
printf '\n# replaced-by-upgrade\n' >>"$slack_protocol"
"$INSTALLED_ROOT/install.sh" upgrade --harness=codex >/dev/null
if grep -Fq "replaced-by-upgrade" "$runtime"; then
  fail "tarball upgrade did not replace the managed runtime"
fi
if grep -Fq "replaced-by-upgrade" "$slack_protocol"; then
  fail "tarball upgrade did not replace the Slack protocol runtime"
fi

"$INSTALLED_ROOT/install.sh" uninstall >/dev/null
[[ ! -e "$launcher" ]] || fail "tarball uninstall retained the launcher"
[[ ! -e "$slack_protocol" ]] ||
  fail "tarball uninstall retained the Slack protocol runtime"
[[ -f "$config" ]] || fail "tarball uninstall removed operator config"
[[ ! -e "$XDG_STATE_HOME/tether-installer/current.tsv" ]] ||
  fail "tarball uninstall retained the active manifest"

echo "release tarball manifest, content, and lifecycle: ok"
