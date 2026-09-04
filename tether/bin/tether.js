#!/usr/bin/env node
"use strict";

const crypto = require("node:crypto");
const fs = require("node:fs");
const net = require("node:net");
const { spawnSync } = require("node:child_process");
const os = require("node:os");
const path = require("node:path");
const { TextDecoder } = require("node:util");

const EXIT_USAGE = 2;
const EXIT_TRANSPORT = 3;
const EXIT_BROKER = 4;
const MAX_REQUEST_FRAME_BYTES = 1_048_576;
const MAX_RESPONSE_FRAME_BYTES = 8 * 1_048_576;
const MAX_MESSAGE_BYTES = 512 * 1024;
const DEFAULT_TIMEOUT_MS = 35_000;
const MIN_TIMEOUT_MS = 50;
const MAX_TIMEOUT_MS = 60_000;
const CHILD_TIMEOUT_MS = 60_000;
const LIFECYCLE_TIMEOUT_MS = 900_000;
const BROKER_PROTOCOL_VERSION = 6;

const packageRoot = path.resolve(__dirname, "..");
const packageInstaller = path.join(packageRoot, "install.sh");
const home = process.env.HOME || os.homedir();
const dataHome = process.env.XDG_DATA_HOME || path.join(home, ".local", "share");
const runtimeHome = path.join(dataHome, "tether");
const installedInstaller = path.join(runtimeHome, "install.sh");
const notifier = path.join(runtimeHome, "tether_notify.py");
const installedRuntime = path.join(runtimeHome, "domain_runtime.py");
const installedSchemaOrchestrator = path.join(runtimeHome, "schema_orchestrator.py");
const stateHome = path.join(
  process.env.XDG_STATE_HOME || path.join(home, ".local", "state"),
  "tether-installer",
);
const installedManifest = path.join(stateHome, "current.tsv");
const MANIFEST_HEADER = "# tether-manifest-v2";
const MANIFEST_METADATA_KEYS = new Set([
  "harness",
  "runtime_home",
  "plugin_home",
  "local_bin",
  "codex_root",
  "claude_root",
  "legacy",
]);

class CliError extends Error {
  constructor(message, exitCode = EXIT_USAGE, code = "cli_error", details = {}) {
    super(message);
    this.name = "CliError";
    this.exitCode = exitCode;
    this.code = code;
    this.details = details;
  }
}

class BrokerError extends CliError {
  constructor(payload) {
    const code = stringValue(payload.code) || "broker_internal_error";
    const message = stringValue(payload.message) || "Tether broker request failed";
    super(message, EXIT_BROKER, code, {
      binding_id: stringValue(payload.binding_id),
      status: stringValue(payload.status) || "failed",
      retryable: payload.retryable === true,
      next_action: stringValue(payload.next_action),
    });
  }
}

function stringValue(value) {
  return typeof value === "string" ? value : "";
}

function packagePayloadAvailable() {
  return fs.existsSync(packageInstaller) &&
    fs.existsSync(path.join(packageRoot, "skills", "tether", "SKILL.md"));
}

function readVersion() {
  const candidates = packagePayloadAvailable()
    ? [path.join(packageRoot, "package.json"), path.join(runtimeHome, "package.json")]
    : [path.join(runtimeHome, "package.json")];
  for (const candidate of candidates) {
    try {
      const value = JSON.parse(fs.readFileSync(candidate, "utf8")).version;
      if (typeof value === "string" && value) return value;
    } catch {
      // Try the installed metadata next.
    }
  }
  return "unknown";
}

function fileSha256(candidate) {
  const digest = crypto.createHash("sha256");
  const descriptor = fs.openSync(candidate, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  try {
    const buffer = Buffer.allocUnsafe(64 * 1024);
    for (;;) {
      const read = fs.readSync(descriptor, buffer, 0, buffer.length, null);
      if (read === 0) break;
      digest.update(buffer.subarray(0, read));
    }
  } finally {
    fs.closeSync(descriptor);
  }
  return digest.digest("hex");
}

function manifestPath(value) {
  if (
    !path.isAbsolute(value) ||
    value.includes("\0") ||
    value.includes("\t") ||
    value.includes("\n") ||
    path.normalize(value) !== value
  ) {
    throw new Error("invalid manifest path");
  }
  return value;
}

function skillTargets(root, legacy = false) {
  const skill = path.join(root, "skills", "tether");
  const targets = [
    path.join(skill, "SKILL.md"),
    path.join(skill, "agents", "openai.yaml"),
    path.join(skill, "references", "setup.md"),
    path.join(skill, "references", "contract.md"),
    path.join(skill, "scripts", "tether_notify.py"),
  ];
  if (legacy) {
    const compatibility = path.join(root, "skills", "hermes-slack-bridge");
    targets.push(
      path.join(compatibility, "SKILL.md"),
      path.join(compatibility, "scripts", "hermes_notify.py"),
    );
  }
  return targets;
}

function expectedManagedTargetModes(metadata) {
  const harness = metadata.harness;
  if (!["codex", "claude-code", "both"].includes(harness)) {
    throw new Error("invalid harness");
  }
  const runtimeRoot = manifestPath(metadata.runtime_home);
  const pluginRoot = manifestPath(metadata.plugin_home);
  const localBin = manifestPath(metadata.local_bin);
  const codexRoot = manifestPath(metadata.codex_root);
  const claudeRoot = manifestPath(metadata.claude_root);
  const legacyValues = metadata.legacy === "none"
    ? []
    : metadata.legacy.split(",");
  if (
    legacyValues.some((value) => !["codex", "claude-code"].includes(value)) ||
    new Set(legacyValues).size !== legacyValues.length
  ) {
    throw new Error("invalid legacy harness list");
  }
  if (
    legacyValues.includes("codex") &&
    !["codex", "both"].includes(harness)
  ) {
    throw new Error("Codex legacy shim does not match selected harness");
  }
  if (
    legacyValues.includes("claude-code") &&
    !["claude-code", "both"].includes(harness)
  ) {
    throw new Error("Claude legacy shim does not match selected harness");
  }

  const targets = new Map([
    [path.join(runtimeRoot, "domain_control.py"), 0o600],
    [path.join(runtimeRoot, "domain_schema.py"), 0o600],
    [path.join(runtimeRoot, "domain_runtime.py"), 0o600],
    [path.join(runtimeRoot, "native_driver.py"), 0o600],
    [path.join(runtimeRoot, "security.py"), 0o600],
    [path.join(runtimeRoot, "tether_notify.py"), 0o700],
    [path.join(runtimeRoot, "tether_team.py"), 0o700],
    [path.join(runtimeRoot, "team", "TEAM.md"), 0o600],
    [path.join(runtimeRoot, "install.sh"), 0o700],
    [path.join(runtimeRoot, "package.json"), 0o600],
    [path.join(pluginRoot, "__init__.py"), 0o600],
    [path.join(pluginRoot, "active.py"), 0o600],
    [path.join(pluginRoot, "admission.py"), 0o600],
    [path.join(pluginRoot, "broker.py"), 0o600],
    [path.join(pluginRoot, "journal.py"), 0o600],
    [path.join(pluginRoot, "slack_egress.py"), 0o600],
    [path.join(pluginRoot, "plugin.yaml"), 0o644],
    [path.join(localBin, "tether"), 0o700],
  ]);
  const addSkill = (root, includeLegacy) => {
    for (const target of skillTargets(root, includeLegacy)) {
      targets.set(target, target.endsWith(".py") ? 0o700 : 0o644);
    }
  };
  if (harness === "codex" || harness === "both") {
    addSkill(codexRoot, legacyValues.includes("codex"));
  }
  if (harness === "claude-code" || harness === "both") {
    addSkill(
      claudeRoot,
      legacyValues.includes("claude-code"),
    );
  }
  return targets;
}

function expectedManagedTargets(metadata) {
  return new Set(expectedManagedTargetModes(metadata).keys());
}

function verifyManagedInstall() {
  let manifestInfo;
  let raw;
  try {
    manifestInfo = fs.lstatSync(installedManifest);
    if (manifestInfo.isSymbolicLink() || !manifestInfo.isFile()) {
      return { ok: false, line: "FAIL installer manifest is not a regular file" };
    }
    if (typeof process.getuid === "function" && manifestInfo.uid !== process.getuid()) {
      return { ok: false, line: "FAIL installer manifest belongs to a different Unix user" };
    }
    if ((manifestInfo.mode & 0o777) !== 0o600) {
      return { ok: false, line: "FAIL installer manifest is not owner-only" };
    }
    if (manifestInfo.size > 4 * 1024 * 1024) {
      return { ok: false, line: "FAIL installer manifest exceeds the size limit" };
    }
    raw = fs.readFileSync(installedManifest, "utf8");
  } catch (error) {
    return {
      ok: false,
      line: `FAIL installer manifest unavailable (${stringValue(error.code) || "read error"})`,
    };
  }

  const lines = raw.split("\n").filter(Boolean);
  if (lines.length === 0 || lines.length > 10_000) {
    return { ok: false, line: "FAIL installer manifest has an invalid record count" };
  }
  if (lines[0] !== MANIFEST_HEADER) {
    return {
      ok: false,
      line: "FAIL installer manifest metadata missing; upgrade Tether to regenerate it",
    };
  }
  const metadata = Object.create(null);
  let rowOffset = 1;
  while (rowOffset < lines.length && lines[rowOffset].startsWith("@")) {
    const fields = lines[rowOffset].split("\t");
    const key = fields[0].slice(1);
    if (
      fields.length !== 2 ||
      !MANIFEST_METADATA_KEYS.has(key) ||
      Object.prototype.hasOwnProperty.call(metadata, key) ||
      fields[1].length === 0
    ) {
      return { ok: false, line: "FAIL installer manifest metadata is invalid" };
    }
    metadata[key] = fields[1];
    rowOffset += 1;
  }
  if (
    Object.keys(metadata).length !== MANIFEST_METADATA_KEYS.size ||
    [...MANIFEST_METADATA_KEYS].some(
      (key) => !Object.prototype.hasOwnProperty.call(metadata, key),
    )
  ) {
    return { ok: false, line: "FAIL installer manifest metadata is incomplete" };
  }
  const rows = lines.slice(rowOffset);
  if (rows.length === 0) {
    return { ok: false, line: "FAIL installer manifest has no managed file records" };
  }
  let expectedModes;
  try {
    expectedModes = expectedManagedTargetModes(metadata);
  } catch {
    return { ok: false, line: "FAIL installer manifest metadata is invalid" };
  }
  const seen = new Set();
  let drifted = 0;
  try {
    for (const row of rows) {
      const fields = row.split("\t");
      if (
        fields.length !== 3 ||
        !path.isAbsolute(fields[0]) ||
        !/^[0-7]{3,4}$/.test(fields[1]) ||
        !/^[0-9a-f]{64}$/.test(fields[2]) ||
        seen.has(fields[0])
      ) {
        return { ok: false, line: "FAIL installer manifest contains an invalid record" };
      }
      const [target, expectedMode, expectedHash] = fields;
      seen.add(target);
      const info = fs.lstatSync(target);
      const actualModeBits = info.mode & 0o777;
      const expectedModeBits = Number.parseInt(expectedMode, 8);
      const canonicalModeBits = expectedModes.get(target);
      if (canonicalModeBits === undefined) {
        drifted += 1;
        continue;
      }
      const modeExpanded = (actualModeBits & ~canonicalModeBits) !== 0;
      const ownerCannotRead = (actualModeBits & 0o400) === 0;
      const ownerCannotExecute =
        (canonicalModeBits & 0o100) !== 0 && (actualModeBits & 0o100) === 0;
      if (
        (expectedModeBits & ~canonicalModeBits) !== 0 ||
        ((canonicalModeBits & 0o100) !== 0 && (expectedModeBits & 0o100) === 0) ||
        info.isSymbolicLink() ||
        !info.isFile() ||
        (typeof process.getuid === "function" && info.uid !== process.getuid()) ||
        modeExpanded ||
        ownerCannotRead ||
        ownerCannotExecute ||
        fileSha256(target) !== expectedHash
      ) {
        drifted += 1;
      }
    }
  } catch {
    drifted += 1;
  }
  const expected = new Set(expectedModes.keys());
  const missing = [...expected].filter((candidate) => !seen.has(candidate)).length;
  const unexpected = [...seen].filter((candidate) => !expected.has(candidate)).length;
  if (missing || unexpected) {
    return {
      ok: false,
      line: `FAIL managed target set mismatch (${missing} missing, ${unexpected} unexpected; harness=${metadata.harness})`,
    };
  }
  if (drifted) {
    return {
      ok: false,
      line: `FAIL managed install drift detected (${drifted} file${drifted === 1 ? "" : "s"}; harness=${metadata.harness})`,
    };
  }
  return {
    ok: true,
    line: `ok managed install integrity verified (${rows.length} files; harness=${metadata.harness})`,
  };
}

function secretEnvironmentValues() {
  const sensitiveName = /(TOKEN|SECRET|PASSWORD|PASSWD|API_KEY|PRIVATE_KEY|CREDENTIAL|COOKIE|AUTH)/i;
  return Object.entries(process.env)
    .filter(([name, value]) => sensitiveName.test(name) && typeof value === "string" && value.length >= 6)
    .map(([, value]) => value)
    .sort((left, right) => right.length - left.length);
}

const environmentSecrets = secretEnvironmentValues();

function redactText(input) {
  let value = String(input ?? "");
  for (const secret of environmentSecrets) {
    value = value.split(secret).join("[REDACTED]");
  }
  return value
    .replace(/\b(?:xox[a-z]-[A-Za-z0-9-]{8,}|gh[pousr]_[A-Za-z0-9_]{12,}|sk-[A-Za-z0-9_-]{12,}|AKIA[0-9A-Z]{16})\b/g, "[REDACTED]")
    .replace(/\bBearer\s+[A-Za-z0-9._~+/=-]{8,}/gi, "Bearer [REDACTED]")
    .replace(/(https?:\/\/[^:/@\s]+:)[^@\s]+@/gi, "$1[REDACTED]@")
    .replace(/\b(token|secret|password|passwd|api[_-]?key|private[_-]?key)\s*[:=]\s*[^\s,;]+/gi, "$1=[REDACTED]");
}

function safeValue(value, key = "", depth = 0) {
  if (depth > 12) return "[TRUNCATED]";
  if (/(TOKEN|SECRET|PASSWORD|PASSWD|API_KEY|PRIVATE_KEY|CREDENTIAL|COOKIE|AUTHORIZATION)/i.test(key)) {
    return "[REDACTED]";
  }
  if (typeof value === "string") return redactText(value);
  if (Array.isArray(value)) return value.map((item) => safeValue(item, "", depth + 1));
  if (value && typeof value === "object") {
    const result = Object.create(null);
    for (const [childKey, childValue] of Object.entries(value)) {
      result[childKey] = safeValue(childValue, childKey, depth + 1);
    }
    return result;
  }
  return value;
}

function writeJson(stream, value) {
  stream.write(`${JSON.stringify(safeValue(value))}\n`);
}

function printHelp(command = "") {
  const commandHelp = {
    status: "tether status [--json] [--socket PATH] [--timeout-ms MS]",
    doctor: "tether doctor [--json] [--socket PATH] [--timeout-ms MS]",
    identity: "tether identity [--json] [--socket PATH] [--timeout-ms MS]",
    maintenance: "tether maintenance [--json] [--socket PATH] [--timeout-ms MS]",
    notify: "tether notify (--text-stdin|--text-fd FD|--text TEXT [deprecated]) --idempotency-key KEY [--run-id ID|--hermes-session-id ID] [--channel ID] [--team ID]",
    reply: "tether reply --bridge-id ID (--text-stdin|--text-fd FD|--text TEXT [deprecated]) --reply-key KEY [--team ID]",
    attach: "tether attach --channel ID --thread-ts TS --idempotency-key KEY [source options]",
    rebind: "tether rebind --channel ID --thread-ts TS [source options] [--team ID]",
    close: "tether close --bridge-id ID | --channel ID --thread-ts TS [--team ID] [--expected-generation N]",
    unbind: "tether unbind --bridge-id ID | --channel ID --thread-ts TS [--team ID] [--expected-generation N]",
    post: "tether post --channel ID --thread-ts TS (--text-stdin|--text-fd FD|--text TEXT [deprecated]) --idempotency-key KEY [--team ID]",
    team: "tether team apply|status   (apply the shared team layer to this agent's SOUL.md)",
    spawn: "tether spawn --task TEXT [--harness claude|codex] [--cwd DIR] [--channel ID] [--thread-ts TS] [--root-text TEXT] [--team ID]",
    unresolved: "tether unresolved [--team ID] [--json]",
    history: "tether history [--channel ID] [--limit N] [--team ID]",
    thread: "tether thread --channel ID --thread-ts TS [--limit N] [--team ID]",
    schema: "tether schema status [--json]",
  };
  if (command && commandHelp[command]) {
    process.stdout.write(`${commandHelp[command]}\n`);
    return;
  }
  process.stdout.write(`Tether ${readVersion()}

Usage:
  tether setup [--harness=codex|claude-code|both]
  tether install|upgrade [installer options]
  tether rollback|uninstall [--dry-run] [--restart]
  tether doctor|status|identity|maintenance [--json]
  tether notify|reply|attach|rebind|close|unbind|post|spawn|history|thread [options]
  tether team apply|status
  tether schema status [--json]
  tether unresolved [options]
  tether version

Broker options:
  --socket PATH       Override TETHER_BROKER_SOCKET and HERMES_HOME.
  --timeout-ms MS     Bound one local broker request (50..60000 ms).
  --json              Emit one redacted JSON object where supported.

Run \`tether <operational-command> --help\` for command-specific arguments.
`);
}

function optionDefinitions(extra = {}) {
  return {
    socket: { type: "value" },
    "timeout-ms": { type: "value" },
    json: { type: "flag" },
    ...extra,
  };
}

function parseOptions(argv, definitions) {
  const result = {};
  const aliases = {};
  for (const [name, definition] of Object.entries(definitions)) {
    for (const alias of definition.aliases || []) aliases[alias] = name;
  }
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (!argument.startsWith("--") || argument === "--") {
      throw new CliError("Unexpected positional argument. Use --help for command syntax.");
    }
    const equals = argument.indexOf("=");
    const rawName = argument.slice(2, equals === -1 ? undefined : equals);
    const name = aliases[rawName] || rawName;
    const definition = definitions[name];
    if (!definition) {
      throw new CliError(`Unknown option --${redactText(rawName)}. Use --help for command syntax.`);
    }
    if (Object.prototype.hasOwnProperty.call(result, name)) {
      throw new CliError(`Option --${name} may be provided only once.`);
    }
    if (definition.type === "flag") {
      if (equals !== -1) throw new CliError(`Option --${name} does not take a value.`);
      result[name] = true;
      continue;
    }
    let value;
    if (equals !== -1) {
      value = argument.slice(equals + 1);
    } else {
      index += 1;
      value = argv[index];
    }
    if (typeof value !== "string" || value.length === 0) {
      throw new CliError(`Option --${name} requires a non-empty value.`);
    }
    result[name] = value;
  }
  return result;
}

function requireOption(options, name) {
  const value = stringValue(options[name]);
  if (!value) throw new CliError(`Missing required option --${name}.`);
  return value;
}

function validateMessageText(value, source) {
  if (Buffer.byteLength(value, "utf8") > MAX_MESSAGE_BYTES) {
    throw new CliError(
      `Message text from ${source} exceeds ${MAX_MESSAGE_BYTES} bytes.`,
      EXIT_USAGE,
      "message_too_large",
    );
  }
  if (!value) {
    throw new CliError(
      `Message text from ${source} is empty.`,
      EXIT_USAGE,
      "message_empty",
    );
  }
  if (value.includes("\0")) {
    throw new CliError(
      "Message text may not contain NUL bytes.",
      EXIT_USAGE,
      "message_invalid",
    );
  }
  return value;
}

function readBoundedText(descriptor, source) {
  const chunks = [];
  let total = 0;
  try {
    for (;;) {
      const remaining = MAX_MESSAGE_BYTES + 1 - total;
      if (remaining <= 0) break;
      const chunk = Buffer.allocUnsafe(Math.min(64 * 1024, remaining));
      const count = fs.readSync(descriptor, chunk, 0, chunk.length, null);
      if (count === 0) break;
      chunks.push(chunk.subarray(0, count));
      total += count;
    }
  } catch (error) {
    throw new CliError(
      `Could not read message text from ${source} (${stringValue(error.code) || "read error"}).`,
      EXIT_USAGE,
      "message_input_unavailable",
    );
  }
  if (total > MAX_MESSAGE_BYTES) {
    throw new CliError(
      `Message text from ${source} exceeds ${MAX_MESSAGE_BYTES} bytes.`,
      EXIT_USAGE,
      "message_too_large",
    );
  }
  let value;
  try {
    value = new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(chunks, total));
  } catch {
    throw new CliError(
      `Message text from ${source} is not valid UTF-8.`,
      EXIT_USAGE,
      "message_invalid_utf8",
    );
  }
  return validateMessageText(value, source);
}

function resolveMessageText(options) {
  const sources = [
    Boolean(options.text),
    options["text-stdin"] === true,
    options["text-fd"] !== undefined,
  ].filter(Boolean).length;
  if (sources !== 1) {
    throw new CliError(
      "Choose exactly one of --text-stdin, --text-fd, or deprecated --text.",
      EXIT_USAGE,
      "message_input_required",
    );
  }
  if (options.text) {
    process.stderr.write(
      "DEPRECATED: --text exposes message content in process arguments; use --text-stdin or --text-fd.\n",
    );
    return validateMessageText(requireOption(options, "text"), "argv");
  }
  if (options["text-stdin"]) {
    return readBoundedText(0, "stdin");
  }
  const rawDescriptor = stringValue(options["text-fd"]);
  if (!/^[0-9]+$/.test(rawDescriptor)) {
    throw new CliError("--text-fd must be an inherited file descriptor of 3 or greater.");
  }
  const descriptor = Number(rawDescriptor);
  if (!Number.isSafeInteger(descriptor) || descriptor < 3) {
    throw new CliError("--text-fd must be an inherited file descriptor of 3 or greater.");
  }
  return readBoundedText(descriptor, `fd ${descriptor}`);
}

function messageDefinitions(extra = {}) {
  return {
    text: { type: "value" },
    "text-stdin": { type: "flag" },
    "text-fd": { type: "value" },
    ...extra,
  };
}

function parseLimit(value, fallback) {
  if (value === undefined) return fallback;
  if (!/^[0-9]+$/.test(String(value))) {
    throw new CliError("--limit must be a positive integer.");
  }
  const result = Number(value);
  if (!Number.isSafeInteger(result) || result < 1 || result > 100) {
    throw new CliError("--limit must be between 1 and 100.");
  }
  return result;
}

function parseGeneration(value) {
  if (value === undefined) return undefined;
  if (!/^[0-9]+$/.test(String(value))) {
    throw new CliError("--expected-generation must be a positive integer.");
  }
  const result = Number(value);
  if (!Number.isSafeInteger(result) || result < 1) {
    throw new CliError("--expected-generation must be a positive integer.");
  }
  return result;
}

function requireChoice(options, name, allowed) {
  const value = requireOption(options, name);
  if (!allowed.includes(value)) {
    throw new CliError(
      `--${name} must be one of: ${allowed.join(", ")}.`,
      EXIT_USAGE,
      "invalid_resolution",
    );
  }
  return value;
}

function resolveTimeout(options) {
  const raw = options["timeout-ms"] ?? process.env.TETHER_BROKER_TIMEOUT_MS;
  if (raw === undefined || raw === "") return DEFAULT_TIMEOUT_MS;
  if (!/^[0-9]+$/.test(String(raw))) {
    throw new CliError("Broker timeout must be an integer number of milliseconds.");
  }
  const timeout = Number(raw);
  if (!Number.isSafeInteger(timeout) || timeout < MIN_TIMEOUT_MS || timeout > MAX_TIMEOUT_MS) {
    throw new CliError(`Broker timeout must be between ${MIN_TIMEOUT_MS} and ${MAX_TIMEOUT_MS} ms.`);
  }
  return timeout;
}

function resolveSocketPath(options) {
  const candidate = stringValue(options.socket) ||
    stringValue(process.env.TETHER_BROKER_SOCKET) ||
    stringValue(process.env.TETHER_SOCKET_PATH) ||
    stringValue(process.env.TETHER_SOCKET) ||
    path.join(process.env.HERMES_HOME || path.join(home, ".hermes"), "bridge.sock");
  if (!path.isAbsolute(candidate)) {
    throw new CliError("The Tether broker socket path must be absolute.");
  }
  if (candidate.includes("\0") || Buffer.byteLength(candidate) > 107) {
    throw new CliError("The Tether broker socket path is invalid or too long.");
  }
  return candidate;
}

function protocolFailure(message) {
  return new CliError(message, EXIT_TRANSPORT, "broker_invalid_response", {
    status: "unavailable",
    retryable: true,
    next_action: "Run `tether doctor`; retry after the local broker is healthy.",
  });
}

function brokerCall(request, options) {
  const socketPath = resolveSocketPath(options);
  const timeoutMs = resolveTimeout(options);
  let encoded;
  try {
    encoded = JSON.stringify(request);
  } catch {
    return Promise.reject(new CliError(
      "The Tether broker request is not valid JSON.",
      EXIT_USAGE,
      "invalid_request",
    ));
  }
  const frame = Buffer.from(`${encoded}\n`, "utf8");
  if (frame.length > MAX_REQUEST_FRAME_BYTES) {
    return Promise.reject(new CliError(
      "The Tether broker request exceeds the 1 MiB protocol limit.",
      EXIT_USAGE,
      "request_too_large",
    ));
  }

  return new Promise((resolve, reject) => {
    let settled = false;
    const responseChunks = [];
    let responseLength = 0;
    const socket = net.createConnection({ path: socketPath });

    function finishError(error) {
      if (settled) return;
      settled = true;
      socket.destroy();
      reject(error);
    }

    function finishResponse() {
      if (settled) return;
      if (responseLength === 0) {
        finishError(new CliError(
          "Tether's local broker closed without a response.",
          EXIT_TRANSPORT,
          "broker_unavailable",
          {
            status: "unavailable",
            retryable: true,
            next_action: "Start or restart the Hermes gateway, then run `tether doctor`.",
          },
        ));
        return;
      }
      const response = Buffer.concat(responseChunks, responseLength);
      if (
        response.length > MAX_RESPONSE_FRAME_BYTES ||
        response[response.length - 1] !== 0x0a ||
        response.subarray(0, response.length - 1).includes(0x0a)
      ) {
        finishError(protocolFailure("Tether's local broker returned invalid JSON framing."));
        return;
      }
      let decoded;
      try {
        decoded = new TextDecoder("utf-8", { fatal: true }).decode(
          response.subarray(0, response.length - 1),
        );
      } catch {
        finishError(protocolFailure("Tether's local broker returned invalid UTF-8."));
        return;
      }
      let payload;
      try {
        payload = JSON.parse(decoded);
      } catch {
        finishError(protocolFailure("Tether's local broker returned malformed JSON."));
        return;
      }
      if (!payload || typeof payload !== "object" || Array.isArray(payload) || typeof payload.ok !== "boolean") {
        finishError(protocolFailure("Tether's local broker returned an invalid response contract."));
        return;
      }
      settled = true;
      socket.destroy();
      if (!payload.ok) {
        reject(new BrokerError(payload));
      } else {
        resolve(payload);
      }
    }

    socket.setTimeout(timeoutMs);
    socket.once("connect", () => socket.end(frame));
    socket.on("data", (chunk) => {
      if (settled) return;
      responseLength += chunk.length;
      responseChunks.push(chunk);
      if (responseLength > MAX_RESPONSE_FRAME_BYTES) {
        finishError(protocolFailure("Tether's local broker response exceeds the 8 MiB protocol limit."));
      }
    });
    socket.once("timeout", () => finishError(new CliError(
      `Tether's local broker did not respond within ${timeoutMs} ms.`,
      EXIT_TRANSPORT,
      "broker_timeout",
      {
        status: "unavailable",
        retryable: true,
        next_action: "Run `tether doctor`; retry after the local broker is responsive.",
      },
    )));
    socket.once("error", (error) => finishError(new CliError(
      `Tether could not reach its local broker (${stringValue(error.code) || "socket error"}).`,
      EXIT_TRANSPORT,
      "broker_unavailable",
      {
        status: "unavailable",
        retryable: true,
        next_action: "Start or restart the Hermes gateway, then run `tether doctor`.",
      },
    )));
    socket.once("end", finishResponse);
  });
}

function assertNonRoot(command) {
  const mutating = new Set([
    "setup", "install", "upgrade", "rollback", "uninstall", "maintenance",
    "notify", "reply", "attach", "rebind", "close", "unbind", "post", "spawn", "team",
  ]);
  if (
    mutating.has(command) &&
    typeof process.getuid === "function" &&
    process.getuid() === 0
  ) {
    throw new CliError(
      "Tether refuses this operation as root. Run it as the dedicated non-root Hermes user.",
      EXIT_USAGE,
      "root_refused",
    );
  }
}

function runChild(
  executable,
  commandArgs,
  timeoutMs = CHILD_TIMEOUT_MS,
  childEnvironment = process.env,
  childInput = null,
) {
  const result = spawnSync(executable, commandArgs, {
    stdio: childInput === null ? "inherit" : ["pipe", "inherit", "inherit"],
    input: childInput === null ? undefined : childInput,
    env: childEnvironment,
    timeout: timeoutMs,
    killSignal: "SIGTERM",
  });
  if (result.error) {
    const timedOut = result.error.code === "ETIMEDOUT";
    throw new CliError(
      timedOut
        ? `Tether's child command exceeded its ${timeoutMs} ms deadline.`
        : `Tether could not start its child command (${stringValue(result.error.code) || "spawn error"}).`,
      timedOut ? EXIT_TRANSPORT : 1,
      timedOut ? "child_timeout" : "child_failed",
    );
  }
  return result.status ?? 1;
}

function requireInstalledRuntime() {
  if (!fs.existsSync(notifier)) {
    throw new CliError(
      "Tether is not installed. Run `npx --yes --package=@parcha/tether tether setup --harness=both`.",
      EXIT_USAGE,
      "runtime_missing",
    );
  }
}

function notifierArguments(argv, hasMessageText = false) {
  const result = [];
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (
      argument === "--socket" ||
      argument === "--timeout-ms" ||
      (hasMessageText && (argument === "--text" || argument === "--text-fd"))
    ) {
      index += 1;
      continue;
    }
    if (
      argument.startsWith("--socket=") ||
      argument.startsWith("--timeout-ms=") ||
      (
        hasMessageText &&
        (
          argument === "--text-stdin" ||
          argument.startsWith("--text=") ||
          argument.startsWith("--text-fd=")
        )
      )
    ) {
      continue;
    }
    result.push(argument);
  }
  if (hasMessageText) result.push("--text-stdin");
  return result;
}

function runNotifier(
  command,
  argv,
  options = {},
  timeoutMs = CHILD_TIMEOUT_MS,
  messageText = null,
) {
  requireInstalledRuntime();
  const socketPath = resolveSocketPath(options);
  if (path.basename(socketPath) !== "bridge.sock") {
    throw new CliError(
      "Live terminal source capture requires a broker socket named bridge.sock.",
      EXIT_USAGE,
      "unsupported_socket_override",
    );
  }
  const childEnvironment = {
    ...process.env,
    HERMES_HOME: path.dirname(socketPath),
  };
  return runChild(
    process.env.PYTHON_BIN || "python3",
    [notifier, command, ...notifierArguments(argv, messageText !== null)],
    timeoutMs,
    childEnvironment,
    messageText,
  );
}

function sourceDefinitions() {
  return {
    "run-id": { type: "value" },
    "hermes-session-id": { type: "value" },
    "claude-session-id": { type: "value" },
    "codex-session-id": { type: "value" },
    "zellij-session": { type: "value" },
    "zellij-pane-id": { type: "value" },
    cwd: { type: "value" },
  };
}

function workingDirectoryIdentity(cwd) {
  let resolved;
  let metadata;
  try {
    resolved = fs.realpathSync.native(cwd);
    metadata = fs.statSync(resolved, { bigint: true });
  } catch {
    throw new CliError(
      "The working directory is unavailable.",
      EXIT_USAGE,
      "cwd_identity_changed",
    );
  }
  if (!metadata.isDirectory()) {
    throw new CliError(
      "The working directory is not a directory.",
      EXIT_USAGE,
      "cwd_identity_changed",
    );
  }
  return {
    cwd: path.resolve(cwd),
    cwd_realpath: resolved,
    cwd_device: metadata.dev.toString(),
    cwd_inode: metadata.ino.toString(),
    cwd_owner_uid: metadata.uid.toString(),
  };
}

function detectSource(options) {
  const cwd = path.resolve(stringValue(options.cwd) || process.cwd());
  const hasHerdrEnvironment = false;
  const hasZellijOption = Boolean(options["zellij-session"] || options["zellij-pane-id"]);
  if (Boolean(options["zellij-session"]) !== Boolean(options["zellij-pane-id"])) {
    throw new CliError("--zellij-session and --zellij-pane-id must be provided together.");
  }
  const explicit = [
    ["headless_run", stringValue(options["run-id"])],
    ["hermes_session", stringValue(options["hermes-session-id"])],
    ["claude_session", stringValue(options["claude-session-id"])],
    ["codex_session", stringValue(options["codex-session-id"])],
  ].filter(([, value]) => value);
  if (explicit.length > 1) {
    throw new CliError("Choose exactly one run, Hermes, Claude, or Codex source.");
  }
  if (hasZellijOption && explicit.length > 0) {
    throw new CliError("Choose a native session source or a Zellij pane, not both.");
  }
  const explicitNonNative =
    stringValue(options["run-id"]) || stringValue(options["hermes-session-id"]);
  if (
    explicitNonNative &&
    (
      stringValue(process.env.CLAUDE_CODE_SESSION_ID) ||
      stringValue(process.env.CODEX_THREAD_ID) ||
      stringValue(process.env.ZELLIJ_SESSION_NAME) ||
      stringValue(process.env.ZELLIJ_PANE_ID) ||
      hasHerdrEnvironment
    )
  ) {
    throw new CliError(
      "An explicit headless or Hermes source cannot replace an active Codex, Claude Code, Herdr, or Zellij binding; repair or rebind the exact native session.",
      EXIT_USAGE,
      "native_binding_required",
    );
  }
  if (hasHerdrEnvironment) return null;
  if (hasZellijOption) return null;
  if (explicit.length === 1) {
    const [kind, identifier] = explicit[0];
    if (kind === "headless_run") {
      return { kind, source: { run_id: identifier, queue_id: identifier, cwd } };
    }
    return {
      kind,
      source: {
        session_id: identifier,
        ...workingDirectoryIdentity(cwd),
      },
    };
  }
  if (process.env.ZELLIJ_SESSION_NAME || process.env.ZELLIJ_PANE_ID) return null;
  const ambient = [
    ["claude_session", stringValue(process.env.CLAUDE_CODE_SESSION_ID)],
    ["codex_session", stringValue(process.env.CODEX_THREAD_ID)],
  ].filter(([, value]) => value);
  if (ambient.length > 1) {
    throw new CliError(
      "Both Claude and Codex session IDs are present without an exact pane identity.",
    );
  }
  if (ambient.length === 1) {
    return {
      kind: ambient[0][0],
      source: {
        session_id: ambient[0][1],
        ...workingDirectoryIdentity(cwd),
      },
    };
  }
  return null;
}

function commonRequestOptions(extra = {}) {
  return optionDefinitions({
    team: { type: "value" },
    ...extra,
  });
}

function emitResult(command, result, options) {
  const safe = safeValue(result);
  if (
    options.json ||
    ["identity", "maintenance", "close", "unbind", "unresolved"].includes(command)
  ) {
    writeJson(process.stdout, safe);
    return;
  }
  if (command === "history" || command === "thread") {
    writeJson(process.stdout, Array.isArray(safe.messages) ? safe.messages : []);
    return;
  }
  const identifier = stringValue(safe.thread_ts) || stringValue(safe.bridge_id) || "ok";
  process.stdout.write(`${identifier}\n`);
}

function statusLines(status) {
  const protocolHealthy = Number.isInteger(status.protocol_version) &&
    status.protocol_version === BROKER_PROTOCOL_VERSION;
  const lines = [
    status.implementation === "tether"
      ? "ok Tether broker implementation active"
      : `FAIL unexpected broker implementation=${stringValue(status.implementation) || "unknown"}`,
    protocolHealthy
      ? `ok broker protocol=${status.protocol_version}`
      : `FAIL unsupported broker protocol=${status.protocol_version ?? "unknown"}`,
    status.allowed_user_count > 0
      ? `ok authorized operators=${status.allowed_user_count}`
      : "FAIL no explicit operator allowlist",
    status.owner_configured
      ? "ok bridge owner configured"
      : "FAIL no bridge owner configured",
    status.peer_uid_enforced === true && status.root_refused === true
      ? "ok local Unix peer boundary enforced"
      : "FAIL local Unix peer boundary is not confirmed",
  ];
  if (status.slack_transport_connected === true) {
    lines.push("ok Slack Socket Mode ingress connected");
  } else if (status.slack_transport_connected === false) {
    lines.push("FAIL Slack Socket Mode ingress is disconnected");
  } else {
    lines.push("FAIL Slack Socket Mode ingress has not connected yet");
  }
  if (status.default_channel_membership === "not_member") {
    lines.push(
      "FAIL Slack bot is not a member of its configured channel; mentions there are dropped before Tether sees them (invite it, or run tether setup)",
    );
  } else if (status.default_channel_membership === "member") {
    lines.push("ok Slack bot is a member of its configured channel");
  }
  if (status.reply_poll_healthy === true) {
    lines.push("ok best-effort Slack polling worker active");
  } else if (status.reply_poll_healthy === false) {
    lines.push("WARN best-effort Slack polling worker is not healthy");
  } else {
    lines.push("WARN best-effort Slack polling health has not been observed");
  }
  const uncertain = Number(status.uncertain_delivery_count || 0);
  const blocked = Number(status.blocked_bridge_count || 0);
  const queued = Number(status.queued_delivery_count || 0);
  if (uncertain > 0 || blocked > 0) {
    lines.push(
      `FAIL durable delivery blocked: unresolved=${uncertain} blocked_threads=${blocked}; run tether unresolved`,
    );
  } else {
    lines.push("ok durable delivery queue has no unresolved blockers");
  }
  if (queued > 0) {
    lines.push(`WARN queued Slack follow-ups=${queued}`);
  }
  return lines;
}

async function runStatus(options) {
  const status = await brokerCall({ op: "status" }, options);
  const checks = statusLines(status);
  if (options.json) {
    writeJson(process.stdout, status);
  } else {
    process.stdout.write(`${checks.join("\n")}\n`);
  }
  const failed = status.implementation !== "tether" ||
    !Number.isInteger(status.protocol_version) ||
    status.protocol_version !== BROKER_PROTOCOL_VERSION ||
    status.peer_uid_enforced !== true ||
    status.root_refused !== true ||
    !status.owner_configured ||
    !(status.allowed_user_count > 0) ||
    status.slack_transport_connected !== true ||
    checks.some((line) => line.startsWith("FAIL"));
  return failed ? 1 : 0;
}

async function runDoctor(options) {
  const socketPath = resolveSocketPath(options);
  const checks = [];
  let healthy = true;
  try {
    const info = fs.lstatSync(socketPath);
    if (info.isSymbolicLink() || !info.isSocket()) {
      healthy = false;
      checks.push("FAIL broker path is not a Unix socket");
    } else {
      const mode = info.mode & 0o777;
      if (mode !== 0o600) {
        healthy = false;
        checks.push(`FAIL broker socket mode is ${mode.toString(8).padStart(4, "0")}; expected 0600`);
      } else {
        checks.push("ok broker socket is private");
      }
      if (typeof process.getuid === "function" && info.uid !== process.getuid()) {
        healthy = false;
        checks.push("FAIL broker socket belongs to a different Unix user");
      } else {
        checks.push("ok broker socket owner matches this user");
      }
    }
  } catch (error) {
    healthy = false;
    checks.push(`FAIL broker socket unavailable (${stringValue(error.code) || "stat error"})`);
  }

  for (const [label, candidate] of [
    ["runtime", installedRuntime],
    ["notifier", notifier],
  ]) {
    if (fs.existsSync(candidate)) {
      checks.push(`ok ${label} installed`);
    } else {
      healthy = false;
      checks.push(`FAIL ${label} missing; reinstall Tether`);
    }
  }
  const installIntegrity = verifyManagedInstall();
  checks.push(installIntegrity.line);
  if (!installIntegrity.ok) healthy = false;

  let status = null;
  try {
    status = await brokerCall({ op: "status" }, options);
    const brokerChecks = statusLines(status);
    checks.push(...brokerChecks);
    if (
      status.implementation !== "tether" ||
      status.protocol_version === undefined ||
      brokerChecks.some((line) => line.startsWith("FAIL"))
    ) {
      healthy = false;
    }
  } catch (error) {
    healthy = false;
    checks.push(`FAIL broker readiness: ${redactText(error.message)}`);
  }

  if (options.json) {
    writeJson(process.stdout, { ok: healthy, checks, status });
  } else {
    process.stdout.write(`${checks.join("\n")}\n`);
  }
  return healthy ? 0 : 1;
}

async function runBrokerCommand(command, argv) {
  const source = sourceDefinitions();
  let definitions;
  switch (command) {
    case "status":
    case "doctor":
    case "identity":
    case "maintenance":
      definitions = optionDefinitions();
      break;
    case "reply":
      definitions = commonRequestOptions(messageDefinitions({
        "bridge-id": { type: "value" },
        "reply-key": { type: "value" },
      }));
      break;
    case "rebind":
      definitions = commonRequestOptions({
        channel: { type: "value" },
        "thread-ts": { type: "value" },
        ...source,
      });
      break;
    case "attach":
      definitions = commonRequestOptions({
        channel: { type: "value" },
        "thread-ts": { type: "value" },
        owner: { type: "value" },
        "idempotency-key": { type: "value" },
        ...source,
      });
      break;
    case "notify":
      definitions = commonRequestOptions(messageDefinitions({
        channel: { type: "value" },
        owner: { type: "value" },
        "idempotency-key": { type: "value" },
        file: { type: "value" },
        ...source,
      }));
      break;
    case "post":
      definitions = commonRequestOptions(messageDefinitions({
        channel: { type: "value" },
        "thread-ts": { type: "value" },
        "idempotency-key": { type: "value" },
      }));
      break;
    case "spawn":
      definitions = commonRequestOptions({
        task: { type: "value" },
        harness: { type: "value" },
        cwd: { type: "value" },
        channel: { type: "value" },
        "thread-ts": { type: "value" },
        "root-text": { type: "value" },
      });
      break;
    case "unresolved":
      definitions = commonRequestOptions();
      break;
    case "resolve":
      throw new CliError(
        "Tether recovery mutation is disabled until an OS-isolated operator authority channel is active.",
        EXIT_USAGE,
        "operator_boundary_unavailable",
      );
    case "history":
      definitions = commonRequestOptions({
        channel: { type: "value" },
        limit: { type: "value" },
      });
      break;
    case "thread":
      definitions = commonRequestOptions({
        channel: { type: "value" },
        "thread-ts": { type: "value" },
        limit: { type: "value" },
      });
      break;
    case "close":
    case "unbind":
      definitions = commonRequestOptions({
        "bridge-id": { type: "value" },
        channel: { type: "value" },
        "thread-ts": { type: "value" },
        "expected-generation": { type: "value" },
      });
      break;
    default:
      throw new CliError(`Unknown Tether command: ${redactText(command)}.`);
  }

  if (argv.includes("--help")) {
    printHelp(command);
    return 0;
  }
  const options = parseOptions(argv, definitions);
  if (command === "team") {
    const script = path.join(runtimeHome, "tether_team.py");
    const teamMd = path.join(runtimeHome, "team", "TEAM.md");
    const sub = argv[1] || "apply";
    const child = spawnSync("python3", [script, sub, "--team-md", teamMd], { stdio: "inherit" });
    return child.status ?? 1;
  }
  if (command === "status") return runStatus(options);
  if (command === "doctor") return runDoctor(options);
  const messageText = ["notify", "reply", "post"].includes(command)
    ? resolveMessageText(options)
    : null;

  let request;
  if (command === "identity" || command === "maintenance") {
    request = { op: command };
  } else if (command === "unresolved") {
    request = {
      op: "unresolved",
      team_id: stringValue(options.team),
    };
  } else if (command === "reply") {
    request = {
      op: "reply",
      bridge_id: requireOption(options, "bridge-id"),
      reply_key: requireOption(options, "reply-key"),
      text: messageText,
      team_id: stringValue(options.team),
    };
  } else if (command === "spawn") {
    request = {
      op: "spawn",
      task: requireOption(options, "task"),
      harness: stringValue(options.harness) || "claude",
      cwd: stringValue(options.cwd) || process.cwd(),
      channel_id: stringValue(options.channel),
      thread_ts: stringValue(options["thread-ts"]),
      root_text: stringValue(options["root-text"]),
      team_id: stringValue(options.team),
    };
  } else if (command === "post") {
    request = {
      op: "thread_reply",
      channel_id: requireOption(options, "channel"),
      thread_ts: requireOption(options, "thread-ts"),
      text: messageText,
      idempotency_key: requireOption(options, "idempotency-key"),
      team_id: stringValue(options.team),
    };
  } else if (command === "history") {
    request = {
      op: "history",
      channel_id: stringValue(options.channel),
      team_id: stringValue(options.team),
      limit: parseLimit(options.limit, 15),
    };
  } else if (command === "thread") {
    request = {
      op: "thread_history",
      channel_id: requireOption(options, "channel"),
      thread_ts: requireOption(options, "thread-ts"),
      team_id: stringValue(options.team),
      limit: parseLimit(options.limit, 100),
    };
  } else if (command === "close" || command === "unbind") {
    const bridgeId = stringValue(options["bridge-id"]);
    const channel = stringValue(options.channel);
    const threadTs = stringValue(options["thread-ts"]);
    if (!bridgeId && !(channel && threadTs)) {
      throw new CliError(
        `${command} requires --bridge-id or both --channel and --thread-ts.`,
      );
    }
    request = {
      op: "close",
      bridge_id: bridgeId,
      team_id: stringValue(options.team),
      channel_id: channel,
      thread_ts: threadTs,
      expected_generation: parseGeneration(options["expected-generation"]),
    };
  } else {
    const resolved = detectSource(options);
    if (!resolved) {
      return runNotifier(
        command,
        argv,
        options,
        Math.min(resolveTimeout(options) + 1_000, MAX_TIMEOUT_MS),
        messageText,
      );
    }
    if (command === "rebind") {
      request = {
        op: "rebind",
        team_id: stringValue(options.team),
        channel_id: requireOption(options, "channel"),
        thread_ts: requireOption(options, "thread-ts"),
        source_kind: resolved.kind,
        source: resolved.source,
      };
    } else if (command === "attach") {
      request = {
        op: "attach",
        source_kind: resolved.kind,
        source: resolved.source,
        owner_user_id: stringValue(options.owner),
        channel_id: requireOption(options, "channel"),
        team_id: stringValue(options.team),
        thread_ts: requireOption(options, "thread-ts"),
        idempotency_key: requireOption(options, "idempotency-key"),
      };
    } else {
      request = {
        op: "notify",
        text: messageText,
        source_kind: resolved.kind,
        source: resolved.source,
        owner_user_id: stringValue(options.owner),
        channel_id: stringValue(options.channel),
        team_id: stringValue(options.team),
        idempotency_key: requireOption(options, "idempotency-key"),
        file_path: stringValue(options.file) || null,
      };
    }
  }

  const result = await brokerCall(request, options);
  emitResult(command, result, options);
  return 0;
}

function renderError(error, jsonOutput) {
  const normalized = error instanceof CliError
    ? error
    : new CliError("Tether failed unexpectedly.", 1, "unexpected_error");
  const payload = {
    ok: false,
    code: normalized.code,
    message: redactText(normalized.message).slice(0, 500),
    ...safeValue(normalized.details),
  };
  if (jsonOutput) {
    writeJson(process.stderr, payload);
    return normalized.exitCode;
  }
  process.stderr.write(`Tether error [${payload.code}]: ${payload.message}\n`);
  if (payload.status) process.stderr.write(`Status: ${payload.status}\n`);
  if (payload.next_action) process.stderr.write(`Next: ${payload.next_action}\n`);
  return normalized.exitCode;
}

async function main() {
  const argv = process.argv.slice(2);
  if (argv.length === 0) argv.push("doctor");
  const command = argv.shift();

  if (command === "--help" || command === "-h" || command === "help") {
    printHelp();
    return 0;
  }
  if (command === "--version" || command === "-V" || command === "version") {
    process.stdout.write(`tether ${readVersion()}\n`);
    return 0;
  }

  assertNonRoot(command);



  if (command === "install" || command === "upgrade") {
    if (!packagePayloadAvailable()) {
      throw new CliError(
        "Install and upgrade require a complete tagged Tether package. Run the documented npx command.",
      );
    }
    const withHerdr = false;
    const installerArgs = argv;
    const installed = runChild(
      packageInstaller,
      [command, ...installerArgs],
      LIFECYCLE_TIMEOUT_MS,
    );
    return installed;
  }

  if (command === "rollback" || command === "uninstall") {
    const installer = fs.existsSync(installedInstaller)
      ? installedInstaller
      : packageInstaller;
    if (!fs.existsSync(installer)) {
      throw new CliError("The Tether lifecycle installer is unavailable.");
    }
    const withHerdr = false;
    const installerArgs = argv;
    const completed = runChild(
      installer,
      [command, ...installerArgs],
      LIFECYCLE_TIMEOUT_MS,
    );
    return completed;
  }

  if (command === "setup") {
    const withHerdr = false;
    const installArgs = argv.filter((argument) =>
      argument.startsWith("--harness=") ||
      ["--both", "--codex", "--claude-code"].includes(argument)
    );
    const setupArgs = argv.filter((argument) =>
      !installArgs.includes(argument)
    );
    if (packagePayloadAvailable()) {
      const installed = runChild(
        packageInstaller,
        ["install", ...installArgs],
        LIFECYCLE_TIMEOUT_MS,
      );
      if (installed !== 0) return installed;
    }
    const configured = runNotifier("setup", setupArgs, {}, LIFECYCLE_TIMEOUT_MS);
    return configured;
  }

  return runBrokerCommand(command, argv);
}

const requestedJson = process.argv.includes("--json");
main()
  .then((exitCode) => {
    process.exitCode = Number.isInteger(exitCode) ? exitCode : 1;
  })
  .catch((error) => {
    process.exitCode = renderError(error, requestedJson);
  });
