"use strict";

const crypto = require("crypto");
const fs = require("fs");
const http = require("http");
const os = require("os");
const path = require("path");
const readline = require("readline/promises");
const { spawn, spawnSync } = require("child_process");

function runtimePackageRoot() {
  const candidates = [
    path.resolve(__dirname, ".."),
    path.resolve(__dirname, "../../../.."),
  ];
  for (const candidate of candidates) {
    if (fs.existsSync(path.join(candidate, "skills", "parable", "scripts", "parable.py"))) {
      return candidate;
    }
  }
  throw new Error("Parable runtime is incomplete: skills/parable/scripts/parable.py is missing");
}

const PACKAGE_ROOT = runtimePackageRoot();
const PARABLE_PY = path.join(
  PACKAGE_ROOT, "skills", "parable", "scripts", "parable.py",
);
const PATCH_PATH = path.join(
  path.resolve(__dirname, ".."), "patches", "cliproxyapi-v7.2.88-claude-effort.patch",
);

const SETUP_SCHEMA_VERSION = 1;
const DEFAULT_PORT = 8317;
const PROXY_REPOSITORY = "https://github.com/router-for-me/CLIProxyAPI.git";
const PROXY_COMMIT = "93d74a890a44802f656d7f39a573916b2611896e";
const PROXY_PATCH_SHA256 = "d35b422da321265150fe393da80a686862ef642ee45c65a3e2fb908d689d5d1f";
const PROXY_BINARY_NAME = "parable-cliproxy-api";
const DEFAULT_PROXY_READY_TIMEOUT_MS = 15_000;
const PROXY_PROBE_TIMEOUT_MS = 750;
const PROXY_STOP_TIMEOUT_MS = 2_000;
const VENDOR_ORDER = Object.freeze(["claude", "chatgpt", "xai"]);
const AUTH_FLAGS = Object.freeze({
  chatgpt: Object.freeze({ browser: "--codex-login", device: "--codex-device-login" }),
  claude: Object.freeze({ browser: "--claude-login", extra: "--no-browser" }),
  xai: Object.freeze({ browser: "--xai-login", extra: "--no-browser" }),
});
const VENDORS = Object.freeze({
  chatgpt: Object.freeze({
    models: Object.freeze(["gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna"]),
  }),
  claude: Object.freeze({
    models: Object.freeze([
      "claude-fable-5",
      "claude-sonnet-5",
      "claude-opus-4-8",
      "claude-haiku-4-5-20251001",
    ]),
  }),
  xai: Object.freeze({ models: Object.freeze(["grok-4.5"]) }),
});

class OnboardingError extends Error {
  constructor(message, exitCode = 1) {
    super(message);
    this.exitCode = exitCode;
  }
}

class PromptSession {
  constructor(input, output) {
    this.output = output;
    this.interface = readline.createInterface({ input, output, terminal: Boolean(output.isTTY) });
    this.lines = this.interface[Symbol.asyncIterator]();
  }

  async ask(question) {
    this.output.write(question);
    const next = await this.lines.next();
    if (next.done) throw new OnboardingError("interactive input ended before setup was complete");
    return next.value;
  }

  close() {
    this.interface.close();
  }
}

function modeOf(stat) {
  return stat.mode & 0o777;
}

function octal(mode) {
  return mode.toString(8).padStart(4, "0");
}

function lstatOrNull(target) {
  try {
    return fs.lstatSync(target);
  } catch (error) {
    if (error.code === "ENOENT") return null;
    throw error;
  }
}

function requirePrivateDirectory(target, label) {
  const stat = lstatOrNull(target);
  if (!stat) throw new OnboardingError(`${label} is missing: ${target}`);
  if (stat.isSymbolicLink() || !stat.isDirectory()) {
    throw new OnboardingError(`${label} must be a real directory, not a symlink: ${target}`);
  }
  if (modeOf(stat) !== 0o700) {
    throw new OnboardingError(
      `${label} must have mode 0700 (found ${octal(modeOf(stat))}): ${target}`,
    );
  }
}

function createPrivateDirectory(target, label) {
  if (lstatOrNull(target)) {
    requirePrivateDirectory(target, label);
    return false;
  }
  fs.mkdirSync(target, { recursive: true, mode: 0o700 });
  fs.chmodSync(target, 0o700);
  requirePrivateDirectory(target, label);
  return true;
}

function requirePrivateFile(target, label) {
  const stat = lstatOrNull(target);
  if (!stat) throw new OnboardingError(`${label} is missing: ${target}`);
  if (stat.isSymbolicLink() || !stat.isFile()) {
    throw new OnboardingError(`${label} must be a real regular file, not a symlink: ${target}`);
  }
  if (modeOf(stat) !== 0o600) {
    throw new OnboardingError(
      `${label} must have mode 0600 (found ${octal(modeOf(stat))}): ${target}`,
    );
  }
}

function parseOptions(argv, valueNames, booleanNames) {
  const result = { _: [] };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (booleanNames.has(arg)) {
      result[arg.slice(2).replaceAll("-", "_")] = true;
      continue;
    }
    if (valueNames.has(arg)) {
      if (index + 1 >= argv.length) {
        throw new OnboardingError(`${arg} requires a value`);
      }
      result[arg.slice(2).replaceAll("-", "_")] = argv[index + 1];
      index += 1;
      continue;
    }
    if (arg.startsWith("-")) throw new OnboardingError(`unknown option: ${arg}`);
    result._.push(arg);
  }
  return result;
}

function parseSetupOptions(argv) {
  const options = parseOptions(
    argv,
    new Set(["--vendors", "--proxy-bin", "--config-dir", "--port"]),
    new Set(["--build-proxy", "--no-auth", "--non-interactive", "--help"]),
  );
  if (options._.length) {
    throw new OnboardingError(`unexpected setup argument: ${options._[0]}`);
  }
  if (options.proxy_bin && options.build_proxy) {
    throw new OnboardingError("--proxy-bin and --build-proxy are mutually exclusive");
  }
  if (options.port !== undefined) {
    if (!/^[0-9]+$/.test(options.port)) {
      throw new OnboardingError("--port must be an integer from 1 through 65535");
    }
    options.port = Number(options.port);
    if (options.port < 1 || options.port > 65535) {
      throw new OnboardingError("--port must be an integer from 1 through 65535");
    }
  }
  return options;
}

function parseBuildOptions(argv) {
  const options = parseOptions(
    argv,
    new Set(["--install-dir"]),
    new Set(["--help"]),
  );
  if (options._.length) {
    throw new OnboardingError(`unexpected proxy build argument: ${options._[0]}`);
  }
  return options;
}

function setupUsage() {
  return [
    "usage: parable setup [--vendors claude[,chatgpt][,xai]] [--proxy-bin PATH]",
    "                     [--config-dir DIR] [--port PORT] [--build-proxy]",
    "                     [--no-auth] [--non-interactive]",
    "       parable setup finalize [--json]",
    "",
    "Create a private, loopback-only Parable + CLIProxyAPI configuration.",
    "Claude is the baseline subscription for the Claude Code harness.",
    "ChatGPT adds optional Sol/Terra/Luna models and the Sol fallback parent.",
    "Interactive setup offers the pinned managed build when no proxy is installed.",
    "Ordinary next step: run `parable claude` in the working repository.",
  ].join("\n");
}

function proxyBuildUsage() {
  return [
    "usage: parable proxy build [--install-dir DIR]",
    "",
    "Build the pinned, patched CLIProxyAPI in a new managed directory.",
  ].join("\n");
}

function parseVendors(raw) {
  const parts = String(raw).split(",").map((value) => value.trim()).filter(Boolean);
  if (!parts.length) throw new OnboardingError("--vendors cannot be empty");
  const unique = new Set();
  for (const vendor of parts) {
    if (!Object.hasOwn(VENDORS, vendor)) {
      throw new OnboardingError(
        `unsupported vendor '${vendor}' (supported: ${VENDOR_ORDER.join(", ")})`,
      );
    }
    if (unique.has(vendor)) throw new OnboardingError(`duplicate vendor '${vendor}'`);
    unique.add(vendor);
  }
  if (!unique.has("claude")) {
    throw new OnboardingError("--vendors must include claude for the Claude Code harness");
  }
  return VENDOR_ORDER.filter((vendor) => unique.has(vendor));
}

async function askYesNo(prompt, question) {
  const answer = (await prompt.ask(`${question} [y/N] `)).trim().toLowerCase();
  return answer === "y" || answer === "yes";
}

async function selectVendors(options, existingManifest, prompt) {
  if (options.vendors !== undefined) return parseVendors(options.vendors);
  if (options.non_interactive) {
    throw new OnboardingError("--non-interactive requires --vendors including claude");
  }
  if (existingManifest) return existingManifest.vendors;
  const vendors = ["claude"];
  if (await askYesNo(prompt, "Add ChatGPT subscription models and Sol fallback?")) {
    vendors.push("chatgpt");
  }
  if (await askYesNo(prompt, "Add xAI Grok 4.5 subscription?")) vendors.push("xai");
  return VENDOR_ORDER.filter((vendor) => vendors.includes(vendor));
}

function executableFromCandidate(candidate) {
  if (!candidate) return null;
  const candidates = [];
  if (candidate.includes(path.sep) || path.isAbsolute(candidate)) {
    candidates.push(path.resolve(candidate));
  } else {
    for (const directory of (process.env.PATH || "").split(path.delimiter)) {
      if (directory) candidates.push(path.join(directory, candidate));
    }
  }
  for (const possible of candidates) {
    try {
      fs.accessSync(possible, fs.constants.X_OK);
      const canonical = fs.realpathSync(possible);
      if (fs.statSync(canonical).isFile()) return canonical;
    } catch {
      // Continue to the next PATH candidate.
    }
  }
  return null;
}

function requireExecutable(candidate, source) {
  const executable = executableFromCandidate(candidate);
  if (!executable) throw new OnboardingError(`${source} is not an executable file: ${candidate}`);
  return executable;
}

function discoverProxy(options) {
  if (options.proxy_bin) return requireExecutable(options.proxy_bin, "--proxy-bin");
  if (process.env.PARABLE_CLIPROXY_BIN) {
    return requireExecutable(
      process.env.PARABLE_CLIPROXY_BIN,
      "PARABLE_CLIPROXY_BIN",
    );
  }
  for (const command of ["parable-cliproxy-api", "cli-proxy-api"]) {
    const executable = executableFromCandidate(command);
    if (executable) return executable;
  }
  return null;
}

function setupPaths(configDir) {
  return {
    proxyConfig: path.join(configDir, "cliproxy.yaml"),
    proxyEnv: path.join(configDir, "cliproxy.env"),
    parableConfig: path.join(configDir, "parable.toml"),
    manifest: path.join(configDir, "setup.json"),
  };
}

function stateOf(paths) {
  const present = Object.entries(paths).filter(([, target]) => lstatOrNull(target));
  if (present.length === 0) return "empty";
  if (present.length !== Object.keys(paths).length) {
    const names = present.map(([name]) => name).join(", ");
    throw new OnboardingError(
      `partial setup state found (${names}); move or remove it before rerunning setup`,
    );
  }
  return "complete";
}

function yamlString(value) {
  return JSON.stringify(value);
}

function renderProxyYaml(port, authDir, token) {
  return [
    'host: "127.0.0.1"',
    `port: ${port}`,
    `auth-dir: ${yamlString(authDir)}`,
    "api-keys:",
    `  - "${token}"`,
    "debug: false",
    "",
  ].join("\n");
}

function renderProxyEnv(token) {
  return `export CLIPROXY_API_KEY='${token}'\n`;
}

function executorBlock(id, model, effort, tags, useFor, avoidFor = "Reviewing its own diff.") {
  return [
    `[executors.${id}]`,
    'provider = "claude"',
    `model = ${JSON.stringify(model)}`,
    `effort = ${JSON.stringify(effort)}`,
    `tags = [${tags.map((tag) => JSON.stringify(tag)).join(", ")}]`,
    `use_for = ${JSON.stringify(useFor)}`,
    `avoid_for = ${JSON.stringify(avoidFor)}`,
    "",
  ];
}

function renderParableToml(port, vendors) {
  const selected = new Set(vendors);
  const hasChatGPT = selected.has("chatgpt");
  const reviewer = "opus_exact";
  const defaultExecutor = hasChatGPT ? "terra" : "sonnet_exact";
  const lines = [
    "# Generated by `parable setup`. Contains no provider OAuth credentials.",
    "[parable]",
    "version = 1",
    `default_executor = ${JSON.stringify(defaultExecutor)}`,
    `default_reviewer = ${JSON.stringify(reviewer)}`,
    "",
    "[claude]",
    `base_url = "http://127.0.0.1:${port}"`,
    'auth_token_env = "CLIPROXY_API_KEY"',
    'brain_model = "claude-fable-5"',
    "",
    "[providers.claude]",
    'type = "subagent"',
    "",
    "# Disable Parable's alias-based built-ins; this setup routes exact ids only.",
    "[executors.sonnet]",
    "enabled = false",
    "",
    "[executors.opus]",
    "enabled = false",
    "",
  ];

  if (hasChatGPT) {
    lines.push(
      ...executorBlock(
        "sol_exact", "gpt-5.6-sol", "xhigh", ["implementer", "reviewer", "subscription"],
        "Long implementation, difficult debugging, or high-recall review when Sol is not the parent model.",
        "Delegation from a Sol parent, routine work, or sole ownership of ambiguous product architecture.",
      ),
      ...executorBlock(
        "terra", "gpt-5.6-terra", "xhigh", ["frontend", "implementer", "subscription"],
        "React and frontend implementation, visual UI work, or a tightly scoped code fix.",
        "Wide architecture, long autonomous refactors, or reviewing its own diff.",
      ),
      ...executorBlock(
        "luna", "gpt-5.6-luna", "medium", ["data", "mechanical", "subscription"],
        "Data transforms, scaffolding, test generation, or other bounded mechanical work.",
        "Complex symbolic constraints, architecture, long-context refactors, or final high-risk review.",
      ),
    );
  }

  lines.push(
      ...executorBlock(
        "fable_exact", "claude-fable-5", "high", ["architect", "implementer", "subscription"],
        "Ambiguous planning, architecture, and long autonomous implementation when Fable is not the parent model.",
        "Delegation from a Fable parent or routine work that should preserve the parent model's quota.",
      ),
      ...executorBlock(
        "sonnet_exact", "claude-sonnet-5", "high", ["debugger", "implementer", "subscription"],
        "Brownfield feature implementation, debugging, and test repair.",
        "Mechanical work or reviewing its own diff.",
      ),
      ...executorBlock(
        "opus_exact", "claude-opus-4-8", "xhigh", ["architect", "reviewer", "subscription"],
        "High-blast-radius review, architecture critique, or difficult debugging.",
        "Routine edits, primary frontend implementation, or reviewing its own diff.",
      ),
      ...executorBlock(
        "haiku_exact", "claude-haiku-4-5-20251001", "low", ["explorer", "mechanical", "subscription"],
        "Fast repository exploration, lookup, and simple mechanical edits.",
        "Architecture, difficult debugging, or final high-risk review.",
      ),
  );
  if (selected.has("xai")) {
    lines.push(
      ...executorBlock(
        "grok", "grok-4.5", "high", ["implementer", "systems", "third-family", "subscription"],
        "Bounded terminal-heavy or systems implementation, especially Rust or C++, plus cross-family smoke testing.",
        "Sole final factual review, orchestration, ambiguous product architecture, or reviewing its own diff.",
      ),
    );
  }

  const mechanical = hasChatGPT ? ["luna", "haiku_exact"] : ["haiku_exact"];
  const dataTransform = hasChatGPT
    ? ["luna", "terra", "sonnet_exact"] : ["sonnet_exact"];
  const frontend = hasChatGPT
    ? ["terra", "sol_exact", "sonnet_exact"] : ["sonnet_exact"];
  const feature = hasChatGPT
    ? ["terra", "sol_exact", "sonnet_exact"] : ["sonnet_exact"];
  if (selected.has("xai")) feature.push("grok");
  const refactorWide = hasChatGPT
    ? ["sol_exact", "sonnet_exact", "opus_exact", "fable_exact"]
    : ["sonnet_exact", "opus_exact", "fable_exact"];
  if (selected.has("xai")) refactorWide.push("grok");
  const gnarly = hasChatGPT
    ? ["sol_exact", "opus_exact", "fable_exact"] : ["opus_exact", "fable_exact"];
  if (selected.has("xai")) gnarly.push("grok");
  const review = hasChatGPT
    ? [reviewer, "sol_exact", "terra", "sonnet_exact"]
    : [reviewer, "sonnet_exact"];
  if (selected.has("xai")) review.push("grok");
  const smoke = hasChatGPT ? ["sol_exact", "luna", "sonnet_exact"] : ["sonnet_exact"];
  if (selected.has("xai")) smoke.unshift("grok");
  const architecture = hasChatGPT
    ? ["fable_exact", "opus_exact", "sol_exact"] : ["fable_exact", "opus_exact"];
  lines.push(
    "[routing]",
    `mechanical = ${JSON.stringify(mechanical)}`,
    `data_transform = ${JSON.stringify(dataTransform)}`,
    `frontend = ${JSON.stringify(frontend)}`,
    `feature = ${JSON.stringify(feature)}`,
    `refactor_wide = ${JSON.stringify(refactorWide)}`,
    `gnarly = ${JSON.stringify(gnarly)}`,
    `review = ${JSON.stringify(review)}`,
    `smoke_test = ${JSON.stringify(smoke)}`,
    `architecture = ${JSON.stringify(architecture)}`,
    `escalation = ${JSON.stringify([...new Set([...architecture, ...gnarly, ...feature])])}`,
    "",
  );
  return lines.join("\n");
}

function manifestFor(configDir, authDir, paths, vendors, proxyBinary, port) {
  return {
    schemaVersion: SETUP_SCHEMA_VERSION,
    vendors,
    proxyBinary,
    port,
    configDir,
    authDir,
    generatedPaths: {
      cliproxyYaml: paths.proxyConfig,
      cliproxyEnv: paths.proxyEnv,
      parableToml: paths.parableConfig,
      setupJson: paths.manifest,
    },
  };
}

function renderManifest(manifest) {
  return `${JSON.stringify(manifest, null, 2)}\n`;
}

function readManifest(paths) {
  try {
    return JSON.parse(fs.readFileSync(paths.manifest, "utf8"));
  } catch (error) {
    throw new OnboardingError(`setup.json is invalid: ${error.message}`);
  }
}

function equalJson(left, right) {
  return JSON.stringify(left) === JSON.stringify(right);
}

function readExistingToken(paths) {
  const value = fs.readFileSync(paths.proxyEnv, "utf8");
  const match = /^export CLIPROXY_API_KEY='([0-9a-f]{64})'\n$/.exec(value);
  if (!match) throw new OnboardingError("cliproxy.env is not a valid generated token file");
  return match[1];
}

function validateExistingSetup(configDir, authDir, paths, desired, options = {}) {
  requirePrivateDirectory(configDir, "configuration directory");
  if (options.requireAuthMode !== false) {
    requirePrivateDirectory(authDir, "CLIProxyAPI auth directory");
  }
  for (const [name, target] of Object.entries(paths)) requirePrivateFile(target, name);

  const actualManifest = readManifest(paths);
  if (!equalJson(actualManifest, desired)) {
    throw new OnboardingError(
      "existing setup does not match the requested vendors, proxy binary, port, or paths",
    );
  }
  const token = readExistingToken(paths);
  const expected = new Map([
    [paths.proxyConfig, renderProxyYaml(desired.port, authDir, token)],
    [paths.proxyEnv, renderProxyEnv(token)],
    [paths.parableConfig, renderParableToml(desired.port, desired.vendors)],
    [paths.manifest, renderManifest(desired)],
  ]);
  for (const [target, content] of expected) {
    if (fs.readFileSync(target, "utf8") !== content) {
      throw new OnboardingError(`generated setup file has changed; refusing to overwrite: ${target}`);
    }
  }
  if (options.requireProxy !== false) {
    requireExecutable(desired.proxyBinary, "configured proxy binary");
  }
}

function writePrivateFileSet(entries) {
  const temporary = [];
  const created = [];
  try {
    for (const [target, content] of entries) {
      const temp = path.join(
        path.dirname(target),
        `.${path.basename(target)}.${process.pid}.${crypto.randomBytes(8).toString("hex")}.tmp`,
      );
      const descriptor = fs.openSync(temp, "wx", 0o600);
      try {
        fs.writeFileSync(descriptor, content, "utf8");
        fs.fsyncSync(descriptor);
      } finally {
        fs.closeSync(descriptor);
      }
      fs.chmodSync(temp, 0o600);
      temporary.push(temp);
    }
    entries.forEach(([target], index) => {
      fs.linkSync(temporary[index], target);
      created.push(target);
      fs.unlinkSync(temporary[index]);
    });
  } catch (error) {
    for (const target of created.reverse()) {
      try { fs.unlinkSync(target); } catch { /* best-effort rollback */ }
    }
    throw error;
  } finally {
    for (const temp of temporary) {
      try { fs.unlinkSync(temp); } catch { /* already linked or best-effort cleanup */ }
    }
  }
}

function validateParableConfig(configPath, configDir) {
  const validationHome = path.join(configDir, ".validation-home-does-not-exist");
  const result = spawnSync(
    "python3",
    [PARABLE_PY, "config", "--validate"],
    {
      cwd: configDir,
      env: {
        ...process.env,
        HOME: validationHome,
        PARABLE_CONFIG: configPath,
      },
      encoding: "utf8",
    },
  );
  if (result.error) {
    throw new OnboardingError(`could not validate generated Parable config: ${result.error.message}`);
  }
  if (result.status !== 0) {
    const detail = (result.stderr || result.stdout || "unknown validation error").trim();
    throw new OnboardingError(`generated Parable config failed validation: ${detail}`);
  }
}

function commandPath(command) {
  return executableFromCandidate(command);
}

function runChecked(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: options.cwd,
    env: process.env,
    stdio: options.capture ? ["ignore", "pipe", "pipe"] : "inherit",
    encoding: options.capture ? "utf8" : undefined,
  });
  if (result.error) throw new OnboardingError(`${command} could not start: ${result.error.message}`);
  if (result.status !== 0) {
    throw new OnboardingError(`${command} failed with exit status ${result.status}`);
  }
  return result;
}

function defaultBuildDirectory() {
  const dataHome = process.env.XDG_DATA_HOME
    ? path.resolve(process.env.XDG_DATA_HOME)
    : path.join(os.homedir(), ".local", "share");
  return path.join(dataHome, "parable", "cliproxyapi", PROXY_COMMIT);
}

function ensureBuildParent(destination, isDefault) {
  const parent = path.dirname(destination);
  if (isDefault) {
    const managedRoot = path.join(
      process.env.XDG_DATA_HOME ? path.resolve(process.env.XDG_DATA_HOME) : path.join(os.homedir(), ".local", "share"),
      "parable",
    );
    for (const directory of [managedRoot, path.join(managedRoot, "cliproxyapi")]) {
      createPrivateDirectory(directory, "managed build directory");
    }
  } else {
    fs.mkdirSync(parent, { recursive: true, mode: 0o700 });
  }
}

function buildProxy(options, log) {
  const digest = crypto.createHash("sha256").update(fs.readFileSync(PATCH_PATH)).digest("hex");
  if (digest !== PROXY_PATCH_SHA256) {
    throw new OnboardingError(
      `vendored CLIProxyAPI patch checksum mismatch (expected ${PROXY_PATCH_SHA256})`,
    );
  }
  const git = commandPath("git");
  const go = commandPath("go");
  if (!git) throw new OnboardingError("git is required to build CLIProxyAPI");
  if (!go) throw new OnboardingError("go is required to build CLIProxyAPI");

  const isDefault = !options.install_dir;
  const destination = options.install_dir
    ? path.resolve(options.install_dir) : defaultBuildDirectory();
  if (lstatOrNull(destination)) {
    throw new OnboardingError(`build destination already exists; refusing to mutate it: ${destination}`);
  }
  ensureBuildParent(destination, isDefault);
  let ownedDestination = false;
  try {
    ownedDestination = true;
    runChecked(git, ["clone", "--no-checkout", PROXY_REPOSITORY, destination]);
    fs.chmodSync(destination, 0o700);
    runChecked(git, ["-C", destination, "checkout", "--detach", PROXY_COMMIT]);
    const revision = runChecked(
      git, ["-C", destination, "rev-parse", "HEAD"], { capture: true },
    ).stdout.trim();
    if (revision !== PROXY_COMMIT) {
      throw new OnboardingError(
        `CLIProxyAPI source pin mismatch (expected ${PROXY_COMMIT}, found ${revision || "empty"})`,
      );
    }
    runChecked(git, [
      "-C", destination,
      "-c", "user.name=Parable managed build",
      "-c", "user.email=parable-build@invalid.example",
      "am", PATCH_PATH,
    ]);
    runChecked(go, [
      "test", "-count=1",
      "./internal/thinking",
      "./internal/translator/codex/claude",
    ], { cwd: destination });
    runChecked(go, [
      "test", "-count=1", "./test",
      "-run", "^TestThinkingE2EClaudeAdaptive_Body$",
    ], { cwd: destination });
    const binary = path.join(destination, PROXY_BINARY_NAME);
    runChecked(go, ["build", "-o", binary, "./cmd/server"], { cwd: destination });
    const binaryStat = lstatOrNull(binary);
    if (!binaryStat || binaryStat.isSymbolicLink() || !binaryStat.isFile()) {
      throw new OnboardingError("go build completed without producing the managed proxy binary");
    }
    fs.chmodSync(binary, 0o700);
    log(`built pinned CLIProxyAPI -> ${binary}`);
    return binary;
  } catch (error) {
    if (ownedDestination) fs.rmSync(destination, { recursive: true, force: true });
    throw error;
  }
}

async function runProxyBuild(argv, log) {
  const options = parseBuildOptions(argv);
  if (options.help) {
    log(proxyBuildUsage());
    return;
  }
  buildProxy(options, log);
}

function existingManifestSkeleton(configDir, paths) {
  if (stateOf(paths) !== "complete") return null;
  requirePrivateDirectory(configDir, "configuration directory");
  for (const [name, target] of Object.entries(paths)) requirePrivateFile(target, name);
  const manifest = readManifest(paths);
  if (
    manifest.schemaVersion !== SETUP_SCHEMA_VERSION
    || !Array.isArray(manifest.vendors)
    || typeof manifest.proxyBinary !== "string"
    || !Number.isInteger(manifest.port)
  ) {
    throw new OnboardingError("setup.json does not describe a supported complete setup");
  }
  return manifest;
}

function activeSetupDirectory() {
  if (process.env.PARABLE_CONFIG) {
    return path.dirname(path.resolve(process.env.PARABLE_CONFIG));
  }
  return path.join(os.homedir(), ".config", "parable");
}

function loadSetupContext(options = {}) {
  const configDir = path.resolve(activeSetupDirectory());
  const authDir = path.resolve(path.join(os.homedir(), ".cli-proxy-api"));
  const paths = setupPaths(configDir);
  if (stateOf(paths) !== "complete") {
    throw new OnboardingError("setup is missing; run `parable setup` first");
  }
  const manifest = existingManifestSkeleton(configDir, paths);
  let vendors;
  try {
    vendors = parseVendors(manifest.vendors.join(","));
  } catch {
    throw new OnboardingError("setup.json contains an invalid vendor selection");
  }
  if (manifest.port < 1 || manifest.port > 65535) {
    throw new OnboardingError("setup.json contains an invalid proxy port");
  }
  const desired = manifestFor(
    configDir,
    authDir,
    paths,
    vendors,
    manifest.proxyBinary,
    manifest.port,
  );
  validateExistingSetup(configDir, authDir, paths, desired, options);
  return { configDir, authDir, paths, manifest: desired, vendors };
}

function setupClientEnvironment() {
  const context = loadSetupContext({ requireProxy: false });
  const token = readExistingToken(context.paths);
  return { ...process.env, CLIPROXY_API_KEY: token };
}

function claudeClientEnvironment() {
  const paths = setupPaths(path.resolve(activeSetupDirectory()));
  if (!lstatOrNull(paths.manifest)) return process.env;
  return setupClientEnvironment();
}

function authUsage() {
  return [
    "usage: parable auth login",
    "       parable auth add chatgpt [--device]",
    "       parable auth add claude",
    "       parable auth add xai",
    "       parable auth status [--json]",
  ].join("\n");
}

function proxyStartUsage() {
  return "usage: parable proxy start";
}

function nativeAuthArgs(context, vendor, device) {
  if (!Object.hasOwn(AUTH_FLAGS, vendor)) {
    throw new OnboardingError(
      `unsupported auth vendor '${vendor}' (supported: ${VENDOR_ORDER.join(", ")})`,
    );
  }
  if (!context.vendors.includes(vendor)) {
    throw new OnboardingError(
      `vendor '${vendor}' is not selected in setup; rerun from a clean setup directory to change vendors`,
    );
  }
  if (device && vendor !== "chatgpt") {
    throw new OnboardingError("--device is supported only for chatgpt");
  }
  const mapping = AUTH_FLAGS[vendor];
  const loginFlag = device ? mapping.device : mapping.browser;
  const args = ["--config", context.paths.proxyConfig, loginFlag];
  if (mapping.extra) args.push(mapping.extra);
  return args;
}

function runNativeAuth(context, vendor, device, log) {
  const args = nativeAuthArgs(context, vendor, device);
  if (vendor === "claude") {
    log("Claude OAuth returns to localhost:54545. On a remote host, forward that port first:");
    log("  ssh -L 54545:localhost:54545 <host>");
    log("Keep this same authorization process running until its new callback completes.");
  }
  log(`starting native ${vendor} authorization`);
  const result = spawnSync(context.manifest.proxyBinary, args, {
    stdio: "inherit",
    env: process.env,
  });
  if (result.error) {
    throw new OnboardingError(`could not start native ${vendor} authorization: ${result.error.message}`);
  }
  if (result.status !== 0) {
    const ended = result.signal ? `signal ${result.signal}` : `exit status ${result.status}`;
    throw new OnboardingError(`native ${vendor} authorization failed with ${ended}`);
  }
  const status = scanAuthStatus(context.authDir);
  if (!status.providers[vendor].present) {
    throw new OnboardingError(
      `native ${vendor} authorization exited without creating a private credential record; `
        + `retry \`parable auth add ${vendor}\``,
    );
  }
}

async function runAuthAdd(argv, log) {
  const options = parseOptions(argv, new Set(), new Set(["--device", "--help"]));
  if (options.help) {
    log(authUsage());
    return;
  }
  if (options._.length !== 1) {
    throw new OnboardingError("auth add requires exactly one vendor: chatgpt, claude, or xai");
  }
  const vendor = options._[0];
  if (!Object.hasOwn(AUTH_FLAGS, vendor)) {
    throw new OnboardingError(
      `unsupported auth vendor '${vendor}' (supported: ${VENDOR_ORDER.join(", ")})`,
    );
  }
  if (options.device && vendor !== "chatgpt") {
    throw new OnboardingError("--device is supported only for chatgpt");
  }
  const context = loadSetupContext();
  runNativeAuth(context, vendor, Boolean(options.device), log);
}

function emptyAuthStatus(directoryModeValid) {
  return {
    schemaVersion: 1,
    directoryModeValid,
    scanned: directoryModeValid,
    providers: {
      chatgpt: { present: false, recordCount: 0 },
      claude: { present: false, recordCount: 0 },
      xai: { present: false, recordCount: 0 },
    },
    records: {
      total: 0,
      userOnly: 0,
      invalidMode: 0,
      parseErrors: 0,
      unrecognized: 0,
      allModesValid: directoryModeValid,
    },
  };
}

function authDirectoryModeValid(authDir) {
  const stat = lstatOrNull(authDir);
  return Boolean(
    stat
    && !stat.isSymbolicLink()
    && stat.isDirectory()
    && modeOf(stat) === 0o700
  );
}

function scanAuthStatus(authDir) {
  const directoryModeValid = authDirectoryModeValid(authDir);
  const status = emptyAuthStatus(directoryModeValid);
  if (!directoryModeValid) {
    status.scanned = false;
    return status;
  }
  const providerByType = { codex: "chatgpt", claude: "claude", xai: "xai" };
  for (const entry of fs.readdirSync(authDir, { withFileTypes: true })) {
    if (!entry.name.endsWith(".json")) continue;
    status.records.total += 1;
    const target = path.join(authDir, entry.name);
    const initial = lstatOrNull(target);
    if (
      !initial
      || initial.isSymbolicLink()
      || !initial.isFile()
      || modeOf(initial) !== 0o600
    ) {
      status.records.invalidMode += 1;
      continue;
    }
    let descriptor;
    try {
      const noFollow = fs.constants.O_NOFOLLOW || 0;
      descriptor = fs.openSync(target, fs.constants.O_RDONLY | noFollow);
      const opened = fs.fstatSync(descriptor);
      if (!opened.isFile() || modeOf(opened) !== 0o600) {
        status.records.invalidMode += 1;
        continue;
      }
      const record = JSON.parse(fs.readFileSync(descriptor, "utf8"));
      if (!record || Array.isArray(record) || typeof record !== "object") {
        throw new Error("record is not a JSON object");
      }
      status.records.userOnly += 1;
      const provider = providerByType[record.type];
      if (!provider) {
        status.records.unrecognized += 1;
        continue;
      }
      status.providers[provider].recordCount += 1;
      status.providers[provider].present = true;
    } catch {
      status.records.parseErrors += 1;
    } finally {
      if (descriptor !== undefined) fs.closeSync(descriptor);
    }
  }
  status.records.allModesValid = status.records.invalidMode === 0;
  return status;
}

function renderAuthStatus(status) {
  const lines = VENDOR_ORDER.map((vendor) => {
    const item = status.providers[vendor];
    return `${vendor.padEnd(8)} present=${item.present ? "yes" : "no"} records=${item.recordCount}`;
  });
  lines.push(
    `records  total=${status.records.total} user_only=${status.records.userOnly} `
      + `invalid_mode=${status.records.invalidMode} parse_errors=${status.records.parseErrors} `
      + `unrecognized=${status.records.unrecognized}`,
  );
  lines.push(
    `auth_dir mode_valid=${status.directoryModeValid ? "yes" : "no"} `
      + `scanned=${status.scanned ? "yes" : "no"}`,
  );
  return lines.join("\n");
}

async function runAuthStatus(argv, log) {
  const options = parseOptions(argv, new Set(), new Set(["--json", "--help"]));
  if (options._.length) {
    throw new OnboardingError(`unexpected auth status argument: ${options._[0]}`);
  }
  if (options.help) {
    log(authUsage());
    return;
  }
  const context = loadSetupContext({ requireAuthMode: false, requireProxy: false });
  const status = scanAuthStatus(context.authDir);
  log(options.json ? JSON.stringify(status, null, 2) : renderAuthStatus(status));
}

async function runAuthLogin(argv, log) {
  const options = parseOptions(argv, new Set(), new Set(["--help"]));
  if (options._.length) {
    throw new OnboardingError(`unexpected auth login argument: ${options._[0]}`);
  }
  if (options.help) {
    log(authUsage());
    return;
  }
  const context = loadSetupContext();
  let status = scanAuthStatus(context.authDir);
  for (const vendor of context.vendors) {
    if (status.providers[vendor].present) {
      log(`${vendor}: already authorized`);
      continue;
    }
    runNativeAuth(context, vendor, false, log);
    status = scanAuthStatus(context.authDir);
  }
  log("authorization complete");
  log("");
  log("In a new terminal, open your project and run:");
  log("");
  log("  parable claude --brain auto -- --effort high");
}

function forwardSignals(children, onSignal = () => {}) {
  const targets = Array.isArray(children) ? children : [children];
  const handlers = new Map();
  for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"]) {
    const handler = () => {
      onSignal(signal);
      for (const child of targets) {
        if (child.exitCode === null && child.signalCode === null) child.kill(signal);
      }
    };
    handlers.set(signal, handler);
    process.on(signal, handler);
  }
  return () => {
    for (const [signal, handler] of handlers) process.removeListener(signal, handler);
  };
}

async function runProxyStart(argv, log) {
  const options = parseOptions(argv, new Set(), new Set(["--help"]));
  if (options._.length) {
    throw new OnboardingError(`unexpected proxy start argument: ${options._[0]}`);
  }
  if (options.help) {
    log(proxyStartUsage());
    return 0;
  }
  const context = loadSetupContext();
  const child = spawn(
    context.manifest.proxyBinary,
    ["--config", context.paths.proxyConfig, "--local-model"],
    { stdio: "inherit", env: process.env },
  );
  const stopForwarding = forwardSignals(child);
  return new Promise((resolve, reject) => {
    let settled = false;
    child.once("error", (error) => {
      if (settled) return;
      settled = true;
      stopForwarding();
      reject(new OnboardingError(`could not start CLIProxyAPI: ${error.message}`));
    });
    child.once("exit", (code, signal) => {
      if (settled) return;
      settled = true;
      stopForwarding();
      if (code !== null) resolve(code);
      else resolve(128 + (os.constants.signals[signal] || 0));
    });
  });
}

function signalExitCode(signal) {
  return 128 + (os.constants.signals[signal] || 0);
}

function childExitCode(outcome) {
  return outcome.code === null ? signalExitCode(outcome.signal) : outcome.code;
}

function observeChild(child, label) {
  return new Promise((resolve, reject) => {
    let settled = false;
    child.once("error", (error) => {
      if (settled) return;
      settled = true;
      reject(new OnboardingError(`${label} could not start: ${error.message}`));
    });
    child.once("exit", (code, signal) => {
      if (settled) return;
      settled = true;
      resolve({ code, signal });
    });
  });
}

function observedChild(child, label) {
  const record = { child, outcome: null, error: null, done: null };
  record.done = observeChild(child, label).then(
    (outcome) => {
      record.outcome = outcome;
      return outcome;
    },
    (error) => {
      record.error = error;
      return null;
    },
  );
  return record;
}

function delay(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function readinessTimeoutMs() {
  const raw = process.env.PARABLE_PROXY_READY_TIMEOUT_MS;
  if (raw === undefined) return DEFAULT_PROXY_READY_TIMEOUT_MS;
  if (!/^[0-9]+$/.test(raw)) {
    throw new OnboardingError("PARABLE_PROXY_READY_TIMEOUT_MS must be an integer");
  }
  const value = Number(raw);
  if (value < 50 || value > 120_000) {
    throw new OnboardingError(
      "PARABLE_PROXY_READY_TIMEOUT_MS must be between 50 and 120000",
    );
  }
  return value;
}

function probeModels(port, token) {
  return new Promise((resolve) => {
    let settled = false;
    const finish = (result) => {
      if (settled) return;
      settled = true;
      resolve(result);
    };
    const request = http.request(
      {
        hostname: "127.0.0.1",
        port,
        path: "/v1/models",
        method: "GET",
        agent: false,
        headers: {
          Accept: "application/json",
          Authorization: `Bearer ${token}`,
        },
      },
      (response) => {
        let body = "";
        let oversized = false;
        response.setEncoding("utf8");
        response.on("data", (chunk) => {
          if (body.length + chunk.length > 1_048_576) {
            oversized = true;
            return;
          }
          body += chunk;
        });
        response.on("end", () => {
          if (response.statusCode !== 200) {
            finish({ kind: "occupied", detail: `HTTP ${response.statusCode}` });
            return;
          }
          if (oversized) {
            finish({ kind: "occupied", detail: "oversized /v1/models response" });
            return;
          }
          try {
            const payload = JSON.parse(body);
            if (!payload || !Array.isArray(payload.data)) throw new Error("missing data array");
            finish({ kind: "ready" });
          } catch {
            finish({ kind: "occupied", detail: "invalid /v1/models response" });
          }
        });
      },
    );
    request.setTimeout(PROXY_PROBE_TIMEOUT_MS, () => {
      const error = new Error("probe timeout");
      error.code = "ETIMEDOUT";
      request.destroy(error);
    });
    request.once("error", (error) => {
      if (error.code === "ECONNREFUSED") finish({ kind: "absent" });
      else finish({ kind: "occupied", detail: error.code || "network error" });
    });
    request.end();
  });
}

function spawnManagedProxy(context) {
  const child = spawn(
    context.manifest.proxyBinary,
    ["--config", context.paths.proxyConfig, "--local-model"],
    { stdio: ["ignore", "ignore", "ignore"], env: process.env },
  );
  return observedChild(child, "CLIProxyAPI");
}

function outcomeDescription(outcome) {
  if (!outcome) return "before reporting an exit status";
  if (outcome.code !== null) return `with exit status ${outcome.code}`;
  return `from signal ${outcome.signal}`;
}

async function waitForOwnedProxy(record, port, token, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let lastDetail = "connection refused";
  while (Date.now() < deadline) {
    if (record.error) throw record.error;
    if (record.outcome) {
      const status = childExitCode(record.outcome);
      throw new OnboardingError(
        `CLIProxyAPI exited ${outcomeDescription(record.outcome)} before readiness`,
        status === 0 ? 1 : status,
      );
    }
    const probe = await probeModels(port, token);
    if (record.error) throw record.error;
    if (record.outcome) {
      const status = childExitCode(record.outcome);
      throw new OnboardingError(
        `CLIProxyAPI exited ${outcomeDescription(record.outcome)} before readiness`,
        status === 0 ? 1 : status,
      );
    }
    if (probe.kind === "ready") return;
    lastDetail = probe.detail || "connection refused";
    if (probe.kind === "occupied") {
      throw new OnboardingError(
        `managed CLIProxyAPI did not expose an authenticated model catalog (${lastDetail})`,
      );
    }
    await Promise.race([delay(100), record.done]);
  }
  throw new OnboardingError(
    `timed out after ${timeoutMs}ms waiting for managed CLIProxyAPI readiness (${lastDetail})`,
  );
}

async function stopObservedChild(record, signal = "SIGTERM") {
  if (!record || record.outcome || record.error) return;
  record.child.kill(signal);
  await Promise.race([record.done, delay(PROXY_STOP_TIMEOUT_MS)]);
  if (!record.outcome && !record.error) {
    record.child.kill("SIGKILL");
    await record.done;
  }
}

function spawnClaude(argv, env) {
  const child = spawn(
    "python3",
    [PARABLE_PY, "claude", "--", ...argv],
    { stdio: "inherit", env },
  );
  return observedChild(child, "python3");
}

function spawnFinalize(argv, env) {
  const child = spawn(
    "python3",
    [PARABLE_PY, "finalize", ...argv],
    { stdio: "inherit", env },
  );
  return observedChild(child, "python3");
}

async function runManagedClient(context, token, spawnClient, clientLabel, log) {
  const initialProbe = await probeModels(context.manifest.port, token);
  if (initialProbe.kind === "occupied") {
    throw new OnboardingError(
      `configured proxy endpoint is occupied or unhealthy (${initialProbe.detail}); `
        + "refusing to start or stop an unknown listener",
    );
  }

  let ownedProxy = null;
  try {
    if (initialProbe.kind === "ready") {
      log("proxy: reusing healthy configured endpoint");
    } else {
      log("proxy: starting managed CLIProxyAPI");
      ownedProxy = spawnManagedProxy(context);
      await waitForOwnedProxy(
        ownedProxy,
        context.manifest.port,
        token,
        readinessTimeoutMs(),
      );
    }

    const client = spawnClient();
    let forwardedSignal = null;
    const signalTargets = ownedProxy
      ? [client.child, ownedProxy.child] : [client.child];
    const stopForwarding = forwardSignals(signalTargets, (signal) => {
      forwardedSignal = signal;
    });
    try {
      if (!ownedProxy) {
        const outcome = await client.done;
        if (client.error) throw client.error;
        return forwardedSignal ? signalExitCode(forwardedSignal) : childExitCode(outcome);
      }
      const winner = await Promise.race([
        client.done.then((outcome) => ({ owner: "client", outcome })),
        ownedProxy.done.then((outcome) => ({ owner: "proxy", outcome })),
      ]);
      if (winner.owner === "client") {
        if (client.error) throw client.error;
        return forwardedSignal ? signalExitCode(forwardedSignal) : childExitCode(winner.outcome);
      }
      await stopObservedChild(client);
      if (ownedProxy.error) throw ownedProxy.error;
      if (forwardedSignal) return signalExitCode(forwardedSignal);
      const status = childExitCode(winner.outcome);
      throw new OnboardingError(
        `managed CLIProxyAPI exited ${outcomeDescription(winner.outcome)} while ${clientLabel} was running`,
        status === 0 ? 1 : status,
      );
    } finally {
      stopForwarding();
    }
  } finally {
    await stopObservedChild(ownedProxy);
  }
}

async function runFinalize(argv) {
  const context = loadSetupContext();
  const token = readExistingToken(context.paths);
  const env = { ...process.env, CLIPROXY_API_KEY: token };
  return runManagedClient(
    context, token, () => spawnFinalize(argv, env), "the finalizer", () => {},
  );
}

async function runClaude(argv, log) {
  const setupDirectory = path.resolve(activeSetupDirectory());
  const paths = setupPaths(setupDirectory);
  if (!lstatOrNull(paths.manifest)) {
    const legacy = spawnClaude(argv, claudeClientEnvironment());
    let forwardedSignal = null;
    const stopForwarding = forwardSignals(legacy.child, (signal) => {
      forwardedSignal = signal;
    });
    try {
      const outcome = await legacy.done;
      if (legacy.error) throw legacy.error;
      return forwardedSignal ? signalExitCode(forwardedSignal) : childExitCode(outcome);
    } finally {
      stopForwarding();
    }
  }

  const context = loadSetupContext();
  const token = readExistingToken(context.paths);
  const env = { ...process.env, CLIPROXY_API_KEY: token };
  return runManagedClient(context, token, () => spawnClaude(argv, env), "Claude", log);
}

async function runSetup(argv, log) {
  const options = parseSetupOptions(argv);
  if (options.help) {
    log(setupUsage());
    return;
  }
  const configDir = path.resolve(options.config_dir || path.join(os.homedir(), ".config", "parable"));
  const authDir = path.resolve(path.join(os.homedir(), ".cli-proxy-api"));
  const paths = setupPaths(configDir);
  const initialState = stateOf(paths);
  const existing = initialState === "complete" ? existingManifestSkeleton(configDir, paths) : null;
  let prompt = null;
  if (!options.non_interactive && (!existing || options.vendors === undefined)) {
    prompt = new PromptSession(process.stdin, process.stdout);
  }
  let context;
  try {
    const vendors = await selectVendors(options, existing, prompt);
    const port = options.port ?? existing?.port ?? DEFAULT_PORT;
    let proxyBinary;
    if (existing && !options.proxy_bin && !process.env.PARABLE_CLIPROXY_BIN) {
      proxyBinary = existing.proxyBinary;
    } else if (options.build_proxy) {
      if (existing) proxyBinary = existing.proxyBinary;
      else proxyBinary = buildProxy({}, log);
    } else {
      proxyBinary = discoverProxy(options);
      if (!proxyBinary && !options.non_interactive) {
        const approved = await askYesNo(
          prompt,
          `No CLIProxyAPI binary was found. Build pinned commit ${PROXY_COMMIT.slice(0, 12)} now?`,
        );
        if (approved) proxyBinary = buildProxy({}, log);
      }
      if (!proxyBinary) {
        throw new OnboardingError(
          "CLIProxyAPI was not found; use --proxy-bin, PARABLE_CLIPROXY_BIN, PATH, or --build-proxy",
        );
      }
    }
    proxyBinary = requireExecutable(proxyBinary, "selected proxy binary");
    const desired = manifestFor(configDir, authDir, paths, vendors, proxyBinary, port);

    if (existing) {
      validateExistingSetup(configDir, authDir, paths, desired);
      validateParableConfig(paths.parableConfig, configDir);
      log(`setup is valid and unchanged -> ${configDir}`);
    } else {
      const configCreated = createPrivateDirectory(configDir, "configuration directory");
      const authCreated = createPrivateDirectory(authDir, "CLIProxyAPI auth directory");
      const token = crypto.randomBytes(32).toString("hex");
      const entries = [
        [paths.proxyConfig, renderProxyYaml(port, authDir, token)],
        [paths.proxyEnv, renderProxyEnv(token)],
        [paths.parableConfig, renderParableToml(port, vendors)],
        [paths.manifest, renderManifest(desired)],
      ];
      try {
        writePrivateFileSet(entries);
        validateParableConfig(paths.parableConfig, configDir);
      } catch (error) {
        for (const [target] of entries) {
          try { fs.unlinkSync(target); } catch { /* best-effort rollback */ }
        }
        if (authCreated) {
          try { fs.rmdirSync(authDir); } catch { /* keep non-empty auth state */ }
        }
        if (configCreated) {
          try { fs.rmdirSync(configDir); } catch { /* keep unexpected state for inspection */ }
        }
        throw error;
      }
      log(`created private setup -> ${configDir}`);
      log(`selected vendors: ${vendors.join(", ")}`);
    }
    context = { configDir, authDir, paths, manifest: desired, vendors };
  } finally {
    if (prompt) prompt.close();
  }
  if (!options.no_auth) {
    for (const vendor of context.vendors) runNativeAuth(context, vendor, false, log);
    log("authorization complete; next: parable claude --brain auto -- --effort high");
  } else {
    log("next: authorize each selected subscription, then run parable claude --brain auto -- --effort high");
  }
  return context;
}

module.exports = {
  OnboardingError,
  PROXY_COMMIT,
  PROXY_PATCH_SHA256,
  VENDORS,
  authUsage,
  proxyStartUsage,
  claudeClientEnvironment,
  runAuthAdd,
  runAuthLogin,
  runAuthStatus,
  runClaude,
  runFinalize,
  runProxyBuild,
  runProxyStart,
  runSetup,
  setupClientEnvironment,
  setupUsage,
  proxyBuildUsage,
};
