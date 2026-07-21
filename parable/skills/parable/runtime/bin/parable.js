#!/usr/bin/env node
/* Parable installer, subscription onboarding, doctor, and launcher. Node 18+, no dependencies. */
"use strict";

const fs = require("fs");
const os = require("os");
const path = require("path");
const { execSync, spawnSync } = require("child_process");
const {
  OnboardingError,
  runAuthAdd,
  runAuthStatus,
  runClaude,
  runFinalize,
  runProxyBuild,
  runProxyStart,
  runSetup,
  setupClientEnvironment,
} = require("../lib/onboarding");

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

const PKG_ROOT = runtimePackageRoot();
const SKILL_SRC = path.join(PKG_ROOT, "skills", "parable");
const PARABLE_PY = path.join(SKILL_SRC, "scripts", "parable.py");

function log(msg) { process.stdout.write(msg + "\n"); }
function fail(msg, exitCode = 1) {
  process.stderr.write("error: " + msg + "\n");
  process.exit(exitCode);
}

function parseArgs(argv) {
  const args = { _: [] };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--project") args.project = true;
    else if (a === "--target") args.target = argv[++i];
    else args._.push(a);
  }
  return args;
}

function installTargets(args) {
  if (args.target) {
    return { skills: path.join(args.target, "skills"), configDir: args.target };
  }
  if (args.project) {
    const claudeDir = path.join(process.cwd(), ".claude");
    return { skills: path.join(claudeDir, "skills"), configDir: claudeDir };
  }
  return {
    skills: path.join(os.homedir(), ".claude", "skills"),
    configDir: path.join(os.homedir(), ".config", "parable"),
  };
}

function cmdInstall(args) {
  if (!fs.existsSync(SKILL_SRC)) fail("package is missing skills/parable — reinstall @parcha/parable");
  const { skills, configDir } = installTargets(args);
  const dest = path.join(skills, "parable");
  try {
    fs.mkdirSync(skills, { recursive: true });
    fs.rmSync(dest, { recursive: true, force: true });
    fs.cpSync(SKILL_SRC, dest, { recursive: true });
    for (const f of fs.readdirSync(path.join(dest, "scripts"))) {
      if (f.endsWith(".sh") || f.endsWith(".py")) fs.chmodSync(path.join(dest, "scripts", f), 0o755);
    }
    fs.chmodSync(path.join(dest, "parable.sh"), 0o755);
    log("installed skill -> " + dest);

    const configPath = path.join(configDir, "parable.toml");
    const globalInstall = !args.target && !args.project;
    if (!fs.existsSync(configPath) && globalInstall) {
      log("configuration not seeded — run `npx @parcha/parable setup` for a private subscription setup");
    } else if (!fs.existsSync(configPath)) {
      fs.mkdirSync(configDir, { recursive: true });
      fs.copyFileSync(path.join(SKILL_SRC, "references", "parable.example.toml"), configPath);
      log("created config  -> " + configPath + "  (edit to add providers/executors)");
    } else {
      log("kept config    -> " + configPath);
    }
  } catch (e) {
    fail("install failed at " + dest + ": " + e.message);
  }
  log("done. Run `parable setup`, then `parable claude --brain auto -- --effort high` in your project.");
}

function cmdDoctor() {
  let ok = true;
  const py = spawnSync("python3", ["-c",
    "import sys;\n" +
    "v=sys.version_info\n" +
    "try:\n import tomllib\n print('python %d.%d tomllib ok'%(v[0],v[1]))\nexcept ImportError:\n" +
    " try:\n  import tomli\n  print('python %d.%d tomli ok'%(v[0],v[1]))\n except ImportError:\n  print('python %d.%d NO toml parser'%(v[0],v[1])); sys.exit(1)"
  ], { encoding: "utf8" });
  if (py.status === 0) log("ok   " + py.stdout.trim());
  else { ok = false; log("FAIL python3 3.11+ (or `pip install tomli`) required: " + (py.stdout || py.stderr || "python3 not found").trim()); }

  try {
    const v = execSync("codex --version", { encoding: "utf8" }).trim();
    log("ok   " + v + " (needed only for codex/codex-native executors)");
  } catch {
    log("note codex CLI not found — Claude-subagent executors still work; install codex for gpt-5.5/third-party executors");
  }

  try {
    const v = execSync("pi --version", { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
    log("ok   pi " + v + " (needed only for pi executors)");
  } catch {
    const nodeMajor = parseInt(process.versions.node.split(".")[0], 10);
    if (nodeMajor < 22) {
      log("note pi executors need node >= 22 (current: v" + process.versions.node +
          " — pi crashes on node 20); put a node 22+ bin dir first on PATH, then npm i -g @earendil-works/pi-coding-agent");
    } else {
      log("note pi CLI not found — install with: npm i -g @earendil-works/pi-coding-agent (only needed for pi executors)");
    }
  }

  const candidates = [
    path.join(process.cwd(), ".claude", "skills", "parable", "scripts", "parable.py"),
    path.join(os.homedir(), ".claude", "skills", "parable", "scripts", "parable.py"),
  ];
  const script = candidates.find((p) => fs.existsSync(p));
  if (!script) { log("FAIL skill not installed (run: npx @parcha/parable install)"); process.exit(1); }
  const cfg = spawnSync("python3", [script, "config", "--validate"], { encoding: "utf8" });
  process.stdout.write(cfg.stdout || "");
  if (cfg.status !== 0) { ok = false; process.stderr.write(cfg.stderr || ""); }
  log(ok ? "doctor: all good" : "doctor: problems found (see above)");
  process.exit(ok ? 0 : 1);
}

function runPython(args, env = process.env) {
  const result = spawnSync("python3", [PARABLE_PY, ...args], { stdio: "inherit", env });
  if (result.error) fail("could not start python3: " + result.error.message);
  process.exit(result.status === null ? 1 : result.status);
}

async function main() {
  const raw = process.argv.slice(2);
  if (raw[0] === "claude") {
    process.exitCode = await runClaude(raw.slice(1), log);
    return;
  }
  if (raw[0] === "agents" && raw[1] === "sync") {
    runPython(["agents", "sync", ...raw.slice(2)]);
  }
  if (raw[0] === "setup" && raw[1] === "finalize") {
    process.exitCode = await runFinalize(raw.slice(2));
    return;
  }
  if (raw[0] === "setup") {
    await runSetup(raw.slice(1), log);
    return;
  }
  if (raw[0] === "proxy" && raw[1] === "build") {
    await runProxyBuild(raw.slice(2), log);
    return;
  }
  if (raw[0] === "proxy" && raw[1] === "start") {
    process.exitCode = await runProxyStart(raw.slice(2), log);
    return;
  }
  if (raw[0] === "auth" && raw[1] === "add") {
    await runAuthAdd(raw.slice(2), log);
    return;
  }
  if (raw[0] === "auth" && raw[1] === "status") {
    await runAuthStatus(raw.slice(2), log);
    return;
  }

  const args = parseArgs(raw);
  const cmd = args._[0];
  if (cmd === "install") cmdInstall(args);
  else if (cmd === "doctor") cmdDoctor();
  else {
    log("usage: parable <install|setup|doctor|auth|proxy|claude|agents sync> [options]");
    log("  install            copy the skill to ~/.claude/skills (or ./.claude/skills with --project)");
    log("  setup              configure subscriptions and offer a pinned proxy build");
    log("  setup finalize     diagnostic exact-catalog check and agent synchronization");
    log("  doctor             check python/codex/config health");
    log("  auth add VENDOR    authorize chatgpt, claude, or xai through CLIProxyAPI");
    log("  auth status        show credential-safe provider presence and record counts");
    log("  proxy build        build the pinned, patched CLIProxyAPI source");
    log("  proxy start        diagnostic foreground CLIProxyAPI process");
    log("  claude [--brain auto|fable|sol|config] [-- ARGS...]  supervise the proxy and launch Claude Code");
    log("  agents sync        synchronize project-local parable-* custom agents");
    process.exit(cmd ? 1 : 0);
  }
}

main().catch((error) => {
  if (error instanceof OnboardingError) fail(error.message, error.exitCode);
  fail(error && error.message ? error.message : String(error));
});
