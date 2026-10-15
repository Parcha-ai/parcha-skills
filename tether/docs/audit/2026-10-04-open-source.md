# Tether OSS and product experience audit — 2026-10-04

Tether has a compelling product foundation: an existing coding session can become a Slack colleague while retaining its working context, tools, exact ownership and recoverable results. The public product currently presents an older architecture and an installation path that cannot be used as documented. The next product milestone should be **a stranger installing Tether, assigning one useful repository task from Slack, reviewing its evidence and continuing the same session**, with a reproducible offline demo available before connecting Slack.

This is a distribution, onboarding and developer-experience roadmap, not a fleet rollout checklist. No adoption, conversion, demand or retention data were available; product recommendations below are hypotheses to test with users.

## Scope and observed checks

Reviewed worktree `/home/ubuntu/worktrees/tether-product-architecture-20261004`, starting commit `60ef226`. Read package/install/CLI/runtime/skills/docs/CI/release sources. Root owns the broad test baseline; this audit did not run that suite. The offline checks used Node v20.20.0, npm10.8.2 and Python3.11.9. Node20 is outside the declared maintained Node22/24 boundary: the successful help/pack observations are limited mechanical checks, not a supported-runtime acceptance claim. The absent command/flag/source findings are independently grounded in the inspected parser and package files. All CLI and npm commands used an isolated temporary HOME, XDG paths, Hermes home and npm cache. No real-home install, Slack call, provider/model call, Herdr invocation, service change or publication occurred.

At **2026-10-04 00:52 UTC**, these bounded checks produced:

| Check | Observed result |
|---|---|
| `node tether/bin/tether.js --help` and `--version` | Exit 0; version `0.4.0`; top-level help lists `schema status`. |
| `node tether/bin/tether.js spawn --help` | Exit 0; exposes task stdin, harness, cwd, thread and `--no-herdr`. |
| `node tether/bin/tether.js schema --help` | Exit 2: `Unknown Tether command: schema.` |
| `node tether/bin/tether.js config --help` | Exit 2: unknown command. A config inspector is a proposed addition, not an existing promise. |
| Python notifier `setup --help` | Exit 0; only `--non-interactive` and `--no-restart` appear. |
| Python notifier `setup --herdr` | Exit 2 during argument parsing: unrecognized `--herdr`; no setup action ran. |
| `npm pack --dry-run --json --ignore-scripts` in `tether/` | Exit 0; version `0.4.0`, 46 files, 665,275 unpacked bytes. This proves package enumeration, not installation or operational correctness. |
| `npm view @parcha/tether versions --json --registry=https://registry.npmjs.org/` | Exit 1, public registry `E404` for the package. This is a point-in-time anonymous registry observation; private registry publication and GitHub release status were not checked. |

Registry source: [official package endpoint](https://registry.npmjs.org/@parcha%2ftether). No claim is made that any version has never existed.

## Concrete product blockers

### 1. The front-door installation does not match an available public artifact

[README.md:64](../../README.md#install) calls its `@parcha/tether@0.3.0-beta.1` command an immutable published version; the public registry query returned E404 for the package itself. Source fallback exists, but requires discovering a matching reviewed release commit. The package metadata and both harness manifests are `0.4.0`, as is `runtime/plugin_next/plugin.yaml`; README and setup examples still select the beta. `.github/SECURITY.md` says fixes apply to a `0.2.0` pre-release. These are three incompatible release stories.

**Work:** decide and document the current release identity, retain an honest source-install path until an actual public artifact exists, then publish through the existing release workflow. Check the exact copyable quickstart against that artifact. Generate version/support snippets from a release manifest rather than editing each independently.

### 2. The documented first run includes unsupported behavior

README lines 67–74 pass `--herdr`; notifier lines 407–409 do not accept it. README lines 88–94 and 124–128 promise `tether/herdr-plugin` and a cockpit, but that directory is absent from both source and the 46-file package. The runtime has Herdr support; the absent companion package is a separate promise that should be corrected rather than conflated with that support.

Top-level help and README lines 226–238 advertise `tether schema status`; the offline CLI rejects `schema`. `bin/tether.js:35` still names `schema_orchestrator.py`, which neither the package nor installer ships. Release guidance points at nonexistent `runtime/plugin/plugin.yaml` (`docs/RELEASING.md:19`); the installer actually copies `runtime/plugin_next` (`install.sh:593–597`).

**Work:** establish one supported command/capability inventory and generate help, quickstart and package-required assets from it. Remove retired commands and obsolete cockpit instructions from the current guide; retain old-release guides under explicit versioned URLs if needed.

### 3. Requesting setup help can install code first

`bin/tether.js:1543–1559` splits setup flags, invokes `install.sh install` whenever the package payload is present, then delegates to notifier argument parsing. Consequently `tether setup --help` can mutate an installation before displaying help; unsupported setup options can likewise be discovered after the install step. This behavior was established by source inspection; that command was deliberately not executed against the real HOME.

**Work:** parse and validate every setup option, including help, before any installation/configuration action. Give setup a clear stage model: prerequisites → selected harness/session mode → Slack configuration → first useful thread. Preserve completed stages and show the specific next incomplete step, rather than asking users to understand a broker architecture before seeing value.

### 4. Public architecture, recovery and security references describe removed code

`docs/ARCHITECTURE.md:3–7` names beta BindingV3/schema17 and links to removed `bridge_runtime.py`, `runtime/plugin/__init__.py` and `routing.py`. `docs/COMPATIBILITY.md:45–65` directs users to schema17 behavior and `bridges.db`; current `plugin_next` stores endpoint/binding/turn/attempt/saved-answer records and plugin data under the Tether plugin home. The changelog still calls `plugin_next` shadow-only and unwired to the installer (98–106), despite the current install plan copying it.

A lightweight relative-file-link scan across README, top-level docs and setup found **45 missing target occurrences**, including security test references and deleted operational plans. This count checks file existence, not anchors or external URLs. Also, README's `../.github/SECURITY.md` link works in a checkout but that parent `.github` content is absent from the npm payload.

**Work:** rewrite the current architecture and operations guide around the shipped code; distinguish historical design from present behavior. Add a link/command-example check to CI and a package-local security/contact page. Organize docs as Start → Use → Troubleshoot → Extend → Internals so design history is available without becoming the onboarding path.

### 5. Public defaults still assume Parcha, and contributor tools need a clear contract

`package.json:56` uses `(command -v tool && tool ... || echo 'tool not installed')` for Bandit and Ruff. An installed tool reporting a violation is also swallowed and described as absent. CI's separate lint steps can still fail; the problem is the misleading local contributor contract. The separate lint script references absent `herdr-plugin` (59).

`runtime/plugin_next/team.md:5–19` tells every installed agent it is a Parcha colleague and supplies the actual private-team roster/human hierarchy. The public package includes that file, the installer copies it, and `__init__.py:183–218` uses it as a system-prompt section. Those public identifiers are not credentials, but they are incorrect defaults for another team. Separate reusable collaboration behavior from instance-specific identities: proposed neutral `team.yaml` (or equivalent) should define colleagues, roles and authorized peer IDs, default to no invented teammates, and keep Parcha configuration outside the OSS default.

The repository does have useful investment to preserve: an exact package inventory/tarball test, lifecycle tests, immutable marketplace pins, pinned CI actions, Linux Python 3.11–3.14/Node22–24 jobs, an arm64 job, MIT license, support guidance and a private security-reporting route. The audit did not verify their current green status or host repository settings. No Tether CONTRIBUTING guide, approachable runnable example, or `demo` command was found in the inspected inventory.

**Work:** ship neutral instance configuration, make `npm run lint` deterministic and fail honestly; publish one short contributor setup path with one focused test command, a fake broker/native example and a small adapter change. Add bug/feature templates that collect redacted versions and typed errors without demanding private transcripts. Preserve the release controls while making the project comprehensible outside its original machine.

## Distinctive product wedge

**“Your coding session, as a Slack colleague.”** The value is continuity and accountability: brief it, ask a follow-up, have another colleague review its work, receive a useful artifact with evidence, and continue the exact original session. Durable delivery and ownership support that experience; they should be visible through plain statuses and receipts, rather than dominate the homepage as implementation jargon.

A compelling demonstration is a small real repository change: the colleague identifies a failing behavior, fixes it, runs a meaningful check, obtains independent review, corrects a finding and shares the artifact. A later question reaches the same session with the same working context. Show what remains queued, waiting for review, done or uncertain. Keep artifact links and a concise account of verification attached to the result. Do not imply that every acknowledged Slack message or saved answer is globally exactly once.

Target an initial user persona as a hypothesis: developers who already run Claude Code or Codex and want to coordinate with those sessions from Slack. Hermes is the interface/credential owner; Tether preserves exact session routing. Do not force an existing native-session user to create a second unrelated agent to try the product.

## First-run journey to build

1. **Understand in a minute:** README shows a short thread transcript and a small diagram, names prerequisites, links one install command and one simulated offline demo. Label the demo simulated; it does not certify live Slack or model behavior.
2. **Try before credentials:** proposed `tether demo` walks through task → session → review → artifact → follow-up using a fixture repository and fake transport. It teaches statuses and the extension contract without tokens, a provider or a running gateway.
3. **Connect one existing session:** setup checks the supported Hermes/harness versions, helps choose Claude/Codex, uses Hermes's Slack setup, asks for the intended channel/operator scope and the actual local team profile, and explains any changes. Secrets stay in Hermes. Proposed `tether config show --redacted` and `tether config explain KEY` make effective settings understandable.
4. **Complete one useful task:** a guided recipe selects a harmless sample-repository task, opens its owned thread, produces an artifact and proves the follow-up reached the same session. A reviewer recipe demonstrates a genuine peer exchange once a second colleague exists. Distinguish simulation, authenticated connectivity and real colleague work.
5. **Recover and grow:** explain a specific failure in user language with its evidence and next supported action; provide a redacted support bundle. Add another colleague or attach another thread with visible ownership, rather than forcing users to read an internal database model.

All proposed commands above are new work, not current supported commands. Avoid claiming a fixed five-minute live setup: Slack app administration and existing account setup vary.

## Developer extensibility

Publish a small versioned client and backend contract, plus a conformance harness. A client submits a task/message with an identity and idempotency key and reads status/receipt/artifact events. A backend owns native session start/continue/status/cancel and exposes its actual capabilities. Document ownership, unknown outcomes, cancellation limits and immutable artifact/delivery identity; do not make an adapter invent capabilities it cannot prove.

Start with an example backend and a local fake Hermes transport that run offline. Document the public Hermes plugin hooks and prepared-delivery seam (`runtime/plugin_next/__init__.py`), and keep transport/authority decisions in code. Expose structured events so contributors can build an IDE view, task board or review workflow without scraping Slack text. Existing Python/Node entrypoints need a shared operation schema and examples before adding more wrappers.

For portability, Linux server-only is a valid initial boundary, already explicit in README and package OS metadata. A subsequent useful expansion is a thin macOS/Windows client attaching to a Linux-hosted colleague, followed by independently validated native-host support. Avoid advertising cross-platform parity from portable instruction skills alone. Packaging alternatives should follow demonstrated user friction rather than adding npm, pip and container distribution simultaneously.

## Prioritized backlog and visible outcomes

| Priority | Work | Product completion criterion |
|---|---|---|
| **P0: one honest release** | Resolve published/source install, version/support identity, obsolete flags/commands, missing assets, help side effects and Parcha-specific defaults. | A fresh Linux user can copy the documented command, read all help without writes, install the packaged artifact and reach a correct prerequisite/readiness explanation. |
| **P0: current docs** | Replace stale architecture/recovery pages; validate file links and command examples; fix contributor lint scripts. | Every front-door example is exercised against the packaged release; no retired runtime path appears as current authority. |
| **P1: visible colleague journey** | Offline demo, guided first real task, exact-session follow-up, reviewed artifact and plain statuses. | An unfamiliar developer completes the recipe without maintainer-only knowledge; simulated and real evidence are clearly distinguished. Record observed setup obstacles and task outcomes with consent, not invented adoption metrics. |
| **P1: extension kit** | Shared operation schemas, documented backend contract, fake transport, minimal adapter and conformance tests. | A contributor can add a small integration and test it offline without Slack credentials, a model budget or access to the original fleet. |
| **P2: team and remote-client experience** | Discoverable colleague roster, task/review ownership, remote client support, redacted diagnostics and release migration notes. | Users can identify who owns work, add a colleague, request review and continue from another machine; supported platforms and unresolved outcomes stay explicit. |

The strongest near-term investment is to make the existing capability legible and usable end to end. Then test the wedge with unfamiliar users and choose subsequent product work from observed friction. More agent backends, richer task coordination and programmable Jev judgments are promising later extensions; this audit supplies no evidence that they currently improve product adoption or semantic correctness.
