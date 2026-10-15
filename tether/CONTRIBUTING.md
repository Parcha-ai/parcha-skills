# Contributing to Tether

Start with the workflow a user can see. A useful change makes a colleague
easier to configure, helps work reach its original session, improves an
artifact or makes a stalled task understandable. The [roadmap](docs/ROADMAP.md)
keeps the wider direction visible.

## Run the product locally

You need Linux, Python 3.11–3.14 and Node.js 22 or 24. From the repository root:

```bash
node tether/bin/tether.js demo
node tether/bin/tether.js demo --team-config tether/examples/team.toml --json
```

The example requires no Hermes installation or credentials. Inspect
[demo.py](runtime/plugin_next/demo.py) and [test_demo.py](tests/test_demo.py):
they run the existing Store, admission, ActiveSlice and SessionDriver through
fake computers and delivery, with a real artifact oracle.

## Verify a change

From `tether/`:

```bash
python3 scripts/run_tests.py
```

Each test file runs in a separate process with a temporary HOME, explicit
environment and Herdr discovery disabled. This prevents tests from borrowing
your live Slack, session or computer configuration. Select a focused file with
`--pattern test_demo.py` or `--pattern test_team.py`.

The package lifecycle checks also use isolated fixtures:

```bash
npm test
```

Install the pinned Ruff and Bandit versions from
[CI](../.github/workflows/tether-ci.yml) to run `npm run lint`. If either tool is
installed, `npm test` propagates its failure; CI requires both. Release checks
pack the exact file inventory and test installation, rollback and uninstall
against a fake Hermes executable. They do not prove a real Slack round trip.

On shared infrastructure, run heavy verification through that host's resource
limiter. Tests and the demo must never discover live Herdr, start real native
computers or use operator credentials.

## Find the right boundary

| Change | Start here |
| --- | --- |
| Colleague context | [team.py](runtime/plugin_next/team.py), [team.md](runtime/plugin_next/team.md) |
| Conversation admission and execution | [admission.py](runtime/plugin_next/admission.py), [active.py](runtime/plugin_next/active.py) |
| Binding and attempt persistence | [store.py](runtime/plugin_next/store.py) |
| Computer protocol | [session_driver.py](runtime/plugin_next/session_driver.py) |
| CLI and installation | [tether.js](bin/tether.js), [install.sh](install.sh) |

Keep colleague, task, native session, attempt and delivery identities distinct.
Preserve the ability to attach an existing desktop session. Add an independent
behavior check when a change affects routing, identity or artifact correctness;
avoid tests that merely restate an implementation.

New computer adapters should begin with a fake offline contract and then map
the real runtime's attach/submit/observe/cancel/artifact capabilities. The
general versioned adapter kit and Grep adapter are roadmap work. Do not add
a parallel task scheduler where Hermes already owns the task.

When sending a change, describe the user's before/after behavior, the checks
you witnessed and any unverified host/runtime assumptions. Include new runtime
modules in package, installer, managed-target inventory and CI checks together.
