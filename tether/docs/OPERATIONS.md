# Tether operations

This runbook describes the `0.4.0` source session runtime. Follow
[Setup](../skills/tether/references/setup.md) for a new instance and
[Compatibility](COMPATIBILITY.md) for the actual host boundary. Historical
schema-17/18 and cockpit commands are not part of this package.

## Inspect the instance

```bash
tether version
tether doctor
tether status
tether unresolved
```

Doctor checks the private broker, operator configuration and Slack egress.
Verify Socket Mode ingress with a real allowed-user thread reply; a successful
Slack authentication probe alone does not establish ingress health. Queued
turns, completed attempts and accepted tasks describe different facts.

Inspect a specific thread through the broker:

```bash
tether thread --channel C01234567 --thread-ts 1234567890.123456
```

Keep secrets and private findings out of Slack messages. CLI commands do not
load a Slack bot token; the gateway owns the credential. Do not bypass the
broker with another local Slack sender when diagnosing a missing result.

## Back up and upgrade

Stop the gateway, then back up
`HERMES_HOME/plugin-data/tether/tether.db` using SQLite backup or preserve its
WAL/SHM files with the database. Keep the source revision and instance
configuration with that backup. The installer snapshots managed code and
plugin state; it is not a database rollback mechanism.

From a reviewed new checkout:

```bash
node tether/bin/tether.js upgrade --harness=both --restart
```

Verify the installed version, doctor result and one actual session reply.
Code changes and model/session configuration are separate; an upgrade does
not authorize replacing a bound session or account.

## Recover an interrupted continuation

Inspect the thread, runtime status and unresolved attempts. Distinguish a
failed execution from a missing platform acknowledgment. The current driver
stores terminal results locally, but the delivery paths do not promise
exactly-once Slack effects or a general saved-answer recovery service.

A stale session should be rebound only from the intended replacement:

```bash
tether rebind --channel C01234567 --thread-ts 1234567890.123456
```

Do not guess another pane or launch a second resume over an active owner. A
cancel/stop message requests interruption; transport support varies, so
inspect actual state before assuming the computer stopped. `tether resolve`
is not an exposed operator-recovery mutation in this source.

## Roll back or uninstall

```bash
tether rollback --restart
tether uninstall
```

Rollback restores the previous managed payload and plugin state. It does not
undo database changes, Slack effects or arbitrary host configuration changes.
Uninstall preserves configuration, runtime state, snapshots and locally
modified managed files. Keep backups until the restored runtime is verified.

## Configure context

Set the colleague manifest using `team_config` or `TETHER_TEAM_CONFIG` and
restart the instance to refresh Hermes's system-prompt section. The full roster
must fit the host's 4,000-character section budget. The manifest does not grant
permissions, authenticate computers or change the model selection.

Runtime storage, task completion and platform delivery need independent
observation. The broad [roadmap](ROADMAP.md) includes useful task status,
durable review ownership and host-delivery consolidation; they should not be
inferred from a generic healthy Boolean.
