# Releasing Tether

[RELEASE.md](RELEASE.md) is the single authoritative release procedure,
including repository controls, OIDC trusted publishing, artifact provenance,
partial-failure recovery, immutable tags, and source-pin updates.

Tether uses semantic versions before 1.0:

- patch for a compatible repair or documentation-only release;
- minor for new behavior, protocol, binding, or database schema;
- a pre-release suffix until live Slack and upgrade/rollback validation pass.

Before following `RELEASE.md`, keep the version identical in:

- `tether/package.json`
- `tether/package-lock.json`
- `tether/.claude-plugin/plugin.json`
- `tether/.codex-plugin/plugin.json`
- `tether/runtime/plugin/plugin.yaml`
- the Tether entry in `.claude-plugin/marketplace.json`

Do not configure `NPM_TOKEN`, publish from a workstation, move a release tag, or
rebuild an existing version.
