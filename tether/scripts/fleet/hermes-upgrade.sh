#!/usr/bin/env bash
# Run ON a primary gateway box as the gateway user. Upgrades Hermes to origin/main, applies the
# Colleague knob set, restarts the gateway, verifies. Idempotent.
#   scp scripts/fleet/hermes-upgrade.sh <host>:~/ && ssh <host> bash ~/hermes-upgrade.sh
set -euo pipefail
export PATH="$HOME/.local/bin:/usr/bin:$PATH"
hermes() { "$HOME/.hermes/hermes-agent/venv/bin/python" -m hermes_cli.main "$@"; }
cd "$HOME/.hermes/hermes-agent"
B="$HOME/.hermes/backups/pre-upgrade-$(date -u +%Y%m%d)"; mkdir -p "$B"
git rev-parse HEAD > "$B/hermes-commit.txt"; cp -a "$HOME/.hermes/config.yaml" "$B/config.yaml"
tar -czf "$B/plugins.tgz" -C "$HOME/.hermes" plugins 2>/dev/null || true
# a stale shallow.lock (Aug 7 on every box) blocks every fetch; no git process of ours holds it
rm -f .git/shallow.lock
hermes update --yes 2>&1 | grep -E "Updat|up to date|Restart|✓|✗|⚠" | tail -8
hermes doctor --fix 2>&1 | grep -E "Fixed|issue" | tail -2 || true
python3 - <<'PY'
import re, pathlib
p = pathlib.Path.home() / ".hermes/config.yaml"; s = p.read_text(); o = s
s = re.sub(r"(?m)^(\s+long_running_notifications:)\s*true\s*$", r"\1 false", s)
m = re.search(r"(?ms)^streaming:\n((?:[ \t]+.*\n)+)", s)
if m:
    blk = m.group(0); s = s.replace(blk, re.sub(r"(?m)^(\s+enabled:)\s*false\s*$", r"\1 true", blk, count=1))
else:
    s += "\nstreaming:\n  enabled: true\n  transport: auto\n"
s = re.sub(r"(?m)^(\s+native_task_cards:)\s*true\s*$", r"\1 false", s)
p.write_text(s); print("config knobs:", "changed" if s != o else "unchanged")
PY
T="$HOME/.config/tether/config.toml"
if grep -q '^presence' "$T"; then sed -i 's/^presence.*/presence = false/' "$T"; else echo 'presence = false' >> "$T"; fi
W=/usr/local/libexec/greppy-hermes-gateway-start
sudo -n grep -q '^export SLACK_REACTIONS=' "$W" || sudo -n sed -i 's|^export SLACK_ALLOW_BOTS=all|export SLACK_ALLOW_BOTS=all\nexport SLACK_REACTIONS=false|' "$W"
sudo -n systemctl restart hermes-gateway.service
until systemctl is-active -q hermes-gateway.service && [ -S "$HOME/.hermes/bridge.sock" ]; do sleep 3; done; sleep 25
echo "hermes: $(hermes --version 2>&1 | head -1)"
D=$(tether doctor 2>&1); echo "tether doctor: $(echo "$D" | grep -cE '^ok') ok $(echo "$D" | grep -cE '^FAIL') fail"
