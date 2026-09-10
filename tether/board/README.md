# Agent Hub Board

One self-contained HTML page showing who is live, what the fleet looks like, and what landed
this week. Published tailnet-only at
<https://docs.greppy3.parcha.dev/2026-09-09-agent-hub-board.html>.

Every number is collected, not entered. Nothing on the page is typed by hand.

## Pieces

| File | What it does |
| --- | --- |
| `schema.json` | The data contract for `board.json`. The only place the shape is defined. |
| `lanes.json` | Static roster: agent name, Slack id, host, lane. The only file to edit when the team changes. |
| `collect.py` | Fills `board.json` by probing every gateway over ssh. |
| `render.py` | Turns `board.json` into the page using `template.html`. |
| `template.html` | The page: inline CSS, no JS, light and dark. |
| `refresh.sh` | collect → render → publish, atomically. |
| `agent-hub-board.service` / `.timer` | Runs `refresh.sh` every 10 minutes. |

## How collection works

`collect.py` ships one self-contained probe script (`REMOTE_PROBE`, stdlib only) to each host
over ssh and merges the JSON each one prints. The gateway does the reading; the local side only
merges. Sources:

- **Agent metrics** — the live tether store, `~/.hermes/plugin-data/tether/domain.db`, opened
  read-only. `live_threads` counts non-terminal `thread_bindings`; `replies_24h` and
  `latency_median_s` come from `native_attempts` that reached a terminal state *with* a response
  (`no_reply` attempts are turns we deliberately did not answer, so they are not latency samples).
  Pre-0.4 hosts still on `bridges.db` fall back automatically.
- **Machine rows** — `/proc/uptime`, `/proc/loadavg`, the root filesystem, `hermes --version`,
  `tether version`, and `tether doctor --json` check counts.
- **Shipped this week** — `git log origin/main` over the trailing 7 days. This repo squash-merges,
  so a shipped item is a commit whose subject ends in `(#NNN)`, not a merge commit.

Hosts are identified by **tailnet name**, not `hostname`: greppy3 reports `g` and the m box
reports `ns1026182`, so `hostname` cannot be trusted here.

A host that cannot be reached becomes a machine row with `error` set and null metrics. It is
shown on the page as unreachable rather than as zero — the board degrades, it does not lie.

## Running it

```bash
python3 board/collect.py                  # -> board/board.json
python3 board/render.py                   # -> board/agent-hub-board.html
./board/refresh.sh                        # all three steps, publishes to ~/docs
python3 board/collect.py --hosts m         # one host, for a fast check
```

## Installing the timer

```bash
install -m 0644 board/agent-hub-board.{service,timer} ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now agent-hub-board.timer
systemctl --user list-timers agent-hub-board.timer
```

`refresh.sh` renders to a temp file and only `install`s it over the published page after checking
that no `{{TOKEN}}` survived substitution, so a failed collection leaves the last good board up.

## Tests

```bash
python3 -m pytest tests/test_board_collect.py -q
```

Eight tests: schema and roster shape, the real probe run locally against this machine, probe
merging, unreachable-host degradation, the `ns1026182` → `m` mapping, squash-merge PR parsing,
and atomic write. No jsonschema dependency and no reachable fleet required — remote probes are
monkeypatched.

## Adding an agent or a host

Add the entry to `lanes.json`, and add the host to `SSH_HOSTS` in `collect.py` if it is new.
An account whose store is not readable by the login user goes in `ISOLATED_USERS`, which reads
it through `sudo runuser -u <user>`; that map is empty today because every host in this fleet
runs a single `ubuntu` user.

## Publishing rules

`~/docs` is a live tailnet website, not a scratch folder. The page is a single self-contained
HTML file with inline CSS, no external references of any kind, and light/dark support. It
carries no secrets and no customer data: agent names, Slack ids, hostnames, versions and
counters only. Public access returns 403 at Traefik; verify with
`curl --resolve docs.greppy3.parcha.dev:443:40.160.129.218`.
