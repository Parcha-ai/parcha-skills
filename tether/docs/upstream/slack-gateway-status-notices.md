# Hermes: gateway status notices have no off switch

Hermes 0.21 posts housekeeping into the chat with no display knob and no outbound
hook to intercept it:

- `gateway/run_busy.py` / `run_inbound.py`: "⏳ Gateway is shutting down and is not
  accepting another turn right now", "queued for the next turn after it comes back",
  "⚡ Interrupting current task", "↪ Redirected current run".
- `gateway/run_shutdown.py`: "⚠️ Gateway shutting down — Your current task will be interrupted."
- `gateway/run_turn.py`: "ℹ️ Context compression deferred — summary still streaming."
- `agent/turn_api_error.py`: "Operation interrupted: retrying API call after error (retry n/m)."

In an eight-agent Slack thread (2026-09-10) a restart round produced dozens of these
per agent, one per queued inbound message, and they outnumbered the work. Tether
wraps `SlackAdapter.send` (`runtime/plugin_next/notices.py`, `quiet_notices = true`)
and drops a message that is exactly one of these notices.

Upstream ask: `display.status_notices: off | on` (default on) honoured by all five
sites, or a `pre_platform_send` hook so a plugin can decide.

## Related ask: token-less `hermes send`

`hermes send` posts straight to the platform with the bot token from the caller's environment
(`hermes_cli/send_cmd.py`). Coding sessions on our gateways never hold the Slack token (privilege
tiers), so from a session shell it answers "Platform 'slack' is not configured" and the Tether
broker post stays. Ask: let `hermes send` route through the running gateway's control socket
(`gateway/control_socket.py` accepts plugin verb handlers) when no token is present, so a
session posts with the gateway's credentials without ever seeing them.
