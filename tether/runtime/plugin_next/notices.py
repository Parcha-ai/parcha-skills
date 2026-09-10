"""Gateway status notices never reach Slack.

Hermes posts its own housekeeping into the chat: "Gateway is shutting down",
"queued for the next turn", "Interrupting current task", "Redirected current
run", "Context compression deferred", "retrying API call". A person's colleague
does not announce those; in an eight-agent thread on 2026-09-10 they were the
majority of 600 messages. Hermes has no outbound hook and no knob for them, so
the Slack adapter's ``send`` is wrapped: a message that *is* one of these
notices (the whole message, structurally, not a guess) is dropped and reported
as sent. Everything else passes untouched.
"""

from __future__ import annotations

import logging
import re
import sys
from typing import Any

logger = logging.getLogger("hermes_plugins.tether_next.notices")

# Anchored on the fixed strings in gateway/run_busy.py, run_inbound.py,
# run_shutdown.py, run_turn.py and agent/turn_api_error.py (Hermes 0.21).
_NOTICE = re.compile(
    r"\A\s*(?:"
    r"(?::hourglass_flowing_sand:|⏳)\s*Gateway (?:is )?\w+.*?(?:not accepting another turn right now|queued for the next turn after it comes back)\.?"
    r"|(?::warning:|⚠️)\s*Gateway (?:shutting down|restarting) — Your current task will be interrupted\..*"
    r"|(?::zap:|⚡)\s*Interrupting current task.*"
    r"|(?::arrow_right_hook:|↪)\s*Redirected current run.*"
    r"|(?::information_source:|ℹ️)\s*(?:Context compression deferred|Configured compression model).*"
    r"|Operation interrupted: retrying API call after error.*"
    r"|\[System: Empty message content sanitised.*"
    r")\s*\Z",
    re.S,
)


def is_gateway_notice(text: str) -> bool:
    return bool(text) and bool(_NOTICE.match(text))


def install(sys_modules: dict[str, Any] | None = None) -> int:
    """Wrap ``send`` on every loaded Slack adapter class. Idempotent; returns how many were wrapped."""
    modules = sys_modules if sys_modules is not None else sys.modules
    wrapped = 0
    for name, module in list(modules.items()):
        if not name.endswith(("slack_platform.adapter", "platforms.slack.adapter")):
            continue
        cls = getattr(module, "SlackAdapter", None)
        send = getattr(cls, "send", None)
        if cls is None or send is None or getattr(send, "_tether_quiet", False):
            continue

        async def quiet_send(self, chat_id, content, reply_to=None, metadata=None, *, _send=send, _module=module):
            if isinstance(content, str) and is_gateway_notice(content):
                logger.info("tether: dropped gateway notice for %s: %.60r", chat_id, content)
                result_cls = _send_result(_module)
                return result_cls(success=True, message_id=None) if result_cls else None
            return await _send(self, chat_id, content, reply_to, metadata)

        quiet_send._tether_quiet = True  # type: ignore[attr-defined]
        cls.send = quiet_send
        wrapped += 1
    return wrapped


def _send_result(module: Any) -> Any:
    cls = getattr(module, "SendResult", None)
    if cls is not None:
        return cls
    try:
        from gateway.platforms.base import SendResult  # type: ignore
        return SendResult
    except Exception:
        return None
