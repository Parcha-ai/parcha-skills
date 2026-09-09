"""Small authenticated-HTTP guardrails shared by Recall clients."""

from __future__ import annotations

import ssl
import random
import urllib.request
from typing import Any


class _RejectRedirect(urllib.request.HTTPRedirectHandler):
    """Turn every redirect into an HTTPError before credentials can move hosts."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def open_no_redirect(request: urllib.request.Request, *, timeout: float) -> Any:
    """Open one HTTP(S) request with normal TLS verification and no redirects."""

    opener = urllib.request.build_opener(
        urllib.request.HTTPSHandler(context=ssl.create_default_context()),
        _RejectRedirect(),
    )
    return opener.open(request, timeout=timeout)


def retry_delay_seconds(error, attempt: int, *, base_cap: float, ceiling: float = 120.0) -> float:
    """Delay before retrying a failed brain request.

    Honors the server's ``Retry-After`` header when present (the brain sends
    one with ``503 brain_busy`` while its database pool is saturated) and
    otherwise uses capped exponential backoff. Jitter spreads the retries of
    many collectors so a recovering instance is not hit by all of them at once.
    """
    delay = min(2.0 ** attempt, base_cap)
    headers = getattr(error, "headers", None)
    raw = headers.get("Retry-After") if headers is not None else None
    if raw is not None:
        try:
            delay = max(delay, float(raw))
        except (TypeError, ValueError):
            pass
    delay = min(delay, ceiling)
    return delay * random.uniform(0.75, 1.25)
