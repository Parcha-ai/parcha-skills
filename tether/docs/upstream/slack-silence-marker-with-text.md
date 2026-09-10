# Upstream: NO_REPLY with surrounding text is posted, and streams before the finalizer can act

Observed on Hermes v0.21.1 in a busy Slack thread (2026-09-10, 85 messages): 18 replies of the form
`NO_REPLY\n\n<narration>` or `<narration>\n\nNO_REPLY` were posted verbatim. `is_intentional_silence_response`
(`gateway/response_filters.py`) is exact-match in interactive chat, and with `streaming.enabled` the text
is already in Slack before `transform_llm_output` can collapse it.

Proposal: treat a reply whose first or last non-empty line is a silence marker as silence in interactive
chat too (the autonomous lanes already do), and make the stream consumer hold back the whole message once
it starts with a marker, not only the bare marker. Until then we run the Slack gateways with streaming off
and a plugin `transform_llm_output` hook that collapses both forms to the exact marker.
