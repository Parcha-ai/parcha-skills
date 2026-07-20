# Why every requested effort became `medium`

This diagnosis is version-pinned. It describes stock Claude Code `2.1.215`
talking to CLIProxyAPI `v7.2.88` at commit
`93d74a890a44802f656d7f39a573916b2611896e`.

The fifteen live invocations all carried the requested value in the inbound
Anthropic-shaped `output_config.effort` field. None carried a `thinking`
object. CLIProxyAPI's pinned Claude-to-Codex translator initializes
`reasoning.effort` to `medium`, then reads `output_config.effort` only inside
the `thinking.type == adaptive || auto` branch:

- [pinned translator, lines 312–341](https://github.com/router-for-me/CLIProxyAPI/blob/93d74a890a44802f656d7f39a573916b2611896e/internal/translator/codex/claude/codex_claude_request.go#L312-L341)
- source SHA-256:
  `85559604a0da1bd57f537baf007d46ab223e32392111763f2cf81cda341a8a74`

That makes the observed mapping deterministic:

| Claude Code `--effort` | Inbound `output_config.effort` | Upstream GPT `reasoning.effort` |
|---|---|---|
| `low` | `low` | `medium` |
| `medium` | `medium` | `medium` |
| `high` | `high` | `medium` |
| `xhigh` | `xhigh` | `medium` |
| `max` | `max` | `medium` |

Setting the original alias's
`CLAUDE_CODE_ALWAYS_ENABLE_EFFORT=1` did not change the wire shape in a
separate Sol/low diagnostic: the request still omitted `thinking`, and the
translated request still used `medium`.

This is a translator compatibility gap, not a model-routing failure. The
smallest general repair belongs in CLIProxyAPI: when a Claude request contains
a valid top-level `output_config.effort`, the Claude-to-Codex translator should
use it even when the third-party model id causes Claude Code to omit
`thinking`. That repair needs translator unit tests and a repeat of this exact
15-cell matrix before any non-medium effort is documented as effective.

No raw prompt, response, proxy log, credential, or local private path is
published here. The sanitized receipt contains only routing fields, status
codes, deterministic hashes, runtime pins, and aggregate counts.
