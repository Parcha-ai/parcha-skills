# Hermes: Claude Code runtime (upstream contribution, staged)

`hermes-claude-code-runtime.patch` adds `model.anthropic_runtime: claude_code` to Hermes: the
whole turn runs in a `claude -p` stream-json subprocess, the Anthropic-side twin of the Codex
app-server runtime. Claude Code owns tools, permissions, hooks, MCP, skills and compaction; Hermes
stays the shell (sessions, gateway platforms, slash commands, memory). One Claude Code session per
Hermes session, resumed across restarts.

Why it matters here: with it, a Slack thread on a gateway *is* a Claude Code session natively, and
the Tether session driver, store, broker post/reply and Slack egress are no longer needed for
Slack-originated work. Tether shrinks to what Hermes cannot do: attach a session started outside
Slack, and the privilege launcher.

Status 2026-09-10: verified live on greppy-bc (Bryan) — the gateway checkout carries the commit
(`git am` on top of 6e2b8e070d), config enables the runtime; a mention in #agent-hub created a
file through Claude Code's `Write` tool and replied in the thread in 6 s.

To open the upstream PR (the machine App cannot fork NousResearch):

```
gh repo fork NousResearch/hermes-agent --org Parcha-ai --clone=false
cd ~/worktrees/hermes-agent && git remote add parcha https://github.com/Parcha-ai/hermes-agent.git
git push -u parcha feat/claude-code-runtime
gh pr create -R NousResearch/hermes-agent --head Parcha-ai:feat/claude-code-runtime \
  --title "feat(runtime): Claude Code runtime (claude -p stream-json), twin of the Codex app-server runtime"
```
