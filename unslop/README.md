# unslop

Cuts AI tells from text written or edited for a human reader: commit messages, PR titles and
bodies, docs, code comments, replies. It works in Claude Code, Codex, and pi.

[![skills.sh](https://skills.sh/b/Parcha-ai/parcha-skills)](https://skills.sh/Parcha-ai/parcha-skills/unslop)

One job: edit prose so it reads as written by a person. It scans for 31 named patterns (puffery,
AI vocabulary, em dashes, inline-header lists, chatbot phrases, abstract metaphor nouns, passive
voice, and so on), rewrites while preserving meaning, and self-audits. It applies to text the
agent wrote or changed, not to prose it did not touch. It does not judge whether the content is
correct; every other skill in this collection writes its output through `unslop` last.

## Install

skills.sh:

```bash
npx skills add Parcha-ai/parcha-skills --skill unslop
```

Claude Code:

```bash
claude plugin marketplace add Parcha-ai/parcha-skills
claude plugin install unslop@unc-skills
```

Codex:

```bash
codex plugin marketplace add Parcha-ai/parcha-skills
codex plugin add unslop@unc-skills
```

In pi, invoke it with `/skill:unslop`.

## Use

```text
/unslop                    edit the text at hand
/unslop <file or PR>       edit a named artifact
```

In Codex, use `$unslop`.

## Provenance

- Upstream: [cursor/plugins, pstack/skills/unslop](https://github.com/cursor/plugins/tree/main/pstack/skills/unslop),
  author Lauren Tan, MIT. Vendored on 2026-09-15 by way of
  [michaelshimeles/skills](https://github.com/michaelshimeles/skills), whose copy carries the
  same body.
- License: MIT. The upstream `LICENSE` file is included verbatim in this package directory.
- Modifications made here:
  - Added `license: MIT` to the SKILL.md frontmatter.
  - Packaged the payload under `skills/unslop/SKILL.md` with the manifests this repository
    uses (`.claude-plugin`, `.codex-plugin`, `package.json`).
  - No changes to the body. The text matches the michaelshimeles/skills copy, which differs
    from the cursor/plugins original in two frontmatter edits: the
    `disable-model-invocation: true` line is dropped, and the description names the trigger
    (text you write or edit for a human reader) instead of "Cut AI tells from any writing. Must
    always apply.", so the skill auto-invokes on its own scope.
  - Compared against the local copy at `~/.agents/skills/unslop/SKILL.md` on 2026-09-15: the
    only difference was that description line, and the upstream text was kept.
