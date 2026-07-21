# Effective Communication

An always-loaded instruction snippet for responses that are relevant,
findable, understandable, and usable. It is designed for root `AGENTS.md` or
`CLAUDE.md` files, where it works even when optional skills are not loaded.

The snippet turns those goals into observable behavior: answer-first writing,
plain language, short critical paths, numbered actions, visible state, concrete
evidence, and one clear next action. It defaults to brevity without suppressing
safety warnings, technical detail, citations, requested explanations, or other
information needed to make a sound decision.

Its design maps the source guidance into a compact response contract:

| Source | Behavior in the snippet |
| --- | --- |
| [ISO 24495-1:2023](https://www.iso.org/standard/78907.html) | Test every response for relevance, findability, understandability, and usability. |
| [W3C COGA Content Usable](https://w3c.github.io/coga/content-usable/) and [Clear Content](https://www.w3.org/WAI/WCAG2/supplemental/objectives/o3-clear-content/) | Use familiar literal language, short blocks, clear headings, visible context, easy recovery, and workflows that do not depend on memory. |
| [US Plain Writing Act](https://uscode.house.gov/view.xhtml?req=granuleid:USC-prelim-title5-section301&num=0&edition=prelim) and [NARA's ten principles](https://www.archives.gov/open/plain-writing/10-principles.html) | Write for the reader, put the main point first, prefer active voice and everyday words, omit needless text, and use headings and lists. |
| [JAN written-instruction guidance](https://askjan.org/solutions/Written-Instructions.cfm) and [organization guidance](https://askjan.org/limitations/Organizing-Planning-Prioritizing.cfm) | Externalize work with written steps, checklists, task separation, expected results, and visible state. |
| [`i-have-adhd`](https://github.com/ayghri/i-have-adhd) | Lead with action, suppress tangents, restate state, show wins, handle errors matter-of-factly, stop after completion, and preserve explicit safety and depth exceptions. |

The snippet does not claim certification or conformance, legal compliance,
WCAG conformance, or medical accommodation. It deliberately avoids diagnosing
or characterizing the reader; it specifies communication behavior that is
broadly useful.

## Install

Place the contents of [`AGENTS.md`](AGENTS.md) near the top of the applicable
root instruction file. The same block can be placed in `CLAUDE.md`. Keep the
markers so deployment tooling can update the block without duplicating it.

Nested agent-instruction files may override root behavior. Install the snippet
in each instruction scope where the contract must remain authoritative.
