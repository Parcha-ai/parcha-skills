# Effective Communication

An always-loaded instruction snippet for responses that are relevant,
findable, understandable, and usable. It is designed for root `AGENTS.md` or
`CLAUDE.md` files, where it works even when optional skills are not loaded.

The snippet turns those goals into observable behavior: answer-first writing,
plain language, short critical paths, numbered actions, visible state, concrete
evidence, and one clear next action. It defaults to brevity without suppressing
safety warnings, technical detail, citations, requested explanations, or other
information needed to make a sound decision.

Its design is informed by ISO 24495-1:2023 plain-language principles, W3C
Cognitive Accessibility Guidance, the US Plain Writing Act, and JAN guidance
on written and structured instructions. It does not claim certification or
conformance with those sources, and it deliberately avoids medical labels so
the instruction describes communication behavior rather than a reader.

## Install

Place the contents of [`AGENTS.md`](AGENTS.md) near the top of the applicable
root instruction file. The same block can be placed in `CLAUDE.md`. Keep the
markers so deployment tooling can update the block without duplicating it.

Nested agent-instruction files may override root behavior. Install the snippet
in each instruction scope where the contract must remain authoritative.
