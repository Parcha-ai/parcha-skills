# Evidence

An always-loaded instruction snippet for making claims traceable to witnessed
results and keeping sensitive evidence out of public repositories.

The snippet separates expectation from observation, requires capability-specific
checks, makes partial results explicit, and defines a conservative publication
boundary.

| Source | Behavior in the snippet |
| --- | --- |
| [SLSA provenance](https://slsa.dev/spec/v1.2/provenance) and [in-toto attestations](https://in-toto.io/) | Tie a claim to the process and inputs that produced it; test the capability actually claimed. |
| [SLSA on verified reproducibility](https://slsa.dev/spec/v0.1/faq#q-what-about-reproducible-builds) | Use independent checks for consequential claims instead of repeating one path. |

The snippet does not claim SLSA or in-toto conformance. It applies their
evidence principles to agent claims and public-repository hygiene.

## Install

Place the contents of [`AGENTS.md`](AGENTS.md) near the top of the applicable
root instruction file. The same block can be placed in `CLAUDE.md`. Keep the
markers so deployment tooling can update the block without duplicating it.

Nested agent-instruction files may override root behavior. Install the snippet
in each instruction scope where the contract must remain authoritative.
