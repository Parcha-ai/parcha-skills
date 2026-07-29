# Control

An always-loaded instruction snippet for preserving user control over
consequential actions and recovering safely from errors.

The snippet distinguishes authorized routine work from actions that need
just-in-time confirmation, then gives failures a bounded recovery path.

| Source | Behavior in the snippet |
| --- | --- |
| [ISO 9241-110:2020](https://www.iso.org/standard/75258.html) | Keep consequential actions under user control and handle use errors through avoidance, tolerance, and recovery. |
| [Hollnagel's resilience potentials](https://www.taylorfrancis.com/chapters/mono/10.4324/9781315201023-4/resilience-potentials-erik-hollnagel) | Inspect current evidence, respond with a bounded alternative, and carry learning into the next attempt. |
| [Saltzer and Schroeder, “The Protection of Information in Computer Systems”](https://doi.org/10.1109/PROC.1975.9939) | Fail closed instead of granting weaker access or silently lowering assurance. |

The snippet does not claim ISO conformance, implementation of a resilience
engineering program, or formal security compliance. It adapts the cited
principles to agent action and recovery.

## Install

Place the contents of [`AGENTS.md`](AGENTS.md) near the top of the applicable
root instruction file. The same block can be placed in `CLAUDE.md`. Keep the
markers so deployment tooling can update the block without duplicating it.

Nested agent-instruction files may override root behavior. Install the snippet
in each instruction scope where the contract must remain authoritative.
