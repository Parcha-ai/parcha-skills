# Systems Thinking

An always-loaded instruction snippet for avoiding local changes that make the
larger system worse.

The snippet directs attention to boundaries, interactions, leverage, delayed
effects, flows, inherited structure, and coordination cost while preserving
permission to finish bounded local work.

| Source | Behavior in the snippet |
| --- | --- |
| [Meadows, *Thinking in Systems*](https://www.penguinrandomhouse.com/books/801035/thinking-in-systems-by-donella-meadows/) | Inspect structure and change stocks through their inflows or outflows. |
| [Meadows, “Leverage Points”](https://donellameadows.org/archives/leverage-points-places-to-intervene-in-a-system/) | Prefer practical changes to interfaces, feedback, and goals over repeated parameter tuning. |
| [Ackoff, “‘Whole-ing’ the Parts and Righting the Wrongs”](https://doi.org/10.1002/sres.3850120107) | Improve a component only when its interactions improve the whole. |
| [Senge, *The Fifth Discipline*](https://www.penguinrandomhouse.com/books/163984/the-fifth-discipline-by-peter-m-senge/) | Distinguish symptomatic from fundamental fixes and surface recurrence or delayed side effects. |
| [Gall, *Systemantics*](https://www.generalsystemantics.com/product/systemantics/) | Grow complexity from a small system that already works. |
| [Conway, “How Do Committees Invent?”](https://www.melconway.com/Home/pdf/committees.pdf) | Read system structure as evidence of the communication structure that produced it. |
| [Chesterton, *The Thing*](https://catholiclibrary.org/library/view?chunk.id=00000011&docId=%2FContemporary-EN%2FXCT.165.html) | Find the purpose of inherited structure before removing it. |
| [Brooks, *The Mythical Man-Month*](https://www.pearson.com/en-us/subject-catalog/p/mythical-man-month-the-essays-on-software-engineering-anniversary-edition/P200000000149/9780201835953) | Add parallel actors only when coordination costs do not erase the gain. |

The snippet does not claim conformance with a formal systems-engineering method
or treat the cited heuristics as universal laws. It turns them into compact
checks for routine agent work.

## Install

Place the contents of [`AGENTS.md`](AGENTS.md) near the top of the applicable
root instruction file. The same block can be placed in `CLAUDE.md`. Keep the
markers so deployment tooling can update the block without duplicating it.

Nested agent-instruction files may override root behavior. Install the snippet
in each instruction scope where the contract must remain authoritative.
