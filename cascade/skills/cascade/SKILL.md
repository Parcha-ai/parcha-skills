---
name: cascade
description: Carry large projects through verifiable tasks, parallel work, and durable progress. Use when asked for Cascade, work in loops, or sustained project execution. Skip small edits.
---

# Cascade

Keep one living plan in `.cascade/<project>.md` or the existing project plan:

- Outcome and final acceptance check.
- Context needed to resume: workspace, constraints, decisions, source links.
- Tasks: ID, deliverable, dependencies, exit check, status, evidence.
- Current owners, blockers, and next actions.

**Decompose.** Split by verifiable outcomes or subsystems; detail only near-term work. Expand large
tasks into children while retaining the parent's acceptance check. Convert chains into dependency
graphs by keeping actual prerequisites and removing ordering that serves no dependency.

**Execute.** Pick tasks whose prerequisites passed. Build, verify, update the plan, repeat while
authorized work remains. Use `todo`, `doing`, `done`, `blocked`. Failed checks remain unfinished;
change approach when attempts stop producing information. Respect session limits and continue
independent work when a task is blocked.

**Parallelize.** Use available subagents for independent ready tasks. Give each worker context,
owned files or worktree, deliverable, and exit check. Serialize overlapping edits. Workers return
changes, evidence, and unresolved issues; the lead owns integration and the plan. Mirror the graph
into native tasks when useful; otherwise the file suffices.

**Verify.** Mark done only when the exit check passes on the actual candidate. Record check, result,
and revision or working-tree identity. Test relevant failure cases; establish a baseline for measured
improvements. Never weaken acceptance to hide failure. Verify the assembled system against the
original outcome after its required tasks pass.

**Resume.** Save progress at meaningful boundaries and before handoff. Compare the plan with actual
files and workers; recheck stale evidence. Continue until the overall acceptance passes or no
authorized work can advance. Leave concrete blockers and next actions when stopping.
