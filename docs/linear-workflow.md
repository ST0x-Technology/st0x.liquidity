# Linear workflow: issue vs project

How we decide where new work lives in Linear. The default is cheap (an issue);
escalate to a project only when the work justifies it.

## Decision rule

Start from conceptual fit, not a time estimate -- estimates are unreliable, and
"does this belong to an existing project's goal?" is the stronger signal.

1. **Fits an existing project?** Put it there. Add it under an existing
   milestone (if the project uses them), or file it directly in the project.
2. **Doesn't fit, but it's small (roughly <= 1-2 weeks)?** Make it an issue.
3. **Doesn't fit, and it's large (roughly > 1-2 weeks)?** Create a project --
   but only when ALL of these hold:
   - it doesn't fit any existing project's scope, AND
   - it's larger than ~1-2 weeks of work, AND
   - it has a distinct owner OR a standalone reason to track it separately (e.g.
     someone else is driving it).

If only some of those hold, prefer an issue (or a milestone inside an existing
project) over spinning up a new project.

## Bundling related issues

When several issues will be completed in quick succession -- possibly under a
single PR, though not necessarily -- group them under one **parent issue**. The
parent issue is the unit that a PR or roadmap entry points at; the children are
the granular steps.

## Graduation

Classification isn't permanent. An issue that grows beyond its original scope
can graduate into a project later. Don't over-classify up front to avoid this --
start small and promote when the work actually outgrows an issue.

## Why these thresholds

The 1-2 week boundary is a heuristic, not a gate. It exists to keep the tracker
flat: most work is an issue, projects are reserved for genuinely separable,
multi-week efforts with their own owner or rationale. When in doubt, file an
issue -- promoting later is cheap; a graveyard of half-used projects is not.
