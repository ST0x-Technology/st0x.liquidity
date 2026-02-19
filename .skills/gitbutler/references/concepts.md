# GitButler CLI Key Concepts

Deep dive into GitButler's conceptual model and philosophy.

## The Workspace Model

### Traditional Git: Serial Branching

```text
main ──┬── feature-a (checkout here, work, commit, checkout back)
       └── feature-b (checkout here, work, commit, checkout back)
```

- Work on ONE branch at a time
- Switch contexts with `git checkout`
- Changes are isolated by branch

### GitButler: Parallel Stacks

```text
workspace (gitbutler/workspace)
  ├─ feature-a (applied, merged into workspace)
  ├─ feature-b (applied, merged into workspace)
  └─ feature-c (unapplied, not in workspace)
```

- Work on MULTIPLE branches simultaneously
- No context switching - all applied branches merged in working directory
- Changes are ASSIGNED to branches, not isolated by checkout

### Key Implications

1. **No `git checkout`**: You don't switch between branches. All applied
   branches exist simultaneously in your workspace.

2. **Multiple staging areas**: Each branch is like having its own `git add`
   staging area. You stage files to specific branches.

3. **The `gitbutler/workspace` branch**: A merge commit containing all applied
   stacks. Don't interact with it directly - use `but` commands.

4. **Applied vs Unapplied**: Control which branches are active:
   - Applied branches: In your working directory
   - Unapplied branches: Exist but not active
   - Use `but apply`/`but unapply` to control

## CLI IDs: Short Identifiers

Every object gets a short, human-readable CLI ID shown in `but status`. IDs are
generated per-session and are unique across all entity types (no two objects
share an ID) — always read them from `but status`.

```text
Commits:    1b, 8f, c2     (short hex prefixes of the SHA, long enough to be unique)
Branches:   fe, bu, ui     (unique 2–3 char substring of the branch name, e.g. "fe" from "feature-x";
                             falls back to auto-generated ID if no unique substring exists)
Files:      g0, h0, i0     (auto-generated, 2–3 chars)
Hunks:      j0, k1, l2     (auto-generated, 2–3 chars)
Stacks:     m0, n0          (auto-generated, 2–3 chars)
```

**Why?** Git commit SHAs are long (40 chars). CLI IDs are short (2-3 chars) and
unique within your current workspace context.

**Usage:** Pass these IDs as arguments to commands:

```bash
but commit <branch-id> -m "message"      # Commit to branch
but stage <file-id> <branch-id>          # Stage file to branch
but rub <commit-id> <commit-id>          # Squash commits
```

## Parallel vs Stacked Branches

### Parallel Branches (Independent Work)

Create with `but branch new <name>`:

```text
main ──┬── api-endpoint (independent)
       └── ui-update    (independent)
```

Use when:

- Tasks don't depend on each other
- Can be merged independently
- No shared code between them

Example: Adding a new API endpoint and updating button styles are independent.

### Stacked Branches (Dependent Work)

Create with `but branch new <name> -a <anchor>`:

```text
main ── authentication ── user-profile ── settings-page
        (base)            (stacked)       (stacked)
```

Use when:

- Feature B needs code from Feature A
- Building incrementally on previous work
- Creating a series of related changes

Example: User profile page needs authentication to be implemented first.

**Dependency tracking:** GitButler automatically tracks which changes depend on
which commits. You can't stage dependent changes to the wrong branch.

## Multiple Staging Areas

Traditional git has ONE staging area:

```bash
git add file1.js    # Stage to THE staging area
git add file2.js    # Stage to THE staging area
git commit          # Commit from THE staging area
```

GitButler has MULTIPLE staging areas (one per branch). Use CLI IDs from
`but status` / `but diff` to target specific files:

```bash
but stage g0 api-branch    # Stage file g0 to api-branch's staging area
but stage h0 ui-branch     # Stage file h0 to ui-branch's staging area
but commit api-branch -m "..."   # Commit from api-branch's staging area
but commit ui-branch -m "..."    # Commit from ui-branch's staging area
```

**Unstaged changes:** Files not staged to any branch yet. Use `but status` to
see them, then `but stage` to assign them.

**Auto-assignment:** If only one branch is applied, changes may auto-assign to
it.

## The `but rub` Philosophy

`but rub` is the core primitive operation: "rub two things together" to perform
an action.

### What Happens Based on Types

The operation performed depends on what you combine:

```text
SOURCE ↓ / TARGET →  │ zz (unassigned) │ Commit     │ Branch      │ Stack
─────────────────────┼─────────────────┼────────────┼─────────────┼────────────
File/Hunk            │ Unstage         │ Amend      │ Stage       │ Stage
Commit               │ Undo            │ Squash     │ Move        │ -
Branch (all changes) │ Unstage all     │ Amend all  │ Reassign    │ Reassign
Stack (all changes)  │ Unstage all     │ -          │ Reassign    │ Reassign
Unassigned (zz)      │ -               │ Amend all  │ Stage all   │ Stage all
File-in-Commit       │ Uncommit        │ Move       │ Uncommit & assign │ -
```

`zz` is a special target meaning "unassigned" (no branch).

**Common examples:**

| Source | Target | Operation              | Example         |
| ------ | ------ | ---------------------- | --------------- |
| File   | Branch | Stage file to branch   | `but rub a1 bu` |
| File   | Commit | Amend file into commit | `but rub a1 c3` |
| Commit | Commit | Squash commits         | `but rub c2 c3` |
| Commit | Branch | Move commit to branch  | `but rub c2 bu` |
| File   | `zz`   | Unstage file           | `but rub a1 zz` |
| Commit | `zz`   | Undo commit            | `but rub c2 zz` |
| `zz`   | Branch | Stage all unassigned   | `but rub zz bu` |

### Higher-Level Conveniences

These commands are wrappers around `but rub`:

- `but stage <file> <branch>` = `but rub <file> <branch>`
- `but amend <file> <commit>` = `but rub <file> <commit>`
- `but squash` = Multiple `but rub <commit> <commit>` operations
- `but move` = `but rub <commit> <target>` with position control

**Why this design?** One powerful primitive is easier to understand and maintain
than many specialized commands. Once you understand `but rub`, you understand
the editing model.

## Dependency Tracking

GitButler tracks dependencies between changes automatically.

### How It Works

```text
Commit C1: Added function foo()
Commit C2: Added function bar()
Uncommitted: Call to foo() in new code
```

The uncommitted change **depends on** C1 (because it calls `foo()`).

**Implications:**

1. Can't stage this change to a branch that doesn't have C1
2. `but absorb` will automatically amend it into C1 (or a commit after C1)
3. If you try to move the change, GitButler prevents invalid operations

### Why This Matters

Prevents you from creating broken states:

- Can't move dependent code away from its dependencies
- Can't stage changes to wrong branches
- Ensures each branch remains independently functional

## Empty Commits as Placeholders

You can create empty commits:

```bash
but commit empty --before c3
but commit empty --after c3
```

**Use cases:**

1. **Mark future work:** Create empty commit as placeholder for changes you'll
   make
2. **Mark targets:** Use with `but mark <empty-commit-id>` so future changes
   auto-amend into it
3. **Organize history:** Add semantic markers in commit history

Example workflow:

```bash
but commit empty -m "TODO: Add error handling" --before c5
but mark <empty-commit-id>
# Now work on error handling, changes auto-amend into the placeholder
```

## Auto-Staging and Auto-Commit (Marks)

Set a "mark" on a branch or commit to automatically organize new changes.

### Mark a Branch

```bash
but mark <branch-id>
```

New unstaged changes automatically stage to this branch. Useful when focused on
one feature.

### Mark a Commit

```bash
but mark <commit-id>
```

New changes automatically amend into this commit. Useful for iterative
refinement.

### Remove Marks

```bash
but mark <id> --delete    # Remove specific mark
but unmark                # Remove all marks
```

**Example workflow:**

```bash
but branch new refactor
but mark <refactor-branch-id>
# Make lots of changes - they all auto-stage to refactor branch
but unmark
```

## Operation History (Oplog)

Every operation in GitButler is recorded in the oplog (operation log).

### What Gets Recorded

- Branch creation/deletion
- Commits
- Stage operations
- Rub/squash/move operations
- Push/pull operations

### Using Oplog

```bash
but oplog                      # View history
but undo                       # Undo last operation
but oplog restore <snapshot-id>  # Restore to specific point
```

Think of it as "git reflog" but for all GitButler operations, not just branch
movements.

**Safety net:** Made a mistake? `but undo` it. Experimented and want to go back?
`but oplog restore` to earlier snapshot.

## Applied vs Unapplied Branches

Branches can be in two states:

### Applied Branches

- Active in your workspace
- Merged into `gitbutler/workspace`
- Changes visible in working directory
- Can make changes, commit, stage files

### Unapplied Branches

- Exist but not active
- Not in working directory
- Can't make changes (must apply first)
- Useful for temporarily setting aside work

### Controlling State

```bash
but apply <id>             # Make branch active
but unapply <id>           # Make branch inactive
```

**Use cases:**

- Unapply branches causing conflicts
- Focus on subset of work (unapply others)
- Temporarily set aside work without deleting

## Conflict Resolution Mode

When `but pull` causes conflicts, affected commits are marked as conflicted.

### Resolution Workflow

1. **Identify:** `but status` shows conflicted commits
2. **Enter mode:** `but resolve <commit-id>`
3. **Fix conflicts:** Edit files, remove conflict markers
4. **Check:** `but resolve status` shows remaining conflicts
5. **Finalize:** `but resolve finish` or `but resolve cancel`

### During Resolution

- You're in a special mode focused on that commit
- Other GitButler operations are limited
- `but status` shows you're in resolution mode
- Must finish or cancel before continuing normal work

## Read-Only Git Commands

Git commands that don't modify state are safe to use:

**Safe (read-only):**

- `git log` - View history
- `git diff` - See changes (but prefer `but diff` — it supports CLI IDs)
- `git show` - View commits
- `git blame` - See line history
- `git reflog` - View reference log

**Don't use in a GitButler workspace:**

- `git status` - Misleading: shows merged workspace state, not individual
  stacks; missing CLI IDs that agents need
- `git commit` - Commits to wrong place (bypasses branch assignment)
- `git checkout` - Breaks workspace model
- `git rebase` - Conflicts with GitButler's management
- `git merge` - Use `but merge` instead

**Rule of thumb:** If it reads, it's fine. If it writes, use `but` instead.

## Known Pitfalls

### Amending a stacked base doesn't update branches above

Each branch in a stack is a separate git ref. When you amend a change into a
base branch commit (e.g., with `but amend` or `but absorb`), only that branch's
tip changes. Branches stacked above still point to the old base on the remote.

**What goes wrong:** You amend a fix into the base branch and push. The stacked
PR above doesn't include the fix — GitHub compares the stacked branch against
`master`, and the stacked branch's history doesn't contain the amended base
commit.

**Fix:** Make changes as new commits on the branch that needs them, or use
`but rub <file-in-commit> <target-commit>` (with `but status -f` to find
file-in-commit IDs) to move changes between commits across the stack. Then push
both branches.

### Hunk locks prevent staging to the wrong place

When an uncommitted change touches lines that were modified in an existing
commit, GitButler "locks" that hunk to the commit's branch/stack. The lock icon
(🔒) appears in `but status`.

**What goes wrong:** `but stage`, `but commit --changes`, or `but absorb` may
silently fail or refuse to operate on locked hunks. The change appears stuck.

**Fix:** Amend the locked hunk into the commit it depends on:
`but amend <file-id> <commit-id> --status-after`. Or use
`but rub <file-id> <commit-id>` to target the specific commit.

### Conflict resolution can leave artifact files

After `but resolve finish`, GitButler may leave `~theirs` or `~ours` suffixed
files in the working directory (e.g., `.claude/skills~theirs`). These get
tracked as new files.

**Fix:** Delete the artifact files manually, then amend the deletion into the
relevant commit. Always run `but status` after `but resolve finish` to check for
stray files.

### Stacked PRs must merge bottom-up

GitHub doesn't enforce stack merge order. If you merge a middle or top PR first,
the next PR in the stack targets a stale branch.

**Fix:** Always merge from the bottom of the stack upward. Enable automatic
branch deletion on GitHub so merged base branches don't leave stale targets. If
you merge out of order: rename the branch, force push, and recreate the affected
PR.

### `but push` is per-branch, not per-stack (CLI)

Unlike the desktop app (which pushes the whole stack at once), `but push` in the
CLI pushes individual branches. After rewriting history (amend, squash, move),
you may need to force push multiple branches.

**Fix:** After history rewrites, run `but push --with-force` (pushes all
branches with unpushed commits) or push each branch explicitly with
`but push <branch-id> --with-force`.

### Prefer `but commit` over `but amend`

`but commit` runs git hooks (formatting, linting, validation). `but amend` and
`but rub` (when amending) skip hooks entirely — no formatting, no lint checks,
no validation.

**Default to new commits.** Even for locked hunks (🔒), prefer
`but commit <branch> -m "message"` over amending into the locked commit. The
history can be cleaned up later with squash before merging.

**When amend is appropriate:** When the user explicitly asks, or when the change
clearly belongs in an existing commit (e.g., fixing a typo you just introduced).
After any amend, run `prek run --all-files` to catch formatting issues the
skipped hooks would have fixed.

### Deleting a branch in a stack can corrupt the stack

Deleting a branch that sits in the middle of a stack (e.g., removing the parent
in a parent→child stack) can leave the stack in a broken state: unnamed
branches, orphaned commits, and commands like `but rub`, `but squash`,
`but unapply` all failing with "stack not found" errors.

**Symptoms:**

- `but status` shows an unnamed branch `[]` with orphaned commits
- `but branch delete`, `but rub`, `but squash` fail with "stack not found"
- `but unapply` fails with "stack not found in workspace"

**Fix — `but oplog` + `but undo`:** GitButler snapshots state before every
operation. Use `but oplog` to find a snapshot from before the corruption, then
`but undo` to roll back (one step) or restore a specific snapshot.

**Fix — `but teardown` + `but setup` (nuclear option):** If oplog can't help:

1. `but teardown` — exits GitButler mode, checks out a regular branch
2. `git checkout <branch-you-want>` — get on the desired branch (Rule 1 does not
   apply here: after teardown, GitButler is inactive and normal git commands are
   required)
3. Confirm the remote is in good shape before proceeding — teardown discards all
   local GitButler state (virtual branch assignments, staging areas)
4. `but setup` — re-initializes GitButler with a clean workspace
5. `but apply <branch>` — re-apply branches from remote

### `but push` on a stacked branch pushes the full stack

When branches are stacked (child sits on parent), `but push <child>` pushes the
child's commit which includes all parent commits in its history. If the parent
branch tracks a different remote ref, this overwrites it with the stacked
version.

**What goes wrong:** You push a child branch and accidentally overwrite the
parent's remote with commits that belong to the child.

**Fix:** Only push the specific branch you intend to update. After a stack
collapse (merging branches together), delete the old remote branches that are no
longer needed: `git push origin --delete <old-branch>`.
