---
name: but
description: "Commit, push, branch, and manage version control with GitButler. Use for: commit my changes, check what changed, create a PR, push my branch, view diff, create branches, stage files, edit commit history, squash commits, amend commits, undo commits, pull requests, merge, stash work. Replaces git - use 'but' instead of git commit, git status, git push, git checkout, git add, git diff, git branch, git rebase, git stash, git merge. Covers all git, version control, and source control operations."
author: GitButler Team
---

# GitButler CLI Skill

Use GitButler CLI (`but`) as the default version-control interface.

## Non-Negotiable Rules

1. Use `but` for all write operations. Never run `git add`, `git commit`,
   `git push`, `git checkout`, `git merge`, `git rebase`, `git stash`, or
   `git cherry-pick`.
2. Start every write/history-edit task with `but status`.
3. For mutation commands, always add `--status-after`.
4. **Always reference branches and commits by semantic name, not short CLI
   IDs.** Short IDs (e.g., `ch`, `at`, `lq`) are ephemeral and unreadable. Use
   the branch name (`chore/gitbutler-cli-nix`) or commit message context
   instead. For example: `but push chore/gitbutler-cli-nix`, not `but push ch`.
   The only exception is `--changes` file IDs, which have no semantic
   alternative. Do not switch branches with `git checkout`.
5. After a successful mutation with `--status-after`, do not run a redundant
   `but status` unless needed for new IDs.
6. If the user says a `git` write command (for example "git push"), translate it
   to the `but` equivalent and execute the `but` command directly.
7. For branch-update tasks, run `but pull --check` before
   `but pull --status-after`. Do not substitute `but fetch` + status summaries
   for this check.
8. Avoid routine `--help` probes before mutations. Use the command patterns in
   this skill (and `references/reference.md`) first; only use `--help` when
   syntax is genuinely unclear or after a failed attempt.
9. **Prefer `but commit` over `but amend`.** `but commit` runs pre-commit hooks
   (formatting, linting, validation); `but amend` skips them. Default to
   creating new commits, even for locked hunks (🔒) — the history can be cleaned
   up later with squash. Use `but amend` only when the user requests it or when
   squashing the change into an existing commit is clearly the right move (e.g.,
   fixing a typo in a commit you just made).

## Core Flow

```bash
but status
# If new branch needed:
but branch new <name>
# Perform task with IDs from status/diff/show
but <mutation> ... --status-after
```

## Canonical Command Patterns

- Commit specific files/hunks:
  `but commit <branch> -m "<message>" --changes <id>,<id> --status-after`
- Create branch while committing:
  `but commit <branch> -c -m "<message>" --changes <id> --status-after`
- Amend into a known commit: `but amend <file-id> <commit-id> --status-after`
- Reorder commits:
  `but move <source-commit-id> <target-commit-id> --status-after`
- Push: `but push` or `but push <branch-name>`
- Pull update safety flow: `but pull --check` then `but pull --status-after`

## Task Recipes

### Commit one file

1. `but status`
2. Find that file's `cliId`
3. `but commit <branch> -c -m "<clear message>" --changes <file-id> --status-after`

### Commit only A, not B

1. `but status`
2. Find `src/a.rs` ID and `src/b.rs` ID
3. Commit with `--changes <a-id>` only

### User says "git push"

Interpret as GitButler push. Run `but push` (or `but push <branch-name>`)
immediately. Do not run `git push`, even if `but push` reports nothing to push.

### Check mergeability, then update branches

1. Run exactly: `but pull --check`
2. If user asked to proceed, run: `but pull --status-after`
3. Do not replace step 1 with `but fetch`, `but status`, or a narrative-only
   summary.

### Amend into existing commit (only when appropriate)

Prefer `but commit` to create a new commit. Only amend when the user requests it
or the change clearly belongs in an existing commit (e.g., fixing a typo you
just introduced). Remember: `but amend` skips pre-commit hooks.

1. `but status`
2. Locate file ID and commit ID from `status` (or `but show <branch-id>`)
3. Run exactly: `but amend <file-id> <commit-id> --status-after`
4. Run `prek run --all-files` afterward to catch formatting issues
5. Never use `git checkout` or `git commit --amend`

### Reorder commits

**Move a commit to a different position:**

1. `but status`
2. Identify the commit to move and the target position by commit message
3. Run: `but move <source-commit> <target-commit> --status-after`
4. Never use `git rebase` for this.

**Swap two commits** (two-step, because the first move invalidates commit IDs):

1. `but status`
2. Run: `but move <commit-a> <commit-b> --status-after`
3. Refresh IDs from the returned `status`, then run the inverse:
   `but move <commit-b-new-id> <commit-a-new-id> --status-after`
4. Never use `git rebase` for this.

## Git-to-But Map

- `git status` -> `but status`
- `git add` + `git commit` -> `but commit ... --changes ... --status-after`
- `git checkout -b` -> `but branch new <name>`
- `git push` -> `but push`
- `git rebase -i` -> `but move`, `but squash`, `but reword`
- `git stash` -> `but unapply` (shelve a branch), `but branch new` + stage
  (snapshot uncommitted work to a new branch)
- `git cherry-pick` -> `but pick`

## Pull Requests

**All PRs must follow `.github/PULL_REQUEST_TEMPLATE.md`.** This applies whether
creating via `but pr`, `gh pr create`, or editing via `gh pr edit`.

The Checks section is a confirmation: "By submitting this for review, I'm
confirming I've done the following." Mark all checks as `[x]` — inapplicable
checks are considered satisfied (e.g., screenshots only apply to dashboard
changes; if no dashboard changes, that check is done).

For chained PRs, include the caution block at the top of the PR body:
`> [!CAUTION]` / `> Chained to #PR`

## Notes

- Prefer explicit IDs over file paths for mutations.
- `--changes` is the safe default for precise commits.
- `--changes` accepts one argument per flag. For multiple IDs, use
  comma-separated values (`--changes a1,b2`) or repeat the flag
  (`--changes a1 --changes b2`), not `--changes a1 b2`.
- Read-only git inspection is allowed (`git log`, `git blame`) when needed.
- Keep skill version checks low-noise:
  - Do not run `but skill check` as a routine preflight on every task.
  - Run `but skill check` when command behavior appears to diverge from this
    skill (for example: unexpected unknown-flag errors, missing subcommands, or
    output shape mismatches), or when the user asks.
  - If update is available, recommend `but skill check --update` (or run it if
    the user asked to update).
- **`but amend` does not run pre-commit hooks.** Prefer `but commit` (which runs
  hooks) by default. When you do use amend, run `prek run --all-files` (prek is
  the project's pre-commit/format runner, installed by git-hooks.nix) and amend
  any formatting fixes before pushing.
- For deeper command syntax and flags, use `references/reference.md`.
- For workspace model and dependency behavior, use `references/concepts.md`.
- For end-to-end workflow patterns, use `references/examples.md`.
