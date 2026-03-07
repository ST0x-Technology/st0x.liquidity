# Restack: rebase dashboard stack onto updated master

## Context

PR #459 was merged into master. The dashboard feature stack needs to be rebased
onto the new master. A `gt restack` was attempted but aborted because it was
touching too many branches and causing problems.

## Investigation phase

Before rebasing anything, understand the current state:

1. Run `gt ls` and `gt ll` to see the full stack structure with commits
2. For each branch in the stack, run `git log --oneline <parent>..<branch>` to
   see which commits are unique to that branch
3. Look for commit duplication — identical commit messages with different SHAs
   across branches (this has already happened between `feat/untouchable` and
   `feat/transfer-tracking`)
4. Run `git log --oneline master` to confirm what master looks like after
   merging #459
5. Check each branch's CI status: `gh pr list --state open` to see which PRs
   exist and their check status

### Snapshot changed files per PR

For each PR in the stack, record which files are changed so you can verify after
restack that nothing was lost or introduced:

```bash
# For each branch, save the diff stat against its parent
git diff --stat <parent-branch>...<branch> > .local/pre-restack/<branch-name>.diff-stat
```

Create `.local/pre-restack/` to store these snapshots. After the restack, re-run
the same diffs and compare — the set of changed files per PR should be
identical.

Document what you find before proceeding to the rebase.

## Known issue: commit duplication

The `feat/untouchable` and `feat/transfer-tracking` branches share ~25 commits
with identical messages but different SHAs. This happened from previous
restacks. Understand the extent of this before rebasing — a naive rebase will
make it worse.

If duplicate commits exist, you may need to use `git rebase --onto` to only
replay the unique commits from each branch, skipping the duplicated history.

## Rebase plan

Do this surgically, one branch at a time, bottom-up. **After each branch, run
`cargo check` to verify compilation before moving to the next.** A broken
intermediate state will cascade up the entire stack.

Stack order (bottom to top):

1. `feat/dto-inventory-transfer-status` — rebase onto `master`
2. `feat/inventory-frontend` — rebase onto `feat/dto-inventory-transfer-status`
3. `feat/dashboard-backend` — rebase onto `feat/inventory-frontend`
4. `feat/live-inventory-updates` — rebase onto `feat/dashboard-backend`
5. `feat/transfer-details` — rebase onto `feat/live-inventory-updates`
6. `feat/transfer-stages` — rebase onto `feat/transfer-details`
7. `chore/orchestration` — rebase onto `feat/transfer-stages`
8. `feat/untouchable` — sibling off `chore/orchestration`, rebase onto
   `chore/orchestration`
9. `feat/transfer-tracking` — rebase onto `chore/orchestration` (NOT onto
   `feat/untouchable`)

For each branch:

```bash
git checkout <branch>
git rebase <parent-branch>
# If conflicts: resolve, git add, git rebase --continue
# If empty commit (already applied): git rebase --skip
gt modify --no-edit
cargo check
```

If `cargo check` fails after a rebase, fix the issue on that branch before
moving up. Do NOT proceed with a broken branch.

## Sibling branches (don't touch)

- `plan`, `widen-denofmt-line-width`, `feat/backlog` — off master, not part of
  the dashboard stack
- `feat/ethereum-usdc-balance-polling` — @findolor's branch, also off master

## Post-rebase verification

After ALL branches are rebased and each individually passes `cargo check`:

```bash
gt ls                    # verify stack structure is intact
cargo nextest run --workspace   # full test suite
cargo clippy --workspace --all-targets --all-features
```

### Verify changed files match pre-restack snapshots

For each branch, re-run `git diff --stat <parent>...<branch>` and compare
against the `.local/pre-restack/` snapshots. The set of changed files per PR
should be identical. Flag any differences.

### Push and verify PR state

```bash
gt ss                    # push everything
```

Then verify on GitHub that each PR still points at the correct base branch. If
any PR's base got confused, fix it via
`gh pr edit <number> --base <correct-base-branch>`.
