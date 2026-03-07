# Re-stack: rebase dashboard stack onto updated master

## Context

The dashboard feature stack needs to be rebased onto master. PR #459 and #460
have been merged. A `gt restack` was attempted but aborted because it was
touching too many branches and causing problems.

The bottom branch (`feat/dto-inventory-transfer-status`) had a mega-commit
(`7cb8183`) that dragged in 68 files / 2905 insertions of unrelated work —
creating `crates/finance/`, rewriting execution, modifying CI/AGENTS/ROADMAP.
The owner has manually reverted `crates/execution/`, `src/wrapper/`, and
`src/cli/` back to master via `git checkout master -- <dir>`, leaving the branch
with only DTO-related changes + the new `crates/finance/` crate. This revert
introduced compilation errors that must be fixed first.

## CRITICAL: Do NOT import code from other branches

The whole reason we're in this mess is that the mega-commit pulled future work
from higher branches into the DTO branch. **Do NOT `git checkout` files from any
other branch to fix errors.** All fixes must be made by editing the code on the
current branch to make it consistent with what's already there + master.

## Step 0: Fix compilation on `feat/dto-inventory-transfer-status`

The branch currently does not compile. You must fix all errors before any
rebasing begins. Run `cargo check` and work through the errors.

### The type duplication problem

Two crates now define overlapping types:

- **`st0x_execution`** (reverted to master): defines `HasZero`, `Positive`,
  `FractionalShares`, `Symbol`, `ArithmeticError`, `InvalidSharesError`,
  `SharesConversionError`, `Shares`, `Direction`, etc.
- **`st0x_finance`** (new crate from this branch): defines `HasZero`,
  `Positive`, `FractionalShares`, `Symbol`, `ArithmeticError`, `NotPositive`,
  `Usdc`, `Id<Tag>`, etc.

These are **different types** — `st0x_finance::Positive` is not
`st0x_execution::Positive`, even though they look the same. Code that uses
`Positive<Usdc>` won't work if `Positive` comes from execution but `Usdc` comes
from finance, because `Usdc` implements `st0x_finance::HasZero` but not
`st0x_execution::HasZero`.

### Error categories and how to fix them

1. **Missing imports from execution** (`NotPositive`, `SharesBlockchain`): These
   types only exist in `st0x_finance`. Change the import to
   `st0x_finance::NotPositive` etc.

2. **Trait mismatch** (`Usdc: st0x_execution::HasZero` not satisfied): Code uses
   `st0x_execution::Positive<st0x_finance::Usdc>`. Fix by using
   `st0x_finance::Positive<Usdc>` instead — both the wrapper and the inner type
   must come from the same crate.

3. **Private re-exports** (`crate::threshold::Usdc` is private): The `threshold`
   module has `use st0x_finance::Usdc;` (private). Change the import site to
   `use st0x_finance::Usdc;` directly, or make the re-export `pub(crate)`.

4. **Methods not on trait** (`lookup_tokenized_equity` not a member of
   `Wrapper`): The master `Wrapper` trait doesn't have these methods. The impl
   block adds methods that don't exist on the trait. Either add the methods to
   the `Wrapper` trait definition, or move them to a separate impl block
   (inherent methods, not trait methods).

### Strategy

For each error, decide:

- If the type/trait exists in `st0x_finance`, import from there
- If the type/trait exists only in `st0x_execution`, import from there
- If both crates define the same type, pick ONE source and use it consistently
  throughout the file. Prefer `st0x_finance` for types that finance defines
  (`Usdc`, `NotPositive`, `Id`, `Positive`, `HasZero`) and `st0x_execution` for
  execution-specific types (`Shares`, `Direction`, `Executor`,
  `SharesConversionError`)
- **Never mix**: don't use `st0x_execution::Positive<st0x_finance::Usdc>` — use
  `st0x_finance::Positive<st0x_finance::Usdc>`

Run `cargo check` after each batch of fixes. Keep going until it compiles clean.

### After compilation passes

1. Run `cargo nextest run --workspace` to make sure tests pass
2. Squash into one clean commit: `gt squash`
3. Run `cargo check` again after squash

## Step 1: Investigation phase

Before rebasing anything, understand the current state:

1. Run `gt ls` and `gt ll` to see the full stack structure with commits
2. For each branch in the stack, run `git log --oneline <parent>..<branch>` to
   see which commits are unique to that branch
3. Look for commit duplication — identical commit messages with different SHAs
   across branches
4. Run `git log --oneline master` to confirm what master looks like
5. Check each branch's CI status: `gh pr list --state open`

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

## Step 2: Known issue — commit duplication

The `feat/untouchable` and `feat/transfer-tracking` branches share ~25 commits
with identical messages but different SHAs. This happened from previous
restacks. Understand the extent of this before rebasing — a naive rebase will
make it worse.

If duplicate commits exist, you may need to use `git rebase --onto` to only
replay the unique commits from each branch, skipping the duplicated history.

## Step 3: Rebase plan

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

**Expect type duplication errors on every branch.** The same `st0x_finance` vs
`st0x_execution` conflict will appear as each branch adds code that imports from
the wrong crate. Apply the same strategy from Step 0: use `st0x_finance` for
finance types, `st0x_execution` for execution types, never mix them in a
`Positive<T>` or similar generic.

## Sibling branches (don't touch)

- `plan`, `widen-denofmt-line-width`, `feat/backlog` — off master, not part of
  the dashboard stack
- `feat/ethereum-usdc-balance-polling` — @findolor's branch, also off master

## Step 4: Post-rebase verification

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
