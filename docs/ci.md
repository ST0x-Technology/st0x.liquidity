# Continuous integration

## Pull request checks

The required `backend`, `dashboard`, and `hooks` jobs run on the `pull_request`
event for every target branch, including Graphite PRs whose base is another
feature branch. GitHub associates those runs with the pull request, so their
results satisfy the required status checks when the PR reaches `master`.

CI runs on `push` only for `master`. Running the same required job names on a
feature-branch push and its pull request would create two check sets for one
commit. A skipped or stale duplicate could then hide the result that branch
protection was meant to require.

CI does not subscribe to the `edited` event. GitHub emits it for title and body
edits, which change no code, and each such event would cancel the running CI and
restart the full suite. Filtering `edited` at job level does not help, because a
filtered run still cancels the real run in the shared concurrency group. GitHub
also emits `edited` when a PR's base is retargeted, for example when the parent
of a stacked PR merges. A retarget without a new push keeps the earlier run's
result, and that result is not always valid for the new base: the earlier run
tested B merged into its old base, so if B is stacked on A and is retargeted to
`master` by hand before A merges, B keeps a pass earned on top of A's code, not
on `master`. GitHub also runs no `pull_request` workflow while a PR conflicts
with its base; if a retarget removes the conflict, no run starts and the
required checks stay missing. After a manual retarget, push or close and reopen
the PR to validate it against the new base. The Graphite flow does this
automatically: the restack and `gt submit` push a new head, which emits
`synchronize`. Graphite merge queue draft PRs open against the previous
`gtmq_spec_*` branch and are later retargeted to `master`; their `opened` run
already tests the same tree, because each speculative branch builds on the one
before it.

All subscribed events for a pull request share one concurrency group, so a later
event cancels and replaces an older run. The replacement performs the same
validation. This avoids a race where a head update is cancelled and the
surviving run reports the required jobs as skipped, which GitHub treats as
successful.

The required checks are never gated by the Graphite CI optimizer. In particular,
middle-of-stack PRs run their own backend validation rather than relying on a
different stack branch. Every work job runs on every change, docs-only ones
included: a change classifier in front of the work jobs cost about 15 seconds on
every run, and the gates wait for all work jobs anyway, so skipping work for
docs-only changes saved no wall time.

The backend build, lint, and test matrix, dashboard build and lint, and
pre-commit hooks run in cancellable work jobs. Separate `backend`, `dashboard`,
and `hooks` gate jobs use `always()` to inspect their results and fail on
skipped, failed, or cancelled work. This keeps replacement runs responsive
without allowing an obsolete run to publish a successful required check.

## Fork pull requests

GitHub holds fork `pull_request` workflows for maintainer approval. The
repository setting must require approval for **all external contributors**.
After a maintainer selects **Approve workflows to run**, the normal required
jobs run directly on the external pull request.

GitHub removes repository secrets from fork runs and limits `GITHUB_TOKEN` to
read access. Dependabot runs receive the same restrictions despite using an
in-repository branch. The workflow treats either case as an explicit trust
boundary:

- Cachix gets no auth token and cannot push;
- Nix caches restore but do not save;
- the persistent target disks are not mounted, so the build runs cold (see
  below).

Blacksmith runs each job in an ephemeral microVM. Do not move untrusted fork
jobs to a persistent self-hosted runner.

Every third-party action is pinned to a full commit SHA. Keep the corresponding
version comment when updating a pin so reviewers can see the intended release
without executing a mutable tag.

## Why CI must not use a mirror dispatch

Do not mirror a fork commit into a repository branch and start CI with
`workflow_dispatch`. Moving a commit between repositories preserves its SHA, but
that does not associate a dispatched check suite with the pull request. GitHub
can report `pull_requests: []` for the suite, so green jobs remain detached and
cannot satisfy pull request branch protection.

Use a pull request event for merge-blocking validation. Reserve
`workflow_dispatch` for manual runs whose result does not need to become a pull
request status check.

## Test shards

The eight `backend test` shards do not `need` `backend build`. They start with
the run, install Nix, and realize the `ci-backend` dev shell while the archive
compiles. Then they poll the run's jobs through the Actions API
(`actions: read`) until `backend build` completes, and fail unless it succeeded.
A re-run of only the failed shards restores the archive from the attempt whose
build succeeded. The slowest e2e tests (about 80 s) start first, so a shard
takes little more than its slowest test.

## Persistent target directories

`backend build` and `backend lint` keep their cargo target directories
(`target/test`, `target/lint`) on Blacksmith sticky disks and build with
`CARGO_INCREMENTAL=1`. A large share of the compile is the `st0x-hedge` test
harness, which no dependency cache or sccache can reuse; rustc's incremental
state can.

- Each trusted run mounts the last saved snapshot. Only master pushes save one,
  so pull request runs never replace it. A manual `workflow_dispatch` run mounts
  and saves its own `-dispatch` disk instead, which no pull request or master
  run reads, so a branch can measure a warm build without writing the master
  disk.
- The `commit` input alone is not a trust boundary: a pull request can edit the
  workflow, and the stickydisk action has no branch scoping. So the disk key
  embeds a masked hash of `CACHIX_AUTH_TOKEN`. Fork and Dependabot runs get no
  secrets, so they cannot name the disk; they skip it and build cold.
  Same-repository runs can derive the key, but they can already push to Cachix
  with that token, so the disk adds no new trust. Rotating the token starts a
  fresh, empty disk.
- Cargo decides whether a path crate is fresh from source mtimes, which a fresh
  checkout resets. Each successful build records the git content hashes of the
  tree it compiled (`scripts/sync-source-mtimes.sh record`). After the mount,
  `sync-source-mtimes.sh prepare` gives every tracked file an old fixed mtime
  and the current time to each file whose content hash differs from the recorded
  tree, including files that master changed after this branch forked. Cargo then
  reruns rustc only for crates with a changed source, and rustc's incremental
  cache reuses whatever still matches. With no recorded tree, every file counts
  as changed. Registry and git dependencies fingerprint by version and stay
  built.
- `rain-math-float` is a path dependency linked into the Nix store, where every
  file has mtime 1, so mtimes cannot refresh it.
  `scripts/clean-stale-rain-math-float.sh` runs `cargo clean -p rain-math-float`
  whenever the linked store path differs from the one recorded on the disk.
- `CARGO_HOME` (registry and git sources) lives on the same disk. These jobs do
  not use `Swatinem/rust-cache`: its save step prunes workspace-crate and
  incremental artifacts from the target directory before the disk commits.
- A saving run wipes the disk once it holds more than 40 GiB, because cargo
  never collects artifacts from older toolchains or dependency versions.
- To start from empty disks, bump the `-v1` suffix of both disk keys
  (`-target-test-v1` and `-target-lint-v1`).
- `scripts/test-fork-pr-ci.sh` checks the key derivation, the untrusted-run
  skip, and the master-only commit for every sticky disk.

## Board and parity checks

The `backend lint` job also runs two Python checks in the `ci-backend` shell,
and `nix run .#ci` runs them too:

- `python3 observability/gen-t0-liquidity.py --check` fails when the committed
  liquidity board JSON differs from the generator output, or when a `liq_`
  selector does not carry the `job=~"$source"` matcher (the job the board's
  `source` variable picks), or another generator self-check fails.
- `python3 scripts/liq-parity/test_compare.py` tests the staging parity tool
  against a checked-in snapshot pair.

The `liq_*` golden files under `src/metrics/liquidity/testdata/` are committed.
CI does not regenerate them, because the exporter source is not in this
repository; `scripts/liq-parity/golden.py` regenerates them from a local
exporter checkout.
