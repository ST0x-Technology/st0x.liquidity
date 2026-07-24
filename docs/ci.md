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

Retargeting a PR emits an `edited` event and reruns CI against the new base.
GitHub emits the same event for title and body edits, which change no code.
Those runs stand the real jobs down through the optimizer `skip` output, and
they use a separate concurrency group so they cannot cancel a live run and
report its required checks as skipped.

Pull requests with at least 3,000 changed files always run full CI because
GitHub's pull request files API truncates responses at that boundary.

## Fork pull requests

GitHub holds fork `pull_request` workflows for maintainer approval. The
repository setting must require approval for **all external contributors**.
After a maintainer selects **Approve workflows to run**, the normal required
jobs run directly on the external pull request.

GitHub removes repository secrets from fork runs and limits `GITHUB_TOKEN` to
read access. Dependabot runs receive the same restrictions despite using an
in-repository branch. The workflow treats either case as an explicit trust
boundary:

- the Graphite optimizer does not run;
- Cachix gets no auth token and cannot push;
- Nix and Rust caches restore but do not save.

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
