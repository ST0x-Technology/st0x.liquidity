#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root="$script_dir/.."
actionlint_file="$repo_root/.github/actionlint.yaml"
workflow_file="$repo_root/.github/workflows/ci.yaml"

assert_line() {
  local expected=$1

  if ! grep -Fqx -- "$expected" "$workflow_file"; then
    echo "missing CI invariant: $expected" >&2
    exit 1
  fi
}

assert_count() {
  local expected=$1
  local pattern=$2
  local actual

  actual=$(grep -Fxc -- "$pattern" "$workflow_file" || true)
  if [ "$actual" != "$expected" ]; then
    echo "expected $expected CI lines matching '$pattern', got $actual" >&2
    exit 1
  fi
}

assert_absent() {
  local unexpected=$1

  if grep -Fq -- "$unexpected" "$workflow_file"; then
    echo "unsafe CI configuration remains: $unexpected" >&2
    exit 1
  fi
}

# Native pull_request runs attach the required jobs to both internal and fork
# pull requests, including Graphite PRs targeting a parent feature branch. Push
# CI is limited to master so one commit cannot get a second set of identically
# named required checks.
assert_count 1 '    branches: [master]'
assert_line '  pull_request:'
assert_line '    types: [opened, synchronize, reopened, ready_for_review]'

# Every subscribed pull request event must leave behind a run that executes the
# required checks. An optimizer-controlled job-level skip is unsafe because
# GitHub reports a skipped required job as successful. A replacement event may
# cancel an older run, but the replacement must use the same concurrency group
# and perform validation itself.
assert_absent 'withgraphite/graphite-ci-action@'
assert_absent 'GRAPHITE_CI_OPTIMIZATION_TOKEN'

# Parse the workflow once for structural, gate, and immutable-action checks.
parsed_action_count=$(
  ruby - "$workflow_file" <<'RUBY'
require "yaml"
require "open3"

workflow = YAML.safe_load(File.read(ARGV.fetch(0)), aliases: true)
jobs = workflow.fetch("jobs")
concurrency = workflow.fetch("concurrency")

# Psych reads the bare `on:` key as boolean true. Title and body edits emit
# `edited` and change no code, so subscribing to it would restart the full
# suite on every description edit. `pull_request_target` would run untrusted
# code with repository secrets.
triggers = workflow["on"] || workflow[true]
pull_request_types = triggers.fetch("pull_request").fetch("types")
unless pull_request_types == %w[opened synchronize reopened ready_for_review]
  warn "pull_request types must be opened, synchronize, reopened, ready_for_review, got #{pull_request_types.inspect}"
  exit 1
end

if triggers.key?("pull_request_target")
  warn "CI must not run on pull_request_target"
  exit 1
end

expected_group = "${{ github.repository }}-${{ github.workflow }}-${{ github.ref }}-${{ github.ref == 'refs/heads/master' && github.sha || ''}}"
unless concurrency["group"] == expected_group && concurrency["cancel-in-progress"] == true
  warn "pull request replacement runs must share a cancel-in-progress concurrency group"
  exit 1
end

if jobs.key?("optimize_ci")
  warn "the Graphite optimizer must not control required CI checks"
  exit 1
end

expected_wiring = {
  "backend-build" => { "needs" => nil, "if" => nil },
  "backend-lint" => { "needs" => nil, "if" => nil },
  "backend-test" => { "needs" => "backend-build", "if" => nil },
  "backend" => {
    "needs" => %w[backend-build backend-lint backend-test],
    "if" => "${{ always() }}"
  },
  "dashboard-build" => { "needs" => nil, "if" => nil },
  "dashboard-lint" => { "needs" => nil, "if" => nil },
  "dashboard" => {
    "needs" => %w[dashboard-build dashboard-lint],
    "if" => "${{ always() }}"
  },
  "hooks-run" => { "needs" => nil, "if" => nil },
  "hooks" => {
    "needs" => "hooks-run",
    "if" => "${{ always() }}"
  }
}

expected_wiring.each do |job_name, expected|
  job = jobs.fetch(job_name)
  expected.each do |key, value|
    next if job[key] == value

    warn "#{job_name}.#{key} must be #{value.inspect}, got #{job[key].inspect}"
    exit 1
  end
end

%w[backend dashboard hooks].each do |job_name|
  effective_name = jobs.fetch(job_name).fetch("name", job_name)
  next if effective_name == job_name

  warn "required job #{job_name} must publish the #{job_name.inspect} check, got #{effective_name.inspect}"
  exit 1
end

# No job may skip validation on a change classification: GitHub reports a
# skipped required job as successful, and every work job feeds a gate.
if jobs.key?("changes")
  warn "a change classifier must not gate the work jobs"
  exit 1
end

workflow_permissions = workflow.fetch("permissions")
unless workflow_permissions == { "contents" => "read" }
  warn "CI must hold only read access to repository contents"
  exit 1
end

permission_sets = [workflow_permissions] + jobs.values.map { |job| job["permissions"] }.compact
permission_sets.each do |permissions|
  grants_write = permissions == "write-all" ||
    (permissions.is_a?(Hash) && permissions.values.any? { |value| value.to_s == "write" })
  next unless grants_write

  warn "CI permissions must remain read-only, got #{permissions.inspect}"
  exit 1
end

check_gate = lambda do |job_name, step_name, expressions, cases|
  gate_job = jobs.fetch(job_name)
  gate_step = gate_job.fetch("steps").find do |step|
    step["name"] == step_name
  end
  unless gate_job.fetch("continue-on-error", false) == false &&
      gate_step&.fetch("if", nil).nil? &&
      gate_step&.fetch("continue-on-error", false) == false
    warn "#{job_name} gate enforcement must run and propagate failures"
    exit 1
  end

  gate_script = gate_step.fetch("run")
  expressions.each do |expression|
    next if gate_script.include?("${{ #{expression} }}")

    warn "#{job_name} gate no longer references #{expression}"
    exit 1
  end

  cases.each do |values, expected_success|
    rendered = expressions.zip(values).reduce(gate_script) do |script, (expression, value)|
      script.gsub("${{ #{expression} }}", value)
    end
    if rendered.include?("$" + "{{")
      warn "#{job_name} gate has unrendered expressions"
      exit 1
    end

    _stdout, _stderr, status = Open3.capture3("bash", "-c", rendered)
    next if status.success? == expected_success

    warn "#{job_name} gate returned #{status.exitstatus} for #{values.inspect}"
    exit 1
  end
end

check_gate.call(
  "backend",
  "Check backend validation",
  %w[
    needs.backend-build.result
    needs.backend-lint.result
    needs.backend-test.result
  ],
  [
    [%w[success success success], true],
    [%w[failure success success], false],
    [%w[skipped success success], false],
    [%w[cancelled success success], false],
    [%w[success failure success], false],
    [%w[success skipped success], false],
    [%w[success cancelled success], false],
    [%w[success success failure], false],
    [%w[success success skipped], false],
    [%w[success success cancelled], false],
    [%w[skipped skipped skipped], false]
  ]
)

check_gate.call(
  "dashboard",
  "Check dashboard validation",
  %w[
    needs.dashboard-build.result
    needs.dashboard-lint.result
  ],
  [
    [%w[success success], true],
    [%w[failure success], false],
    [%w[skipped success], false],
    [%w[cancelled success], false],
    [%w[success failure], false],
    [%w[success skipped], false],
    [%w[success cancelled], false],
    [%w[skipped skipped], false]
  ]
)

check_gate.call(
  "hooks",
  "Check hooks validation",
  %w[needs.hooks-run.result],
  [
    [%w[success], true],
    [%w[failure], false],
    [%w[skipped], false],
    [%w[cancelled], false]
  ]
)

# A sticky disk snapshot is shared by every later run, and a pull request can
# edit this workflow, including the `commit` input. So the disk key must embed
# a value derived from a secret that fork runs never receive, untrusted runs
# must skip the disk, and only master pushes may commit.
untrusted_only = "${{ env.UNTRUSTED_PR != 'true' }}"
# Master pushes save the shared disk. A manual dispatch saves only its own
# -dispatch disk, which the key suffix keeps apart from the shared one.
commit_scope = "${{ github.event_name == 'workflow_dispatch' || (github.event_name == 'push' && github.ref == 'refs/heads/master') }}"
dispatch_suffix = "${{ github.event_name == 'workflow_dispatch' && '-dispatch' || '' }}"
sticky_disk_jobs = jobs.select do |_, job|
  job.fetch("steps", []).any? { |step| step["uses"].to_s.start_with?("useblacksmith/stickydisk@") }
end
unless sticky_disk_jobs.keys.sort == %w[backend-build backend-lint]
  warn "only backend-build and backend-lint may use sticky disks, got #{sticky_disk_jobs.keys.inspect}"
  exit 1
end
sticky_disk_jobs.each do |name, job|
  steps = job.fetch("steps")
  key_step = steps.find { |step| step["id"] == "target-disk" }
  unless key_step &&
         key_step["if"] == untrusted_only &&
         key_step.fetch("env", {})["DISK_KEY_SECRET"] == "${{ secrets.CACHIX_AUTH_TOKEN }}" &&
         key_step["run"].include?('echo "::add-mask::$key"')
    warn "#{name} must derive its sticky disk key from a secret, mask it, and skip untrusted runs"
    exit 1
  end

  steps.select { |step| step["uses"].to_s.start_with?("useblacksmith/stickydisk@") }.each do |step|
    with = step.fetch("with")
    unless step["if"] == untrusted_only &&
           with["key"].start_with?("${{ steps.target-disk.outputs.key }}-") &&
           with["key"].end_with?(dispatch_suffix) &&
           with["commit"] == commit_scope
      warn "#{name} sticky disk must skip untrusted runs, use the secret-derived key, keep dispatch runs on their own disk, and commit only on master pushes and dispatches"
      exit 1
    end
  end
end

actions = []
visit = lambda do |value|
  case value
  when Hash
    action = value["uses"]
    if action.is_a?(String) && action.start_with?("actions/checkout@")
      persist_credentials = value.fetch("with", {})["persist-credentials"]
      unless persist_credentials == false
        warn "checkout must set persist-credentials: false"
        exit 1
      end
    end

    value.each do |key, child|
      actions << child if key == "uses"
      visit.call(child)
    end
  when Array
    value.each { |child| visit.call(child) }
  end
end

visit.call(workflow)
actions.each do |action|
  unless action.is_a?(String) && action.match?(/\A[^@\s]+@[0-9a-f]{40}\z/)
    warn "CI action is not pinned to a commit SHA: #{action.inspect}"
    exit 1
  end
end

puts actions.length
RUBY
)

# Every third-party action is immutable. Reject alternate YAML spellings so a
# valid inline map or spaced key cannot bypass the scanner. Version comments
# keep update intent readable without trusting a mutable tag at execution time.
action_line_pattern='^[[:space:]]*(-[[:space:]]+)?uses:[[:space:]]+[^[:space:]@]+@[0-9a-f]{40}[[:space:]]+# v[0-9][^[:space:]]*$'
action_line_count=0
while IFS= read -r action_line; do
  action_line_count=$((action_line_count + 1))
  if [[ ! "$action_line" =~ $action_line_pattern ]]; then
    echo "CI action must use canonical SHA pinning with a version comment: $action_line" >&2
    exit 1
  fi
done < <(grep -E "[\"']?uses[\"']?[[:space:]]*:" "$workflow_file")

if [ "$parsed_action_count" -ne "$action_line_count" ]; then
  echo "every parsed CI action must use the canonical uses line with a version comment" >&2
  exit 1
fi

assert_count 6 '      - uses: actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803  # v6'
assert_absent 'dorny/paths-filter@'
assert_count 6 '      - uses: nixbuild/nix-quick-install-action@2c9db80fb984ceb1bcaa77cdda3fdf8cfba92035  # v34'
assert_count 6 '      - uses: cachix/cachix-action@5f2d7c5294214f71b873db4b969586b980625e71  # v17'
assert_count 6 '      - uses: nix-community/cache-nix-action@7df957e333c1e5da7721f60227dbba6d06080569  # v7'
# Swatinem/rust-cache prunes workspace-crate and incremental artifacts from the
# target directory in its save step, which would empty the sticky disks.
assert_absent 'Swatinem/rust-cache@'
assert_count 2 '        uses: useblacksmith/stickydisk@3f6be1451e3cad893b778bc93495c80452a5da26  # v1.7.2'
assert_count 1 '        uses: actions/cache/save@55cc8345863c7cc4c66a329aec7e433d2d1c52a9  # v6'
assert_count 1 '        uses: actions/cache/restore@55cc8345863c7cc4c66a329aec7e433d2d1c52a9  # v6'

# Keep actionlint aware of every custom runner label used by this workflow.
ruby - "$actionlint_file" <<'RUBY'
require "yaml"

config = YAML.safe_load(File.read(ARGV.fetch(0)), aliases: true)
labels = config.dig("self-hosted-runner", "labels")
expected = %w[
  blacksmith-2vcpu-ubuntu-2404
  blacksmith-4vcpu-ubuntu-2404
  blacksmith-8vcpu-ubuntu-2404
]

unless labels == expected
  warn "actionlint must declare the complete Blacksmith runner matrix"
  exit 1
end
RUBY

# Every untrusted execution guard must derive from the event payload, never
# from a branch naming convention. Dependabot uses an in-repository branch but
# receives the same restricted token and secret treatment as a fork.
assert_line "  UNTRUSTED_PR: \${{ github.event_name == 'pull_request' && (github.event.pull_request.head.repo.fork || github.event.pull_request.user.login == 'dependabot[bot]') }}"
assert_count 6 "          authToken: \${{ env.UNTRUSTED_PR != 'true' && secrets.CACHIX_AUTH_TOKEN || '' }}"
assert_count 6 "          skipPush: \${{ env.UNTRUSTED_PR == 'true' }}"
assert_count 6 "          save: \${{ env.UNTRUSTED_PR != 'true' }}"
# A sticky disk snapshot is shared by every later run, so only reviewed code
# may commit one: master pushes. Pull requests (fork or not) and manual
# dispatches of arbitrary branches must not.
# A manual dispatch (any branch) may commit only its own -dispatch disk, which
# no pull request or master run mounts.
assert_count 2 "          commit: \${{ github.event_name == 'workflow_dispatch' || (github.event_name == 'push' && github.ref == 'refs/heads/master') }}"
assert_count 1 "          key: \${{ steps.target-disk.outputs.key }}-target-test-v1\${{ github.event_name == 'workflow_dispatch' && '-dispatch' || '' }}"
assert_count 1 "          key: \${{ steps.target-disk.outputs.key }}-target-lint-v1\${{ github.event_name == 'workflow_dispatch' && '-dispatch' || '' }}"
# The over-40 GiB wipe must not touch the root-owned lost+found of the fresh
# ext4 disk, or rm fails, the master push job fails, and no snapshot commits.
assert_count 2 "            sudo find \"\$CARGO_TARGET_DIR\" -mindepth 1 -maxdepth 1 ! -name lost+found -exec rm -rf {} +"
# A snapshot keeps its mtimes and may be newer than this checkout, so after
# the mount every source whose content differs from the tree the snapshot
# recorded must be made newer than it, or cargo can skip a changed crate; each
# build records its tree only after it succeeds. rain-math-float lives in the
# Nix store (mtime 1), so its artifacts must be cleaned when its source
# changes, before each build on the disk.
assert_count 2 '          ./scripts/sync-source-mtimes.sh prepare'
assert_count 2 '            ./scripts/sync-source-mtimes.sh record'
assert_line '        run: ./scripts/test-sync-source-mtimes.sh'
assert_count 2 '            ./scripts/clean-stale-rain-math-float.sh'
assert_line '        run: ./scripts/test-clean-stale-rain-math-float.sh'

if grep -R -n -F 'external-pr/' \
  --exclude='test-fork-pr-ci.sh' \
  "$repo_root/.github/workflows" \
  "$repo_root/scripts"
then
  echo "the detached external-pr mirror path must not remain" >&2
  exit 1
fi

echo "fork pull request CI invariants passed"
