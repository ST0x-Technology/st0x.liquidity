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
assert_line '    types: [opened, synchronize, reopened, ready_for_review, edited]'

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
  "changes" => { "needs" => nil, "if" => nil },
  "backend-build" => {
    "needs" => "changes",
    "if" => "${{ needs.changes.outputs.code == 'true' }}"
  },
  "backend-test" => {
    "needs" => %w[changes backend-build],
    "if" => "${{ needs.changes.outputs.code == 'true' }}"
  },
  "backend" => {
    "needs" => %w[changes backend-build backend-test],
    "if" => "${{ always() }}"
  },
  "dashboard-build" => {
    "needs" => "changes",
    "if" => "${{ needs.changes.outputs.code == 'true' }}"
  },
  "dashboard" => {
    "needs" => %w[changes dashboard-build],
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

filter_step = jobs.fetch("changes").fetch("steps").find { |step| step["id"] == "filter" }
filter_config = filter_step.fetch("with")
filters = YAML.safe_load(filter_config.fetch("filters")).fetch("code")
expected_filters = %w[** !**/*.md !docs/** !AGENTS.md !README*]
unless filter_config["predicate-quantifier"] == "every" && filters == expected_filters
  warn "the code classifier must exclude only documentation paths"
  exit 1
end

checkout_step = jobs.fetch("changes").fetch("steps").find do |step|
  step.fetch("uses", "").start_with?("actions/checkout@")
end
unless checkout_step&.fetch("if", nil) == "${{ github.event_name != 'pull_request' }}"
  warn "changes checkout must run for push detection and stay off pull_request API detection"
  exit 1
end

workflow_permissions = workflow.fetch("permissions")
unless workflow_permissions["contents"] == "read" &&
    workflow_permissions["pull-requests"] == "read"
  warn "paths-filter permissions must remain explicitly read-only"
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
    needs.changes.result
    needs.changes.outputs.code
    needs.backend-build.result
    needs.backend-test.result
  ],
  [
    [%w[success false skipped skipped], true],
    [%w[success true success success], true],
    [%w[success true failure success], false],
    [%w[success true skipped success], false],
    [%w[success true cancelled success], false],
    [%w[success true success failure], false],
    [%w[success true success skipped], false],
    [%w[success true success cancelled], false],
    [%w[failure unknown skipped skipped], false],
    [%w[skipped unknown skipped skipped], false],
    [%w[cancelled unknown skipped skipped], false]
  ]
)

check_gate.call(
  "dashboard",
  "Check dashboard validation",
  %w[
    needs.changes.result
    needs.changes.outputs.code
    needs.dashboard-build.result
  ],
  [
    [%w[success false skipped], true],
    [%w[success true success], true],
    [%w[success true failure], false],
    [%w[success true skipped], false],
    [%w[success true cancelled], false],
    [%w[failure unknown skipped], false],
    [%w[skipped unknown skipped], false],
    [%w[cancelled unknown skipped], false]
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

assert_count 5 '      - uses: actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803  # v6'
assert_count 1 '      - uses: dorny/paths-filter@d1c1ffe0248fe513906c8e24db8ea791d46f8590  # v3'
assert_count 4 '      - uses: nixbuild/nix-quick-install-action@2c9db80fb984ceb1bcaa77cdda3fdf8cfba92035  # v34'
assert_count 4 '      - uses: cachix/cachix-action@5f2d7c5294214f71b873db4b969586b980625e71  # v17'
assert_count 4 '      - uses: nix-community/cache-nix-action@7df957e333c1e5da7721f60227dbba6d06080569  # v7'
assert_count 1 '      - uses: Swatinem/rust-cache@e18b497796c12c097a38f9edb9d0641fb99eee32  # v2'
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
assert_count 4 "          authToken: \${{ env.UNTRUSTED_PR != 'true' && secrets.CACHIX_AUTH_TOKEN || '' }}"
assert_count 4 "          skipPush: \${{ env.UNTRUSTED_PR == 'true' }}"
assert_count 4 "          save: \${{ env.UNTRUSTED_PR != 'true' }}"
assert_count 1 "          save-if: \${{ env.UNTRUSTED_PR != 'true' }}"

# The pull request files API truncates after 3,000 files. The workflow must run
# full CI at that boundary rather than accepting a docs-only false negative.
assert_line "      code: \${{ github.event_name == 'pull_request' && github.event.pull_request.changed_files >= 3000 && 'true' || steps.filter.outputs.code }}"

if grep -R -n -F 'external-pr/' \
  --exclude='test-fork-pr-ci.sh' \
  "$repo_root/.github/workflows" \
  "$repo_root/scripts"
then
  echo "the detached external-pr mirror path must not remain" >&2
  exit 1
fi

echo "fork pull request CI invariants passed"
