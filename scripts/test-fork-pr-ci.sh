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

# Native pull_request runs attach the required jobs to both internal and fork
# pull requests, including Graphite PRs targeting a parent feature branch. Push
# CI is limited to master so one commit cannot get a second set of identically
# named required checks.
assert_count 1 '    branches: [master]'
assert_line '  pull_request:'
assert_line '    types: [opened, synchronize, reopened, ready_for_review, edited]'

# paths-filter needs this permission on pull_request events. It remains
# read-only for fork runs.
assert_line '  contents: read'
assert_line '  pull-requests: read'
if grep -Eq '^[[:space:]]*permissions:[[:space:]]*write-all|^[[:space:]]*[[:alnum:]_-]+:[[:space:]]*write$' "$workflow_file"; then
  echo "CI permissions must remain read-only" >&2
  exit 1
fi

# Every third-party action is immutable. Reject alternate YAML spellings so a
# valid inline map or spaced key cannot bypass the scanner. Version comments
# keep update intent readable without trusting a mutable tag at execution time.
parsed_action_count=$(
  ruby - "$workflow_file" <<'RUBY'
require "yaml"

workflow = YAML.safe_load(File.read(ARGV.fetch(0)), aliases: true)
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

assert_count 1 '        uses: withgraphite/graphite-ci-action@9bc969adfd43bb790da3b64b543c78c75cef9689  # v0.0.9'
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
assert_line "        if: \${{ env.UNTRUSTED_PR != 'true' && steps.metadata_edit.outputs.skip != 'true' }}"
assert_count 4 "          authToken: \${{ env.UNTRUSTED_PR != 'true' && secrets.CACHIX_AUTH_TOKEN || '' }}"
assert_count 4 "          skipPush: \${{ env.UNTRUSTED_PR == 'true' }}"
assert_count 4 "          save: \${{ env.UNTRUSTED_PR != 'true' }}"
assert_count 1 "          save-if: \${{ env.UNTRUSTED_PR != 'true' }}"

# A metadata-only `edited` event must stand the real jobs down and must not
# share the concurrency group with a real run. Without the separate group a
# title edit would cancel a live run and then report every required check as
# skipped, which branch protection reads as a pass on untested code.
assert_line "        if: \${{ github.event.action == 'edited' && !github.event.changes.base }}"
assert_line "      skip: \${{ steps.metadata_edit.outputs.skip == 'true' && 'true' || steps.check_skip.outputs.skip }}"
assert_line "  group: \${{ github.repository }}-\${{ github.workflow }}-\${{ github.ref }}-\${{ github.event.action == 'edited' && !github.event.changes.base && 'metadata-edit' || '' }}-\${{ github.ref == 'refs/heads/master' && github.sha || ''}}"

# The pull request files API truncates after 3,000 files. The workflow must run
# full CI at that boundary rather than accepting a docs-only false negative.
assert_line "      code: \${{ github.event_name == 'pull_request' && github.event.pull_request.changed_files >= 3000 && 'true' || steps.filter.outputs.code }}"

# Branch protection requires these exact GitHub Actions job names.
assert_line '  backend:'
assert_line '  dashboard:'
assert_line '  hooks:'

if grep -R -n -F 'external-pr/' \
  --exclude='test-fork-pr-ci.sh' \
  "$repo_root/.github/workflows" \
  "$repo_root/scripts"
then
  echo "the detached external-pr mirror path must not remain" >&2
  exit 1
fi

echo "fork pull request CI invariants passed"
