#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
test_root=$(mktemp -d "${TMPDIR:-/tmp}/external-pr-ci-test.XXXXXX")
trap 'rm -r -- "$test_root"' EXIT

workflow_file="$script_dir/../.github/workflows/external-pr-ci.yaml"
concurrency_block=$(sed -n '/^concurrency:/,/^[^ ]/p' "$workflow_file")
if ! grep -Fqx '  queue: max' <<< "$concurrency_block"; then
  echo "external PR CI concurrency must use the expanded queue with queue: max" >&2
  exit 1
fi
if grep -Fqx '  cancel-in-progress: true' <<< "$concurrency_block"; then
  echo "queue: max cannot be combined with cancel-in-progress: true" >&2
  exit 1
fi

fake_bin="$test_root/bin"
mkdir "$fake_bin"

cat > "$fake_bin/gh" <<'FAKE_GH'
#!/usr/bin/env bash

set -euo pipefail

endpoint=""
method="GET"
for argument in "$@"; do
  case "$argument" in
    /repos/*) endpoint="$argument" ;;
  esac
done

previous=""
for argument in "$@"; do
  if [ "$previous" = "-X" ]; then
    method="$argument"
  fi
  previous="$argument"
done

printf '%s %s %s\n' "$method" "$endpoint" "$*" >> "$GH_CALL_LOG"

case "$endpoint" in
  */pulls/*)
    if [ "${PR_SCENARIO:-}" = "query_failure" ]; then
      exit 1
    fi
    printf '%s\n' "$PR_JSON"
    ;;
  */actions/workflows/ci.yaml/runs\?*)
    count=0
    if [ -f "$LIST_COUNT_FILE" ]; then
      count=$(<"$LIST_COUNT_FILE")
    fi
    count=$((count + 1))
    printf '%s\n' "$count" > "$LIST_COUNT_FILE"

    case "${LIST_SCENARIO:-}:$count" in
      late_active_then_terminal:2)
        printf '101\n102\n'
        ;;
      late_active_then_terminal:3)
        printf '101\n'
        ;;
      stuck:*)
        printf '303\n'
        ;;
      list_failure:*)
        exit 1
        ;;
    esac
    ;;
  */actions/runs/*/force-cancel)
    ;;
  */git/ref/heads/external-pr/*)
    ;;
  */git/refs/heads/external-pr/*)
    if [ "$method" != "DELETE" ]; then
      echo "expected mirror deletion to use DELETE" >&2
      exit 1
    fi
    ;;
  *)
    echo "unexpected fake gh call: $*" >&2
    exit 1
    ;;
esac
FAKE_GH
chmod +x "$fake_bin/gh"

export GH_CALL_LOG="$test_root/gh-calls.log"
export LIST_COUNT_FILE="$test_root/list-count"
export GITHUB_REPOSITORY="ST0x-Technology/st0x.liquidity"
export PR_NUMBER="1083"
export GH_TOKEN="test-token"
export PR_JSON='{"state":"open","head":{"sha":"approved-head"},"labels":[]}'

reset_fake_gh() {
  : > "$GH_CALL_LOG"
  rm -f -- "$LIST_COUNT_FILE"
}

assert_call_count() {
  local pattern=$1
  local expected=$2
  local actual

  actual=$(grep -c -- "$pattern" "$GH_CALL_LOG" || true)
  if [ "$actual" != "$expected" ]; then
    echo "expected $expected calls matching '$pattern', got $actual" >&2
    sed -n '1,120p' "$GH_CALL_LOG" >&2
    exit 1
  fi
}

run_cleanup() {
  PATH="$fake_bin:$PATH" \
    EXTERNAL_PR_CLEANUP_POLL_SECONDS=0 \
    "$script_dir/cleanup-external-pr-ci.sh"
}

reset_fake_gh
LIST_SCENARIO=late_active_then_terminal \
  EXTERNAL_PR_CLEANUP_MAX_ATTEMPTS=6 \
  EXTERNAL_PR_CLEANUP_QUIET_CHECKS=2 \
  run_cleanup
assert_call_count '/actions/runs/101/force-cancel' 1
assert_call_count '/actions/runs/102/force-cancel' 1
assert_call_count '/git/refs/heads/external-pr/1083' 1
assert_call_count '--paginate' 5

reset_fake_gh
PR_JSON='{"state":"open","head":{"sha":"approved-head"},"labels":[{"name":"ok-to-test"}]}' \
  LIST_SCENARIO=stuck \
  EXTERNAL_PR_CLEANUP_MAX_ATTEMPTS=1 \
  EXTERNAL_PR_CLEANUP_QUIET_CHECKS=1 \
  run_cleanup
assert_call_count '/actions/workflows/ci.yaml/runs' 0
assert_call_count '/actions/runs/303/force-cancel' 0
assert_call_count '/git/ref/heads/external-pr/1083' 0
assert_call_count '/git/refs/heads/external-pr/1083' 0

reset_fake_gh
PR_JSON='{"state":"closed","head":{"sha":"approved-head"},"labels":[{"name":"ok-to-test"}]}' \
  EXTERNAL_PR_CLEANUP_MAX_ATTEMPTS=1 \
  EXTERNAL_PR_CLEANUP_QUIET_CHECKS=1 \
  run_cleanup
assert_call_count '/actions/workflows/ci.yaml/runs' 1
assert_call_count '/git/ref/heads/external-pr/1083' 1
assert_call_count '/git/refs/heads/external-pr/1083' 1

reset_fake_gh
if PR_SCENARIO=query_failure \
  EXTERNAL_PR_CLEANUP_MAX_ATTEMPTS=1 \
  EXTERNAL_PR_CLEANUP_QUIET_CHECKS=1 \
  run_cleanup
then
  echo "cleanup unexpectedly succeeded without proving the live approval state" >&2
  exit 1
fi
assert_call_count '/actions/workflows/ci.yaml/runs' 0
assert_call_count '/git/ref/heads/external-pr/1083' 0
assert_call_count '/git/refs/heads/external-pr/1083' 0

reset_fake_gh
if LIST_SCENARIO=stuck \
  EXTERNAL_PR_CLEANUP_MAX_ATTEMPTS=2 \
  EXTERNAL_PR_CLEANUP_QUIET_CHECKS=1 \
  run_cleanup
then
  echo "cleanup unexpectedly succeeded while a workflow run stayed active" >&2
  exit 1
fi
assert_call_count '/actions/runs/303/force-cancel' 1
assert_call_count '/git/ref/heads/external-pr/1083' 0
assert_call_count '/git/refs/heads/external-pr/1083' 0

reset_fake_gh
if LIST_SCENARIO=list_failure \
  EXTERNAL_PR_CLEANUP_MAX_ATTEMPTS=2 \
  EXTERNAL_PR_CLEANUP_QUIET_CHECKS=1 \
  run_cleanup
then
  echo "cleanup unexpectedly succeeded without proving workflow state" >&2
  exit 1
fi
assert_call_count '/git/ref/heads/external-pr/1083' 0
assert_call_count '/git/refs/heads/external-pr/1083' 0

export EXPECTED_HEAD_SHA="approved-head"

reset_fake_gh
PR_JSON='{"state":"open","head":{"sha":"approved-head"},"labels":[{"name":"ok-to-test"}]}' \
  PATH="$fake_bin:$PATH" \
  "$script_dir/confirm-external-pr-approved.sh"

for rejected_pr in \
  '{"state":"closed","head":{"sha":"approved-head"},"labels":[{"name":"ok-to-test"}]}' \
  '{"state":"open","head":{"sha":"new-head"},"labels":[{"name":"ok-to-test"}]}' \
  '{"state":"open","head":{"sha":"approved-head"},"labels":[]}'
do
  if PR_JSON="$rejected_pr" \
    PATH="$fake_bin:$PATH" \
    "$script_dir/confirm-external-pr-approved.sh"
  then
    echo "approval check unexpectedly accepted: $rejected_pr" >&2
    exit 1
  fi
done

echo "external PR CI script tests passed"
