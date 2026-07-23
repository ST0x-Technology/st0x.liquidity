#!/usr/bin/env bash

set -euo pipefail

: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY must be set}"
: "${PR_NUMBER:?PR_NUMBER must be set}"

poll_seconds=${EXTERNAL_PR_CLEANUP_POLL_SECONDS:-3}
max_attempts=${EXTERNAL_PR_CLEANUP_MAX_ATTEMPTS:-40}
quiet_checks_required=${EXTERNAL_PR_CLEANUP_QUIET_CHECKS:-5}
mirror_branch="external-pr/$PR_NUMBER"
requested_cancellations=" "
quiet_checks=0
terminal_state_confirmed=false

pr_data=$(gh api \
  -H "Accept: application/vnd.github+json" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  "/repos/$GITHUB_REPOSITORY/pulls/$PR_NUMBER")

state=$(printf '%s' "$pr_data" | jq -r '.state')
has_label=$(printf '%s' "$pr_data" | jq -r '[.labels[].name == "ok-to-test"] | any')
case "$state:$has_label" in
  open:true)
    echo "Skipping cleanup: #$PR_NUMBER is open and currently carries ok-to-test."
    exit 0
    ;;
  open:false|closed:true|closed:false) ;;
  *)
    echo "ERROR: unexpected live approval state for #$PR_NUMBER (state: $state, ok-to-test present: $has_label); leaving CI and the mirror branch unchanged." >&2
    exit 1
    ;;
esac

echo "Confirmed cleanup is still required: #$PR_NUMBER is $state and ok-to-test present is $has_label."

list_non_completed_runs() {
  gh api --paginate \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    "/repos/$GITHUB_REPOSITORY/actions/workflows/ci.yaml/runs?branch=$mirror_branch&per_page=100" \
    --jq '.workflow_runs[] | select(.status != "completed") | .id'
}

for ((attempt = 1; attempt <= max_attempts; attempt++)); do
  if active_runs=$(list_non_completed_runs); then
    if [ -z "$active_runs" ]; then
      quiet_checks=$((quiet_checks + 1))
      echo "No non-completed ci.yaml runs on $mirror_branch ($quiet_checks/$quiet_checks_required confirmations)."
      if [ "$quiet_checks" -ge "$quiet_checks_required" ]; then
        terminal_state_confirmed=true
        break
      fi
    else
      quiet_checks=0
      while IFS= read -r run_id; do
        [ -n "$run_id" ] || continue
        case "$requested_cancellations" in
          *" $run_id "*) continue ;;
        esac

        echo "Force-cancelling non-completed ci.yaml run $run_id on $mirror_branch"
        if gh api -X POST \
          -H "Accept: application/vnd.github+json" \
          -H "X-GitHub-Api-Version: 2022-11-28" \
          "/repos/$GITHUB_REPOSITORY/actions/runs/$run_id/force-cancel"
        then
          requested_cancellations="$requested_cancellations$run_id "
        else
          echo "WARNING: cancellation request for run $run_id failed; it will be retried while the run remains non-completed." >&2
        fi
      done <<< "$active_runs"
    fi
  else
    quiet_checks=0
    echo "WARNING: could not list ci.yaml runs on $mirror_branch; retrying instead of assuming none are active." >&2
  fi

  if [ "$attempt" -lt "$max_attempts" ]; then
    sleep "$poll_seconds"
  fi
done

if [ "$terminal_state_confirmed" != "true" ]; then
  echo "ERROR: could not prove that every ci.yaml run on $mirror_branch reached terminal state; leaving the mirror branch in place for a safe retry." >&2
  exit 1
fi

# Numeric HTTP status, not a swallowed error: 404 means the branch is genuinely
# absent. Any other failure leaves its state unknown and must fail loudly.
if response=$(gh api -i \
  -H "Accept: application/vnd.github+json" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  "/repos/$GITHUB_REPOSITORY/git/ref/heads/$mirror_branch" 2>/dev/null)
then
  gh api -X DELETE \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    "/repos/$GITHUB_REPOSITORY/git/refs/heads/$mirror_branch"
  echo "Deleted $mirror_branch"
else
  status=$(printf '%s' "$response" | head -1 | awk '{print $2}')
  if [ "$status" = "404" ]; then
    echo "No $mirror_branch branch to delete."
  else
    echo "ERROR: could not confirm whether $mirror_branch exists (HTTP status: ${status:-unknown})." >&2
    printf '%s\n' "$response" >&2
    exit 1
  fi
fi

echo "Cleanup confirmed: every fork CI run is terminal and no mirror branch remains for $mirror_branch."
