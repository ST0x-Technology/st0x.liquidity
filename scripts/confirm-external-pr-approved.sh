#!/usr/bin/env bash

set -euo pipefail

: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY must be set}"
: "${PR_NUMBER:?PR_NUMBER must be set}"
: "${EXPECTED_HEAD_SHA:?EXPECTED_HEAD_SHA must be set}"

pr_data=$(gh api \
  -H "Accept: application/vnd.github+json" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  "/repos/$GITHUB_REPOSITORY/pulls/$PR_NUMBER")

state=$(printf '%s' "$pr_data" | jq -r '.state')
if [ "$state" != "open" ]; then
  echo "ERROR: pull request #$PR_NUMBER is $state; not dispatching." >&2
  exit 1
fi

live_head=$(printf '%s' "$pr_data" | jq -r '.head.sha')
if [ "$live_head" != "$EXPECTED_HEAD_SHA" ]; then
  echo "ERROR: pull request #$PR_NUMBER moved from $EXPECTED_HEAD_SHA to $live_head. Re-apply the label to test the new commit." >&2
  exit 1
fi

has_label=$(printf '%s' "$pr_data" | jq -r '[.labels[].name == "ok-to-test"] | any')
if [ "$has_label" != "true" ]; then
  echo "ERROR: the ok-to-test label is no longer present on #$PR_NUMBER; not dispatching." >&2
  exit 1
fi

echo "Confirmed: #$PR_NUMBER is open and carries ok-to-test at $EXPECTED_HEAD_SHA."
