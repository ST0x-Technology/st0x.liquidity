#!/usr/bin/env bash
# Waits for a job of the current workflow run to complete, and fails unless it
# succeeded. The test shards use it to start with the run, instead of after
# backend-build through `needs`.
#
#   usage: wait-for-job.sh <job name>
#
# Needs GH_TOKEN (with actions: read), GITHUB_REPOSITORY, GITHUB_RUN_ID, and
# GITHUB_RUN_ATTEMPT.
#
# The GITHUB_TOKEN gets 1,000 API requests per hour per repository, shared by
# every run. So the job is polled with conditional requests: a 304 Not Modified
# does not count against that limit. Each step that starts or ends in the job
# changes it, and the next poll after that costs one request. A rate-limited
# request waits for the reset when it is near, and fails the shard with a clear
# message when it is not. Other API errors only delay the next poll.
set -euo pipefail

job_name=${1:?usage: $0 <job name>}
# A 304 costs nothing, so a short interval only shortens the shards' wait.
poll_seconds=${WAIT_FOR_JOB_POLL_SECONDS:-3}
# The run lists every job from its start, so a job that is missing for this
# long does not exist (for example, after a rename).
lookup_seconds=${WAIT_FOR_JOB_LOOKUP_SECONDS:-300}
max_rate_limit_wait=600
api=${GITHUB_API_URL:-https://api.github.com}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

header() {
  sed -n "s/^$1: *//Ip" "$work/headers" | tr -d '\r' | tail -n 1
}

# Sends one GET and sets `code`. Returns 0 on 200 or 304. Waits out or fails on
# a rate limit, and returns 1 on any other error.
get() {
  local url=$1
  local etag=${2:-}
  local wait
  local reset

  : > "$work/headers"
  : > "$work/body"
  code=$(curl -sS --connect-timeout 10 --max-time 30 \
    -o "$work/body" -D "$work/headers" -w '%{http_code}' \
    -H "Authorization: Bearer $GH_TOKEN" -H 'Accept: application/vnd.github+json' \
    -H "If-None-Match: $etag" "$url") || code=000

  case "$code" in
    200 | 304) return 0 ;;
    403 | 429)
      wait=$(header retry-after)
      reset=$(header x-ratelimit-reset)
      if [ -z "$wait" ] && [ "$(header x-ratelimit-remaining)" = 0 ] && [ -n "$reset" ]; then
        wait=$((reset - $(date +%s)))
        # A reset that already passed still needs a short pause.
        if [ "$wait" -lt 1 ]; then
          wait=1
        fi
      fi
      if [ -z "$wait" ] && grep -qi 'rate limit' "$work/body"; then
        # A secondary rate limit without headers: GitHub asks for a minute.
        wait=60
      fi
      if [ -z "$wait" ]; then
        echo "the GitHub API denied the request (HTTP $code); does the job have actions: read?" >&2
        cat "$work/body" >&2
        exit 1
      fi
      if [ "$wait" -gt "$max_rate_limit_wait" ]; then
        echo "the GitHub API rate limit resets in ${wait} s, more than ${max_rate_limit_wait} s" >&2
        exit 1
      fi
      echo "::warning::GitHub API rate limit hit (HTTP $code); waiting ${wait} s"
      sleep "$wait"
      rate_limit_waited=$((rate_limit_waited + wait))
      return 1
      ;;
    *)
      echo "::warning::GitHub API request returned HTTP $code; retrying"
      return 1
      ;;
  esac
}

# Each attempt lists its jobs on one page. The search runs from this attempt
# down and takes the first attempt that has a job of this name: a re-run of
# all jobs creates the job again in the new attempt, and a re-run of only the
# failed shards leaves the build that succeeded in an earlier attempt.
attempt=${GITHUB_RUN_ATTEMPT:-1}
deadline=$((SECONDS + lookup_seconds))
rate_limit_waited=0
job_url=
while :; do
  for ((searched = attempt; searched >= 1; searched--)); do
    # After an API error, retry the whole search rather than fall back to an
    # older attempt.
    if ! get "$api/repos/$GITHUB_REPOSITORY/actions/runs/$GITHUB_RUN_ID/attempts/$searched/jobs?per_page=100" ||
      [ "$code" != 200 ]; then
      break
    fi
    job_url=$(jq -r --arg name "$job_name" \
      'first(.jobs[] | select(.name == $name) | .url) // empty' "$work/body") || job_url=
    [ -n "$job_url" ] && break
  done
  [ -n "$job_url" ] && break
  # Time spent waiting out a rate limit does not count against the lookup.
  if [ "$SECONDS" -ge "$((deadline + rate_limit_waited))" ]; then
    echo "no job named '$job_name' in this run after ${lookup_seconds} s" >&2
    exit 1
  fi
  sleep "$poll_seconds"
done

etag=
state=
while :; do
  if get "$job_url" "$etag" && [ "$code" = 200 ]; then
    # Keep the ETag only with a parsed state, so a bad body is fetched again.
    if new_state=$(jq -er '"\(.status) \(.conclusion)"' "$work/body"); then
      state=$new_state
      etag=$(header etag)
    else
      echo "::warning::the '$job_name' job status was not valid JSON; retrying"
    fi
  fi
  case "$state" in
    "completed success")
      echo "'$job_name' succeeded"
      exit 0
      ;;
    completed*)
      echo "'$job_name' did not succeed: $state" >&2
      exit 1
      ;;
  esac
  sleep "$poll_seconds"
done
