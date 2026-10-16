#!/usr/bin/env bash
# Runs wait-for-job.sh against a stub curl that replays scripted API responses.
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
mkdir -p "$repo_root/.tmp"
fixture=$(mktemp -d "$repo_root/.tmp/wait-for-job-test.XXXXXX")
trap 'rm -rf "$fixture"' EXIT

mkdir -p "$fixture/bin"
# The stub requires a --max-time, serves response N from
# $fixture/responses/N.{code,headers,body}, and logs each request's URL and
# If-None-Match header.
cat > "$fixture/bin/curl" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
body_file=
headers_file=
if_none_match=
max_time=
url=
while [ $# -gt 0 ]; do
  case "$1" in
    -o) body_file=$2; shift 2 ;;
    -D) headers_file=$2; shift 2 ;;
    --max-time) max_time=$2; shift 2 ;;
    -w | --connect-timeout) shift 2 ;;
    -H)
      case "$2" in
        "If-None-Match: "*) if_none_match=${2#If-None-Match: } ;;
      esac
      shift 2
      ;;
    -*) shift ;;
    *) url=$1; shift ;;
  esac
done
# A request without a time limit could hang the shard until its timeout.
if [ -z "$max_time" ]; then
  echo "curl ran without --max-time" >&2
  exit 7
fi
count=$(($(cat "$STUB_DIR/count") + 1))
echo "$count" > "$STUB_DIR/count"
echo "$url|$if_none_match" >> "$STUB_DIR/requests"
response="$STUB_DIR/responses/$count"
if [ ! -f "$response.code" ]; then
  echo "no scripted response $count" >&2
  exit 7
fi
cp "$response.headers" "$headers_file"
cp "$response.body" "$body_file"
printf '%s' "$(cat "$response.code")"
STUB
chmod +x "$fixture/bin/curl"

scenario=
respond() {
  local code=$1
  local headers=$2
  local body=$3
  local index
  index=$(($(find "$scenario/responses" -name '*.code' | wc -l) + 1))
  echo "$code" > "$scenario/responses/$index.code"
  printf '%b' "$headers" > "$scenario/responses/$index.headers"
  printf '%s' "$body" > "$scenario/responses/$index.body"
}

new_scenario() {
  scenario="$fixture/$1"
  mkdir -p "$scenario/responses"
  echo 0 > "$scenario/count"
  : > "$scenario/requests"
}

jobs_list='{"jobs":[{"name":"backend lint","run_attempt":1,"url":"https://api.test/jobs/1"},{"name":"backend build","run_attempt":1,"url":"https://api.test/jobs/2"}]}'
running='{"status":"in_progress","conclusion":null}'
success='{"status":"completed","conclusion":"success"}'
failure='{"status":"completed","conclusion":"failure"}'

run_wait() {
  PATH="$fixture/bin:$PATH" STUB_DIR="$scenario" GH_TOKEN=token \
    GITHUB_API_URL=https://api.test GITHUB_REPOSITORY=owner/repo GITHUB_RUN_ID=42 GITHUB_RUN_ATTEMPT="${run_attempt:-1}" \
    WAIT_FOR_JOB_POLL_SECONDS=0 WAIT_FOR_JOB_LOOKUP_SECONDS="${lookup_seconds:-60}" \
    "$repo_root/scripts/wait-for-job.sh" "backend build" > "$scenario/out" 2>&1
}

expect_exit() {
  local expected=$1
  local actual=0
  run_wait || actual=$?
  if [ "$actual" != "$expected" ]; then
    echo "$(basename "$scenario"): expected exit $expected, got $actual" >&2
    cat "$scenario/out" >&2
    exit 1
  fi
}

expect_output() {
  if ! grep -qF -- "$1" "$scenario/out"; then
    echo "$(basename "$scenario"): output lacks '$1'" >&2
    cat "$scenario/out" >&2
    exit 1
  fi
}

expect_request() {
  local line=$1
  local expected=$2
  local actual
  actual=$(sed -n "${line}p" "$scenario/requests")
  if [ "$actual" != "$expected" ]; then
    echo "$(basename "$scenario"): request $line was '$actual', expected '$expected'" >&2
    exit 1
  fi
}

# A build that succeeds after a few polls: the polls after the first send the
# ETag, and a 304 keeps the last state.
new_scenario success
respond 200 '' "$jobs_list"
respond 200 'etag: "a"\r\n' "$running"
respond 304 '' ''
respond 200 'ETag: "b"\r\n' "$success"
expect_exit 0
expect_request 1 'https://api.test/repos/owner/repo/actions/runs/42/attempts/1/jobs?per_page=100|'
expect_request 2 'https://api.test/jobs/2|'
expect_request 3 'https://api.test/jobs/2|"a"'
expect_request 4 'https://api.test/jobs/2|"a"'

# Any completion other than success fails the shard.
new_scenario failure
respond 200 '' "$jobs_list"
respond 200 'etag: "a"\r\n' "$failure"
expect_exit 1
expect_output 'did not succeed: completed failure'

# Transient errors and a body that is not JSON only delay the next poll, and a
# bad body does not keep its ETag.
new_scenario transient
respond 502 '' 'bad gateway'
respond 200 '' "$jobs_list"
respond 500 '' ''
respond 200 'etag: "bad"\r\n' 'not json'
respond 200 'etag: "a"\r\n' "$success"
expect_exit 0
expect_request 5 'https://api.test/jobs/2|'

# A near rate-limit reset is waited out.
new_scenario near-rate-limit
respond 200 '' "$jobs_list"
respond 429 'retry-after: 1\r\n' ''
respond 200 'etag: "a"\r\n' "$success"
expect_exit 0
expect_output 'rate limit hit'

# A reset that already passed is a short pause, not a negative sleep.
new_scenario past-rate-limit-reset
respond 200 '' "$jobs_list"
respond 403 "x-ratelimit-remaining: 0\r\nx-ratelimit-reset: $(($(date +%s) - 30))\r\n" '{"message":"API rate limit exceeded"}'
respond 200 'etag: "a"\r\n' "$success"
expect_exit 0
expect_output 'waiting 1 s'

# A rate-limit wait during the lookup does not count against its deadline.
new_scenario lookup-rate-limit
respond 429 'retry-after: 3\r\n' ''
respond 200 '' "$jobs_list"
respond 200 'etag: "a"\r\n' "$success"
lookup_seconds=2 expect_exit 0

# The search starts at this run's attempt: a full re-run waits for its own
# build.
new_scenario current-attempt
respond 200 '' '{"jobs":[{"name":"backend build","url":"https://api.test/jobs/9"}]}'
respond 200 'etag: "a"\r\n' "$success"
run_attempt=2 expect_exit 0
expect_request 1 'https://api.test/repos/owner/repo/actions/runs/42/attempts/2/jobs?per_page=100|'
expect_request 2 'https://api.test/jobs/9|'

# A re-run of only the failed shards falls back to the earlier attempt's build.
new_scenario earlier-attempt
respond 200 '' '{"jobs":[{"name":"backend test 1 of 8","url":"https://api.test/jobs/8"}]}'
respond 200 '' "$jobs_list"
respond 200 'etag: "a"\r\n' "$success"
run_attempt=2 expect_exit 0
expect_request 2 'https://api.test/repos/owner/repo/actions/runs/42/attempts/1/jobs?per_page=100|'
expect_request 3 'https://api.test/jobs/2|'

# An API error during the search retries it from this attempt, rather than
# falling back to an older attempt.
new_scenario search-error
respond 502 '' ''
respond 200 '' '{"jobs":[{"name":"backend build","url":"https://api.test/jobs/9"}]}'
respond 200 'etag: "a"\r\n' "$success"
run_attempt=2 expect_exit 0
expect_request 2 'https://api.test/repos/owner/repo/actions/runs/42/attempts/2/jobs?per_page=100|'

# A far rate-limit reset fails at once with a clear message.
new_scenario far-rate-limit
respond 200 '' "$jobs_list"
respond 403 "x-ratelimit-remaining: 0\r\nx-ratelimit-reset: $(($(date +%s) + 3000))\r\n" '{"message":"API rate limit exceeded"}'
expect_exit 1
expect_output 'rate limit resets in'

# A 403 that is not a rate limit is a permission error and fails at once.
new_scenario forbidden
respond 403 'x-ratelimit-remaining: 900\r\n' '{"message":"Resource not accessible by integration"}'
expect_exit 1
expect_output 'actions: read'

# A run without the job fails once the lookup deadline passes.
new_scenario missing-job
respond 200 '' '{"jobs":[{"name":"backend lint","run_attempt":1,"url":"https://api.test/jobs/1"}]}'
lookup_seconds=0 expect_exit 1
expect_output "no job named 'backend build'"

echo "wait-for-job invariants passed"
