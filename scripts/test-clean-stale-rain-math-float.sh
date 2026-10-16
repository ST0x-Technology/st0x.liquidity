#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
mkdir -p "$repo_root/.tmp"
fixture=$(mktemp -d "$repo_root/.tmp/clean-float-test.XXXXXX")
trap 'rm -rf "$fixture"' EXIT
mkdir -p "$fixture/scripts" "$fixture/.tmp" "$fixture/bin" "$fixture/store-a" "$fixture/store-b"
cp "$repo_root/scripts/clean-stale-rain-math-float.sh" "$fixture/scripts/"

cat > "$fixture/bin/cargo" <<'CARGO'
#!/usr/bin/env bash
set -euo pipefail
if [ "$PWD" != "$FIXTURE_ROOT" ]; then
  echo "cargo must run from the repository root, ran from $PWD" >&2
  exit 1
fi
echo "$*" >> "$FIXTURE_ROOT/cargo.log"
if [ "${FAIL_CARGO:-}" = 1 ]; then
  exit 1
fi
CARGO
chmod +x "$fixture/bin/cargo"

run_clean() {
  (cd / && FIXTURE_ROOT="$fixture" PATH="$fixture/bin:$PATH" CARGO_TARGET_DIR=target/test \
    "$fixture/scripts/clean-stale-rain-math-float.sh" > /dev/null)
}

cargo_calls() {
  if [ -f "$fixture/cargo.log" ]; then
    wc -l < "$fixture/cargo.log" | tr -d ' '
  else
    echo 0
  fi
}

expect_calls() {
  local expected=$1 context=$2
  if [ "$(cargo_calls)" != "$expected" ]; then
    echo "$context: expected $expected cargo clean calls, got $(cargo_calls)" >&2
    exit 1
  fi
}

ln -sfn "$fixture/store-a" "$fixture/.tmp/rain-math-float"

run_clean
expect_calls 1 "an empty target directory"
if [ "$(cat "$fixture/cargo.log")" != "clean -p rain-math-float" ]; then
  echo "unexpected cargo invocation: $(cat "$fixture/cargo.log")" >&2
  exit 1
fi
if [ "$(cat "$fixture/target/test/.rain-math-float-source")" != "$fixture/store-a" ]; then
  echo "the stamp must record the linked store path" >&2
  exit 1
fi

run_clean
expect_calls 1 "an unchanged source"

ln -sfn "$fixture/store-b" "$fixture/.tmp/rain-math-float"
run_clean
expect_calls 2 "a changed source"

# A failed clean must leave the old stamp, so the next run cleans again.
ln -sfn "$fixture/store-a" "$fixture/.tmp/rain-math-float"
if FAIL_CARGO=1 run_clean 2> /dev/null; then
  echo "a failed cargo clean must fail the script" >&2
  exit 1
fi
if [ "$(cat "$fixture/target/test/.rain-math-float-source")" != "$fixture/store-b" ]; then
  echo "a failed clean must not update the stamp" >&2
  exit 1
fi
run_clean
expect_calls 4 "a retry after a failed clean"

# A missing link must fail rather than skip the clean.
rm "$fixture/.tmp/rain-math-float"
if run_clean 2> /dev/null; then
  echo "a missing rain-math-float link must fail the script" >&2
  exit 1
fi

echo "rain-math-float stale-artifact cleanup invariants passed"
