#!/usr/bin/env bash
set -euo pipefail

: "${RAIN_MATH_FLOAT_SOURCE:?ci-audit must provide RAIN_MATH_FLOAT_SOURCE}"
mkdir -p .tmp
rm -rf .tmp/rain-math-float
ln -sfn "$RAIN_MATH_FLOAT_SOURCE" .tmp/rain-math-float

assert_config_line() {
  local expected="$1"

  if ! grep -Fqx "$expected" .cargo/audit.toml; then
    printf 'Missing required cargo-audit policy: %s\n' "$expected" >&2
    exit 1
  fi
}

for required_policy in \
  '[database]' \
  'url = "https://github.com/RustSec/advisory-db"' \
  'fetch = true' \
  'stale = false' \
  '[yanked]' \
  'enabled = true' \
  'update_index = true'; do
  assert_config_line "$required_policy"
done

assert_tree() {
  local dependency="$1"
  shift

  local actual
  actual="$(cargo tree --workspace -i "$dependency" --target all --edges normal,dev --depth 1 --prefix none | sort -u)"

  local expected
  expected="$(printf '%s\n' "$@" | sort -u)"

  if [[ "$actual" != "$expected" ]]; then
    printf 'Unexpected dependency paths for %s\nExpected:\n%s\nActual:\n%s\n' \
      "$dependency" "$expected" "$actual" >&2
    exit 1
  fi
}

assert_tree "rsa@0.9.10"
assert_tree "tracing-subscriber@0.2.25"
assert_tree "derivative@2.2.0"
assert_tree "h2@0.3.27" \
  "h2 v0.3.27" \
  "hyper v0.14.32" \
  "reqwest v0.11.27"
assert_tree "paste@1.0.15" \
  "alloy-primitives v1.6.0" \
  "ark-ff v0.5.0" \
  "paste v1.0.15 (proc-macro)" \
  "syn-solidity v1.5.7" \
  "wasm-bindgen-utils v0.1.2"
assert_tree "proc-macro-error2@2.0.1" \
  "alloy-sol-macro v1.5.7 (proc-macro)" \
  "alloy-sol-macro-expander v1.5.7" \
  "proc-macro-error2 v2.0.1"
assert_tree "rustls-pemfile@1.0.4" \
  "reqwest v0.11.27" \
  "rustls-pemfile v1.0.4"
assert_tree "lru@0.16.4" \
  "alloy-provider v1.6.3" \
  "lru v0.16.4"
assert_tree "scc@2.4.0" \
  "scc v2.4.0" \
  "serial_test v3.4.0"
assert_tree "spin@0.9.8" \
  "flume v0.11.1" \
  "flume v0.12.0" \
  "spin v0.9.8"

if ! audit_json="$(cargo audit --json)"; then
  printf '%s' "$audit_json" | jq . >&2
  exit 1
fi

yanked="$(printf '%s' "$audit_json" | jq -r '.warnings.yanked[] | [.package.name, .package.version] | join(" ")' | sort -u)"
if [[ "$yanked" != "spin 0.9.8" ]]; then
  printf 'Unexpected yanked dependencies:\n%s\n' "$yanked" >&2
  exit 1
fi
