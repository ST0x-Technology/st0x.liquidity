#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
mkdir -p "$repo_root/.tmp"
fixture=$(mktemp -d "$repo_root/.tmp/audit-test.XXXXXX")
trap 'rm -rf "$fixture"' EXIT
mkdir -p "$fixture/.cargo" "$fixture/bin" "$fixture/float"
cp "$repo_root/.cargo/audit.toml" "$fixture/.cargo/audit.toml"

cp "$repo_root/Cargo.lock" "$fixture/Cargo.lock"

cat > "$fixture/bin/cargo" <<'CARGO'
#!/usr/bin/env bash
set -euo pipefail

if [[ "$1" == tree ]]; then
  arguments=" $* "
  for required in --locked --workspace --all-features '--target all' '--edges normal,build,dev'; do
    if [[ "$arguments" != *" $required "* ]]; then
      echo "missing dependency coverage: $required" >&2
      exit 1
    fi
  done

  while [[ "$1" != -i ]]; do shift; done
  dependency=$2
  if [[ "${TEST_AUDIT_CASE:-}" == tree_failure ]]; then
    exit 1
  fi

  case "$dependency" in
    rsa@*)
      if [[ "${TEST_AUDIT_CASE:-}" == direct_rsa ]]; then
        printf '%s\n' 'rsa v0.9.10' 'st0x-hedge v0.1.0'
      fi
      ;;
    tracing-subscriber@* | derivative@*) ;;
    h2@*)
      printf '%s\n' 'h2 v0.3.27' 'hyper v0.14.32' 'reqwest v0.11.27'
      if [[ "${TEST_AUDIT_CASE:-}" == new_parent ]]; then
        printf '%s\n' 'another-http-client v1.0.0'
      fi
      ;;
    paste@*)
      printf '%s\n' 'alloy-primitives v1.6.0' 'ark-ff v0.5.0' \
        'paste v1.0.15 (proc-macro)' 'syn-solidity v1.5.7' 'wasm-bindgen-utils v0.1.2'
      ;;
    proc-macro-error2@*)
      printf '%s\n' 'alloy-sol-macro v1.5.7 (proc-macro)' \
        'alloy-sol-macro-expander v1.5.7' 'proc-macro-error2 v2.0.1'
      ;;
    rustls-pemfile@*) printf '%s\n' 'reqwest v0.11.27' 'rustls-pemfile v1.0.4' ;;
    lru@*) printf '%s\n' 'alloy-provider v1.6.3' 'lru v0.16.4' ;;
    scc@*) printf '%s\n' 'scc v2.4.0' 'serial_test v3.4.0' ;;
    spin@*) printf '%s\n' 'flume v0.11.1' 'flume v0.12.0' 'spin v0.9.8' ;;
    *) echo "unexpected dependency: $dependency" >&2; exit 1 ;;
  esac
elif [[ "$1" == audit ]]; then
  if [[ "${TEST_AUDIT_CASE:-}" == audit_failure ]]; then
    printf '%s\n' '{"vulnerabilities":{"found":true}}'
    exit 1
  fi

  if [[ "${TEST_AUDIT_CASE:-}" == new_yank ]]; then
    printf '%s\n' '{"warnings":{"yanked":[{"package":{"name":"spin","version":"0.9.8"}},{"package":{"name":"another-crate","version":"1.0.0"}}]}}'
  else
    printf '%s\n' '{"warnings":{"yanked":[{"package":{"name":"spin","version":"0.9.8"}}]}}'
  fi
else
  echo "unexpected cargo command: $1" >&2
  exit 1
fi
CARGO
chmod +x "$fixture/bin/cargo"

run_audit() {
  (
    cd "$fixture"
    PATH="$fixture/bin:$PATH" RAIN_MATH_FLOAT_SOURCE="$fixture/float" \
      TEST_AUDIT_CASE="$1" bash "$repo_root/scripts/audit-rust.sh"
  )
}

run_audit baseline

assert_rejected() {
  local scenario=$1
  local expected=$2

  if run_audit "$scenario" > "$fixture/result" 2>&1; then
    echo "audit unexpectedly accepted $scenario" >&2
    exit 1
  fi

  if ! grep -Fq "$expected" "$fixture/result"; then
    cat "$fixture/result" >&2
    echo "missing failure context for $scenario: $expected" >&2
    exit 1
  fi
}

assert_rejected direct_rsa 'Unexpected dependency paths for rsa@0.9.10'
assert_rejected new_parent 'Unexpected dependency paths for h2@0.3.27'
assert_rejected tree_failure 'Unable to inspect dependency paths for rsa@0.9.10'
assert_rejected audit_failure '"found": true'
assert_rejected new_yank 'another-crate 1.0.0'

cat >> "$fixture/Cargo.lock" <<'LOCK'

[[package]]
name = "rsa"
version = "0.8.0"
LOCK
assert_rejected baseline 'Unexpected locked versions for rsa'
cp "$repo_root/Cargo.lock" "$fixture/Cargo.lock"

sed '/^ignore = \[/a\
  "RUSTSEC-2099-9999",' "$fixture/.cargo/audit.toml" > "$fixture/changed-policy"
mv "$fixture/changed-policy" "$fixture/.cargo/audit.toml"
assert_rejected baseline 'Unexpected ignored advisories'
cp "$repo_root/.cargo/audit.toml" "$fixture/.cargo/audit.toml"

sed 's/deny = \["unmaintained", "unsound"\]/deny = []/' \
  "$fixture/.cargo/audit.toml" > "$fixture/changed-policy"
mv "$fixture/changed-policy" "$fixture/.cargo/audit.toml"
assert_rejected baseline 'Missing required cargo-audit policy: deny'

echo 'Rust dependency audit regression tests passed.'
