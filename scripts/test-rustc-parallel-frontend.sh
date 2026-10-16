#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT

cat > "$fixture/rustc" <<'RUSTC'
#!/usr/bin/env bash
printf '%s\n' "RUSTC_BOOTSTRAP=${RUSTC_BOOTSTRAP:-unset}" "$@" > "$FIXTURE/args"
RUSTC
chmod +x "$fixture/rustc"

# Runs the wrapper with the fake rustc and compares what rustc received.
expect_rustc_args() {
  local description=$1
  shift
  local expected=$1
  shift
  (unset RUSTC_BOOTSTRAP && FIXTURE="$fixture" "$repo_root/scripts/rustc-parallel-frontend.sh" "$fixture/rustc" "$@")
  if [ "$(cat "$fixture/args")" != "$expected" ]; then
    echo "$description, rustc got:" >&2
    cat "$fixture/args" >&2
    exit 1
  fi
}

expect_rustc_args "a crate compilation must get -Zthreads and RUSTC_BOOTSTRAP, with its arguments unchanged" \
  "$(printf '%s\n' "RUSTC_BOOTSTRAP=1" --crate-name x 'a b' "-Zthreads=$(nproc)")" \
  --crate-name x 'a b'

# Cargo's target-info probe also goes through the wrapper. With RUSTC_BOOTSTRAP
# it reports unstable cfgs that cargo then gives to every package.
expect_rustc_args "the target-info probe must reach rustc unchanged" \
  "$(printf '%s\n' "RUSTC_BOOTSTRAP=unset" - --crate-name ___ --print=file-names --print=cfg)" \
  - --crate-name ___ --print=file-names --print=cfg

expect_rustc_args "a separate --print argument must also reach rustc unchanged" \
  "$(printf '%s\n' "RUSTC_BOOTSTRAP=unset" - --crate-name ___ --print cfg)" \
  - --crate-name ___ --print cfg

# The parallel frontend crashed rustc's incremental dependency graph.
expect_rustc_args "an incremental compilation must reach rustc unchanged" \
  "$(printf '%s\n' "RUSTC_BOOTSTRAP=unset" --crate-name x -C incremental=/tmp/inc)" \
  --crate-name x -C incremental=/tmp/inc

expect_rustc_args "a joined -Cincremental must also reach rustc unchanged" \
  "$(printf '%s\n' "RUSTC_BOOTSTRAP=unset" --crate-name x -Cincremental=/tmp/inc)" \
  --crate-name x -Cincremental=/tmp/inc

expect_rustc_args "a version query must reach rustc unchanged" \
  "$(printf '%s\n' "RUSTC_BOOTSTRAP=unset" -vV)" \
  -vV

echo "rustc parallel frontend wrapper invariants passed"
