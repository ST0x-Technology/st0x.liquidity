#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT

mkdir -p "$fixture/bin"
cat > "$fixture/bin/cargo" <<'CARGO'
#!/usr/bin/env bash
if [ "$1" = metadata ]; then
  printf '%s' '{"packages":[{"targets":[{"name":"st0x-hedge"},{"name":"e2e"}]},{"targets":[{"name":"st0x_cli"},{"name":"st0x-cli"}]}]}'
  exit 0
fi
printf '%s\n' "RUSTC_BOOTSTRAP=${RUSTC_BOOTSTRAP:-unset}" "$@" > "$FIXTURE/args"
CARGO
chmod +x "$fixture/bin/cargo"

(unset RUSTC_BOOTSTRAP && FIXTURE="$fixture" PATH="$fixture/bin:$PATH" "$repo_root/scripts/clippy-ci.sh")

expected=$(printf '%s\n' "RUSTC_BOOTSTRAP=e2e,st0x_cli,st0x_hedge" \
  clippy --workspace --all-targets --all-features -- "-Zthreads=$(nproc)")
if [ "$(cat "$fixture/args")" != "$expected" ]; then
  echo "clippy must run with RUSTC_BOOTSTRAP scoped to the workspace's crate names and -Zthreads, got:" >&2
  cat "$fixture/args" >&2
  exit 1
fi

echo "clippy CI wrapper invariants passed"
