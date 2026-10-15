#!/usr/bin/env bash
# rain-math-float is a path dependency. The dev shell links .tmp/rain-math-float
# into the Nix store, where every file has mtime 1. Cargo fingerprints path
# crates by mtime, so with a persistent target directory it would keep the
# artifacts built from an older flake input. This cleans them whenever the
# linked store path changes, and records the path it cleaned for.
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
target_dir=${CARGO_TARGET_DIR:?CARGO_TARGET_DIR must be set}
case "$target_dir" in
  /*) ;;
  *) target_dir="$repo_root/$target_dir" ;;
esac

source_path=$(readlink -e "$repo_root/.tmp/rain-math-float")
stamp="$target_dir/.rain-math-float-source"

if [ -f "$stamp" ] && [ "$(cat "$stamp")" = "$source_path" ]; then
  echo "rain-math-float source unchanged: $source_path"
  exit 0
fi

echo "rain-math-float source is now $source_path; cleaning its artifacts"
(cd "$repo_root" && cargo clean -p rain-math-float)
mkdir -p "$target_dir"
printf '%s\n' "$source_path" > "$stamp"
