#!/usr/bin/env bash
# Keeps cargo's mtime fingerprints honest on a persistent target directory.
#
#   record   after a successful build: store the git content hashes of the
#            tree that build compiled.
#   prepare  before a build: give every tracked file an old fixed mtime, then
#            the current time to each file whose content hash differs from the
#            recorded tree (or every file, when nothing was recorded).
#
# Cargo treats a path crate as fresh when none of its sources is newer than its
# last build output. A fresh checkout makes every file new, so without this
# cargo reruns rustc for every workspace crate. With it, only crates with a
# changed, added, or deleted source rebuild. Content hashes, not checkout
# times, decide what changed, so a changed file can never look fresh.
set -euo pipefail

old_mtime=@946684800

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
target_dir=${CARGO_TARGET_DIR:?CARGO_TARGET_DIR must be set}
case "$target_dir" in
  /*) ;;
  *) target_dir="$repo_root/$target_dir" ;;
esac
manifest="$target_dir/.source-manifest"

cd "$repo_root"

case "${1:-}" in
  record)
    mkdir -p "$target_dir"
    git ls-files -s -z > "$manifest.tmp"
    mv "$manifest.tmp" "$manifest"
    echo "recorded the source tree for $target_dir"
    ;;
  prepare)
    if [ ! -f "$manifest" ]; then
      echo "no recorded source tree in $target_dir; every source counts as changed"
      git ls-files -z | xargs -0 -r touch -h
      exit 0
    fi
    git ls-files -z | xargs -0 -r touch -h -d "$old_mtime"
    changed=$(mktemp)
    trap 'rm -f "$changed"' EXIT
    # Records are "<mode> <blob> <stage>\t<path>". Any record not in the
    # manifest is an added file or a changed mode or content.
    comm -z -23 <(git ls-files -s -z | sort -z) <(sort -z "$manifest") |
      cut -z -f2- > "$changed"
    xargs -0 -r touch -h < "$changed"
    echo "sources changed since the recorded tree: $(tr -cd '\0' < "$changed" | wc -c)"
    ;;
  *)
    echo "usage: $0 record|prepare" >&2
    exit 2
    ;;
esac
