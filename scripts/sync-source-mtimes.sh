#!/usr/bin/env bash
# Keeps cargo's mtime fingerprints honest on a persistent target directory.
#
#   record   after a successful build: store the git content hashes of the
#            tree that build compiled.
#   prepare  before a build: give every tracked file an old fixed mtime, then
#            the current time to each file whose content hash differs from the
#            recorded tree. Every file gets the current time when nothing was
#            recorded, or when a build-wide input changed (see below).
#
# Cargo treats a path crate as fresh when none of its sources is newer than its
# last build output. A fresh checkout makes every file new, so without this
# cargo reruns rustc for every workspace crate. With it, only crates with a
# changed, added, or deleted source rebuild. Content hashes, not checkout
# times, decide what changed, so a changed file can never look fresh.
#
# Some inputs are not crate sources in cargo's dep-info, so a changed file
# there cannot mark the right crate stale:
#   - Cargo.lock, Cargo.toml, toolchain, and cargo config changes select another
#     artifact variant, and the disk may still hold an old variant whose sources
#     look older than its output.
#   - Nix files and flake.lock pick the ABI paths that `sol!` reads from
#     untracked env vars.
#   - `sqlx::migrate!` lists only the migrations it saw at its last expansion,
#     and offline `query!` data in .sqlx/ is read without dep-info tracking.
#   - The cargo flags in ci.yaml, and adding or removing a build.rs, select an
#     artifact variant too.
# A change to any of them touches every file, as with no recorded tree.
#
# The record describes the disk only if it was the last write after a build.
# prepare consumes the record, so a build that fails after it but still leaves
# its artifacts on a saved disk makes the next prepare touch every file. A
# build that does not use this script (a re-run of a run from before it, or a
# revert) leaves fingerprints newer than the record, and those also make
# prepare touch every file.
set -euo pipefail

old_mtime=@946684800
build_wide_inputs='(^|/)(Cargo\.toml|Cargo\.lock|flake\.lock|rust-toolchain(\.toml)?|build\.rs)$|^\.cargo/|\.nix$|^migrations/|^\.sqlx/|^\.github/workflows/ci\.yaml$'

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
    recorded=$(mktemp)
    changed=$(mktemp)
    trap 'rm -f "$recorded" "$changed"' EXIT
    if [ ! -f "$manifest" ]; then
      echo "no recorded source tree in $target_dir; every source counts as changed"
      git ls-files -z | xargs -0 -r touch -h --
      exit 0
    fi
    # Unit fingerprints live in <target>/[<triple>/]<profile>/.fingerprint.
    newer_build=$(find "$target_dir" -maxdepth 3 -type d -name .fingerprint \
      -exec find {} -maxdepth 2 -newer "$manifest" -print -quit \;)
    sort -z "$manifest" > "$recorded"
    rm -f "$manifest"
    if [ -n "$newer_build" ]; then
      echo "the disk holds a build newer than the recorded tree; every source counts as changed"
      git ls-files -z | xargs -0 -r touch -h --
      exit 0
    fi
    # Records are "<mode> <blob> <stage>\t<path>". Records on either side only
    # are changed, added, or deleted files.
    comm -z -3 <(git ls-files -s -z | sort -z) "$recorded" |
      sed -z 's/^\t//' | cut -z -f2- | tr '\0' '\n' > "$changed"
    if grep -Eq "$build_wide_inputs" "$changed"; then
      echo "a build-wide input changed since the recorded tree; every source counts as changed"
      git ls-files -z | xargs -0 -r touch -h --
      exit 0
    fi
    git ls-files -z | xargs -0 -r touch -h -d "$old_mtime" --
    # A record that is in the tree but not in the manifest is an added file or
    # a changed mode or content.
    comm -z -23 <(git ls-files -s -z | sort -z) "$recorded" |
      cut -z -f2- > "$changed"
    xargs -0 -r touch -h -- < "$changed"
    echo "sources changed since the recorded tree: $(tr -cd '\0' < "$changed" | wc -c)"
    ;;
  *)
    echo "usage: $0 record|prepare" >&2
    exit 2
    ;;
esac
