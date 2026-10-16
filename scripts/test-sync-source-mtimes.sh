#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
mkdir -p "$repo_root/.tmp"
fixture=$(mktemp -d "$repo_root/.tmp/sync-mtimes-test.XXXXXX")
trap 'rm -rf "$fixture"' EXIT

mkdir -p "$fixture/scripts" "$fixture/src"
cp "$repo_root/scripts/sync-source-mtimes.sh" "$fixture/scripts/"
git -C "$fixture" init -q
git -C "$fixture" config user.email test@example.com
git -C "$fixture" config user.name test
printf 'a\n' > "$fixture/src/unchanged.rs"
printf 'b\n' > "$fixture/src/edited.rs"
printf 'c\n' > "$fixture/src/removed.rs"
git -C "$fixture" add -A
git -C "$fixture" commit -qm base

sync() {
  (cd / && CARGO_TARGET_DIR=target/test "$fixture/scripts/sync-source-mtimes.sh" "$1" > /dev/null)
}

mtime() {
  stat -c %Y "$fixture/$1"
}

expect_old() {
  if [ "$(mtime "$1")" != 946684800 ]; then
    echo "$1 should keep the old mtime, got $(mtime "$1")" >&2
    exit 1
  fi
}

expect_new() {
  if [ "$(mtime "$1")" -le 946684800 ]; then
    echo "$1 should be newer than the recorded build, got $(mtime "$1")" >&2
    exit 1
  fi
}

# Without a recorded tree every source must count as changed.
touch -d @946684800 "$fixture/src/unchanged.rs"
sync prepare
expect_new src/unchanged.rs
if [ -e "$fixture/target/test/.source-manifest" ]; then
  echo "prepare must not record a tree" >&2
  exit 1
fi

sync record
if [ ! -s "$fixture/target/test/.source-manifest" ]; then
  echo "record must write the manifest under CARGO_TARGET_DIR" >&2
  exit 1
fi

# The next commit edits one file, removes one, and adds one.
printf 'b2\n' > "$fixture/src/edited.rs"
git -C "$fixture" rm -q src/removed.rs
printf 'd\n' > "$fixture/src/added.rs"
git -C "$fixture" add -A
git -C "$fixture" commit -qm next

sync prepare
expect_old src/unchanged.rs
expect_new src/edited.rs
expect_new src/added.rs

# A mode change alone is a change.
chmod +x "$fixture/src/unchanged.rs"
git -C "$fixture" add -A
git -C "$fixture" commit -qm mode
sync prepare
expect_new src/unchanged.rs

# A content change with the same size and an old mtime is still caught.
sync record
printf 'x\n' > "$fixture/src/edited.rs"
touch -d @946684800 "$fixture/src/edited.rs"
git -C "$fixture" add -A
git -C "$fixture" commit -qm same-size
sync prepare
expect_new src/edited.rs
expect_old src/added.rs

if (cd / && CARGO_TARGET_DIR=target/test "$fixture/scripts/sync-source-mtimes.sh" bogus > /dev/null 2>&1); then
  echo "an unknown mode must fail" >&2
  exit 1
fi

echo "source mtime sync invariants passed"
