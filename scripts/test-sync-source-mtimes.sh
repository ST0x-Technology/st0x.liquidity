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
# The real target directory is ignored, so the manifest is never a tracked file.
printf 'target/\n' > "$fixture/.gitignore"
mkdir -p "$fixture/migrations" "$fixture/nix"
printf '[workspace]\n' > "$fixture/Cargo.toml"
printf '{}\n' > "$fixture/nix/abi.nix"
printf 'create table a (x);\n' > "$fixture/migrations/1_a.sql"
# A leading dash must not turn a tracked path into a touch option.
printf 'dash\n' > "$fixture/-dash.txt"
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
sync record
chmod +x "$fixture/src/unchanged.rs"
git -C "$fixture" add -A
git -C "$fixture" commit -qm mode
sync prepare
expect_new src/unchanged.rs
expect_old src/edited.rs

# A content change with the same size and an old mtime is still caught.
sync record
printf 'b3\n' > "$fixture/src/edited.rs"
touch -d @946684800 "$fixture/src/edited.rs"
git -C "$fixture" add -A
git -C "$fixture" commit -qm same-size
sync prepare
expect_new src/edited.rs
expect_old src/added.rs

# prepare consumes the record: a build that fails after it records nothing,
# so the next prepare must treat every source as changed.
sync prepare
expect_new src/added.rs

# A fresh disk holds a root-owned lost+found that the runner user cannot read.
# prepare must still work with one on the disk.
mkdir -p "$fixture/target/test/lost+found"
chmod 000 "$fixture/target/test/lost+found"
trap 'chmod 700 "$fixture/target/test/lost+found"; rm -rf "$fixture"' EXIT

# A build that ran after the record (for example, a re-run of a run from
# before this script) leaves newer fingerprints, so every source counts as
# changed. Fingerprints older than the record keep the partial sync.
fingerprint="$fixture/target/test/debug/.fingerprint/w-1/lib-w"
mkdir -p "$(dirname "$fingerprint")"
touch -d @946684800 "$fingerprint"
sync record
(cd "$fixture" && git ls-files -z | xargs -0 touch -h -d @946684800 --)
touch -d "@$(($(date +%s) + 60))" "$fingerprint"
sync prepare
expect_new src/unchanged.rs
touch -d @946684800 "$fingerprint"
sync record
(cd "$fixture" && git ls-files -z | xargs -0 touch -h -d @946684800 --)
sync prepare
expect_old src/unchanged.rs

# A changed, added, or deleted build-wide input marks every source changed,
# even one whose content matches the record. Other paths keep the partial sync.
commit_change() {
  sync prepare
  sync record
  (cd "$fixture" && git ls-files -z | xargs -0 touch -h -d @946684800 --)
  "$@"
  git -C "$fixture" add -A
  git -C "$fixture" commit -qm "$*"
  sync prepare
}

write_file() {
  mkdir -p "$(dirname "$fixture/$1")"
  printf '%s\n' "$RANDOM$RANDOM" > "$fixture/$1"
}

delete_file() {
  rm "$fixture/$1"
}

expect_all_new_after() {
  commit_change "$@"
  expect_new src/unchanged.rs
  expect_new src/added.rs
  expect_new -dash.txt
}

expect_partial_after() {
  commit_change "$@"
  expect_old src/unchanged.rs
  expect_new "$2"
}

expect_all_new_after write_file Cargo.toml
expect_all_new_after write_file crates/x/Cargo.toml
expect_all_new_after write_file Cargo.lock
expect_all_new_after write_file flake.lock
expect_all_new_after write_file rust-toolchain.toml
expect_all_new_after write_file .cargo/config.toml
expect_all_new_after write_file nix/abi.nix
expect_all_new_after write_file migrations/2_b.sql
expect_all_new_after delete_file migrations/1_a.sql
expect_all_new_after write_file .sqlx/query-1.json
expect_all_new_after write_file .github/workflows/ci.yaml
expect_all_new_after write_file scripts/rustc-parallel-frontend.sh
expect_all_new_after write_file scripts/clippy-ci.sh
expect_all_new_after write_file src/build.rs
expect_all_new_after delete_file src/build.rs
expect_partial_after write_file src/Cargo.toml.rs
expect_partial_after write_file docs/migrations/notes.md
expect_partial_after write_file scripts/other.sh

if (cd / && CARGO_TARGET_DIR=target/test "$fixture/scripts/sync-source-mtimes.sh" bogus > /dev/null 2>&1); then
  echo "an unknown mode must fail" >&2
  exit 1
fi

echo "source mtime sync invariants passed"
