#!/usr/bin/env bash
# Runs CI's clippy with rustc's parallel frontend on workspace crates.
#
# `cargo clippy` sets its own RUSTC_WORKSPACE_WRAPPER, so the build jobs'
# wrapper cannot add the flag here. Arguments after `--` reach clippy-driver for
# workspace crates only, and RUSTC_BOOTSTRAP set to a list of crate names lets
# only those crates use unstable flags, so dependencies and cargo's
# target-info probe (crate `___`) build as on stable. Cold, clippy took 136 s
# instead of 173 s locally. CI runs this without incremental compilation:
# with `-C incremental` the parallel frontend crashed rustc's dependency
# graph on 1.94.
set -euo pipefail

crates=$(cargo metadata --no-deps --format-version 1 | python3 -c '
import json, sys
names = {target["name"].replace("-", "_")
         for package in json.load(sys.stdin)["packages"]
         for target in package["targets"]}
print(",".join(sorted(names)))
')

RUSTC_BOOTSTRAP=$crates exec cargo clippy --workspace --all-targets --all-features -- "-Zthreads=$(nproc)"
