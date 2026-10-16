#!/usr/bin/env bash
# RUSTC_WORKSPACE_WRAPPER for CI test builds: runs rustc's parallel frontend
# on workspace crates only. The `st0x-hedge` library and its unit-test harness
# dominate the build, and their type checking runs on one core by default;
# with one frontend thread per core a cold workspace build took 140 s instead
# of 221 s. `-Zthreads` is unstable, so RUSTC_BOOTSTRAP enables it on the
# stable toolchain, for these invocations only. Dependencies build unchanged.
#
# Cargo also sends its target-info probe (`--print=cfg` and friends) and its
# version queries through this wrapper. With RUSTC_BOOTSTRAP the probe reports
# unstable cfgs, and cargo hands that cfg set to every package's build script
# and `cfg(..)` dependency table. So only crate compilations get the flags.
#
# The parallel frontend is not used for incremental compilations: with
# `-C incremental` on rustc 1.94 it crashed in the dependency graph
# (rustc_query_system/src/dep_graph, CI run 38863026755), and only
# non-incremental runs were measured.
set -euo pipefail

rustc=$1
shift

compiles_crate=false
for arg in "$@"; do
  case $arg in
    --print | --print=*) exec "$rustc" "$@" ;;
    incremental=* | -Cincremental=*) exec "$rustc" "$@" ;;
    --crate-name) compiles_crate=true ;;
  esac
done

if [ "$compiles_crate" = false ]; then
  exec "$rustc" "$@"
fi

RUSTC_BOOTSTRAP=1 exec "$rustc" "$@" "-Zthreads=$(nproc)"
