#!/usr/bin/env python3
"""Writes the expected exporter output for the liq_* golden tests.

    python3 -I scripts/liq-parity/golden.py <exporter.py> [--check]

<exporter.py> is the exporter sidecar's source from a local t0.devops
checkout; this repository does not carry a copy. For each case, the script
loads that file as a module, replaces its bot HTTP and WebSocket reads with
the committed fixture, runs one collector, and writes the registry render to
src/metrics/liquidity/testdata/<case>.prom. The Rust golden tests compare
the bot's builders against those committed files, so CI needs no exporter.

With --check, nothing is written: the script fails if a committed file
differs from what the given exporter produces.

The script refuses an exporter.py whose SHA-256 is not EXPORTER_SHA256, so
the revision written into the headers is the one that produced them. When
the exporter changes, update EXPORTER_REVISION and EXPORTER_SHA256 together
(`git show <rev>:terraform/liquidity-exporter/exporter.py | sha256sum`),
regenerate, and review the .prom diff: every change is a change to the
published liq_* contract.
"""

import hashlib
import importlib.util
import json
import os
import sys

EXPORTER_REVISION = "226c029"
EXPORTER_SHA256 = "95a894a6671b300a4536f94e54a4a933bd49b4e7ddb2619a0c27d4438cfd4d2f"

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TESTDATA = os.path.join(REPO, "src", "metrics", "liquidity", "testdata")


def run_state(exporter, fixture):
    """The settings part of the WebSocket current_state seed."""
    exporter.ws_seed_current_state = lambda timeout=20: {"settings": fixture}
    exporter.collect_state()


def run_full_state(exporter, fixture):
    """The whole WebSocket current_state seed: settings, inventory,
    positions and equity prices."""
    exporter.ws_seed_current_state = lambda timeout=20: fixture
    exporter.collect_state()


def serve(fixture_by_path):
    """A bot_get stub that answers each path from the fixture."""
    def bot_get(path, params=None, timeout=15):
        return fixture_by_path[path]
    return bot_get


def run_latencies(exporter, fixture):
    exporter.bot_get = serve({"/performance/latencies": fixture})
    exporter.collect_latencies("from", "to")


def run_reliability(exporter, fixture):
    exporter.bot_get = serve({"/performance/reliability": fixture})
    exporter.collect_reliability("from", "to")


def run_infra(exporter, fixture):
    exporter.bot_get = serve({"/performance/infra": fixture})
    exporter.collect_infra("from", "to")


def run_rebalance(exporter, fixture):
    """The fixture holds both reports: {"usdc": ..., "equity": ...}."""
    exporter.bot_get = serve({
        "/performance/rebalances": fixture["usdc"],
        "/performance/equity-rebalances": fixture["equity"],
    })
    exporter.collect_rebalance_timings("from", "to")


def run_pnl(exporter, fixture):
    """Every /pnl call, the range probe and each window, returns the
    fixture, so each window publishes the same report under its own
    window label."""
    def bot_get(path, params=None, timeout=15):
        if path != "/pnl":
            raise RuntimeError(f"unexpected bot read {path}")
        return fixture

    exporter.bot_get = bot_get
    exporter.collect_pnl()


def run_pnl_window_all(exporter, fixture):
    """One report as the `all` window, through the exporter's two PnL
    builders, as collect_pnl combines them."""
    exporter.REGISTRY.set_family(
        "pnl",
        exporter.pnl_samples(fixture, "all") + exporter.pnl_day_samples(fixture, "all"))


class NoCloudLogging:
    """collect_orders also ships the Raindex orders as log rows."""

    def write_entries(self, log_name, entries):
        pass


def run_orders(exporter, fixture):
    """/orders/pending and /orders/raindex answer from the fixture."""
    def bot_get(path, params=None, timeout=15):
        if path == "/orders/pending":
            return fixture["pending"]
        if path == "/orders/raindex":
            return fixture["raindex"]
        raise RuntimeError(f"unexpected bot read {path}")

    exporter.bot_get = bot_get
    exporter.collect_orders(NoCloudLogging())


# case name -> how the fixture reaches the exporter
CASES = {
    "settings": run_state,
    "settings-minimal": run_state,
    "state": run_full_state,
    "state-reserved": run_full_state,
    "latencies": run_latencies,
    "reliability": run_reliability,
    "infra": run_infra,
    "rebalance": run_rebalance,
    # PnL windows and day buckets
    "pnl": run_pnl,
    "pnl-days": run_pnl_window_all,
    # pending and Raindex orders
    "orders": run_orders,
    "orders-unavailable": run_orders,
}


def load_exporter(path):
    """A fresh module per case, so each case starts with an empty registry."""
    spec = importlib.util.spec_from_file_location("liq_parity_exporter", path)
    if spec is None or spec.loader is None:
        raise SystemExit(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    def no_bot(*args, **kwargs):
        raise RuntimeError("golden cases must not reach a bot")

    module.bot_get = no_bot
    module.ws_seed_current_state = no_bot
    return module


def render_case(exporter_path, case, run):
    with open(os.path.join(TESTDATA, f"{case}.json")) as f:
        fixture = json.load(f)
    exporter = load_exporter(exporter_path)
    run(exporter, fixture)
    header = (
        f"# Expected exporter output for {case}.json, written by\n"
        f"# scripts/liq-parity/golden.py from t0.devops revision {EXPORTER_REVISION}.\n"
    )
    return header + exporter.REGISTRY.render()


def main(argv):
    if len(argv) not in (1, 2) or (len(argv) == 2 and argv[1] != "--check"):
        raise SystemExit(f"usage: {sys.argv[0]} <exporter.py> [--check]")
    exporter_path, check = argv[0], len(argv) == 2
    with open(exporter_path, "rb") as f:
        digest = hashlib.sha256(f.read()).hexdigest()
    if digest != EXPORTER_SHA256:
        raise SystemExit(
            f"{exporter_path} is not t0.devops revision {EXPORTER_REVISION} "
            f"(sha256 {digest}); check out that revision or update "
            "EXPORTER_REVISION and EXPORTER_SHA256 together")

    stale = []
    for case, run in CASES.items():
        rendered = render_case(exporter_path, case, run)
        path = os.path.join(TESTDATA, f"{case}.prom")
        if check:
            committed = open(path).read() if os.path.exists(path) else None
            if committed != rendered:
                stale.append(path)
        else:
            with open(path, "w") as f:
                f.write(rendered)
            print(f"wrote {path}")

    for path in stale:
        print(f"{path} differs from the exporter output", file=sys.stderr)
    if stale:
        raise SystemExit("golden files are stale: rerun without --check")
    if check:
        print(f"{len(CASES)} golden files match the exporter")


if __name__ == "__main__":
    main(sys.argv[1:])
