#!/usr/bin/env python3
"""Compares the bot's liq_* series with the exporter sidecar's.

    python3 scripts/liq-parity/compare.py [--drop-list] [--known-diffs] \\
        BOT.prom EXPORTER.prom [BOT.prom EXPORTER.prom ...]

Each pair is one snapshot of both /metrics bodies, read back to back (the
runbook in docs/observability.md reads both in one SSH command). The tool
does not check when a body was read. Even bodies read in the same second can
hold values read from the source at different times: the bot publishes each
family on its own refresh and the exporter polls on its own cycle. So with
several pairs (for example three, two minutes apart) only findings present in
every pair are reported: a one-off difference is timing, not a defect.

Findings, per series `(name, sorted labels)`:

  missing from bot  the exporter has it and its name is ported
  extra in bot      the bot has it and neither side should
  value differs     both have it and the values differ by more than 1e-9
                    relative

and, per name both sides publish:

  type differs      the `# TYPE` lines disagree; a name without one is
                    untyped

Exporter names not ported yet are listed once and are not findings. Lines
that are not liq_* (the bot recorder's own metrics) are ignored.

--drop-list removes the names nobody reads, which the bot never ports.
--known-diffs applies the documented differences below, including
KNOWN_TYPE_DIFF: the bot types every liq_* name as a gauge and the exporter
writes no `# TYPE` lines, so bot gauge against exporter untyped is expected.
Any other type difference (for example a bot counter) is still a finding.

A finding is identified by its series alone, not by its kind or the values
it prints, so a series that disagrees in every pair is reported even when
its values move or its kind changes (for example missing in one pair and a
different value in the next). The report shows the last pair's message.

A bot liq_* name in neither PORTED nor BOT_ONLY is always a finding: the
lists below must name everything the bot publishes.

Exit status: 0 with no findings, 1 with findings, 2 when a snapshot is not
UTF-8 Prometheus text, holds none of the ported liq_* series (a wrong file,
an empty body, an HTML or JSON error page, or a degraded target that only
reports liq_up), or lacks an ALWAYS_PRESENT name another snapshot on its
side has. That last body is partly filled, for example a target read just
after a restart, before its first read of every source. Its pair compares
fewer series, and a series it does not compare gives no finding there, so
the every-pair rule would drop a real difference the other pairs report.


Items that port more names extend PORTED, BOT_ONLY and KNOWN_DIFFS.
"""

import argparse
import math
import re
import sys

# Exporter names the bot publishes with the same labels and meaning.
PORTED = {
    "liq_bot_info",
    "liq_bot_start_timestamp_seconds",
    "liq_settings_info",
    "liq_settings_equity_target",
    "liq_settings_equity_deviation",
    "liq_settings_usdc_target",
    "liq_settings_usdc_deviation",
    "liq_settings_cash_reserved",
    "liq_settings_execution_threshold_usd",
    "liq_settings_order_polling_seconds",
    "liq_settings_inventory_poll_seconds",
    "liq_settings_deployment_block",
    "liq_asset_counter_trading",
    "liq_asset_extended_hours",
    "liq_asset_rebalancing",
    "liq_equity_onchain_available",
    "liq_equity_offchain_available",
    "liq_equity_inflight_total",
    "liq_equity_total",
    "liq_equity_unwrapped",
    "liq_equity_wrapped",
    "liq_equity_ratio",
    "liq_usdc_onchain_available",
    "liq_usdc_onchain_inflight",
    "liq_usdc_offchain_available",
    "liq_usdc_offchain_gross",
    "liq_usdc_offchain_inflight",
    "liq_usdc_alpaca_usdc",
    "liq_usdc_alpaca_total",
    "liq_usdc_inflight_total",
    "liq_usdc_inflight_ethereum_wallet",
    "liq_usdc_inflight_base_wallet",
    "liq_usdc_total",
    "liq_usdc_ratio",
    "liq_usdc_rebalanceable",
    "liq_position_last_price_usd",
    "liq_equity_exposure_usd",
    "liq_hedge_latency_ms",
    "liq_hedge_latency_ms_samples",
    "liq_open_exposure_fill_count",
    "liq_open_exposure_oldest_ts_seconds",
    "liq_reliability_log_count_24h",
    "liq_log_target_count_24h",
    "liq_failure_event_count_24h",
    "liq_job_queue",
    "liq_block_lag_blocks",
    "liq_block_lag_sampled_ts_seconds",
    "liq_poll_cycles_24h",
    "liq_poll_errors_24h",
    "liq_poll_skipped_ticks_24h",
    "liq_poll_duration_ms",
    "liq_dependency_calls_24h",
    "liq_dependency_errors_24h",
    "liq_dependency_latency_ms",
    "liq_rebalance_stage_ms",
    "liq_attestation_last_ms",
}

# Every PnL series: one family per window, `window` label on each.
PNL_NAMES = {
    "liq_pnl_summary_usd",
    "liq_pnl_summary_shares",
    "liq_pnl_summary_count",
    "liq_pnl_cost_usd",
    "liq_pnl_revenue_usd",
    "liq_pnl_cost_entries",
    "liq_pnl_cost_missing_observations",
    "liq_pnl_cost_coverage",
    "liq_pnl_capital_avg_deployed_usd",
    "liq_pnl_capital_annualized_return_pct",
    "liq_pnl_capital_coverage_days",
    "liq_pnl_capital_sample_days",
    "liq_pnl_symbol_usd",
    "liq_pnl_symbol_shares",
    "liq_pnl_symbol_lots",
    "liq_pnl_symbol_volume_shares",
    "liq_pnl_sample_total_fills",
    "liq_pnl_sample_symbols",
    "liq_pnl_sample_first_ts_seconds",
    "liq_pnl_sample_last_ts_seconds",
    "liq_pnl_warnings",
    "liq_pnl_day_usd",
    "liq_pnl_day_cum_usd",
    "liq_pnl_day_stream_usd",
    "liq_pnl_day_cum_stream_usd",
}
PORTED |= PNL_NAMES

# Pending broker orders and the Raindex order total.
PORTED |= {
    "liq_pending_orders",
    "liq_pending_orders_total",
    "liq_raindex_orders_total",
    "liq_raindex_orders_unavailable",
}

# Names only the bot publishes.
BOT_ONLY = {
    "liq_collector_last_success_ts_seconds",
    "liq_usdc_corridor_target",
    "liq_usdc_corridor_deviation",
    "liq_usdc_corridor_active",
    "liq_equity_chain_available",
    "liq_equity_chain_inflight",
    "liq_equity_chain_share",
    "liq_equity_chain_verdict",
    "liq_usdc_chain_available",
    "liq_usdc_chain_inflight",
    "liq_usdc_chain_ratio",
    "liq_pending_orders_uncapped_total",
}

# Exporter names that are not ported, because nothing reads them or the bot
# has a native replacement.
DROP_LIST = {
    "liq_up",
    "liq_exporter_last_success_ts_seconds",
    "liq_exporter_collect_seconds",
    "liq_asset_flags",
    "liq_asset_operational_limit",
    "liq_position_net",
    "liq_equity_onchain_inflight",
    "liq_equity_offchain_inflight",
    "liq_usdc_withdrawable",
    "liq_equity_ratio_deviation",
    "liq_usdc_ratio_deviation",
    "liq_hedge_cycles_24h",
    "liq_hedge_fill_count_24h",
    "liq_failure_event_last_ts_seconds",
    "liq_job_queue_oldest_pending_ts_seconds",
    "liq_rebalance_operations",
    "liq_rebalance_skipped",
    "liq_poll_duration_ms_samples",
    "liq_dependency_latency_ms_samples",
    "liq_rebalance_stage_ms_samples",
    "liq_raindex_order_vault_balance",
    "liq_raindex_order_io_ratio",
    "liq_raindex_order_created_ts_seconds",
}

# Ported names every snapshot holds once its source has been read: both sides
# publish them unconditionally. A body that lacks one that another body on its
# side has is partly filled, and it is unusable: a series its pair does not
# compare gives no finding in that pair, so the every-pair intersection would
# drop a real difference that every other pair reports. Names that can be
# legitimately absent (a value not read yet, a price that expired, a symbol
# without a position) are not here, so they never make a snapshot unusable.
ALWAYS_PRESENT = {
    "liq_bot_info",
    "liq_bot_start_timestamp_seconds",
    "liq_settings_info",
    "liq_settings_equity_deviation",
    "liq_settings_order_polling_seconds",
    "liq_settings_inventory_poll_seconds",
    "liq_settings_deployment_block",
    "liq_usdc_onchain_available",
    "liq_usdc_onchain_inflight",
    "liq_usdc_offchain_available",
    "liq_usdc_offchain_inflight",
    "liq_usdc_alpaca_total",
    "liq_usdc_inflight_total",
    "liq_usdc_total",
    "liq_usdc_ratio",
}

# Documented differences. ("absolute", tolerance): values may differ by up to
# the tolerance. "values": keys are compared, values are not. "ignore": the
# name is not compared at all. "kept_window": a series of a `window` the
# exporter has no series of at all is not reported as extra in the bot.
# "kept_window_bot_absent": as "kept_window", and a series the exporter has
# and the bot lacks is not reported either, while the bot publishes that name
# for the same window; values are still compared where both have the series.
# The item that ports a name adds its entry, with the evidence for it.
KNOWN_DIFFS = {
    # The exporter derives the start from integer uptime at poll time.
    "liq_bot_start_timestamp_seconds": ("absolute", 2.0),
    # The exporter's infra collector has failed on every cycle since the bot
    # began serving /performance/infra per chain: t0.devops 226c029
    # exporter.py collect_infra calls poll.get() on the per-chain `poll`
    # list, guarded() swallows the AttributeError, and the infra family is
    # never set. So the exporter has no infra series to compare with, and
    # the bot's carry a `chain` label the exporter never had. The golden
    # tests pin the dependency series against the exporter instead.
    "liq_block_lag_blocks": "ignore",
    "liq_block_lag_sampled_ts_seconds": "ignore",
    "liq_poll_cycles_24h": "ignore",
    "liq_poll_errors_24h": "ignore",
    "liq_poll_skipped_ticks_24h": "ignore",
    "liq_poll_duration_ms": "ignore",
    "liq_dependency_calls_24h": "ignore",
    "liq_dependency_errors_24h": "ignore",
    "liq_dependency_latency_ms": "ignore",
    # The exporter reads /performance/reliability, which scans the log files
    # for an exact [now - 24h, now] interval and stops at 50,000 entries
    # (MAX_RELIABILITY_LOG_ENTRIES in src/api.rs, logEntriesTruncated in the
    # response). The bot counts the same events as they are written, in
    # one-minute buckets and without a cap (src/metrics/liquidity/
    # log_counts.rs; the_boundary_minute_counts_or_leaves_as_a_whole pins
    # the bucket rule). It also counts only its own process live: lines an
    # operator's st0x-cli run appends to the same files are read by the
    # endpoint but reach the bot only through the seed at its next restart.
    # The bot also counts an event the lossy non-blocking file writer then
    # drops when its queue is full or the disk write fails; the endpoint
    # never sees that line. So the counts differ by the events of the first
    # minute of the window, by every entry past the cap while the endpoint
    # is truncated, by CLI lines, and by lines the file writer dropped. The
    # two level rows exist on both sides, so their keys are still compared;
    # the bot leaves them out only until its background seed finishes, about
    # one refresh after a start, so a comparison right after a restart
    # reports them missing. A target whose only events are of those kinds
    # has a row on one side only, so the per-target name is not compared.
    "liq_reliability_log_count_24h": "values",
    "liq_log_target_count_24h": "ignore",
}


def _add_known_diffs(rule, names):
    """Gives every name in `names` the rule. A name that already has a rule
    raises, so a later entry can never silently replace an earlier one."""
    repeated = sorted(name for name in names if name in KNOWN_DIFFS)
    if repeated:
        raise ValueError(f"KNOWN_DIFFS already has a rule for {', '.join(repeated)}")
    KNOWN_DIFFS.update({name: rule for name in names})


# A day component of /pnl that does not parse: the exporter counts it as 0
# (t0.devops 8dca7be exporter.py pnl_day_samples, `dec(row.get(field)) or
# Decimal(0)` and `dec(row.get("totalPnlUsd")) or Decimal(0)`), so it still
# publishes that day's value and every running total after it. The bot leaves
# those series out and logs an error (src/metrics/liquidity/pnl.rs
# day_component; every_window_matches_the_exporter_golden pins the two series
# the fixture's bad component feeds). So a day series can be missing from the
# bot only. The values both sides publish are still compared.
PNL_DAY_NAMES = {
    "liq_pnl_day_usd",
    "liq_pnl_day_cum_usd",
    "liq_pnl_day_stream_usd",
    "liq_pnl_day_cum_stream_usd",
}
# A PnL window whose report failed or did not fit the cycle budget keeps its
# last value in the bot, with a stalled liq_collector_last_success_ts_seconds
# {collector="pnl_<window>"}. The exporter drops that window from its pnl
# family until its next cycle (exporter.py collect_pnl: a failed or skipped
# window adds no samples before set_family). A deliberate difference.
# "kept_window_bot_absent" includes "kept_window", so the day names take only
# that rule.
_add_known_diffs("kept_window", PNL_NAMES - PNL_DAY_NAMES)
_add_known_diffs("kept_window_bot_absent", PNL_DAY_NAMES)

# (bot type, exporter type) that --known-diffs accepts for every name. The
# bot publishes every liq_* name as a gauge; the exporter is untyped.
KNOWN_TYPE_DIFF = ("gauge", "untyped")

RELATIVE_TOLERANCE = 1e-9

# The Prometheus text format, one sample line at a time. A line that does not
# match the whole grammar makes the snapshot unusable rather than being read
# loosely. Only spaces and tabs separate tokens, as in Prometheus: `\s` would
# also accept separators such as NBSP or form feed that make it drop a scrape.
_LABEL = r'[A-Za-z_][A-Za-z0-9_]*[ \t]*=[ \t]*"(?:[^"\\\n]|\\[\\"n])*"'
_LABEL_PAIR = re.compile(r'([A-Za-z_][A-Za-z0-9_]*)[ \t]*=[ \t]*"((?:[^"\\\n]|\\[\\"n])*)"')
# A decimal float, Inf, Infinity, or NaN (any case, as Go reads them). Python's float() also takes forms such as
# `1_0` that Prometheus rejects, so the token is checked before conversion.
_VALUE = (r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?"
          r"|[+-]?(?i:inf(?:inity)?)|(?i:nan)")
_SAMPLE_LINE = re.compile(
    r"(?P<name>[A-Za-z_:][A-Za-z0-9_:]*)"
    r"(?:\{[ \t]*(?:(?P<labels>" + _LABEL + r"(?:[ \t]*,[ \t]*" + _LABEL + r")*)"
    r"[ \t]*,?[ \t]*)?\})?"
    r"[ \t]+(?P<value>" + _VALUE + r")"
    r"(?:[ \t]+(?P<timestamp>-?[0-9]+))?"
)
_UNESCAPE = {"\\\\": "\\", '\\"': '"', "\\n": "\n"}
# Like Prometheus, blanks after the `#` are optional: `#TYPE` is a TYPE line.
_TYPE_LINE = re.compile(r"#[ \t]*TYPE(?:[ \t]+(?P<rest>.*))?")
_TYPE_BODY = re.compile(
    r"(?P<name>[A-Za-z_:][A-Za-z0-9_:]*)[ \t]+"
    r"(?P<type>counter|gauge|histogram|summary|untyped)[ \t]*")


def parse_exposition(text):
    """`(name, sorted label tuple) -> value` for every sample line."""
    if text and not text.endswith("\n"):
        raise ValueError("no line feed after the last line")
    series = {}
    # Only LF ends a line. splitlines() would also split on characters such as
    # U+2028, which HELP text and label values may hold.
    for raw in text.split("\n"):
        line = raw.strip(" \t")
        if not line or line.startswith("#"):
            continue
        match = _SAMPLE_LINE.fullmatch(line)
        if match is None:
            raise ValueError(f"not a sample line: {raw!r}")
        labels = [(key, re.sub(r"\\[\\\"n]", lambda m: _UNESCAPE[m.group()], value))
                  for key, value in _LABEL_PAIR.findall(match.group("labels") or "")]
        if len({label for label, _ in labels}) != len(labels):
            raise ValueError(f"repeated label name: {raw!r}")
        key = (match.group("name"), tuple(sorted(labels)))
        if key in series:
            raise ValueError(f"repeated series: {raw!r}")
        series[key] = float(match.group("value"))
    return series


def parse_types(text):
    """`name -> type` for every `# TYPE` line. Other comments are ignored.
    Like Prometheus, a TYPE line must come before the first sample of its
    name."""
    types = {}
    sampled = set()
    for raw in text.split("\n"):
        line = raw.strip(" \t")
        match = _TYPE_LINE.fullmatch(line)
        if match is None:
            sample = _SAMPLE_LINE.fullmatch(line)
            if sample is not None:
                sampled.add(sample.group("name"))
            continue
        body = _TYPE_BODY.fullmatch(match.group("rest") or "")
        if body is None:
            raise ValueError(f"not a TYPE line: {raw!r}")
        if body.group("name") in types:
            raise ValueError(f"repeated TYPE: {raw!r}")
        if body.group("name") in sampled:
            raise ValueError(f"TYPE after a sample: {raw!r}")
        types[body.group("name")] = body.group("type")
    return types


def escape_label_value(value):
    """Prometheus text escaping, so a finding stays on one line."""
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def format_series(key):
    name, labels = key
    if not labels:
        return name
    rendered = ",".join(f'{label}="{escape_label_value(value)}"' for label, value in labels)
    return f"{name}{{{rendered}}}"


def values_differ(bot, exporter, rule):
    if rule == "values":
        return False
    if math.isnan(bot) or math.isnan(exporter):
        return not (math.isnan(bot) and math.isnan(exporter))
    if isinstance(rule, tuple):
        return abs(bot - exporter) > rule[1]
    return not math.isclose(bot, exporter, rel_tol=RELATIVE_TOLERANCE)


def type_findings(bot_text, exporter_text, dropped, known_diffs):
    """`{(name, "# TYPE"): message}` for each liq_* name both sides publish
    whose types differ. A KNOWN_DIFFS rule only skips values, so a name it
    ignores still has its type compared. A string never equals a label tuple,
    so the key cannot collide with a series."""
    def names(text):
        return {name for name, _ in parse_exposition(text)
                if name.startswith("liq_") and name not in dropped}

    bot_types = parse_types(bot_text)
    exporter_types = parse_types(exporter_text)
    findings = {}
    for name in names(bot_text) & names(exporter_text):
        types = (bot_types.get(name, "untyped"), exporter_types.get(name, "untyped"))
        if types[0] != types[1] and not (known_diffs and types == KNOWN_TYPE_DIFF):
            findings[(name, "# TYPE")] = (
                f"type differs: {name} bot={types[0]} exporter={types[1]}")
    return findings


def compare_pair(bot_text, exporter_text, drop_list, known_diffs, bot_names):
    """Returns ({series: message}, not_ported_names) for one pair.
    `bot_names` holds every liq_* name the bot published in any pair, so a
    name the bot publishes only in some pairs is still compared in all."""
    known = KNOWN_DIFFS if known_diffs else {}
    dropped = DROP_LIST if drop_list else set()

    def compared(key):
        name = key[0]
        return (name.startswith("liq_") and name not in dropped
                and known.get(name) != "ignore")

    bot = {key: value for key, value in parse_exposition(bot_text).items()
           if compared(key)}
    exporter = {key: value for key, value in parse_exposition(exporter_text).items()
                if compared(key)}

    not_ported = {name for name, _ in exporter
                  if name not in PORTED and name not in bot_names}
    exporter = {key: value for key, value in exporter.items()
                if key[0] not in not_ported}

    def window(key):
        return dict(key[1]).get("window")

    windowed = {"kept_window", "kept_window_bot_absent"}
    exporter_windows = {window(key) for key in exporter
                        if known.get(key[0]) in windowed}

    def kept_window(key):
        return known.get(key[0]) in windowed and window(key) not in exporter_windows

    # A bot_absent name excuses a missing series only while the bot still
    # publishes that name for the window: a bad day component leaves out
    # single series, so a whole name missing from a window is a finding.
    bot_absent_windows = {(key[0], window(key)) for key in bot
                          if known.get(key[0]) == "kept_window_bot_absent"}

    def excused_absence(key):
        return (known.get(key[0]) == "kept_window_bot_absent"
                and (key[0], window(key)) in bot_absent_windows)

    findings = type_findings(bot_text, exporter_text, dropped, known_diffs)
    for key in exporter.keys() - bot.keys():
        if not excused_absence(key):
            findings[key] = f"missing from bot: {format_series(key)}"
    for key in bot.keys() - exporter.keys():
        if key[0] not in BOT_ONLY and not kept_window(key):
            findings[key] = f"extra in bot: {format_series(key)}"
    for key in bot.keys() & exporter.keys():
        if values_differ(bot[key], exporter[key], known.get(key[0])):
            findings[key] = (
                f"value differs: {format_series(key)} "
                f"bot={bot[key]!r} exporter={exporter[key]!r}")
    return findings, not_ported


def compare(pairs, drop_list, known_diffs):
    """Findings present in every pair, plus every bot liq_* name that is in
    neither PORTED nor BOT_ONLY, and the union of unported names."""
    bot_names = {name for bot_text, _ in pairs for name, _ in parse_exposition(bot_text)
                 if name.startswith("liq_")}
    unlisted = sorted(f"unlisted bot name: {name}" for name in bot_names
                      if name not in PORTED and name not in BOT_ONLY)
    persistent = None
    latest = {}
    not_ported = set()
    for bot_text, exporter_text in pairs:
        findings, unported = compare_pair(
            bot_text, exporter_text, drop_list, known_diffs, bot_names)
        series = findings.keys()
        persistent = set(series) if persistent is None else persistent & series
        latest = findings
        not_ported |= unported
    persistent_findings = sorted(latest[identity] for identity in persistent or ())
    return unlisted + persistent_findings, sorted(not_ported)


def unusable_snapshots(paths, texts):
    """`path: reason` for each body that is not UTF-8, does not parse, has no
    ported liq_* sample, or lacks an ALWAYS_PRESENT name that another body on
    the same side has (a partly filled body, such as an exporter just after a
    restart, would otherwise hide a finding from the every-pair
    intersection). Bodies alternate bot, exporter. An undecodable body is
    passed as None."""
    unusable = []
    always_present = {}
    for index, (path, text) in enumerate(zip(paths, texts)):
        if text is None:
            unusable.append(f"{path}: not UTF-8 text")
            continue
        try:
            series = parse_exposition(text)
            parse_types(text)
        except ValueError as error:
            message = str(error)
            reason = message.split(":", 1)[0] if message.startswith("repeated") \
                else "not a Prometheus text body"
            unusable.append(f"{path}: {reason}")
            continue
        if not any(name in PORTED for name, _ in series):
            unusable.append(f"{path}: no ported liq_* series")
            continue
        always_present[index] = {name for name, _ in series if name in ALWAYS_PRESENT}

    for index, names in always_present.items():
        side = {name for other, other_names in always_present.items()
                if other % 2 == index % 2 for name in other_names}
        missing = sorted(side - names)
        if missing:
            unusable.append(f"{paths[index]}: partial snapshot, missing {', '.join(missing)}")
    return unusable


def report(pairs, findings, not_ported):
    lines = [f"compared {pairs} snapshot pair(s); findings present in every pair:"]
    lines += findings or ["none"]
    if not_ported:
        lines.append("not yet ported (exporter-only names, ignored): "
                     + ", ".join(not_ported))
    lines.append(f"{len(findings)} finding(s)")
    return "\n".join(lines) + "\n"


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--drop-list", action="store_true",
                        help="skip exporter names the bot never ports")
    parser.add_argument("--known-diffs", action="store_true",
                        help="apply the documented differences")
    parser.add_argument("files", nargs="+", metavar="BOT.prom EXPORTER.prom")
    args = parser.parse_args(argv)
    if len(args.files) % 2:
        parser.error("files come in BOT.prom EXPORTER.prom pairs")

    texts = []
    for path in args.files:
        with open(path, "rb") as f:
            body = f.read()
        try:
            texts.append(body.decode("utf-8"))
        except UnicodeDecodeError:
            texts.append(None)
    unusable = unusable_snapshots(args.files, texts)
    if unusable:
        sys.stderr.write("unusable snapshot(s):\n" + "".join(f"  {line}\n" for line in unusable))
        return 2
    pairs = list(zip(texts[::2], texts[1::2]))

    findings, not_ported = compare(pairs, args.drop_list, args.known_diffs)
    sys.stdout.write(report(len(pairs), findings, not_ported))
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
