#!/usr/bin/env python3
"""Tests for compare.py against the checked-in snapshot pair in testdata/.

    python3 scripts/liq-parity/test_compare.py
"""

import io
import math
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock

# Keep the import from leaving a __pycache__ directory in the repository.
sys.dont_write_bytecode = True

import compare  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
BOT = os.path.join(HERE, "testdata", "bot.prom")
EXPORTER = os.path.join(HERE, "testdata", "exporter.prom")


def run(*args):
    out = io.StringIO()
    with redirect_stdout(out):
        status = compare.main(list(args))
    return status, out.getvalue()


class CompareTest(unittest.TestCase):
    def test_reports_missing_extra_and_value_findings_after_drop_list_and_known_diffs(self):
        status, report = run("--drop-list", "--known-diffs", BOT, EXPORTER)

        self.assertEqual(status, 1)
        self.assertEqual(report, (
            "compared 1 snapshot pair(s); findings present in every pair:\n"
            "extra in bot: liq_settings_cash_reserved\n"
            'missing from bot: liq_asset_rebalancing{symbol="TSLA"}\n'
            "value differs: liq_settings_usdc_target bot=0.45 exporter=0.4\n"
            "not yet ported (exporter-only names, ignored): "
            "liq_pending_orders_total, liq_pnl_summary_usd\n"
            "3 finding(s)\n"
        ))

    def test_without_known_diffs_the_start_time_jitter_and_the_gauge_types_are_findings(self):
        status, report = run(BOT, EXPORTER)

        self.assertEqual(status, 1)
        self.assertEqual(report, (
            "compared 1 snapshot pair(s); findings present in every pair:\n"
            "extra in bot: liq_settings_cash_reserved\n"
            'missing from bot: liq_asset_rebalancing{symbol="TSLA"}\n'
            "type differs: liq_asset_rebalancing bot=gauge exporter=untyped\n"
            "type differs: liq_bot_info bot=gauge exporter=untyped\n"
            "type differs: liq_bot_start_timestamp_seconds bot=gauge exporter=untyped\n"
            "type differs: liq_equity_total bot=gauge exporter=untyped\n"
            "type differs: liq_settings_equity_target bot=gauge exporter=untyped\n"
            "type differs: liq_settings_usdc_target bot=gauge exporter=untyped\n"
            "value differs: liq_bot_start_timestamp_seconds "
            "bot=1700000000.0 exporter=1700000001.9\n"
            "value differs: liq_settings_usdc_target bot=0.45 exporter=0.4\n"
            "not yet ported (exporter-only names, ignored): "
            "liq_asset_flags, liq_equity_ratio_deviation, liq_pending_orders_total, "
            "liq_pnl_summary_usd, liq_up\n"
            "10 finding(s)\n"
        ))

    def test_known_diffs_accept_only_a_bot_gauge_against_an_untyped_exporter(self):
        exporter = (
            "liq_settings_usdc_target 0.4\n"
            "liq_settings_equity_target 0.5\n"
            "# TYPE liq_settings_cash_reserved gauge\n"
            "liq_settings_cash_reserved 1000\n"
            "liq_bot_info 1\n")
        bot = (
            "# TYPE liq_settings_usdc_target gauge\n"
            "liq_settings_usdc_target 0.4\n"
            "# TYPE liq_settings_equity_target counter\n"
            "liq_settings_equity_target 0.5\n"
            "liq_settings_cash_reserved 1000\n"
            "# TYPE liq_bot_info untyped\n"
            "liq_bot_info 1\n")

        findings, _ = compare.compare([(bot, exporter)], True, True)

        self.assertEqual(findings, [
            "type differs: liq_settings_cash_reserved bot=untyped exporter=gauge",
            "type differs: liq_settings_equity_target bot=counter exporter=untyped",
        ])

    def test_a_type_is_compared_only_for_a_name_both_sides_publish(self):
        bot = "# TYPE liq_settings_cash_reserved counter\nliq_settings_cash_reserved 1\n"

        findings, _ = compare.compare([(bot, "liq_bot_info 1\n")], True, True)

        self.assertEqual(findings, [
            "extra in bot: liq_settings_cash_reserved",
            "missing from bot: liq_bot_info",
        ])

    def test_a_type_difference_present_in_only_one_pair_is_not_reported(self):
        exporter = "liq_bot_info 1\n"
        counter = "# TYPE liq_bot_info counter\nliq_bot_info 1\n"
        gauge = "# TYPE liq_bot_info gauge\nliq_bot_info 1\n"

        findings, _ = compare.compare([(counter, exporter), (gauge, exporter)], True, True)

        self.assertEqual(findings, [])

    def test_a_name_whose_values_are_ignored_still_has_its_type_compared(self):
        bot = "# TYPE liq_bot_info counter\nliq_bot_info 2\n"

        with mock.patch.dict(compare.KNOWN_DIFFS, {"liq_bot_info": "ignore"}):
            findings, _ = compare.compare([(bot, "liq_bot_info 1\n")], True, True)

        self.assertEqual(findings, ["type differs: liq_bot_info bot=counter exporter=untyped"])

    def test_a_type_line_without_a_blank_after_the_hash_is_read(self):
        bot = "#TYPE liq_bot_info counter\nliq_bot_info 1\n"

        findings, _ = compare.compare([(bot, "liq_bot_info 1\n")], True, True)

        self.assertEqual(findings, ["type differs: liq_bot_info bot=counter exporter=untyped"])
        self.assertEqual(
            compare.unusable_snapshots(
                ["a"], ["# TYPE liq_bot_info gauge\n#TYPE liq_bot_info gauge\nliq_bot_info 1\n"]),
            ["a: repeated TYPE"])

    def test_a_malformed_or_repeated_type_line_makes_the_snapshot_unusable(self):
        sample = "liq_bot_info 1\n"
        bodies = [
            "# TYPE liq_bot_info\n" + sample,
            "# TYPE liq_bot_info gauges\n" + sample,
            "# TYPE liq_bot_info gauge\n# TYPE liq_bot_info gauge\n" + sample,
        ]

        self.assertEqual(compare.unusable_snapshots(["a", "b", "c"], bodies), [
            "a: not a Prometheus text body",
            "b: not a Prometheus text body",
            "c: repeated TYPE",
        ])

    def test_a_type_line_after_a_sample_of_its_name_makes_the_snapshot_unusable(self):
        with tempfile.NamedTemporaryFile("w", suffix=".prom") as late:
            late.write("liq_bot_info 1\n#TYPE liq_bot_info gauge\n")
            late.flush()
            err = io.StringIO()
            with redirect_stderr(err):
                status, report = run(late.name, EXPORTER)

        self.assertEqual(status, 2)
        self.assertEqual(report, "")
        self.assertEqual(err.getvalue(),
                         f"unusable snapshot(s):\n  {late.name}: not a Prometheus text body\n")
        self.assertEqual(
            compare.unusable_snapshots(["a"], ["liq_bot_info 1\n# TYPE liq_bot_info gauge\n"]),
            ["a: not a Prometheus text body"])

    def test_type_lines_parse_and_other_comments_are_ignored(self):
        body = ("# HELP liq_bot_info Build.\n"
                "  # TYPE liq_bot_info\tgauge \n"
                "# TYPEWRITER liq_a counter\n"
                "# a TYPE comment\n"
                "# TYPE hedge_trades_total counter\n")

        self.assertEqual(compare.parse_types(body),
                         {"liq_bot_info": "gauge", "hedge_trades_total": "counter"})

    def test_only_findings_present_in_every_pair_are_reported(self):
        with open(BOT) as f:
            bot = f.read()
        with open(EXPORTER) as f:
            exporter = f.read()
        caught_up = exporter.replace("liq_settings_usdc_target 0.4", "liq_settings_usdc_target 0.45")

        findings, _ = compare.compare([(bot, exporter), (bot, caught_up)], True, True)

        self.assertEqual(findings, [
            "extra in bot: liq_settings_cash_reserved",
            'missing from bot: liq_asset_rebalancing{symbol="TSLA"}',
        ])

    def test_a_mismatch_whose_values_move_between_pairs_is_still_reported(self):
        bot = "liq_bot_start_timestamp_seconds 100\n"
        pairs = [(bot, f"liq_bot_start_timestamp_seconds {start}\n")
                 for start in ("103.1", "103.2", "103.3")]

        findings, _ = compare.compare(pairs, True, True)

        self.assertEqual(findings, [
            "value differs: liq_bot_start_timestamp_seconds bot=100.0 exporter=103.3",
        ])

    def test_a_snapshot_without_liq_series_fails_instead_of_passing(self):
        empty = os.path.join(HERE, "testdata", "empty.prom")
        err = io.StringIO()
        with redirect_stderr(err):
            status, report = run(BOT, empty)

        self.assertEqual(status, 2)
        self.assertEqual(report, "")
        self.assertEqual(err.getvalue(), f"unusable snapshot(s):\n  {empty}: no ported liq_* series\n")

    def test_a_body_that_is_not_prometheus_text_fails_instead_of_crashing(self):
        html = os.path.join(HERE, "testdata", "not-prometheus.txt")
        err = io.StringIO()
        with redirect_stderr(err):
            status, report = run(BOT, html)

        self.assertEqual(status, 2)
        self.assertEqual(report, "")
        self.assertEqual(err.getvalue(),
                         f"unusable snapshot(s):\n  {html}: not a Prometheus text body\n")

    def test_a_series_that_changes_kind_between_pairs_is_still_reported(self):
        exporter = "liq_settings_usdc_target 0.4\nliq_bot_info{git_commit=\"a\"} 1\n"
        differs = "liq_settings_usdc_target 0.45\nliq_bot_info{git_commit=\"a\"} 1\n"
        missing = "liq_bot_info{git_commit=\"a\"} 1\n"

        findings, _ = compare.compare(
            [(differs, exporter), (missing, exporter), (differs, exporter)], True, True)

        self.assertEqual(findings, [
            "value differs: liq_settings_usdc_target bot=0.45 exporter=0.4",
        ])

    def test_a_partly_filled_snapshot_is_unusable(self):
        full = "liq_bot_info{git_commit=\"a\"} 1\nliq_usdc_total 5\n"
        partial = "liq_bot_info{git_commit=\"a\"} 1\n"

        self.assertEqual(
            compare.unusable_snapshots(
                ["bot1", "exp1", "bot2", "exp2"], [full, full, full, partial]),
            ["exp2: partial snapshot, missing liq_usdc_total"])

    def test_a_legitimately_absent_series_does_not_make_a_snapshot_unusable(self):
        read = "liq_bot_info{git_commit=\"a\"} 1\nliq_usdc_offchain_gross 7\n"
        not_read = "liq_bot_info{git_commit=\"a\"} 1\n"

        self.assertEqual(
            compare.unusable_snapshots(
                ["bot1", "exp1", "bot2", "exp2"], [read, read, not_read, not_read]),
            [])

    def test_a_bot_name_published_in_one_pair_is_compared_in_every_pair(self):
        exporter = "liq_bot_info{git_commit=\"a\"} 1\nliq_pending_orders_total 2\n"
        with_name = exporter
        without_name = "liq_bot_info{git_commit=\"a\"} 1\n"

        findings, not_ported = compare.compare(
            [(with_name, exporter), (without_name, exporter)], True, True)

        self.assertEqual(findings, ["unlisted bot name: liq_pending_orders_total"])
        self.assertEqual(not_ported, [])

    def test_a_bot_name_outside_the_lists_is_a_finding(self):
        bot = "liq_bot_info{git_commit=\"a\"} 1\nliq_made_up 1\n"
        exporter = "liq_bot_info{git_commit=\"a\"} 1\n"

        findings, _ = compare.compare([(bot, exporter)], True, True)

        self.assertEqual(findings, [
            "unlisted bot name: liq_made_up",
            "extra in bot: liq_made_up",
        ])

    def test_a_repeated_series_makes_the_snapshot_unusable(self):
        doubled = os.path.join(HERE, "testdata", "repeated.prom")
        err = io.StringIO()
        with redirect_stderr(err):
            status, report = run(doubled, EXPORTER)

        self.assertEqual(status, 2)
        self.assertEqual(report, "")
        self.assertEqual(err.getvalue(), f"unusable snapshot(s):\n  {doubled}: repeated series\n")

    def test_a_repeated_label_name_makes_the_snapshot_unusable(self):
        doubled = 'liq_bot_info{git_commit="a",git_commit="b"} 1\n'

        self.assertEqual(compare.unusable_snapshots(["bot.prom"], [doubled]),
                         ["bot.prom: repeated label name"])

    def test_a_snapshot_that_is_not_utf8_fails_instead_of_crashing(self):
        with tempfile.NamedTemporaryFile(suffix=".prom") as binary:
            binary.write(b"liq_bot_info{git_commit=\"\xff\"} 1\n")
            binary.flush()
            err = io.StringIO()
            with redirect_stderr(err):
                status, report = run(BOT, binary.name)

        self.assertEqual(status, 2)
        self.assertEqual(report, "")
        self.assertEqual(err.getvalue(),
                         f"unusable snapshot(s):\n  {binary.name}: not UTF-8 text\n")

    def test_a_body_without_a_final_line_feed_fails_instead_of_passing(self):
        with tempfile.NamedTemporaryFile("w", suffix=".prom") as cut:
            cut.write("liq_bot_info 1")
            cut.flush()
            err = io.StringIO()
            with redirect_stderr(err):
                status, report = run(cut.name, cut.name)

        self.assertEqual(status, 2)
        self.assertEqual(report, "")
        self.assertEqual(err.getvalue(), (
            "unusable snapshot(s):\n"
            f"  {cut.name}: not a Prometheus text body\n"
            f"  {cut.name}: not a Prometheus text body\n"))

    def test_a_comma_needs_a_label_before_it(self):
        self.assertEqual(compare.parse_exposition("liq_a{} 1\nliq_b{x=\"1\",} 2\n"),
                         {("liq_a", ()): 1.0, ("liq_b", (("x", "1"),)): 2.0})
        with self.assertRaises(ValueError):
            compare.parse_exposition("liq_bot_info{,} 1\n")

    def test_only_a_line_feed_ends_a_line(self):
        body = '# HELP liq_bot_info Build\u2028stamp.\nliq_bot_info{git_commit="a\u2028b"} 1\n'

        self.assertEqual(compare.parse_exposition(body),
                         {("liq_bot_info", (("git_commit", "a\u2028b"),)): 1.0})

    def test_matching_snapshots_have_no_findings(self):
        status, report = run(BOT, BOT)

        self.assertEqual(status, 0)
        self.assertEqual(report, (
            "compared 1 snapshot pair(s); findings present in every pair:\n"
            "none\n"
            "0 finding(s)\n"
        ))

    def test_leading_whitespace_and_tabs_parse(self):
        series = compare.parse_exposition(
            '  # HELP liq_bot_info Build.\n\tliq_settings_usdc_target\t0.4\n'
            '  liq_bot_info{git_commit="abc"}  1\n')

        self.assertEqual(series, {
            ("liq_settings_usdc_target", ()): 0.4,
            ("liq_bot_info", (("git_commit", "abc"),)): 1.0,
        })

    def test_spaces_after_label_commas_give_the_same_series(self):
        spaced = compare.parse_exposition('liq_asset_rebalancing{chain="base", symbol="X"} 1\n')
        tight = compare.parse_exposition('liq_asset_rebalancing{chain="base",symbol="X"} 1\n')

        self.assertEqual(spaced, tight)

    def test_a_sample_line_allows_only_a_value_and_an_integer_timestamp(self):
        self.assertEqual(compare.parse_exposition("liq_bot_start_timestamp_seconds 5 1700000000000\n"),
                         {("liq_bot_start_timestamp_seconds", ()): 5.0})
        with self.assertRaises(ValueError):
            compare.parse_exposition("liq_bot_start_timestamp_seconds 5 garbage\n")
        with self.assertRaises(ValueError):
            compare.parse_exposition("liq_bot_start_timestamp_seconds\n")

    def test_a_value_that_prometheus_rejects_fails_instead_of_passing(self):
        with tempfile.NamedTemporaryFile("w", suffix=".prom") as underscored:
            underscored.write("liq_bot_info 1_0\n")
            underscored.flush()
            err = io.StringIO()
            with redirect_stderr(err):
                status, report = run(underscored.name, EXPORTER)

        self.assertEqual(status, 2)
        self.assertEqual(report, "")
        self.assertEqual(err.getvalue(), (
            "unusable snapshot(s):\n"
            f"  {underscored.name}: not a Prometheus text body\n"))
        for value in ("0x10", "1e", "infin", "--1"):
            with self.assertRaises(ValueError):
                compare.parse_exposition(f"liq_a {value}\n")
        self.assertEqual(compare.parse_exposition("liq_a 1.5e3\nliq_b .5\nliq_c -2.\nliq_d -Infinity\n"),
                         {("liq_a", ()): 1500.0, ("liq_b", ()): 0.5, ("liq_c", ()): -2.0, ("liq_d", ()): -math.inf})

    def test_a_degraded_snapshot_with_only_liq_up_is_unusable(self):
        degraded = os.path.join(HERE, "testdata", "degraded.prom")
        err = io.StringIO()
        with redirect_stderr(err):
            status, _ = run(BOT, degraded)

        self.assertEqual(status, 2)
        self.assertEqual(err.getvalue(), f"unusable snapshot(s):\n  {degraded}: no ported liq_* series\n")

    def test_a_trailing_label_comma_parses_and_an_unknown_escape_does_not(self):
        self.assertEqual(compare.parse_exposition('liq_asset_rebalancing{symbol="AAPL",} 1\n'),
                         {("liq_asset_rebalancing", (("symbol", "AAPL"),)): 1.0})
        with self.assertRaises(ValueError):
            compare.parse_exposition('liq_bot_info{git_commit="a\\qb"} 1\n')

    def test_labels_without_a_separator_are_rejected(self):
        with self.assertRaises(ValueError):
            compare.parse_exposition('liq_bot_info{git_commit="a"chain="base"} 1\n')

    def test_special_values_parse(self):
        series = compare.parse_exposition("liq_a NaN\nliq_b +Inf\nliq_c -Inf\n")

        self.assertTrue(math.isnan(series[("liq_a", ())]))
        self.assertEqual(series[("liq_b", ())], math.inf)
        self.assertEqual(series[("liq_c", ())], -math.inf)

    def test_findings_re_escape_label_values(self):
        key = ("liq_bot_info", (("git_commit", 'a\\b"c\nd'),))

        self.assertEqual(compare.format_series(key), 'liq_bot_info{git_commit="a\\\\b\\"c\\nd"}')

    def test_nan_against_a_number_differs_under_every_rule(self):
        nan = float("nan")

        self.assertTrue(compare.values_differ(nan, 1.0, ("absolute", 2.0)))
        self.assertTrue(compare.values_differ(1.0, nan, None))
        self.assertFalse(compare.values_differ(nan, nan, ("absolute", 2.0)))
        self.assertFalse(compare.values_differ(nan, 1.0, "values"))

    def test_escaped_label_values_parse_back(self):
        series = compare.parse_exposition('liq_bot_info{git_commit="a\\\\b\\"c\\nd"} 1\n')

        self.assertEqual(series, {("liq_bot_info", (("git_commit", 'a\\b"c\nd'),)): 1.0})


if __name__ == "__main__":
    sys.exit(unittest.main())
