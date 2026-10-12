#!/usr/bin/env python3
"""Generator for dashboards/liquidity/t0-liquidity.json, the Liquidity bot
board, a 1:1 Grafana rendering of the bot's own SvelteKit dashboard.

Regenerate after edits:

    python3 observability/gen-t0-liquidity.py

Check that the committed JSON matches the generator (CI runs this):

    python3 observability/gen-t0-liquidity.py --check

Data comes from the exporter sidecar on the liquidity VM
(terraform/liquidity-exporter/exporter.py in T0Trade/t0.devops) or, for the
liq_* gauges, from the bot's own /metrics, as the `source` variable picks:

  cloudmon  liq_* gauges via Ops Agent scrape -> managed prometheus
  cloudlog  logNames liquidity-trades / liquidity-transfers /
            liquidity-botlogs via Cloud Logging entries.write

Layout mirrors the SPA's tabs as rows, in tab order: header strip,
Dashboard (inventory | trades | transfers, 14/10 split: the SPA's 11fr/9fr
is 13/11, but Grafana's panel padding needs the extra unit to fit the USDC
row's three panels; checked with no sideways scroll on a 1934px-wide
viewport, and the fixed column widths do overflow near 1600px), Orders, PnL, Performance, Logs — then the pre-exporter
native-metric rows (t0-liquidity-native-rows.json), collapsed.

Two Grafana-side patterns worth knowing before editing:

  - Per-symbol tables are ONE instant query: an `or`-chain of
    label_replace(metric, "col", "<Column>", "", "") terms, pivoted by the
    groupingToMatrix transformation on (symbol, col). Column order is then
    enforced with organize/indexByName keyed on the matrix field names.
  - cloudlog tables follow the auth0-board pattern. Since
    googlecloud-logging-datasource 1.8.0 a logs query returns ONE frame in
    Grafana's `log-lines` shape: `timestamp`, `body` (the entry's message),
    `severity`, `id`, `labels` (a per-row JSON object holding every other
    payload leaf, flattened and dot-joined: jsonPayload.symbol,
    protoPayload.method_name, ...), `traceId`. So: extractFields from
    `labels`, filter by name, organize. (Before 1.8.0 it was one frame
    per entry with the leaves as Grafana labels on a `content` field,
    hence labelsToFields + merge; that shape stopped rendering the day
    the plugin auto-updated on the box, 2026-09-16.) The row limit is the
    query's maxDataPoints: 100 when it is unset (a bare /api/ds/query),
    and the panel's pixel width inside a panel, unless the panel sets
    maxDataPoints itself (see latest_status_table).
"""

import filecmp
import json
import os
import re
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
NATIVE_ROWS = os.path.join(HERE, "t0-liquidity-native-rows.json")

# The bot serves liq_* on its own /metrics too, so an unpinned liq_ selector
# would sum the exporter and the bot together. Every liq_ selector on these
# boards reads the one job the `source` variable picks (SOURCE_VAR). PromQL
# anchors a regex matcher, so the bot's value never matches the exporter job.
LIQ_JOB_MATCHER = 'job=~"$source"'
# The header's Bot light. The bot source has no liq_up and reads its scrape's
# up. The exporter source never falls back to up: the exporter's own up says
# nothing about the bot, so a stopped exporter shows no data, not red.
# pin_liq leaves both selectors alone, since each already names its job.
LIQ_UP = ('max(liq_up{job=~"$source"}) or '
          'max(up{job=~"$source",job!="t0-liquidity-exporter"})')
LIQ_NAME = re.compile(r"(?<![A-Za-z0-9_:])liq_[a-z0-9_]+")
LIQ_PREFIX = re.compile(r"(?<![A-Za-z0-9_:])liq_")

CM = {"type": "stackdriver", "uid": "cloudmon"}
CL = {"type": "googlecloud-logging-datasource", "uid": "cloudlog"}

_id = [0]


def nid():
    _id[0] += 1
    return _id[0]


# --------------------------------------------------------------------------
# Target + panel helpers
# --------------------------------------------------------------------------

def pin_liq(expr):
    """Adds the `source` job matcher to every liq_ selector in `expr`.

    Handles bare names (`liq_x` -> `liq_x{job="..."}`) and names that already
    carry matchers (`liq_x{a="b"}` -> `liq_x{job="...",a="b"}`). A selector
    that already names a job is left for `unpinned_liq_selectors` to judge.
    """
    out = []
    pos = 0
    for match in LIQ_NAME.finditer(expr):
        out.append(expr[pos:match.end()])
        pos = match.end()
        if not expr.startswith("{", pos):
            out.append("{" + LIQ_JOB_MATCHER + "}")
            continue
        close = expr.find("}", pos)
        if close == -1:
            raise SystemExit(f"unterminated label matcher in PromQL: {expr}")
        matchers = expr[pos + 1:close].strip()
        if re.search(r"(?<![A-Za-z0-9_])job\s*[=!]", matchers):
            continue
        pinned = LIQ_JOB_MATCHER + ("," + matchers if matchers else "")
        out.append("{" + pinned + "}")
        pos = close + 1
    out.append(expr[pos:])
    return "".join(out)


def unpinned_liq_selectors(expr):
    """Every liq_ occurrence in `expr` that is not a selector pinned to
    exactly LIQ_JOB_MATCHER. A templated name such as `liq_$col` is never a valid
    selector, so it is reported too: the check fails closed."""
    pinned = {match.start() for match in LIQ_NAME.finditer(expr)
              if _selector_is_pinned(expr, match.end())}
    return [expr[found.start():found.start() + 40]
            for found in LIQ_PREFIX.finditer(expr)
            if found.start() not in pinned]


def _selector_is_pinned(expr, name_end):
    if not expr.startswith("{", name_end):
        return False
    close = expr.find("}", name_end)
    if close == -1:
        return False
    matchers = [part.strip() for part in expr[name_end + 1:close].split(",")]
    jobs = [part for part in matchers if re.match(r"job\s*(=|!=|=~|!~)", part)]
    return jobs == [LIQ_JOB_MATCHER]


def promql(expr, legend=None, instant=False, ref="A", step="60s"):
    expr = pin_liq(expr)
    target = {
        "refId": ref,
        "datasource": CM,
        "queryType": "promQL",
        "promQLQuery": {"projectName": "$env", "expr": expr, "step": step},
        "timeSeriesList": {"projectName": "", "filters": [], "view": "FULL",
                           "groupBys": []},
    }
    if instant:
        target["instant"] = True
    if legend:
        # NOTE: top-level, not inside promQLQuery — the stackdriver plugin
        # only honors it there (see st0x-pricing.json).
        target["legendFormat"] = legend
    return target


def cloudlog(query, ref="A"):
    return {"refId": ref, "datasource": CL, "projectId": "$env",
            "queryText": query, "queryType": "logs"}


def stat(title, desc, expr, display=None, unit="short", decimals=None,
         steps=None, mappings=None, w=3, h=3, x=0, y=0, legend=None,
         text_mode="value_and_name", color_mode="value", value_size=None):
    options_text = {}
    if value_size:
        options_text = {"text": {"valueSize": value_size, "titleSize": 12}}
    defaults = {
        "mappings": (mappings or []) + [
            {"type": "special",
             "options": {"match": "null+nan",
                         "result": {"text": "—", "color": "text", "index": 99}}}
        ],
        "thresholds": {"mode": "absolute",
                       "steps": steps or [{"color": "text", "value": None}]},
        "unit": unit,
    }
    if display is not None:
        defaults["displayName"] = display
    if decimals is not None:
        defaults["decimals"] = decimals
    return {
        "id": nid(), "type": "stat", "title": title, "description": desc,
        "datasource": CM,
        "targets": [promql(expr, instant=True, legend=legend)],
        "transparent": True,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {
            "colorMode": color_mode, "graphMode": "none",
            "textMode": text_mode, "justifyMode": "center", **options_text,
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "",
                              "values": False},
        },
    }


def timeseries(title, desc, targets, w=8, h=8, x=0, y=0, unit="short",
               decimals=None, fill=0, stack=False, draw="line",
               legend_mode="list", legend_placement="bottom", calcs=None,
               min_=None, max_=None, overrides=None):
    defaults = {
        "unit": unit,
        "custom": {
            "drawStyle": draw, "lineInterpolation": "linear", "lineWidth": 1,
            "fillOpacity": fill, "showPoints": "never", "pointSize": 5,
            "spanNulls": True,
            "stacking": {"mode": "normal" if stack else "none", "group": "A"},
            "axisPlacement": "auto",
        },
    }
    if decimals is not None:
        defaults["decimals"] = decimals
    if min_ is not None:
        defaults["min"] = min_
    if max_ is not None:
        defaults["max"] = max_
    return {
        "id": nid(), "type": "timeseries", "title": title, "description": desc,
        "datasource": CM, "targets": targets,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "fieldConfig": {"defaults": defaults, "overrides": overrides or []},
        "options": {
            "legend": {"displayMode": legend_mode,
                       "placement": legend_placement,
                       "showLegend": True, "calcs": calcs or []},
            "tooltip": {"mode": "multi", "sort": "desc"},
        },
    }


def bargauge(title, desc, expr, legend, unit="ms", w=8, h=8, x=0, y=0,
             steps=None, decimals=0):
    return {
        "id": nid(), "type": "bargauge", "title": title, "description": desc,
        "datasource": CM,
        "targets": [promql(expr, legend=legend, instant=True)],
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "fieldConfig": {
            "defaults": {
                "unit": unit, "decimals": decimals,
                "thresholds": {"mode": "absolute",
                               "steps": steps or [{"color": "blue", "value": None}]},
                "mappings": [],
            },
            "overrides": [],
        },
        "options": {
            "displayMode": "gradient", "orientation": "horizontal",
            "valueMode": "color", "showUnfilled": True, "sizing": "auto",
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "",
                              "values": False},
        },
    }


def row(title, y, collapsed=False, panels=None):
    return {"id": nid(), "type": "row", "title": title, "collapsed": collapsed,
            "gridPos": {"h": 1, "w": 24, "x": 0, "y": y},
            "panels": panels or []}


def or_chain(cols, row_label="symbol"):
    """cols: list of (metric_expr, column_name) -> a single instant expr whose
    series carry ONLY (row_label, col) labels. The sum by strips scrape labels
    (instance, job, cluster...) so the table transformations see clean frames.
    """
    return " or ".join(
        f'sum by ({row_label}, col) '
        f'(label_replace({expr}, "col", "{name}", "", ""))'
        for expr, name in cols
    )


def matrix_table(title, desc, expr, row_label, w=12, h=10, x=0, y=0,
                 column_order=None, renames=None, unit_overrides=None,
                 sort_by=None, first_col=None, filterable=False, decimals=3):
    """Instant or-chain query pivoted into a table by groupingToMatrix on
    (row_label, col). Field names post-matrix are the `col` values, plus the
    join field named '<row_label>\\col'."""
    matrix_key = f"{row_label}\\col"
    transformations = [
        {"id": "labelsToFields", "options": {}},
        # Each series arrives as its own frame; merge them into one table
        # before grouping or the panel renders only the first frame.
        {"id": "merge", "options": {}},
        {"id": "groupBy",
         "options": {"fields": {
             row_label: {"operation": "groupby", "aggregations": []},
             "col": {"operation": "groupby", "aggregations": []},
             "Value": {"operation": "aggregate",
                        "aggregations": ["lastNotNull"]},
         }}},
        {"id": "groupingToMatrix",
         "options": {"columnField": "col", "rowField": row_label,
                     "valueField": "Value (lastNotNull)",
                     "emptyValue": "null"}},
    ]
    organize = {"excludeByName": {}, "renameByName": {}, "indexByName": {}}
    if first_col:
        organize["renameByName"][matrix_key] = first_col
    if renames:
        organize["renameByName"].update(renames)
    if column_order:
        organize["indexByName"] = {name: index for index, name
                                   in enumerate([matrix_key] + column_order)}
    transformations.append({"id": "organize", "options": organize})
    overrides = []
    for field, props in (unit_overrides or {}).items():
        overrides.append({"matcher": {"id": "byName", "options": field},
                          "properties": props})
    return {
        "id": nid(), "type": "table", "title": title, "description": desc,
        "datasource": CM,
        "targets": [promql(expr, instant=True)],
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "transformations": transformations,
        "fieldConfig": {
            "defaults": {"unit": "short", "decimals": decimals,
                         "custom": {"align": "auto", "filterable": filterable,
                                    "cellOptions": {"type": "auto"}},
                         "thresholds": {"mode": "absolute",
                                        "steps": [{"color": "text", "value": None}]},
                         "mappings": []},
            "overrides": overrides,
        },
        "options": {"showHeader": True, "cellHeight": "sm",
                    **({"sortBy": [sort_by]} if sort_by else {})},
    }


def day_barchart(title, desc, metric, series_label, w=12, h=7, x=0, y=0,
                 stack=True, unit="currencyUSD", decimals=2, overrides=None,
                 legend_mode="list", legend_placement="bottom"):
    """The SPA's PnL bar charts: one bar per calendar day, split by symbol or
    by PnL stream.

    x is the `day` LABEL, kept a string on purpose. The bot's day buckets are
    sparse (staging jumps Jul 8 -> Jul 22), and the SPA draws them as adjacent
    categories with no gap; converting `day` to a time field would reopen
    those two weeks as dead axis the SPA never shows. So: barchart with a
    categorical x, pivoted the same way the per-symbol tables are.
    """
    matrix_key = f"day\\{series_label}"
    return {
        "id": nid(), "type": "barchart", "title": title, "description": desc,
        "datasource": CM,
        "targets": [promql(
            f'sum by (day, {series_label}) ({metric}{{window="$window"}})',
            instant=True)],
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "transformations": [
            {"id": "labelsToFields", "options": {}},
            {"id": "merge", "options": {}},
            {"id": "groupBy", "options": {"fields": {
                "day": {"operation": "groupby", "aggregations": []},
                series_label: {"operation": "groupby", "aggregations": []},
                "Value": {"operation": "aggregate",
                          "aggregations": ["lastNotNull"]},
            }}},
            {"id": "groupingToMatrix",
             "options": {"columnField": series_label, "rowField": "day",
                         "valueField": "Value (lastNotNull)",
                         # A symbol with no fill that day is a zero bar in the
                         # SPA, not a hole.
                         "emptyValue": "zero"}},
            {"id": "organize",
             "options": {"excludeByName": {}, "indexByName": {},
                         "renameByName": {matrix_key: "Day"}}},
            # groupingToMatrix does not promise row order; ISO days sort
            # lexically = chronologically, so this is the whole fix.
            {"id": "sortBy",
             "options": {"fields": {},
                         "sort": [{"field": "Day", "desc": False}]}},
        ],
        "fieldConfig": {
            "defaults": {
                "unit": unit, "decimals": decimals,
                "custom": {"lineWidth": 1, "fillOpacity": 80,
                           "gradientMode": "none", "axisPlacement": "auto",
                           "axisLabel": "", "thresholdsStyle": {"mode": "off"}},
                "thresholds": {"mode": "absolute",
                               "steps": [{"color": "text", "value": None}]},
                "mappings": [],
            },
            "overrides": overrides or [],
        },
        "options": {
            "xField": "Day", "orientation": "auto",
            "stacking": "normal" if stack else "none",
            # The SPA sizes a bar `min(34px, slot * 0.68)`. Grafana's
            # barchart takes only a slot FRACTION, no pixel cap (checked in
            # the 13.1.0 panel editor: Bar width and Bar radius, nothing
            # else), so no single number reproduces that at every bucket
            # count. 0.35 lands on ~34px at this panel width for a normal
            # range; a range with only two buckets still draws two wide
            # bars where the SPA would draw two narrow ones. Cosmetic, and
            # the alternative is thread-thin bars on every normal range.
            "showValue": "never", "barWidth": 0.35, "groupWidth": 0.7,
            "xTickLabelRotation": -45, "xTickLabelSpacing": 0,
            "legend": {"displayMode": legend_mode, "placement": legend_placement,
                       "showLegend": True, "calcs": []},
            "tooltip": {"mode": "multi", "sort": "desc"},
        },
    }


def cloudlog_table(title, desc, query, columns, w=12, h=9, x=0, y=0,
                   widths=None, mappings_by_field=None, time_from=None,
                   number_columns=None, time_columns=None, time_name="Time",
                   order_override=None, time_unit="time:MMM D, HH:mm:ss",
                   hide_time=False, dedup_by=None):
    """columns: list of (raw_field, display_name).

    number_columns: {display_name: decimals} — the payload ships decimal
      STRINGS; convertFieldType turns them numeric so decimals apply
      (the SPA shows 0.049, not 0.0493843...).
    time_columns: display names formatted in the SPA's "Jul 31, 13:31:50"
      style (override with time_unit, e.g. TIME_FMT for the "... [UTC]"
      variant); string RFC3339 columns (e.g. Started) are also
      convertFieldType'd to real time fields.
    hide_time: drop the Cloud Logging entry's own ingestion timestamp
      (the field normally renamed to time_name) from the rendered table,
      for panels where a business timestamp already in time_columns is
      the only one the SPA shows (e.g. Orders' "Created" vs. the
      exporter's poll-time entry timestamp). Sort then follows the first
      time_columns entry instead of time_name.
    """
    includes = ["timestamp"] + [raw for raw, _ in columns]
    renames = {"timestamp": time_name, **{raw: name for raw, name in columns}}
    order = order_override or (["timestamp"] + [raw for raw, _ in columns])
    # convertFieldType's targetField matches Field.name, which the later
    # `organize` step's renameByName does NOT change (it only sets
    # config.displayName — byName field-matchers resolve through that, but
    # the transformer itself does not). So conversions must run BEFORE
    # organize and target the raw pre-rename field names, even though every
    # caller thinks in display names (number_columns/time_columns keys).
    raw_by_display = {time_name: "timestamp",
                      **{disp: raw for raw, disp in columns}}
    # When the poll-time column is hidden (Orders shows the order's own
    # created_at instead), sort on the first real time column instead.
    sort_field = time_columns[0] if (hide_time and time_columns) else time_name
    overrides = [
        {"matcher": {"id": "byName", "options": name},
         "properties": [{"id": "custom.width", "value": width}]}
        for name, width in (widths or {}).items()
    ]
    for field, mappings in (mappings_by_field or {}).items():
        overrides.append({
            "matcher": {"id": "byName", "options": field},
            "properties": [
                {"id": "mappings", "value": mappings},
                {"id": "custom.cellOptions", "value": {"type": "color-text"}},
            ],
        })
    conversions = []
    for name, decimals in (number_columns or {}).items():
        conversions.append({"targetField": raw_by_display[name],
                            "destinationType": "number"})
        overrides.append({
            "matcher": {"id": "byName", "options": name},
            "properties": [{"id": "unit", "value": "locale"},
                           {"id": "decimals", "value": decimals}],
        })
    for name in (time_columns or []):
        if name != time_name:  # string RFC3339 field -> real time field
            conversions.append({"targetField": raw_by_display[name],
                                "destinationType": "time"})
        overrides.append({
            "matcher": {"id": "byName", "options": name},
            "properties": [{"id": "unit", "value": time_unit}],
        })
    escaped = [raw.replace(".", r"\.") for raw in includes]
    return {
        "id": nid(), "type": "table", "title": title, "description": desc,
        "datasource": CL,
        "targets": [cloudlog(query)],
        **({"timeFrom": time_from} if time_from else {}),
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "transformations": [
            # `labels` is a JSON object per row; extractFields lifts each
            # key into its own column, dotted names kept verbatim (it
            # only JSON-parses strings, an object goes straight to
            # Object.entries). No jsonPaths: those would read the dots
            # as nesting.
            {"id": "extractFields",
             "options": {"source": "labels", "format": "json",
                         "replace": False, "keepTime": False}},
            {"id": "filterFieldsByName",
             "options": {"include": {"pattern": "^(" + "|".join(escaped) + ")$"}}},
            *([{"id": "convertFieldType",
                "options": {"conversions": conversions}}] if conversions else []),
            {"id": "organize",
             "options": {"excludeByName": ({"timestamp": True}
                                           if hide_time else {}),
                         "indexByName": {name: i for i, name in enumerate(order)},
                         "renameByName": renames}},
            # Current-state tables (Orders) ship one row per poll, so a
            # multi-poll window stacks repeats of the same entity. Collapse
            # to the newest row per key, then strip the " (first)" suffix
            # groupBy appends so the headers read like the SPA's. The plugin
            # returns entries newest first, so `first` is the newest row
            # (`lastNotNull` kept the oldest one in the window).
            *([
                {"id": "groupBy",
                 "options": {"fields": {
                     dedup_by: {"operation": "groupby", "aggregations": []},
                     **{disp: {"operation": "aggregate",
                               "aggregations": ["first"]}
                        for _, disp in columns if disp != dedup_by},
                 }}},
                {"id": "organize",
                 "options": {
                     "excludeByName": {},
                     "indexByName": {
                         (f"{n} (first)" if n != dedup_by else n): i
                         for i, n in enumerate(
                             [d for _, d in columns] if not order_override
                             else [renames.get(o, o) for o in order
                                   if renames.get(o, o) != time_name or not hide_time])
                     },
                     "renameByName": {f"{d} (first)": d
                                      for _, d in columns if d != dedup_by},
                 }},
            ] if dedup_by else []),
        ],
        "fieldConfig": {
            "defaults": {"custom": {"align": "auto", "filterable": True}},
            "overrides": overrides,
        },
        "options": {"showHeader": True, "cellHeight": "sm",
                    **({} if dedup_by else
                       {"sortBy": [{"displayName": sort_field, "desc": True}]})},
    }


STATUS_MAPPINGS = [
    {"type": "value", "options": {
        "filled": {"text": "filled", "color": "green", "index": 0},
        "completed": {"text": "completed", "color": "green", "index": 1},
        "failed": {"text": "failed", "color": "red", "index": 2},
        "cancelled": {"text": "cancelled", "color": "orange", "index": 3},
    }},
]
SIDE_MAPPINGS = [
    {"type": "value", "options": {
        "buy": {"text": "buy", "color": "green", "index": 0},
        "sell": {"text": "sell", "color": "red", "index": 1},
    }},
]
LIGHT_MAPPINGS = [
    {"type": "value", "options": {
        "1": {"text": "●", "color": "green", "index": 0},
        "0": {"text": "●", "color": "red", "index": 1},
    }},
    {"type": "special", "options": {"match": "null+nan",
                                    "result": {"text": "○", "color": "text",
                                               "index": 2}}},
]

USD = [{"id": "unit", "value": "currencyUSD"}, {"id": "decimals", "value": 2}]
USD_SIGNED = USD + [
    {"id": "custom.cellOptions", "value": {"type": "color-text"}},
    {"id": "thresholds", "value": {"mode": "absolute", "steps": [
        {"color": "red", "value": None}, {"color": "text", "value": -0.01},
        {"color": "green", "value": 0.01}]}},
]
PCT = [
    {"id": "unit", "value": "percentunit"}, {"id": "decimals", "value": 1},
    # Inline bar plus percent, like the SPA's Ratio progress bar. The bar
    # shares the cell with the percent text, so the column needs the width.
    {"id": "custom.cellOptions", "value": {"type": "gauge", "mode": "basic",
                                            "valueDisplayMode": "text"}},
    {"id": "min", "value": 0}, {"id": "max", "value": 1},
    {"id": "thresholds", "value": {"mode": "absolute", "steps": [
        {"color": "blue", "value": None}]}},
]
LIGHT = [
    {"id": "mappings", "value": LIGHT_MAPPINGS},
    {"id": "custom.cellOptions", "value": {"type": "color-text"}},
    {"id": "custom.align", "value": "center"},
    {"id": "custom.width", "value": 40},
]

# ==========================================================================
# Value mappings shared by the row tables — the SPA capitalizes and
# color-codes these, we mirror it exactly (venue palette from
# trade-history-panel.svelte).
# ==========================================================================
VENUE_MAPPINGS = [
    {"type": "value", "options": {
        "raindex": {"text": "Raindex", "color": "blue", "index": 0},
        "bebop": {"text": "Bebop", "color": "purple", "index": 1},
        "uniswap_v4": {"text": "Uniswap v4", "color": "#e91e63", "index": 2},
        "unknown_onchain": {"text": "Unknown Onchain", "color": "text", "index": 3},
        "alpaca": {"text": "Alpaca", "color": "orange", "index": 4},
        "dry_run": {"text": "DryRun", "color": "text", "index": 5},
    }},
]
STATUS_CAP_MAPPINGS = [
    {"type": "value", "options": {
        "filled": {"text": "Filled", "color": "green", "index": 0},
        "completed": {"text": "Completed", "color": "green", "index": 1},
        "failed": {"text": "Failed", "color": "red", "index": 2},
        "cancelled": {"text": "Cancelled", "color": "orange", "index": 3},
        "minting": {"text": "Minting", "color": "blue", "index": 4},
        "wrapping": {"text": "Wrapping", "color": "blue", "index": 5},
        "depositing": {"text": "Depositing", "color": "blue", "index": 6},
        "withdrawing": {"text": "Withdrawing", "color": "blue", "index": 7},
        "unwrapping": {"text": "Unwrapping", "color": "blue", "index": 8},
        "sending": {"text": "Sending", "color": "blue", "index": 9},
        "pending_confirmation": {"text": "Pending confirmation", "color": "blue", "index": 10},
        "converting": {"text": "Converting", "color": "blue", "index": 11},
        "bridging": {"text": "Bridging", "color": "blue", "index": 12},
        "reconciled": {"text": "Reconciled", "color": "yellow", "index": 13},
    }},
]
TYPE_MAPPINGS = [
    {"type": "value", "options": {
        "equity_mint": {"text": "Mint", "color": "text", "index": 0},
        "equity_redemption": {"text": "Redeem", "color": "text", "index": 1},
        # A USDC bridge row reads its direction, like the SPA's
        # transferTypeLabel (dashboard/src/lib/transfer.ts).
        "alpaca_to_base": {"text": "Alpaca → Raindex", "color": "text", "index": 3},
        "base_to_alpaca": {"text": "Raindex → Alpaca", "color": "text", "index": 4},
    }},
]
# D13: transfer status carries the SPA's leading dot ("● Completed"); trade
# status (STATUS_CAP_MAPPINGS above) does NOT, so this is a separate mapping
# rather than a shared-constant edit.
STATUS_DOT_MAPPINGS = [
    {"type": "value", "options": {
        text: {**opts, "text": "● " + opts["text"]}
        for text, opts in STATUS_CAP_MAPPINGS[0]["options"].items()
    }},
]

# The SPA renders plain locale numbers (37,934.83) — no $ prefix, no SI
# abbreviation — in the inventory tables. "locale" is Grafana's
# toLocaleString unit; it never abbreviates.
NUM = [{"id": "unit", "value": "locale"}, {"id": "decimals", "value": 2}]
# The stackdriver plugin runs a range query even for an `instant` target, so
# a latest-value panel on the 24h dashboard pulled every 60s point of the day
# (27,092 points for the equity table) just to show the newest one. A 5m
# panel window keeps the same answer and makes the query about 60x smaller.
# It applies only while the board range is relative (the default now-24h):
# Grafana ignores a panel's relative override on an absolute range.
LATEST_ONLY = {"timeFrom": "5m", "hideTimeOverride": False}
# The one-row header pills have no room for the "Last 5 minutes" badge, so
# they hide it; the board description says the header shows live state.
LATEST_ONLY_HIDDEN = {"timeFrom": "5m", "hideTimeOverride": True}


# The SPA packs the inventory columns tight (64px bars, short numbers), so the
# whole table fits beside the trade list. Grafana's auto widths are about
# twice that and push Ratio and Exposure off the right edge.
def width(px):
    return [{"id": "custom.width", "value": px}]


# The SPA's timestamp style: "Jul 31, 13:31:50 UTC" (D7).
TIME_FMT = [{"id": "unit", "value": "time:MMM D, HH:mm:ss [UTC]"}]
# D5: narrow ratio-deviation column after "Ratio", in band widths
# ((ratio - target) / deviation): outside +/-1 is colored, inside the band
# renders in plain text.
DELTA = [
    {"id": "unit", "value": "short"}, {"id": "decimals", "value": 1},
    {"id": "custom.cellOptions", "value": {"type": "color-text"}},
    {"id": "custom.width", "value": 45},
    {"id": "thresholds", "value": {"mode": "absolute", "steps": [
        {"color": "red", "value": None}, {"color": "text", "value": -1},
        {"color": "green", "value": 1}]}},
]

# ==========================================================================
# The tab suite: five dashboards, one per SPA tab, cross-linked with a
# button row that mimics the SPA's tab bar. Every link carries
# theme=light so the suite renders in the SPA's light look regardless of
# the viewer's Grafana theme preference.
# ==========================================================================
TABS = [
    ("Dashboard", "t0-liquidity"),
    ("Orders", "t0-liquidity-orders"),
    ("PnL", "t0-liquidity-pnl"),
    ("Performance", "t0-liquidity-performance"),
    ("Logs", "t0-liquidity-logs"),
]


def tab_links(active):
    links = []
    for name, uid in TABS:
        links.append({
            "type": "link",
            "title": f"▸ {name}" if name == active else name,
            # autofitpanels stretches the board to the window height, like
            # the SPA's full-height layout. Only the Dashboard tab: the
            # other tabs hold too many panels to squeeze into one screen.
            "url": f"/d/{uid}/?theme=light"
                   + ("&autofitpanels" if uid == "t0-liquidity" else ""),
            "icon": "dashboard", "targetBlank": False,
            "keepTime": True, "includeVars": True,
            "asDropdown": False, "tags": [], "tooltip": "",
        })
    links.append({
        "type": "dashboards", "title": "Related", "tags": ["t0"],
        "asDropdown": True, "icon": "external link", "includeVars": False,
        "keepTime": True, "targetBlank": False,
        "tooltip": "Other T0 dashboards",
    })
    return links


def pills(y):
    """The SPA's header + settings bar as one thin pill row, repeated on
    every tab like the SPA repeats its header. One grid row tall: a pill
    with an empty title gets a hover-only header, so the value fills the
    30px row instead of sitting under a title line.

    The SPA has no Mode or CT-assets pill, so this row has neither. A stat cannot
    join two series into one string, so the Equity and USDC pills show
    the target only; the band is in their descriptions."""
    def pill(title, desc, expr, display=None, unit="short", decimals=None,
             mappings=None, legend=None, text_mode="value_and_name", x=0,
             w=3):
        return {**stat(title, desc, expr, display=display, unit=unit,
                       decimals=decimals, mappings=mappings, legend=legend,
                       text_mode=text_mode,
                       color_mode="none" if not mappings else "value",
                       w=w, h=1, x=x, y=y, value_size=14),
                **LATEST_ONLY_HIDDEN}

    return [
        pill("", "The SPA header's connection light: is the bot process up "
             "and answering? The exporter source reads liq_up, which is 0 "
             "when the exporter cannot reach the bot's /health. The bot "
             "never emits liq_up, so the bot source reads the scrape's own "
             "up. '—' means the source is not reporting.",
             LIQ_UP,
             display="Bot",
             mappings=[{"type": "value", "options": {
                 "1": {"text": "Connected", "color": "green", "index": 0},
                 "0": {"text": "Disconnected", "color": "red", "index": 1}}}],
             x=0, w=3),
        {**pill("", "Deployed commit, from /health, cut to 7 "
                "characters like the SPA header. The stackdriver plugin "
                "runs a range query even for an instant target, so over "
                "the dashboard's 24h the previous commit is a second series "
                "and its name drew over this one. PromQL also keeps a "
                "stopped series for its 5m lookback, so topk on the sample "
                "timestamp keeps only the commit with the newest sample, "
                "over a 1m window.",
                # max by drops every other label: this datasource ignores
                # legendFormat, so the series name must be the commit alone.
                'max by (git_commit) (topk(1, label_replace('
                'timestamp(liq_bot_info), "git_commit", "$1", "git_commit", '
                '"(.{7}).*")))',
                legend="{{git_commit}}", text_mode="name", x=3, w=3),
         "timeFrom": "1m"},
        pill("", "Bot process uptime, from /health uptimeSeconds.",
             "time() - max(liq_bot_start_timestamp_seconds)",
             display="Uptime", unit="s", decimals=0, x=6, w=3),
        pill("", "The SPA settings bar's Broker pill.",
             "sum by (broker) (liq_settings_info)",
             legend="{{broker}}", text_mode="name", x=9, w=3),
        pill("", "Equity rebalance target. The SPA pill also shows the "
             "band: target +/- liq_settings_equity_deviation.",
             "max(liq_settings_equity_target)", display="Equity",
             unit="percentunit", decimals=0, x=12, w=4),
        pill("", "USDC rebalance target. The SPA pill also shows the "
             "band: target +/- liq_settings_usdc_deviation.",
             "max(liq_settings_usdc_target)", display="USDC",
             unit="percentunit", decimals=0, x=16, w=4),
        pill("", "Hedge execution threshold (settings bar 'Trigger').",
             "max(liq_settings_execution_threshold_usd)", display="Trigger",
             unit="currencyUSD", decimals=0, x=20, w=4),
    ]


# The SPA's PnL range buttons (1W / 1M / YTD / 1Y / All). PnL is a windowed
# server-side computation, so the exporter runs one /pnl query per window
# and tags every series with window=<key>; this variable picks which set the
# board shows. Default 1w, because that is what the SPA's PnL tab opens on —
# with any other default the tiles disagree with the SPA at a glance.
# Windows are anchored on the last date WITH DATA, not today (see the
# exporter's collect_pnl docstring).
WINDOW_VAR = {
    "name": "window", "type": "custom", "label": "PnL range",
    "description": "Matches the SPA's PnL range buttons. Windows are "
                   "relative to the last date with fills, not to today.",
    "query": "1D : 1d, 1W : 1w, 1M : 1m, YTD : ytd, 1Y : 1y, All : all",
    "includeAll": False, "multi": False, "hide": 0,
    "current": {"selected": True, "text": "1W", "value": "1w"},
    "options": [
        {"selected": False, "text": "1D", "value": "1d"},
        {"selected": True, "text": "1W", "value": "1w"},
        {"selected": False, "text": "1M", "value": "1m"},
        {"selected": False, "text": "YTD", "value": "ytd"},
        {"selected": False, "text": "1Y", "value": "1y"},
        {"selected": False, "text": "All", "value": "all"},
    ],
}

# Which job's liq_* series the boards read. The exporter sidecar is the
# default until Stage 4 of the migration flips it to the bot; the bot's job is
# t0-liquidity in production and t0-liquidity-staging in staging, and `env`
# already picks the project, so one regex covers both.
SOURCE_VAR = {
    "name": "source", "type": "custom", "label": "Source",
    "description": "Which process serves the liq_* metrics: the exporter "
                   "sidecar (the default for now) or the bot's own "
                   "/metrics. Pick bot to compare the two.",
    "query": "exporter : t0-liquidity-exporter, "
             "bot : t0-liquidity|t0-liquidity-staging",
    "includeAll": False, "multi": False, "hide": 0,
    "current": {"selected": True, "text": "exporter",
                "value": "t0-liquidity-exporter"},
    "options": [{"selected": True, "text": "exporter",
                 "value": "t0-liquidity-exporter"},
                {"selected": False, "text": "bot",
                 "value": "t0-liquidity|t0-liquidity-staging"}],
}

ENV_VAR = {
    "name": "env", "type": "custom", "label": "Environment",
    "description": "GCP project id — both the Cloud Monitoring and Cloud "
                   "Logging queries take it directly.",
    "query": "production : t0-liquidity, staging : t0-liquidity-staging",
    # Visible now that production exists (it was hide: 2 while staging was
    # the only option). Production first and default: it is the env people
    # open the board for. This dict is shared by reference across all five
    # make_dashboard() calls, so one edit here changes it everywhere at
    # once, by design and not by accident.
    "includeAll": False, "multi": False, "hide": 0,
    "current": {"selected": True, "text": "production",
                "value": "t0-liquidity"},
    "options": [{"selected": True, "text": "production",
                 "value": "t0-liquidity"},
                {"selected": False, "text": "staging",
                 "value": "t0-liquidity-staging"}],
}


def make_dashboard(uid, title, description, panels, links, variables):
    if uid != "t0-liquidity":
        # Tab boards live in the liquidity/tabs folder; say so, since a
        # viewer landing here from search has no other hint that the main
        # board (liquidity folder) is the entry point.
        description = ("Sub-page of the Liquidity bot board in the "
                       "liquidity folder. " + description)
    return {
        "uid": uid, "title": title, "description": description,
        "tags": ["liquidity", "bot", "t0"],
        "timezone": "utc", "editable": True, "graphTooltip": 1,
        "schemaVersion": 39, "refresh": "1m",
        "time": {"from": "now-24h", "to": "now"},
        "templating": {"list": variables},
        "links": links,
        "panels": panels,
    }


dashboards = []

# ==========================================================================
# Tab 1: Dashboard — the SPA's Inventory card (cash row + equity table)
# on the left, Trade History / Cross-venue Transfers stacked right.
# ==========================================================================
panels = []
panels += pills(0)

def usdc(metric):
    return f'label_replace({metric}, "row", "USDC", "", "")'


def usdc_group(title, desc, cols, w, x, overrides, show_asset=False,
               latest=LATEST_ONLY):
    """One slice of the SPA's USDC row. The SPA groups its right-hand
    columns under "Alpaca" and "Wallets" header cells; a Grafana table has
    no grouped headers, so each group is its own panel and the panel title
    is the group header."""
    panel = matrix_table(
        title, desc, or_chain([(usdc(metric), name) for metric, name in cols],
                              row_label="row"),
        "row", w=w, h=4, x=x, y=1, first_col="Asset",
        column_order=[name for _, name in cols], unit_overrides=overrides,
        decimals=2)
    if not show_asset:
        panel["transformations"][-1]["options"]["excludeByName"]["row\\col"] = True
    # Grafana gives a column without a width a 150px minimum, which made
    # these narrow panels scroll; 40px lets an auto column take what is left.
    panel["fieldConfig"]["defaults"]["custom"]["minWidth"] = 40
    return {**panel, **latest}


panels.append(usdc_group(
    "Inventory",
    "The SPA's USDC row: Raindex + Alpaca Total + Inflight = Total; Ratio "
    "= Raindex/(Raindex+Alpaca Total); Δ = deviation from the target ratio "
    "in band widths, colored outside +/-1, where the SPA colors its bar.",
    [("liq_usdc_onchain_available", "Raindex"),
     ("liq_usdc_inflight_total", "Inflight"),
     ("liq_usdc_alpaca_total", "Alpaca Total"),
     ("liq_usdc_total", "Total"),
     ("liq_usdc_ratio", "Ratio"),
     ("liq_usdc_ratio_deviation and liq_usdc_total > 0", "Δ")],
    w=7, x=0, show_asset=True,
    # 90px fits a six-figure balance ("104,302.08"); Ratio gives up the
    # width, since its bar is redrawn in part 2 anyway.
    overrides={"Asset": width(50), "Inflight": NUM + width(80),
               **{c: NUM + width(90) for c in
                  ["Raindex", "Alpaca Total", "Total"]},
               "Ratio": PCT + width(75), "Δ": DELTA}))
panels.append(usdc_group(
    "Alpaca",
    "Rebalanceable = max(0, withdrawable − reserve). Counter-tradeable "
    "deliberately equals Alpaca Total (reserve NOT subtracted, same as the "
    "SPA).",
    [("liq_usdc_alpaca_usdc", "USDC"),
     ("liq_usdc_rebalanceable", "Rebalanceable"),
     ("liq_usdc_alpaca_total", "Counter-tradeable")],
    w=4, x=7, overrides={"USDC": NUM + width(90),
                         "Rebalanceable": NUM + width(105),
                         # No width: it takes the rest of the panel.
                         "Counter-tradeable": NUM}))
panels.append(usdc_group(
    "Wallets",
    "USDC seen in the bot's Ethereum and Base wallets: sanity checks, not "
    "part of Total.",
    [("liq_usdc_inflight_ethereum_wallet", "Eth"),
     ("liq_usdc_inflight_base_wallet", "Base")],
    w=3, x=11, overrides={"Eth": NUM + width(80), "Base": NUM},
    # Three units are too narrow for the title and the badge: Grafana drops
    # the title, so this panel hides the badge like the header pills.
    latest=LATEST_ONLY_HIDDEN))

equity_expr = or_chain([
    ("liq_asset_counter_trading", "CT"),
    ("liq_asset_rebalancing", "Rebal"),
    ("liq_asset_extended_hours", "Ext"),
    ("liq_equity_onchain_available", "Raindex"),
    ("liq_equity_inflight_total", "Inflight"),
    ("liq_equity_offchain_available", "Alpaca"),
    ("liq_equity_total", "Total"),
    ("liq_equity_ratio", "Ratio"),
    ("liq_equity_ratio_deviation and liq_equity_total > 0", "Δ"),
    # The exporter emits Exposure only for a priced symbol. NaN for the rest
    # keeps the column in the matrix when no symbol has a price, and NaN hits
    # the "—" mapping like the SPA's dash.
    ("liq_equity_exposure_usd or (liq_equity_total * NaN)", "Exposure"),
    ("liq_equity_unwrapped", "Unwrapped"),
    ("liq_equity_wrapped", "Wrapped"),
])
panels.append({**matrix_table(
    "",
    "The SPA inventory equity table: CT / Rebal / Ext status lights from "
    "settings; Raindex / Inflight / Alpaca / Total share balances; Ratio = "
    "onchain/(onchain+offchain); Δ = deviation from the target ratio in "
    "band widths, colored outside +/-1; Exposure = net × last price. "
    "Unwrapped/Wrapped are wallet-observed and not part of Total. Sorted "
    "CT-first (desc) so counter-tradeable assets float to the top, like "
    "the SPA.",
    equity_expr, "symbol", w=14, h=24, x=0, y=5,
    first_col="Asset",
    column_order=["CT", "Rebal", "Ext", "Raindex", "Inflight", "Alpaca",
                  "Total", "Ratio", "Δ", "Exposure", "Unwrapped", "Wrapped"],
    unit_overrides={
        "Asset": width(60),
        "CT": LIGHT, "Rebal": LIGHT, "Ext": LIGHT,
        **{c: NUM + width(70) for c in ["Raindex", "Inflight", "Alpaca",
                                        "Total", "Wrapped"]},
        "Unwrapped": NUM + width(80),
        # Wider than the USDC row's: the gauge draws its bar in what the
        # percent text leaves, and the SPA's bar is 64px.
        "Ratio": PCT + width(140), "Δ": DELTA,
        # The SPA prints "—" when the pricing service has no live price.
        "Exposure": USD_SIGNED + width(80) + [{"id": "mappings", "value": [
            {"type": "special", "options": {"match": "null+nan", "result": {
                "text": "—", "color": "text", "index": 0}}}]}],
    },
    sort_by={"displayName": "CT", "desc": True},
    decimals=2,
), **LATEST_ONLY})

def latest_status_table(title, desc, log, fields, shown, w, h, x, y,
                        body_regex=None, number_columns=None, time_columns=(),
                        overrides=(), sort_by=None):
    """A Cloud Logging table with one row per entity at its latest status,
    like the SPA's Trade History and Cross-venue Transfers cards.

    The exporter writes one log entry per status change (insertId
    `<id>:<status>`), so the raw feed holds several rows per trade or
    transfer. The plugin returns entries newest first, so groupBy on
    jsonPayload.id with `first` keeps each entity's latest status.

    fields: {payload leaf: column name}; the entry's own timestamp is
      always kept, as fields["timestamp"].
    shown: column names in display order.
    body_regex: an extra extractFields over the entry's JSON body, with
      named groups as new columns. Grafana only treats the pattern as a
      regex when it is wrapped in slashes; without them it silently
      returns the whole body as one "NewField" column.
    number_columns: {column: decimals}; the payload ships decimal strings.
    time_columns: string RFC3339 columns to turn into real time fields.
    sort_by: newest-first column; defaults to the entry timestamp.
    """
    raw = {f"jsonPayload.{leaf}": name for leaf, name in fields.items()
           if leaf != "timestamp"}
    raw["timestamp"] = fields["timestamp"]
    raw["jsonPayload.id"] = "Id"
    by_name = {name: leaf for leaf, name in raw.items()}
    body_columns = [name for name in shown if name not in by_name]
    conversions = (
        [{"targetField": by_name[name], "destinationType": "number"}
         for name in (number_columns or {})]
        + [{"targetField": by_name[name], "destinationType": "time"}
           for name in time_columns])
    return {
        "id": nid(), "type": "table", "title": title, "description": desc,
        "datasource": CL,
        "timeFrom": "30d",
        # The plugin takes the row limit from maxDataPoints, which Grafana
        # sets to the panel's pixel width when the panel does not set it.
        # 500 newest entries load in about 1.5s and still leave well over
        # the SPA's 100 rows after grouping by id.
        "maxDataPoints": 500,
        "targets": [cloudlog(log)],
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "transformations": [
            # `labels` is a JSON object per row; extractFields lifts each
            # key into its own column, dotted names kept verbatim.
            {"id": "extractFields",
             "options": {"source": "labels", "format": "json",
                         "replace": False, "keepTime": False}},
            *([{"id": "extractFields",
                "options": {"source": "body", "format": "regexp",
                            "regExp": f"/{body_regex}/",
                            "replace": False, "keepTime": False}}]
              if body_regex else []),
            {"id": "filterFieldsByName",
             "options": {"include": {"names": list(raw) + body_columns}}},
            # convertFieldType matches Field.name, which organize's rename
            # does not change, so it runs first on the raw names.
            *([{"id": "convertFieldType",
                "options": {"conversions": conversions}}] if conversions else []),
            {"id": "organize",
             "options": {"excludeByName": {}, "indexByName": {},
                         "renameByName": raw}},
            {"id": "groupBy", "options": {"fields": {
                "Id": {"operation": "groupby", "aggregations": []},
                **{name: {"operation": "aggregate", "aggregations": ["first"]}
                   for name in shown},
            }}},
            {"id": "organize",
             "options": {"excludeByName": {"Id": True},
                         "indexByName": {f"{name} (first)": index
                                         for index, name in enumerate(shown)},
                         "renameByName": {f"{name} (first)": name
                                          for name in shown}}},
            {"id": "sortBy", "options": {"fields": {}, "sort": [
                {"field": sort_by or fields["timestamp"], "desc": True}]}},
        ],
        "fieldConfig": {
            "defaults": {"custom": {"align": "auto", "filterable": True}},
            "overrides": [
                *[{"matcher": {"id": "byName", "options": name},
                   "properties": [{"id": "unit", "value": "locale"},
                                  {"id": "decimals", "value": decimals}]}
                  for name, decimals in (number_columns or {}).items()],
                *[{"matcher": {"id": "byName", "options": name},
                   "properties": TIME_FMT}
                  for name in [fields["timestamp"], *time_columns]],
                *overrides,
            ],
        },
        "options": {"showHeader": True, "cellHeight": "sm"},
    }


def column(name, px, mappings=None, color_text=False):
    properties = [{"id": "custom.width", "value": px}] if px else []
    if mappings:
        properties.append({"id": "mappings", "value": mappings})
    if color_text:
        properties.append({"id": "custom.cellOptions",
                           "value": {"type": "color-text"}})
    return {"matcher": {"id": "byName", "options": name},
            "properties": properties}


panels.append(latest_status_table(
    "Trade History",
    "Direct Raindex fills, fills routed through supported adapters such as "
    "Bebop, and the corresponding Alpaca hedge trades placed to offset "
    "exposure. One row per trade at its latest status. Built from the "
    "newest 500 status entries, so a busy period can push older rows out "
    "of the 30-day window.",
    'logName="projects/$env/logs/liquidity-trades"',
    fields={"timestamp": "Time", "symbol": "Asset", "venue": "Venue",
            "direction": "Side", "shares": "Size", "status": "Status"},
    shown=["Time", "Asset", "Venue", "Side", "Size", "Status"],
    w=10, h=14, x=14, y=1,
    number_columns={"Size": 3},
    overrides=[
        # No width on the last column: it takes what is left, so the
        # table fills the card like the SPA's.
        column("Time", 170), column("Asset", 80),
        column("Venue", 110, VENUE_MAPPINGS, color_text=True),
        column("Side", 70, SIDE_MAPPINGS, color_text=True),
        column("Size", 110),
        column("Status", None, STATUS_CAP_MAPPINGS, color_text=True),
    ],
))

panels.append(latest_status_table(
    "Cross-venue Transfers",
    "Asset movements between venues to rebalance inventory: equity mints "
    "(Alpaca to onchain), redemptions (onchain to Alpaca), and USDC bridges "
    "(Base/Ethereum via CCTP). One row per transfer at its latest status. "
    "A USDC bridge can show the previous status for a while: the exporter "
    "stamps each status with the transfer's updatedAt, which is not always "
    "later than the one before (bridging starts from initiated_at). "
    "Built from the newest 500 status entries, so a busy period can push "
    "older rows out of the 30-day window.",
    'logName="projects/$env/logs/liquidity-transfers"',
    fields={"timestamp": "Updated", "started_at": "Started", "symbol": "Asset",
            "amount": "Amount", "status": "Status"},
    shown=["Started", "Type", "Asset", "Amount", "Status", "Updated"],
    # Type reads the direction for a USDC bridge and the kind for an equity
    # transfer, like the SPA's transferTypeLabel. A bridge's kind is
    # usdc_bridge and an equity transfer's direction is "", so each row can
    # match only one alternative, whatever the body's key order.
    body_regex='"(?:direction|kind)":"(?<Type>alpaca_to_base|base_to_alpaca'
               '|equity_mint|equity_redemption)"',
    w=10, h=14, x=14, y=15,
    number_columns={"Amount": 3},
    time_columns=["Started"],
    overrides=[
        column("Started", 135), column("Type", 125, TYPE_MAPPINGS),
        # A USDC bridge carries no symbol; the SPA's Asset column reads
        # "USDC" for it.
        column("Asset", 60, [{"type": "special", "options": {
            "match": "empty", "result": {"text": "USDC", "index": 0}}}]),
        column("Amount", 85),
        column("Status", 110, STATUS_DOT_MAPPINGS, color_text=True),
        column("Updated", None),
    ],
))


dashboards.append(make_dashboard(
    "t0-liquidity", "Liquidity bot",
    "1:1 Grafana rendering of the liquidity bot's own dashboard, fed by "
    "the exporter sidecar on the VM. Five linked boards mimic the SPA's "
    "tabs — use the Dashboard/Orders/PnL/Performance/Logs links above "
    "(they carry theme=light so the suite matches the SPA's look). "
    "Refresh is 1m polling, not WebSocket-live. On a relative range (the "
    "default now-24h) the header and the inventory show the live state, "
    "the last 5 minutes, and trades and transfers show the last 30 days. "
    "An absolute range shows that range's data in every panel. Open it with "
    "&autofitpanels (the Dashboard tab link does) to fill the window "
    "height like the SPA. Full history and drilldowns: the IAP "
    "dashboard at https://liquidity.t0trade.com (production), or gcloud "
    "compute start-iap-tunnel <vm> 8080 "
    "--local-host-port=localhost:8080 --zone europe-west3-b "
    "--project $env (VM name = project id)",
    panels, tab_links("Dashboard"), [ENV_VAR, SOURCE_VAR]))

# ==========================================================================
# Tab 2: Orders — the SPA's Raindex Orders table (+ ops extras).
# ==========================================================================
panels = []
panels += pills(0)
panels.append(cloudlog_table(
    "Raindex Orders",
    "The SPA's Orders tab, columns in SPA order (Output | Input | Vault "
    "Balance | IO Ratio | Order Hash | Created). Shipped by the exporter's "
    "collect_orders() as one Cloud Logging row per order per poll cycle "
    "(logName liquidity-orders); insertId buckets by poll-hour so a "
    "still-active order dedupes to ~1 stored row/hour rather than one per "
    "60s poll — with a 2h window that means a couple of rows per order is "
    "expected, not a bug. Created is the order's own on-chain creation "
    "time (jsonPayload.created_at), not the row's ingestion time. The "
    "datasource returns at most about the panel's pixel width in entries.",
    'logName="projects/$env/logs/liquidity-orders"',
    columns=[("jsonPayload.output", "Output"), ("jsonPayload.input", "Input"),
             ("jsonPayload.vault_balance", "Vault Balance"),
             ("jsonPayload.io_ratio", "IO Ratio"),
             ("jsonPayload.order_hash", "Order Hash"),
             ("jsonPayload.created_at", "Created")],
    w=24, h=10, x=0, y=2,
    widths={"Output": 90, "Input": 90, "Vault Balance": 140,
            "IO Ratio": 120, "Order Hash": 260, "Created": 170},
    number_columns={"Vault Balance": 3, "IO Ratio": 3},
    time_columns=["Created"],
    hide_time=True,
    dedup_by="Order Hash",
    time_from="2h",
))
panels.append(stat(
    "", "In-flight broker orders right now (/orders/pending).",
    "max(liq_pending_orders_total) or vector(0)", display="PENDING ORDERS",
    decimals=0, steps=[{"color": "text", "value": None}],
    w=4, h=4, x=12, y=12))
panels.append(stat(
    "", "Raindex orders owned by the bot (upstream REST API total).",
    "max(liq_raindex_orders_total)", display="RAINDEX ORDERS", decimals=0,
    color_mode="none", w=4, h=4, x=16, y=12))
panels.append(stat(
    "", "1 when the bot's upstream REST API for Raindex orders is "
    "unavailable.",
    "max(liq_raindex_orders_unavailable) or vector(0)",
    display="UPSTREAM",
    mappings=[{"type": "value", "options": {
        "0": {"text": "OK", "color": "green", "index": 0},
        "1": {"text": "UNAVAILABLE", "color": "red", "index": 1}}}],
    w=4, h=4, x=20, y=12))
dashboards.append(make_dashboard(
    "t0-liquidity-orders", "Liquidity bot: Orders",
    "The SPA's Orders tab.",
    panels, tab_links("Orders"), [ENV_VAR, SOURCE_VAR]))

# ==========================================================================
# Tab 3: PnL — tiles, per-asset table, the four charts in SPA order.
# ==========================================================================
# P2: "currency:financial:$" is Grafana's non-abbreviating currency format
# (Intl.NumberFormat under the hood, grouped thousands, forced decimals,
# literal "$" prefix — added in grafana/grafana#106604, present since
# Grafana 13.x). Unlike "currencyUSD" it never switches to K/M/B, so it
# reproduces the SPA's tile style exactly: "$-25.77", "$130,861.46". The
# audit's fallback ("keep currencyUSD on tiles, full locale in tables") was
# only needed because no unit did both — this one does, so it's used on
# both the tiles below and the Per Asset PnL table's dollar columns
# (PNL_TABLE_USD_SIGNED further down); do NOT touch the shared USD_SIGNED
# constant (line ~344) — Tab 1 owns it (its Exposure column).
PNL_TILE_USD = "currency:financial:$"
PNL_TABLE_USD_SIGNED = [
    {"id": "unit", "value": PNL_TILE_USD}, {"id": "decimals", "value": 2},
    {"id": "custom.cellOptions", "value": {"type": "color-text"}},
    {"id": "thresholds", "value": {"mode": "absolute", "steps": [
        {"color": "red", "value": None}, {"color": "text", "value": -0.01},
        {"color": "green", "value": 0.01}]}},
]
panels = []
panels += pills(0)
pnl_tiles = [
    ("Net Realized PnL", "net_realized", "Gross minus costs plus revenues."),
    ("Realized Gross PnL", "gross_realized", "Closed-lot fill replay before costs."),
    ("Tracked Costs", "tracked_costs", "Cost/revenue ledger entries."),
    ("Tracked Revenue", "tracked_revenue", "Dividends and broker ledger credits."),
    ("Counter-Trade PnL", "counter_trade", "Broker hedge stream."),
    ("On-Chain Netting PnL", "onchain_netting", "Users passively crossed open inventory."),
    ("Directional Realized PnL", "directional_imbalance_excess", "Closed delayed or offchain-origin exposure."),
    ("Baseline Drift PnL", "directional_inventory_baseline", "Requires historical snapshots."),
    ("Directional Realized Total", "directional_exposure", "All directional streams together."),
    ("Total PnL", "total", "Every stream summed."),
]
for index, (label, stream, desc) in enumerate(pnl_tiles):
    signed = stream not in ("tracked_costs",)
    panels.append(stat(
        "", f"{desc} (SPA PnL tile, computed by the bot's replay — 5-min "
        "refresh cadence.)",
        f'max(liq_pnl_summary_usd{{stream="{stream}",window="$window"}})',
        display=label.upper(), unit=PNL_TILE_USD, decimals=2,
        steps=([{"color": "red", "value": None},
                {"color": "text", "value": -0.01},
                {"color": "green", "value": 0.01}] if signed
               else [{"color": "text", "value": None}]),
        w=4, h=3, x=(index % 5) * 4, y=2 + (index // 5) * 3,
        value_size=22))
panels.append(stat(
    "", "Open replay inventory in shares (the SPA's muted tile).",
    'max(liq_pnl_summary_shares{kind="inventory_drift",window="$window"})',
    display="OPEN INVENTORY (SH)", decimals=3, color_mode="none",
    w=4, h=3, x=20, y=2))
panels.append(stat(
    "", "Total fills in the PnL database sample.",
    'max(liq_pnl_sample_total_fills{window="$window"})', display="TOTAL FILLS", decimals=0,
    color_mode="none", w=4, h=3, x=20, y=5))
panels.append(stat(
    "", "Average deployed capital from daily snapshots — computed by the "
    "bot, never rendered by the SPA.",
    'max(liq_pnl_capital_avg_deployed_usd{window="$window"})', display="AVG DEPLOYED CAPITAL",
    unit="currencyUSD", decimals=0, color_mode="none", w=8, h=3, x=0, y=8))
panels.append(stat(
    "", "Annualized return on that capital — computed by the bot, never "
    "rendered by the SPA.",
    'max(liq_pnl_capital_annualized_return_pct{window="$window"})', display="ANNUALIZED RETURN",
    unit="percent", decimals=2,
    steps=[{"color": "red", "value": None}, {"color": "text", "value": 0},
           {"color": "green", "value": 0.01}],
    w=8, h=3, x=8, y=8))
panels.append(stat(
    "", "Days of snapshot coverage behind the capital figures.",
    'max(liq_pnl_capital_coverage_days{window="$window"})', display="COVERAGE DAYS", decimals=0,
    color_mode="none", w=8, h=3, x=16, y=8))

pnl_cols = [
    ("Gross", "gross_realized"), ("Costs", "tracked_costs"),
    ("Revenue", "tracked_revenue"), ("Net", "net_realized"),
    ("Counter", "counter_trade"), ("On-chain", "onchain_netting"),
    ("Directional", "directional_imbalance_excess"),
    ("Baseline Drift", "directional_inventory_baseline"),
    ("Total", "total"),
]
# P5: the SPA's Volume column, in shares rather than USD, so it gets its own
# override rather than joining unit_overrides' USD_SIGNED sweep below. The
# SPA places it right after Net; decimals=4 matches its "20.0723 sh"
# precision. It counts a matched lot on both legs, so the exporter serves
# 2 x matchedShares (verified against the entries page: their shares sum
# to matchedShares exactly). Its USD sub-line is not reproduced; that one
# needs per-entry open/close prices from the full entries page.
VOLUME_SHARES = [{"id": "unit", "value": "locale"}, {"id": "decimals", "value": 4}]
panels.append(matrix_table(
    "Per Asset PnL",
    "The SPA's Per Asset PnL table (closed-lot replay per symbol; Lots = "
    "matched lot count; Volume = both legs of the matched shares, without "
    "the SPA's USD sub-line).",
    or_chain([(f'liq_pnl_symbol_usd{{col="{stream}",window="$window"}}', name)
              for name, stream in pnl_cols]
             + [('liq_pnl_symbol_lots{window="$window"}', "Lots"),
                ('liq_pnl_symbol_volume_shares{window="$window"}', "Volume")]),
    "symbol", w=24, h=8, x=0, y=11,
    first_col="Asset",
    column_order=["Gross", "Costs", "Revenue", "Net", "Volume", "Counter",
                  "On-chain", "Directional", "Baseline Drift", "Total", "Lots"],
    unit_overrides={**{name: PNL_TABLE_USD_SIGNED for name, _ in pnl_cols},
                    "Volume": VOLUME_SHARES},
    sort_by={"displayName": "Total", "desc": True},
    filterable=True,
))
# P4: fixed per-stream colors, matched to the live SPA's own color map —
# read directly out of its shipped JS bundle (node2.js): counterTradePnlUsd
# -> var(--chart-sky), onchainNettingPnlUsd -> var(--chart-green),
# directionalInventoryBaselinePnlUsd -> var(--chart-amber),
# directionalImbalanceExcessPnlUsd -> var(--chart-rose); hex values read
# from the SPA's computed --chart-* CSS custom properties. That map has
# exactly these four entries because the SPA's per-stream chart plots only
# these four components — "directional_exposure" (baseline_drift +
# directional_excess) and "total" (every stream summed) are tile-only
# aggregates there, never their own chart line. The exporter's day series
# are already scoped to those four (CHART_STREAMS), so nothing here has to
# filter them back out.
PNL_STREAM_COLORS = {
    "counter_trade": "#0284c7",                    # sky
    "onchain_netting": "#16a34a",                   # green
    "directional_inventory_baseline": "#d97706",    # amber
    "directional_imbalance_excess": "#e11d48",      # rose
}
PNL_STREAM_OVERRIDES = [
    {"matcher": {"id": "byName", "options": stream},
     "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": color}}]}
    for stream, color in PNL_STREAM_COLORS.items()
]
# SPA chart order: the daily bar pair first, the cumulative pair below it,
# with the SPA's exact titles. All four read the bot's own day buckets
# (/pnl `windows[]`), the same array the SPA charts, so they carry the
# bot's full history for the selected range from the first scrape, and they
# move with the range pill exactly as the SPA's charts move with its date
# inputs.
panels.append(day_barchart(
    "PnL by Asset per Time Unit",
    "One bar per calendar day, split by asset and stacked, straight from "
    "the bot's daily PnL buckets. A day with no closed lot for an asset is "
    "a zero, as in the SPA.",
    "liq_pnl_day_usd", "symbol", w=12, h=7, x=0, y=19))
panels.append(day_barchart(
    "PnL by PnL-Stream per Time Unit",
    "The same days split by PnL stream, the SPA's four component streams "
    "only, since directional_exposure and total are tile-only aggregates "
    "there and stacking them beside their own components double-counts.",
    "liq_pnl_day_stream_usd", "stream", w=12, h=7, x=12, y=19,
    overrides=PNL_STREAM_OVERRIDES))
panels.append(day_barchart(
    "Cumulative PnL by Asset",
    "Running total per asset through the selected range. Unstacked: the "
    "SPA draws one independent line per asset here, not a stack. The last "
    "day's bars sum to the Total PnL tile above.",
    "liq_pnl_day_cum_usd", "symbol", w=12, h=7, x=0, y=26, stack=False,
    legend_mode="table", legend_placement="right"))
panels.append(day_barchart(
    "Cumulative PnL by PnL-Stream",
    "Running total per stream, unstacked like the SPA's lines. Grafana has "
    "no categorical-axis line chart, so these are bars over the same day "
    "buckets: the numbers are the SPA's, the shape is not.",
    "liq_pnl_day_cum_stream_usd", "stream", w=12, h=7, x=12, y=26,
    stack=False, overrides=PNL_STREAM_OVERRIDES,
    legend_mode="table", legend_placement="right"))
dashboards.append(make_dashboard(
    "t0-liquidity-pnl", "Liquidity bot: PnL",
    "The SPA's PnL tab, tiles, table and charts all driven by the range "
    "pill; closed-lot and cost-ledger drilldowns live in the SPA.",
    panels, tab_links("PnL"), [ENV_VAR, SOURCE_VAR, WINDOW_VAR]))

# ==========================================================================
# Tab 4: Performance — SLO cards, stage charts, health tables + the
# pre-exporter native-metric rows, collapsed.
# ==========================================================================
panels = []
panels += pills(0)
panels.append(stat(
    "Detection latency", "p95, 24h. Fill on-chain → bot noticed. good "
    "≤30s, warn ≤120s.",
    'max(liq_hedge_latency_ms{stage="detection",quantile="p95"})',
    unit="ms", decimals=0,
    steps=[{"color": "green", "value": None}, {"color": "yellow", "value": 30000},
           {"color": "red", "value": 120000}],
    w=5, h=4, x=0, y=2, text_mode="value"))
panels.append(stat(
    "Exposure window", "p95, 24h. Fill on-chain → hedge filled: how long "
    "capital stays exposed. good ≤60s, warn ≤300s.",
    'max(liq_hedge_latency_ms{stage="exposure_window",quantile="p95"})',
    unit="ms", decimals=0,
    steps=[{"color": "green", "value": None}, {"color": "yellow", "value": 60000},
           {"color": "red", "value": 300000}],
    w=5, h=4, x=5, y=2, text_mode="value"))
panels.append(stat(
    "Errors (24h)",
    "ERROR-level log lines in 24h. good ≤0, warn ≤10. Money-at-risk "
    "lifecycle failures are broken out per event type in the 'Errors & "
    "warnings by module' table below.",
    'max(liq_reliability_log_count_24h{level="error"}) or vector(0)',
    decimals=0,
    steps=[{"color": "green", "value": None}, {"color": "yellow", "value": 1},
           {"color": "red", "value": 11}],
    w=5, h=4, x=10, y=2, text_mode="value"))
panels.append(stat(
    "Oldest unhedged fill",
    "Age of the oldest on-chain fill not yet hedged. good ≤5m, warn ≤30m; "
    "'—' = nothing unhedged (the SPA shows 'none').",
    "time() - min(liq_open_exposure_oldest_ts_seconds)",
    unit="s", decimals=0,
    steps=[{"color": "green", "value": None}, {"color": "yellow", "value": 300},
           {"color": "red", "value": 1800}],
    w=5, h=4, x=15, y=2, text_mode="value"))
panels.append(stat(
    "Block lag",
    "Chain blocks the fill-detection checkpoint trails by. good ≤30, warn "
    "≤300.",
    "max(liq_block_lag_blocks)", decimals=0,
    steps=[{"color": "green", "value": None}, {"color": "yellow", "value": 30},
           {"color": "red", "value": 300}],
    w=4, h=4, x=20, y=2, text_mode="value"))

# F1: the SPA's secondary context line per SLO card (e.g. "p50 118ms · 42
# fills") as a thin strip of small stats directly under the card row —
# chosen over hover descriptions because the pill row above already
# establishes "small transparent value_and_name stats in a compact strip"
# as this dashboard's visual language for secondary context, so this reads
# as an extension of an existing pattern rather than a new one. h=1, one
# extra grid row (y=6..7); every card gets a strip, kept consistent (all
# eleven sub-stats use the same title="" + displayName + value_and_name
# shape — no panel breaks the pattern).
panels.append(stat(
    "", "SPA secondary line: p50 detection latency.",
    'max(liq_hedge_latency_ms{stage="detection",quantile="p50"})',
    display="p50", unit="ms", decimals=0, color_mode="none",
    w=2, h=1, x=0, y=6, value_size=10))
panels.append(stat(
    "", "SPA secondary line: fill sample count backing the detection "
    "latency percentiles.",
    'max(liq_hedge_latency_ms_samples{stage="detection"}) or vector(0)',
    display="fills (24h)", decimals=0, color_mode="none",
    w=3, h=1, x=2, y=6, value_size=10))
panels.append(stat(
    "", "SPA secondary line: p50 exposure-window latency.",
    'max(liq_hedge_latency_ms{stage="exposure_window",quantile="p50"})',
    display="p50", unit="ms", decimals=0, color_mode="none",
    w=2, h=1, x=5, y=6, value_size=10))
panels.append(stat(
    "", "SPA secondary line: hedge sample count backing the exposure-"
    "window percentiles.",
    'max(liq_hedge_latency_ms_samples{stage="exposure_window"}) or vector(0)',
    display="hedges (24h)", decimals=0, color_mode="none",
    w=3, h=1, x=7, y=6, value_size=10))
panels.append(stat(
    "", "SPA secondary line: WARN-level log lines in 24h.",
    'max(liq_reliability_log_count_24h{level="warning"}) or vector(0)',
    display="warnings (24h)", decimals=0, color_mode="none",
    w=2, h=1, x=10, y=6, value_size=10))
panels.append(stat(
    "", "SPA secondary line: money-at-risk lifecycle failure events in "
    "24h (the 'Errors & warnings by module' table breaks these out by "
    "event type).",
    'sum(liq_failure_event_count_24h) or vector(0)',
    display="lifecycle fails", decimals=0, color_mode="none",
    w=2, h=1, x=12, y=6, value_size=10))
panels.append(stat(
    "", "SPA secondary line: apalis job queues with a failed or killed "
    "job right now (instantaneous, not windowed) — the SPA folds killed "
    "into the same 'queue(s) failed' phrase.",
    'count(sum by (job_type) (liq_job_queue{state=~"failed|killed"}) > 0) '
    'or vector(0)',
    display="bad queues", decimals=0, color_mode="none",
    steps=[{"color": "text", "value": None}, {"color": "red", "value": 1}],
    w=1, h=1, x=14, y=6, value_size=10))
panels.append(stat(
    "", "SPA secondary line: on-chain fills still uncovered by a hedge, "
    "summed across symbols.",
    'sum(liq_open_exposure_fill_count) or vector(0)',
    display="uncovered fills", decimals=0, color_mode="none",
    w=2, h=1, x=15, y=6, value_size=10))
panels.append(stat(
    "", "SPA secondary line: symbols with an open (unhedged) exposure — "
    "0 is the SPA's 'all fills hedged'.",
    'count(liq_open_exposure_fill_count > 0) or vector(0)',
    display="symbols exposed", decimals=0, color_mode="none",
    mappings=[{"type": "value", "options": {
        "0": {"text": "all hedged", "color": "green", "index": 0}}}],
    w=3, h=1, x=17, y=6, value_size=10))
panels.append(stat(
    "", "SPA secondary line: age of the current block-lag sample.",
    "time() - max(liq_block_lag_sampled_ts_seconds)",
    display="sampled ago", unit="s", decimals=0, color_mode="none",
    w=2, h=1, x=20, y=6, value_size=10))
panels.append(stat(
    "", "SPA secondary line: order-fill monitor poll ticks skipped "
    "(overrun) in 24h.",
    'max(liq_poll_skipped_ticks_24h) or vector(0)',
    display="skipped ticks", decimals=0, color_mode="none",
    w=2, h=1, x=22, y=6, value_size=10))

panels.append(bargauge(
    "Hedge cycle stages (p95, 24h)",
    "The SPA waterfall's aggregate form: p95 per stage. Per-cycle "
    "waterfalls live in the SPA.",
    'max by (stage) (liq_hedge_latency_ms{quantile="p95"})', "{{stage}}",
    w=8, h=8, x=0, y=7,
    steps=[{"color": "green", "value": None}, {"color": "yellow", "value": 30000},
           {"color": "red", "value": 120000}]))
panels.append(timeseries(
    "Latency percentiles over time — $stage",
    "p50/p90/p99 of the selected hedge stage, from the exporter's rolling "
    "24h summary scraped once a minute.",
    [promql('max(liq_hedge_latency_ms{stage="$stage",quantile="p50"})', legend="p50", ref="A"),
     promql('max(liq_hedge_latency_ms{stage="$stage",quantile="p90"})', legend="p90", ref="B"),
     promql('max(liq_hedge_latency_ms{stage="$stage",quantile="p99"})', legend="p99", ref="C")],
    w=8, h=8, x=8, y=7, unit="ms",
    overrides=[
        {"matcher": {"id": "byName", "options": "p50"},
         "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "green"}}]},
        {"matcher": {"id": "byName", "options": "p90"},
         "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "yellow"}}]},
        {"matcher": {"id": "byName", "options": "p99"},
         "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "red"}}]},
    ]))
errors_by_module = matrix_table(
    "Errors & warnings by module (24h)",
    "The SPA's per-target error/warning counts. The Logs tab has the "
    "lines themselves. Click a module to jump to the Logs board "
    "pre-filtered on it (the SPA's 'logs →' button); sparkline cells are "
    "skipped as low-value/fiddly here.",
    'label_replace(liq_log_target_count_24h, "col", "$1", "level", "(.*)")',
    "target", w=8, h=8, x=16, y=7,
    first_col="Module",
    column_order=["ERROR", "WARN"],
    unit_overrides={
        "ERROR": [{"id": "custom.cellOptions", "value": {"type": "color-text"}},
                   {"id": "thresholds", "value": {"mode": "absolute", "steps": [
                       {"color": "text", "value": None},
                       {"color": "red", "value": 1}]}}],
        "WARN": [{"id": "custom.cellOptions", "value": {"type": "color-text"}},
                  {"id": "thresholds", "value": {"mode": "absolute", "steps": [
                      {"color": "text", "value": None},
                      {"color": "yellow", "value": 1}]}}],
    },
    sort_by={"displayName": "ERROR", "desc": True},
    decimals=0)
# F4: data link on the Module column -> Logs board, pre-filtered via the
# `target` textbox var there (labels.target=~"${target}"), keeping the
# current time range. Added by mutating the returned panel, not by
# changing the shared matrix_table() helper.
errors_by_module["fieldConfig"]["overrides"].append({
    "matcher": {"id": "byName", "options": "Module"},
    "properties": [{"id": "links", "value": [{
        "title": "View in Logs tab",
        "url": "/d/t0-liquidity-logs/?${__url_time_range}"
               "&var-target=${__value.text}&theme=light",
        "targetBlank": False,
    }]}],
})
panels.append(errors_by_module)

panels.append(bargauge(
    "USDC rebalance stages (p95, 30d)",
    "Stage timings for USDC rebalances. From the server's stageSummary; "
    "per-operation waterfalls live in the SPA.",
    'max by (stage) (liq_rebalance_stage_ms{kind="usdc",quantile="p95"})', "{{stage}}",
    w=8, h=7, x=0, y=15))
panels.append(bargauge(
    "Equity rebalance stages (p95, 30d)",
    "Stage timings for equity mints/redemptions.",
    'max by (stage) (liq_rebalance_stage_ms{kind="equity",quantile="p95"})', "{{stage}}",
    w=8, h=7, x=8, y=15))
panels.append(matrix_table(
    "Dependency health (24h)",
    "Every RPC / broker operation: calls, errors, latency percentiles.",
    'label_join(label_replace(liq_dependency_calls_24h, "col", "calls", "", ""), '
    '"dep", " · ", "dependency", "operation") or '
    'label_join(label_replace(liq_dependency_errors_24h, "col", "errors", "", ""), '
    '"dep", " · ", "dependency", "operation") or '
    'label_join(label_replace(liq_dependency_latency_ms{quantile="p50"}, "col", "p50 ms", "", ""), '
    '"dep", " · ", "dependency", "operation") or '
    'label_join(label_replace(liq_dependency_latency_ms{quantile="p95"}, "col", "p95 ms", "", ""), '
    '"dep", " · ", "dependency", "operation") or '
    'label_join(label_replace(liq_dependency_latency_ms{quantile="max"}, "col", "max ms", "", ""), '
    '"dep", " · ", "dependency", "operation")',
    "dep", w=8, h=7, x=16, y=15,
    first_col="Dependency",
    column_order=["calls", "errors", "p50 ms", "p95 ms", "max ms"],
    unit_overrides={
        "errors": [{"id": "custom.cellOptions", "value": {"type": "color-text"}},
                    {"id": "thresholds", "value": {"mode": "absolute", "steps": [
                        {"color": "text", "value": None},
                        {"color": "red", "value": 1}]}}],
    },
    sort_by={"displayName": "calls", "desc": True},
    decimals=0))

# F5: the SPA's CCTP attestation-time chart under the USDC rebalance
# section. liq_attestation_last_ms is a gauge of the single most-recent
# attestation duration per kind (the exporter only gets the last entry of
# the bot's attestationTrend each poll) — there is no bucketed history to
# backfill, so the trend only accrues going forward from whenever this
# metric started being scraped. Documented in the panel description.
panels.append(timeseries(
    "CCTP attestation time",
    "Duration of the most recent CCTP attestation per rebalance kind, "
    "sampled once per scrape. The SPA can chart the full attestationTrend "
    "history for the selected range; this gauge only has 'last value at "
    "scrape time', so the line only accrues from whenever this panel "
    "started being scraped forward — a short/flat history here does not "
    "mean attestations were fast, it means the metric is young.",
    [promql('max(liq_attestation_last_ms{kind="usdc"})', legend="usdc", ref="A"),
     promql('max(liq_attestation_last_ms{kind="equity"})', legend="equity", ref="B")],
    w=24, h=6, x=0, y=22, unit="ms",
    overrides=[
        {"matcher": {"id": "byName", "options": "usdc"},
         "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "blue"}}]},
        {"matcher": {"id": "byName", "options": "equity"},
         "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "purple"}}]},
    ]))

panels.append(timeseries(
    "Block lag over time",
    "The ingestion-health chart: how far fill detection trails the chain.",
    [promql("max(liq_block_lag_blocks)", legend="blocks behind")],
    w=8, h=6, x=0, y=28, decimals=0, fill=15))
panels.append(stat(
    "Poll cycles (24h)", "Order-fill monitor poll cycles.",
    "max(liq_poll_cycles_24h)", decimals=0, color_mode="none",
    w=3, h=6, x=8, y=28, text_mode="value"))
panels.append(stat(
    "Poll errors (24h)", "Failed poll cycles.",
    "max(liq_poll_errors_24h) or vector(0)", decimals=0,
    steps=[{"color": "green", "value": None}, {"color": "red", "value": 1}],
    w=3, h=6, x=11, y=28, text_mode="value"))
panels.append(stat(
    "Skipped ticks (24h)", "Poll ticks skipped (overrun).",
    "max(liq_poll_skipped_ticks_24h) or vector(0)", decimals=0,
    steps=[{"color": "green", "value": None}, {"color": "yellow", "value": 1}],
    w=3, h=6, x=14, y=28, text_mode="value"))
panels.append(bargauge(
    "Poll duration percentiles",
    "The monitor's own cycle duration.",
    "max by (quantile) (liq_poll_duration_ms)", "{{quantile}}",
    w=7, h=6, x=17, y=28))
panels.append(matrix_table(
    "Job queues",
    "The bot's apalis job queues (instantaneous whole-table counts, not "
    "windowed). killed or failed above zero is the SPA's critical Errors "
    "state.",
    'label_replace(liq_job_queue, "col", "$1", "state", "(.*)")',
    "job_type", w=12, h=8, x=0, y=34,
    first_col="Job",
    column_order=["pending", "running", "done", "failed", "awaiting_retry",
                  "killed", "retried"],
    unit_overrides={
        "failed": [{"id": "custom.cellOptions", "value": {"type": "color-text"}},
                    {"id": "thresholds", "value": {"mode": "absolute", "steps": [
                        {"color": "text", "value": None},
                        {"color": "red", "value": 1}]}}],
        "killed": [{"id": "custom.cellOptions", "value": {"type": "color-text"}},
                    {"id": "thresholds", "value": {"mode": "absolute", "steps": [
                        {"color": "text", "value": None},
                        {"color": "red", "value": 1}]}}],
    },
    decimals=0,
))

native_y = 42
with open(NATIVE_ROWS) as f:
    native = json.load(f)
for entry in native:
    row_panel = row(entry["row"]["title"] + " (native bot metrics)",
                    native_y, collapsed=True)
    inner = []
    for panel in entry["panels"]:
        panel = dict(panel)
        panel["id"] = nid()
        inner.append(panel)
    row_panel["panels"] = inner
    panels.append(row_panel)
    native_y += 1

STAGE_VAR = {
    "name": "stage", "type": "custom", "label": "Hedge stage",
    "query": "detection, decision, submission, execution, exposure_window",
    "includeAll": False, "multi": False, "hide": 0,
    "current": {"selected": True, "text": "execution", "value": "execution"},
    "options": [
        {"selected": False, "text": "detection", "value": "detection"},
        {"selected": False, "text": "decision", "value": "decision"},
        {"selected": False, "text": "submission", "value": "submission"},
        {"selected": True, "text": "execution", "value": "execution"},
        {"selected": False, "text": "exposure_window",
         "value": "exposure_window"},
    ],
}
dashboards.append(make_dashboard(
    "t0-liquidity-performance", "Liquidity bot: Performance",
    "The SPA's Performance tab, plus the pre-exporter native bot metrics "
    "as collapsed rows at the bottom.",
    panels, tab_links("Performance"), [ENV_VAR, SOURCE_VAR, STAGE_VAR]))

# ==========================================================================
# Tab 5: Logs.
# ==========================================================================
panels = []
panels += pills(0)
panels.append(cloudlog_table(
    "Log History",
    "The bot's structured log lines, re-emitted by the exporter with real "
    "severity and target (the raw container stream is all-INFO). Filter "
    "with the level/category/target/search variables above. ⚠️ Only the "
    "newest matching entries, about the panel's pixel width of them. "
    "Narrow the filters, or use the SPA for deep digs. DEBUG/TRACE are "
    "not shipped.",
    'logName="projects/$env/logs/liquidity-botlogs" '
    'labels.level=~"^(${level:pipe})$" labels.target=~"${target}" '
    'labels.target=~"^(${category:pipe})$" '
    'jsonPayload.message=~"${search}"',
    # The message is the log-lines frame's `body`; level/target are
    # leaves in `labels` like every other payload field.
    columns=[("jsonPayload.level", "Level"), ("jsonPayload.target", "Target"),
             ("body", "Message")],
    w=24, h=19, x=0, y=2,
    widths={"Time": 170, "Level": 70, "Target": 220},
    mappings_by_field={"Level": [
        {"type": "value", "options": {
            "ERROR": {"text": "ERROR", "color": "red", "index": 0},
            "WARN": {"text": "WARN", "color": "yellow", "index": 1},
            "INFO": {"text": "INFO", "color": "blue", "index": 2}}},
    ]},
))
# L2: ms-precision timestamps, matching the SPA's "Aug 18, 13:27:21.965
# UTC". cloudlog_table's time_columns path (shared helper, other tabs own
# it) only offers second precision and there's no ms-precision knob, so
# time_columns is left off this call (it would add nothing but a
# second-precision override to immediately shadow) and the ms override is
# appended directly on this Logs-only panel instead of touching the
# shared helper.
panels[-1]["fieldConfig"]["overrides"].append({
    "matcher": {"id": "byName", "options": "Time"},
    "properties": [{"id": "unit", "value": "time:MMM D, HH:mm:ss.SSS"}],
})
# L2, Fields column: investigated and deliberately skipped — see report/
# commit message. The exporter ships jsonPayload.fields as a JSON object
# with per-entry-varying keys (error/et_day, total_enqueued,
# from_block/cutoff_block, ...); the googlecloud-logging-datasource plugin
# flattens each leaf into its OWN dynamic `labels` key (jsonPayload.fields.error,
# jsonPayload.fields.et_day, ...) rather than one blob field, so there is
# no single field to bind a "Fields" column to. Hard-coding today's known
# keys as fixed columns would silently miss future ones and would be
# mostly-empty per row (matches the audit's own "may be noisy" warning) —
# worse than the SPA's dynamic key=value suffix, not a match for it.
LEVEL_VAR = {
    "name": "level", "type": "custom", "label": "Log level",
    "query": "ERROR, WARN, INFO", "includeAll": True, "multi": True,
    "hide": 0,
    "current": {"selected": True, "text": ["All"], "value": ["$__all"]},
    "options": [
        {"selected": True, "text": "All", "value": "$__all"},
        {"selected": False, "text": "ERROR", "value": "ERROR"},
        {"selected": False, "text": "WARN", "value": "WARN"},
        {"selected": False, "text": "INFO", "value": "INFO"},
    ],
    "allValue": "ERROR|WARN|INFO",
}
# L1(a): the SPA's Category multi-select. The bot's own backend matches
# these as an EXACT target-field value (`"target":"<cat>"` literal
# substring on the raw log line — see st0x.liquidity src/api.rs), not a
# crate/module prefix, so this is anchored (^...$) rather than a bare
# substring — an unanchored pipe would also swallow crate-path targets
# like "st0x_hedge::portfolio_snapshot::write" (which contains "hedge")
# and diverge from the SPA's own filter semantics.
CATEGORY_VAR = {
    "name": "category", "type": "custom", "label": "Category",
    "description": "The SPA's log Category filter — exact-match domain "
                   "tags the bot's tracing spans set (bridge/broker/cqrs/"
                   ".../wallet), distinct from crate/module paths.",
    "query": "bridge, broker, cqrs, dashboard, hedge, inventory, "
             "orderbook, rebalance, startup, tokenization, wallet",
    "includeAll": True, "multi": True, "hide": 0,
    "current": {"selected": True, "text": ["All"], "value": ["$__all"]},
    "options": [
        {"selected": True, "text": "All", "value": "$__all"},
        {"selected": False, "text": "bridge", "value": "bridge"},
        {"selected": False, "text": "broker", "value": "broker"},
        {"selected": False, "text": "cqrs", "value": "cqrs"},
        {"selected": False, "text": "dashboard", "value": "dashboard"},
        {"selected": False, "text": "hedge", "value": "hedge"},
        {"selected": False, "text": "inventory", "value": "inventory"},
        {"selected": False, "text": "orderbook", "value": "orderbook"},
        {"selected": False, "text": "rebalance", "value": "rebalance"},
        {"selected": False, "text": "startup", "value": "startup"},
        {"selected": False, "text": "tokenization", "value": "tokenization"},
        {"selected": False, "text": "wallet", "value": "wallet"},
    ],
    # "All" must stay a true passthrough (".*"), not a pipe of the 11
    # known values — target strings the taxonomy doesn't cover (e.g. the
    # crate-path "st0x_hedge::portfolio_snapshot::write") would otherwise
    # vanish from the default view even though the SPA's own default
    # (categories ∪ crates) still surfaces them. Verified: pipe-of-11 as
    # allValue collapsed level=ERROR from 100 rows to 0 in live staging
    # data, exactly the "over-constrained -> silent zero rows" trap this
    # item warns about.
    "allValue": ".*",
}
TARGET_VAR = {
    "name": "target", "type": "textbox", "label": "Log target regex",
    "query": ".*", "hide": 0,
    "current": {"selected": True, "text": ".*", "value": ".*"},
    "options": [{"selected": True, "text": ".*", "value": ".*"}],
}
# L1(b): the SPA's free-text search box, matched against jsonPayload.message.
SEARCH_VAR = {
    "name": "search", "type": "textbox", "label": "Message search",
    "description": "The SPA's free-text search box — regex against "
                   "jsonPayload.message.",
    "query": ".*", "hide": 0,
    "current": {"selected": True, "text": ".*", "value": ".*"},
    "options": [{"selected": True, "text": ".*", "value": ".*"}],
}
dashboards.append(make_dashboard(
    "t0-liquidity-logs", "Liquidity bot: Logs",
    "The SPA's Logs tab: level/category/target/search-filterable "
    "structured log lines.",
    panels, tab_links("Logs"),
    [ENV_VAR, SOURCE_VAR, LEVEL_VAR, CATEGORY_VAR, TARGET_VAR, SEARCH_VAR]))

# ==========================================================================
# Emit.
# ==========================================================================
def board_exprs(node):
    """Every PromQL `expr` string in a board, wherever it is nested."""
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "expr" and isinstance(value, str):
                yield value
            else:
                yield from board_exprs(value)
    elif isinstance(node, list):
        for value in node:
            yield from board_exprs(value)


def check_liq_pinned(boards):
    failures = [(board["uid"], expr, unpinned)
                for board in boards
                for expr in board_exprs(board)
                for unpinned in [unpinned_liq_selectors(expr)]
                if unpinned]
    for uid, expr, unpinned in failures:
        print(f"{uid}: unpinned liq_ selector {unpinned} in: {expr}",
              file=sys.stderr)
    if failures:
        raise SystemExit(
            f"{len(failures)} PromQL expressions read liq_* without "
            f"{LIQ_JOB_MATCHER}")


def board_path(root, board):
    # A board's directory IS its Grafana folder (the provider builds folders
    # from the directory tree). The main board sits in dashboards/liquidity/,
    # so that folder lists one Liquidity bot entry; the four tab boards are
    # sub-pages of it and go to dashboards/liquidity/tabs/, so they do not
    # crowd that list. Moving a file between these directories moves the
    # board between folders.
    subdir = ("liquidity" if board["uid"] == "t0-liquidity"
              else os.path.join("liquidity", "tabs"))
    return os.path.join(root, "dashboards", subdir, board["uid"] + ".json")


def write_boards(root, boards, quiet=False):
    for board in boards:
        path = board_path(root, board)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(board, f, indent=2, sort_keys=False)
            f.write("\n")
        if not quiet:
            print(f"wrote {path}: {len(board['panels'])} panels")


def check_committed(boards):
    with tempfile.TemporaryDirectory() as scratch:
        write_boards(scratch, boards, quiet=True)
        drifted = [board_path(HERE, board) for board in boards
                   if not os.path.exists(board_path(HERE, board))
                   or not filecmp.cmp(board_path(scratch, board),
                                      board_path(HERE, board), shallow=False)]
    for path in drifted:
        print(f"{path} differs from the generator output", file=sys.stderr)
    if drifted:
        raise SystemExit(
            "committed boards are stale: run python3 "
            "observability/gen-t0-liquidity.py and commit the result")
    print(f"{len(boards)} boards match the generator")


check_liq_pinned(dashboards)
if sys.argv[1:] == ["--check"]:
    check_committed(dashboards)
elif sys.argv[1:]:
    raise SystemExit(f"usage: {sys.argv[0]} [--check]")
else:
    write_boards(HERE, dashboards)
