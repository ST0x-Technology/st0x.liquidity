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
  cloudlog  the exporter's logNames liquidity-trades / liquidity-transfers /
            liquidity-botlogs (Cloud Logging entries.write), or the bot's
            own log, liquidity-bot (its JSON stdout)

Layout mirrors the SPA's tabs as rows, in tab order: header strip,
Dashboard (inventory tables | trades over rebalances, a 13/11 split like
the SPA's 11fr/9fr; checked with no sideways scroll on a 1934px-wide
viewport), Orders, PnL, Performance, Logs — then the pre-exporter
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

# The log panels follow `source` too. A Cloud Logging query cannot pick its
# log by a variable, so each panel queries both logs, as refIds
# exporter-<name> and bot-<name>, and its first transformation keeps the
# frame of the source picked (pick_source). check_log_sources fails on a
# log panel that does not. The bot's log is its stdout, with every field of a
# JSON line at jsonPayload.<field>; it holds lines only once the bot logs
# JSON.
SOURCES = ("exporter", "bot")
BOT_LOG = 'logName="projects/$env/logs/liquidity-bot"'
EXPORTER_LOGS = ("liquidity-trades", "liquidity-transfers", "liquidity-botlogs")
# The bot writes no order lines, so Orders reads the exporter's log on
# either source.
EXPORTER_ONLY_LOG = "liquidity-orders"

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


def source_targets(name, exporter_query, bot_query):
    """One log panel's two queries: the exporter's and the bot's."""
    return [cloudlog(exporter_query, ref=f"exporter-{name}"),
            cloudlog(bot_query, ref=f"bot-{name}")]


def pick_source(name):
    """Keeps only the frame of the source picked. Grafana interpolates
    variables in transformation options, and refIds match exactly."""
    return {"id": "filterByRefId",
            "options": {"include": "${source:text}-" + name}}


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
        # name None: the expression already carries its own col label (a
        # column per chain, see native_inventory).
        expr if name is None else
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
                   hide_time=False, dedup_by=None, source=None):
    """columns: list of (raw_field, display_name).

    source: (name, bot_query) for a panel that follows `source`: `query`
      is then the exporter's, and the panel keeps the picked source's frame
      (pick_source). Without it the panel reads only `query`.

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
        "targets": (source_targets(source[0], query, source[1]) if source
                    else [cloudlog(query)]),
        **({"timeFrom": time_from} if time_from else {}),
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "transformations": [
            *([pick_source(source[0])] if source else []),
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


# The SPA's timestamp style: "Jul 31, 13:31:50 UTC" (D7).
TIME_FMT = [{"id": "unit", "value": "time:MMM D, HH:mm:ss [UTC]"}]

# ==========================================================================
# The tab suite: five dashboards, one per SPA tab, cross-linked with a
# button row that mimics the SPA's tab bar. Every link carries
# theme=light so the suite renders in the SPA's light look regardless of
# the viewer's Grafana theme preference.
# ==========================================================================
# Each tab carries its own symbol in its label. Grafana's link buttons take
# only seven built-in icons (external link, dashboard, question, info, bolt,
# doc, cloud), too few to tell five tabs apart.
TABS = [
    ("Dashboard", "t0-liquidity", "📊"),
    ("Orders", "t0-liquidity-orders", "📋"),
    ("PnL", "t0-liquidity-pnl", "💵"),
    ("Performance", "t0-liquidity-performance", "⚡"),
    ("Logs", "t0-liquidity-logs", "📜"),
]


def tab_links(active):
    links = []
    for name, uid, symbol in TABS:
        links.append({
            "type": "link",
            # A no-break space: Grafana trims a plain one after the symbol.
            "title": (f"▸ {symbol}\u00a0{name}" if name == active
                      else f"{symbol}\u00a0{name}"),
            # autofitpanels stretches the board to the window height, like
            # the SPA's full-height layout. Only the Dashboard tab: the
            # other tabs hold too many panels to squeeze into one screen.
            "url": f"/d/{uid}/?theme=light"
                   + ("&autofitpanels" if uid == "t0-liquidity" else ""),
            "targetBlank": False,
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


def panel_module(name):
    """A liquidity-panels script to prepend to an afterRender. Its export
    line is for the SPA test; afterRender is not a module."""
    with open(os.path.join(HERE, "liquidity-panels", name)) as f:
        return "".join(line for line in f if not line.startswith("export "))


def pills(y, w=24):
    """The SPA's HeaderBar and SettingsBar as one Business Text row,
    repeated on every tab like the SPA repeats its header: the settings
    pills (broker, Equity and USDC targets with their bands, Trigger,
    Reserve) and a Config button on the left; the CLI recovery guide
    button, a UTC clock, the commit, uptime, and the connection badge on
    the right. Config and the recovery guide open the SPA's dialogs.

    Every series carries a `k` label naming it; liquidity-panels/header.js
    reads the rows by `k`. The commit series is its sample timestamp, so
    the script can keep the newest commit when a deploy leaves the previous
    one in the lookback window. The recovery guide is static data exported
    from the SPA (liquidity-panels/recovery-guide.json) and prepended to
    the script with client-env.js, which retargets its commands at the
    board's environment.

    w: the Dashboard tab narrows it to 23 to fit the detail panel beside
    it (see detail_panel()).
    """
    def named(expr, name):
        return f'label_replace({expr}, "k", "{name}", "", "")'

    expr = " or ".join([
        named(LIQ_UP, "up"),
        named("time() - max(liq_bot_start_timestamp_seconds)", "uptime"),
        named("max by (git_commit) (timestamp(liq_bot_info))", "commit"),
        named("max by (broker, log_level, wallet_kind, wallet_address, orderbook, "
              "turnkey_organization, server_port) (liq_settings_info)", "info"),
        named("max(liq_settings_equity_target)", "equity_target"),
        named("max(liq_settings_equity_deviation)", "equity_deviation"),
        named("max(liq_settings_usdc_target)", "usdc_target"),
        named("max(liq_settings_usdc_deviation)", "usdc_deviation"),
        named("max(liq_settings_execution_threshold_usd)", "trigger"),
        named("max(liq_settings_cash_reserved)", "cash_reserved"),
        named("max(liq_settings_order_polling_seconds)", "order_polling"),
        named("max(liq_settings_inventory_poll_seconds)", "inventory_polling"),
        named("max(liq_settings_deployment_block)", "deployment_block"),
    ])
    with open(os.path.join(HERE, "liquidity-panels", "recovery-guide.json")) as f:
        guide = json.load(f)
    with open(os.path.join(HERE, "liquidity-panels", "header.js")) as f:
        after_render = (f"const RECOVERY_GUIDE = {json.dumps(guide)};\n\n"
                        + panel_module("client-env.js") + "\n" + f.read())
    with open(os.path.join(HERE, "liquidity-panels", "header.css")) as f:
        styles = f.read()
    return [{
        "id": nid(), "type": "marcusolsson-dynamictext-panel", "title": "",
        "description": "The SPA header: settings pills, Config, the CLI "
                       "recovery guide, the deployed commit, uptime, and "
                       "whether the bot is up: the exporter's liq_up, or "
                       "the bot scrape's up on the bot source.",
        "datasource": CM,
        "targets": [promql(expr, instant=True)],
        "transparent": True,
        "gridPos": {"h": 1, "w": w, "x": 0, "y": y},
        "transformations": [
            {"id": "labelsToFields", "options": {}},
            {"id": "merge", "options": {}},
        ],
        "options": {
            "renderMode": "allRows",
            "editor": {"format": "auto", "language": "html"},
            "content": "<div></div>",
            "defaultContent": "No bot data.",
            "helpers": "",
            "afterRender": after_render,
            "styles": styles,
            "wrap": False,
        },
        **LATEST_ONLY_HIDDEN,
    }]


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
# Tab 1: Dashboard: the SPA's Inventory card as Grafana tables (USD at
# Alpaca, USD on each chain, equities) on the left, Trades / Rebalances
# stacked right.
# ==========================================================================
panels = []
panels += pills(0, w=23)

def native_table(title, desc, expr, row_label, columns, w, h, x, y,
                 first_col, sort_by=None, widths=None, bars=(),
                 align="right", stretch=None, first_col_mappings=None):
    """A Grafana table from one or-chain query pivoted on (row_label, col),
    like matrix_table. columns: [(col, display name, [field override
    properties])]; bars: columns drawn as HTML bars (see bar_cell)."""
    matrix_key = f"{row_label}\\col"
    names = {col: name for col, name, _ in columns}
    # Fixed widths, except one `stretch` column that takes the rest of the
    # card, so the table ends at the card's edge.
    widths = {col: px for col, px in (widths or {}).items() if col != stretch}
    # A stretching column otherwise never goes under Grafana's 150px
    # default, which overflows a card whose fixed columns leave less.
    stretch_props = ([{"matcher": {"id": "byName", "options": names[stretch]},
                       "properties": [{"id": "custom.minWidth", "value": 60}]}]
                     if stretch else [])
    overrides = [{"matcher": {"id": "byName", "options": names[col]},
                  "properties": [*(props or []),
                                 *([{"id": "custom.width", "value": widths[col]}]
                                   if col in widths else [])]}
                 for col, _, props in columns if props or col in widths]
    return {
        "id": nid(), "type": "table", "pluginVersion": "13.1.0",
        "title": title, "description": desc,
        "datasource": CM,
        "targets": [promql(expr, instant=True)],
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "transformations": [
            {"id": "labelsToFields", "options": {}},
            {"id": "merge", "options": {}},
            {"id": "groupBy", "options": {"fields": {
                row_label: {"operation": "groupby", "aggregations": []},
                "col": {"operation": "groupby", "aggregations": []},
                "Value": {"operation": "aggregate",
                          "aggregations": ["lastNotNull"]},
            }}},
            {"id": "groupingToMatrix",
             "options": {"columnField": "col", "rowField": row_label,
                         "valueField": "Value (lastNotNull)",
                         "emptyValue": "null"}},
            {"id": "organize", "options": {
                "excludeByName": {},
                "indexByName": {name: index for index, name in enumerate(
                    [matrix_key] + [col for col, _, _ in columns])},
                "renameByName": {matrix_key: first_col, **names}}},
            # Regex value mappings only read strings (see bar_cell).
            *([{"id": "convertFieldType", "options": {"conversions": [
                {"targetField": col, "destinationType": "string"}
                for col in bars]}}] if bars else []),
        ],
        "fieldConfig": {
            "defaults": {"unit": "locale", "decimals": 2,
                         "custom": {"align": align, "filterable": True,
                                    "cellOptions": {"type": "auto"}},
                         "thresholds": {"mode": "absolute", "steps": [
                             {"color": "red", "value": None}]},
                         "mappings": []},
            "overrides": [
                {"matcher": {"id": "byName", "options": first_col},
                 "properties": [{"id": "custom.align", "value": "left"},
                                {"id": "custom.width",
                                 "value": widths.get(row_label, 90)},
                                *([{"id": "mappings",
                                    "value": first_col_mappings}]
                                  if first_col_mappings else [])]},
                *overrides,
                *stretch_props,
            ],
        },
        "options": {"showHeader": True, "cellHeight": "md",
                    **({"sortBy": [sort_by]} if sort_by else {})},
        **LATEST_ONLY,
    }



# Added to a banded percent before its sign, so an out-of-band 0% is -0.0001
# and not -0, which Grafana writes as "0" (in band). The bar cell's regex
# keeps one decimal, so the offset never shows.
BAND_SIGN_OFFSET = 0.0001


def banded_pct(expr, inside):
    """pct_bar's sign encoding: the percent rounded to 0.1, negative when
    `inside` (a 0/1 PromQL bool) says it is outside the band."""
    return (f"(round(100 * ({expr}), 0.1) + {BAND_SIGN_OFFSET}) "
            f"* (2 * {inside} - 1)")


# Added to a percent the board cannot judge against a band, so the bar cell
# draws it grey. A percent is at most 100, so a judged value never has four
# integer digits and an unjudged one always does.
UNJUDGED_OFFSET = 1000


def unjudged_pct(expr):
    """A percent with no band verdict: grey in a banded bar cell."""
    return (f"round(100 * ({expr}), 0.1) + {UNJUDGED_OFFSET} "
            f"+ {BAND_SIGN_OFFSET}")


def pct_bar(expr, band=None):
    """A percent for an HTML bar cell (see BAR_CELL): the value as a percent
    rounded to 0.1, negative when it is outside the rebalance band. A table
    cell can only style its own value, so the band verdict travels in the
    sign. band: (target metric, deviation metric), or None for a neutral
    share."""
    if not band:
        return f"round(100 * ({expr}), 0.1)"
    target, deviation = band
    inside = (f"(abs(({expr}) - scalar(max({target}))) "
              f"<= bool scalar(max({deviation})))")
    return banded_pct(expr, inside)


def bar_html(fill, text):
    # One div: Grafana's sanitizer drops position, top, left and
    # line-height, so the fill is a hard-stop gradient behind the text.
    return ('<div style="height:20px;border-radius:4px;font-weight:600;'
            f'text-align:center;background:linear-gradient(90deg, {fill} '
            f'{text}%, rgba(128,128,128,0.15) {text}%)">{text}%</div>')


# A percent drawn as the custom card's bar: the fill as wide as the percent
# and the percent written inside it. Grafana's gauge cell puts the number
# beside its bar, so this is a Markdown + HTML cell: the value turns into a
# string and a regex mapping writes the HTML, its capture group the percent.
# Negative means outside the band (see pct_bar): red, else green. With
# `unjudged`, a value with four integer digits is a percent plus
# UNJUDGED_OFFSET (see unjudged_pct): grey, its percent read after the offset.
GREY_FILL = "rgba(148,163,184,0.45)"
UNJUDGED_PATTERN = r"^(?=\d{4})10*(\d+(?:\.[1-9])?).*$"


def bar_cell(neutral=False, unjudged=False):
    # No value (a chain without a vault) shows nothing.
    mappings = [{"type": "regex", "options": {"pattern": "^(null|NaN)?$",
                 "result": {"text": " ", "index": 0}}}]
    mappings += ([] if not unjudged else [
        {"type": "regex", "options": {"pattern": UNJUDGED_PATTERN, "result": {
            "text": bar_html(GREY_FILL, "$1"), "index": len(mappings)}}}])
    # The percent keeps at most one decimal, which drops pct_bar's offset
    # and any float noise from the rounding.
    mappings += ([] if neutral else [
        {"type": "regex", "options": {"pattern": r"^-(\d+(?:\.[1-9])?).*$", "result": {
            "text": bar_html("rgba(239,68,68,0.6)", "$1"), "index": len(mappings)}}}])
    mappings.append({"type": "regex", "options": {
        "pattern": r"^(\d+(?:\.[1-9])?).*$" if not neutral else "^(.*)$", "result": {
        "text": bar_html(GREY_FILL if neutral else "rgba(34,197,94,0.6)", "$1"),
        "index": len(mappings)}}})
    # The cell is an encoded string, so sorting or filtering it would order
    # the encodings, not the percents.
    return [{"id": "mappings", "value": mappings},
            {"id": "custom.cellOptions", "value": {"type": "markdown"}},
            {"id": "custom.align", "value": "center"},
            {"id": "custom.filterable", "value": False}]

# CT, Rebal and Ext as one column of three dots. Grafana renders no column
# under 50px, so three one-dot columns took 150px. The query packs the flags
# into one number, CT x 100 + Rebal x 10 + Ext, with each digit 1 off, 2 on
# or 3 not set (never 0, so the number never loses a leading digit), and one
# regex mapping per combination draws the dots as HTML.
def flag_digit(metric):
    return (f"(max by (symbol) ({metric}) + 1 "
            "or (max by (symbol) (liq_equity_total) * 0 + 3))")


FLAGS_EXPR = (f"{flag_digit('liq_asset_counter_trading')} * 100 + "
              f"{flag_digit('liq_asset_rebalancing')} * 10 + "
              f"{flag_digit('liq_asset_extended_hours')}")
FLAG_DOT = {"2": '<span style="color:#22c55e">●</span>',
            "1": '<span style="color:#ef4444">●</span>',
            "3": '<span style="color:#6b7280">○</span>'}
FLAGS_CELL = [
    {"id": "mappings", "value": [
        {"type": "regex", "options": {"pattern": f"^{a}{b}{c}$", "result": {
            "text": "&nbsp;".join(FLAG_DOT[d] for d in (a, b, c)),
            "index": index}}}
        for index, (a, b, c) in enumerate(
            (a, b, c) for a in "123" for b in "123" for c in "123")]},
    {"id": "custom.cellOptions", "value": {"type": "markdown"}},
    {"id": "custom.align", "value": "center"},
    {"id": "custom.filterable", "value": False},
]

# Unpriced symbols arrive as NaN (see native_inventory) and read as a dash.
NO_PRICE = {"id": "mappings", "value": [{"type": "special", "options": {
    "match": "null+nan", "result": {"text": "—", "color": "#6b7280",
                                    "index": 0}}}]}

# Display names for the chains the bot knows (config keys). A chain not
# listed here still gets its row and column, under its raw key.
CHAIN_NAMES = {"base": "Base", "robinhood": "Robinhood", "hyperevm": "HyperEVM",
               "ethereum": "Ethereum"}


# The Equities Ratio is Base vault / (Base vault + Alpaca), both available,
# against the default Base target. The planner instead sizes each chain's
# share of the total over the chains whose listing rebalances, in underlying
# shares, against that listing's own target. The board can see the other
# chains only on the bot source, so it judges a symbol only there: rebalanced
# on Base, with a balance, the default target set, its Base vault read, and no
# vault row on any other chain, empty or not (a multi-chain listing such as
# DNUT sets its own target_share). Every other symbol is grey, no verdict.
# The verdict can still differ from the bot's for a Base-only listing with
# its own target_share, while a wrapper ratio is not 1, or while a transfer is
# in flight.
EQUITY_JUDGED = (
    '(liq_equity_total > 0) '
    'and on (symbol) (max by (symbol) (liq_asset_rebalancing) == 1) '
    'and on () count(liq_settings_equity_target) '
    'and on (symbol) max by (symbol) '
    '(liq_equity_chain_available{chain="base"}) '
    'unless on (symbol) max by (symbol) '
    '(liq_equity_chain_available{chain!="base"})')
EQUITY_RATIO = (
    f'({pct_bar("liq_equity_ratio", ("liq_settings_equity_target", "liq_settings_equity_deviation"))}'
    f' and on (symbol) ({EQUITY_JUDGED})) '
    f'or ({unjudged_pct("liq_equity_ratio")} unless on (symbol) ({EQUITY_JUDGED}))')


def native_inventory(w, x, y, heights):
    """The Inventory card as three Grafana tables: USD at Alpaca, USD on each
    chain, and the equities."""
    alpaca_h, chains_h, equity_h = heights
    usd = lambda metric, row: (f'label_replace({metric}, "venue", "{row}", '
                               '"", "")')
    alpaca = or_chain([
        (usd("liq_usdc_alpaca_total", "Alpaca"), "cash"),
        (usd("liq_usdc_alpaca_usdc", "Alpaca"), "usdc"),
        # The reserve setting, else gross minus available cash, else Alpaca
        # total minus available (0 when the bot ships no gross figure), as
        # the custom card computed it.
        (usd("(liq_settings_cash_reserved or (liq_usdc_offchain_gross "
             "- liq_usdc_offchain_available) or (liq_usdc_alpaca_total "
             "- liq_usdc_offchain_available))", "Alpaca"), "reserve"),
        (usd("liq_usdc_rebalanceable", "Alpaca"), "rebalanceable"),
        (usd("liq_usdc_offchain_inflight", "Alpaca"), "inflight"),
        (usd(pct_bar("liq_usdc_alpaca_total / liq_usdc_total"), "Alpaca"),
         "share"),
        (usd("liq_usdc_total", "Alpaca"), "total"),
    ], row_label="venue")
    # One row per chain from the bot's per-chain series, else (the exporter
    # source, which has none) the single Base row from the unlabelled ones.
    # The fallback is per source, not per row: while the bot publishes
    # liq_usdc_chain_available, a chain without a labelled value shows none,
    # so a chain without a corridor has no target and no ratio, and a ratio
    # the bot leaves out is not filled from the unlabelled one. Each side is
    # reduced to just the venue label so `or` can tell they are the same row.
    def per_chain(labelled, base):
        return (f'max by (venue) (label_replace({labelled}, "venue", "$1", '
                f'"chain", "(.*)")) or (max by (venue) (label_replace({base}, '
                '"venue", "base", "", "")) unless on () '
                'count(liq_usdc_chain_available))')
    ratio = per_chain("liq_usdc_chain_ratio", "liq_usdc_ratio")
    target = per_chain("liq_usdc_corridor_target", "liq_settings_usdc_target")
    deviation = per_chain("liq_usdc_corridor_deviation",
                          "liq_settings_usdc_deviation")
    inside = (f"(abs(({ratio}) - ({target})) <= bool ({deviation}))")
    chains = or_chain([
        (per_chain("liq_usdc_chain_available", "liq_usdc_onchain_available"),
         "vault"),
        (per_chain("liq_usdc_chain_inflight", "liq_usdc_onchain_inflight"),
         "inflight"),
        ('max by (venue) (label_replace(liq_usdc_inflight_base_wallet, '
         '"venue", "base", "", "")) or max by (venue) (label_replace('
         'liq_usdc_inflight_ethereum_wallet, "venue", "ethereum", "", ""))',
         "wallet"),
        # pct_bar's sign encoding, with each chain's own band.
        (banded_pct(ratio, inside), "ratio"),
        (target, "target"),
        (deviation, "deviation"),
    ], row_label="venue")
    equity = or_chain([
        (FLAGS_EXPR, "flags"),
        # A column per chain (col onchain_<chain>) from the per-chain metric,
        # else (the exporter source) the onchain total as the Base column.
        ('max by (symbol, col) (label_replace(liq_equity_chain_available, '
         '"col", "onchain_$1", "chain", "(.*)")) or (max by (symbol, col) '
         '(label_replace(liq_equity_onchain_available, "col", "onchain_base", '
         '"", "")) unless on () count(liq_equity_chain_available))', None),
        ("liq_equity_inflight_total", "inflight"),
        ("liq_equity_offchain_available", "alpaca"),
        ("liq_equity_total", "total"),
        # Priced symbols only: the last price from the pricing feed. The NaN
        # fallback keeps the column on the board for unpriced symbols (a
        # dash, see NO_PRICE), where Grafana would drop a column that has no
        # values at all.
        ("(liq_equity_total * on (symbol) group_left "
         "max by (symbol) (liq_position_last_price_usd)) "
         "or (liq_equity_total * NaN)", "total_usd"),
        (EQUITY_RATIO, "ratio"),
        ("liq_equity_exposure_usd or (liq_equity_total * NaN)", "exposure"),
        ("liq_equity_unwrapped", "unwrapped"),
        ("liq_equity_wrapped", "wrapped"),
    ])
    pct = [{"id": "unit", "value": "percentunit"}, {"id": "decimals", "value": 0}]
    return [
        native_table(
            "USD · Alpaca",
            "Alpaca's cash: no rebalance target, so it shows its share of "
            "the USD total. The USD total is Alpaca, the primary chain's "
            "vault and the cash in flight; other chains' vaults are not in "
            "it. Rebalanceable = max(0, withdrawable - reserve).",
            alpaca, "venue",
            [("cash", "Cash", None), ("usdc", "USDC", None),
             ("reserve", "Reserve", None),
             ("rebalanceable", "Rebalanceable", None),
             ("inflight", "In flight", None),
             ("share", "Share of total", bar_cell(neutral=True)),
             ("total", "USD total", None)],
            w, alpaca_h, x, y, first_col="Venue", bars=("share",),
            stretch="share",
            widths={"venue": 80, "cash": 110, "usdc": 140, "reserve": 95,
                    "rebalanceable": 140, "inflight": 95, "share": 180,
                    "total": 120}),
        native_table(
            "USD · Onchain",
            "Each chain's vault and the ratio the bot rebalances on, vault "
            "/ (vault + Alpaca cash), against the chain's band. It is not a "
            "share of all USD: other chains are not in it. Ethereum is the "
            "hub wallet Alpaca deposits to and withdraws from, with no band.", chains, "venue",
            [("vault", "Vault", None), ("inflight", "In flight", None),
             ("wallet", "Wallet", None),
             ("ratio", "Ratio", bar_cell()),
             ("target", "Target", pct), ("deviation", "±", pct)],
            w, chains_h, x, y + alpaca_h, first_col="Chain",
            first_col_mappings=[{"type": "value", "options": {
                key: {"text": name if key != "ethereum" else "Ethereum (hub)",
                      "index": index}
                for index, (key, name) in enumerate(CHAIN_NAMES.items())}}],
            bars=("ratio",), stretch="ratio",
            widths={"venue": 125, "vault": 110, "inflight": 95, "wallet": 85,
                    "target": 85, "deviation": 60},
            ),
        native_table(
            "Equities",
            "Flags: counter trading, rebalancing and extended hours, green "
            "when on, red when off, hollow grey when not set. "
            "Share balances per venue, one column per chain. Total and "
            "Total USD count only the primary chain (Base) with Alpaca and "
            "the shares in flight: a wrapped share on another chain is "
            "worth that chain's underlying, so the chains are not added "
            "together. Ratio = Base vault available / (Base vault available "
            "+ Alpaca available), green inside the default Base band and "
            "red outside it. It is grey, with no verdict, where that is not "
            "the bot's own band: a symbol not rebalanced on Base, with no "
            "balance, with a vault on another chain, or with no default "
            "target, and "
            "every symbol on the exporter source, which cannot see the "
            "other chains. A click on Ratio sorts by its encoded text, not by "
            "percent. Exposure = net x live price, empty without a "
            "price.", equity, "symbol",
            [("flags", "Flags", FLAGS_CELL),
             *[(f"onchain_{key}", name, None)
               for key, name in CHAIN_NAMES.items() if key != "ethereum"],
             ("inflight", "Inflight", None),
             ("alpaca", "Alpaca", None), ("total", "Total", None),
             ("total_usd", "Total USD", [
                 {"id": "unit", "value": "currencyUSD"},
                 {"id": "decimals", "value": 0}, NO_PRICE]),
             ("ratio", "Ratio", bar_cell(unjudged=True)),
             ("exposure", "Exposure", [
                 {"id": "unit", "value": "currencyUSD"}, NO_PRICE,
                 {"id": "thresholds", "value": {"mode": "absolute", "steps": [
                     {"color": "red", "value": None},
                     {"color": "text", "value": -0.005},
                     {"color": "green", "value": 0.005}]}},
                 {"id": "custom.cellOptions", "value": {"type": "color-text"}}]),
             # Left-aligned like the SPA's table, except the wrap columns.
             ("unwrapped", "Unwrapped", [{"id": "custom.align", "value": "right"}]),
             ("wrapped", "Wrapped", [{"id": "custom.align", "value": "right"}])],
            w, equity_h, x, y + alpaca_h + chains_h, first_col="Asset",
            # Each width fits its header's text plus the filter icon
            # (measured). A column under Grafana's 50px minimum renders at the
            # minimum and would leave the stretching Ratio column too wide by
            # the difference. With Total USD and Exposure both
            # showing, the fixed columns no longer leave Ratio its 50px.
            widths={"symbol": 96, "flags": 64,
                    # One per chain; only chains with balances show up.
                    **{f"onchain_{key}": 92 for key in CHAIN_NAMES},
                    "inflight": 86, "alpaca": 86, "total": 74,
                    "total_usd": 100, "ratio": 158, "exposure": 95,
                    "unwrapped": 118, "wrapped": 102},
            bars=("ratio", "flags"), align="left", stretch="ratio",
            sort_by={"displayName": "Asset", "desc": False}),
    ]


# The inventory tables (8 + 9 + 35 rows) beside Trades stacked on
# Rebalances (25 + 26). Change these heights only after checking a new set on
# a preview board (observability/README.md says why and how).
TRADES_H, TRANSFERS_H = 25, 26
# USD · Alpaca gets 8 so its one row shows under the column headers: at 6 the
# fit left it a header and a scrollbar on windows under ~1600px tall.
panels += native_inventory(w=13, x=0, y=1, heights=(8, 9, 35))

# A row's detail dialog is the hidden `detail` variable: a table row's link
# sets it to the row's id, the detail panel picks that id out of the Trades
# and Rebalances tables' own results (it runs no query of its own) and
# opens the dialog, and closing the dialog clears the variable again.
# Do not add $detail to its A and B: they are the tables' results, not
# queries.
#
# The id is read by the field NAME. The groupBy key keeps the raw field name
# (jsonPayload.id); organize's rename only set its display name, and the Id
# column's override replaces that with a blank header. ${__url.params:...}
# already starts with "?", so the URL appends straight to it.
DETAIL_URL = ('/d/${__dashboard.uid}/${__url.params:exclude:var-detail}'
              '&var-detail=${__data.fields["jsonPayload.id"]}')


DETAIL_VAR = {"type": "textbox", "name": "detail", "hide": 2, "query": "",
              "current": {"text": "", "value": ""}, "options": []}


def detail_panel(x, y, trades_id, transfers_id):
    """The SPA's trade and transfer detail dialogs, opened from a row of the
    Trades or Rebalances table (see DETAIL_URL).

    The panel itself draws nothing; it takes the header row's last column so
    the tab's panel heights stay as they are. It reads the Trades and
    Rebalances tables' own results (the newest 500 entries of each log)
    through the Dashboard datasource, so the board scans each log once per
    refresh, and not per click: a query filtered on one id still scans the
    whole window (about 13s for 30 days), so a click only picks the id out
    of data the browser already has and the dialog opens at once. The
    Dashboard datasource keeps each table's refIds, so the script picks the
    picked source's frames by name (<source>-trades, <source>-transfers).
    On the bot source it also reads the newest 500 liq_event lines, the
    event timeline. The queries do not use
    `$detail`, so a click does not re-run them; the script reads `${detail}`,
    which makes Grafana redraw the panel when the variable changes.
    liquidity-panels/detail.js builds the dialog. renderMode "data" hands
    the script both frames, where "allRows" would draw a frame picker.
    MODE_LABELS (how each recovery command runs) is prepended from
    recovery-guide.json, like the header's guide, the client prefix from
    client-env.js, the per-row command builders from recovery-commands.js,
    the status-history order from status-history.js, and the bot-line
    helpers from log-lines.js.
    """
    with open(os.path.join(HERE, "liquidity-panels", "recovery-guide.json")) as f:
        mode_labels = json.load(f)["modeLabels"]
    commands = "".join(panel_module(module) + "\n" for module in (
        "client-env.js", "recovery-commands.js", "status-history.js",
        "log-lines.js"))
    with open(os.path.join(HERE, "liquidity-panels", "detail.js")) as f:
        after_render = (f"const MODE_LABELS = {json.dumps(mode_labels)};\n\n"
                        + commands + "\n" + f.read())
    with open(os.path.join(HERE, "liquidity-panels", "detail.css")) as f:
        styles = f.read()
    return {
        "id": nid(), "type": "marcusolsson-dynamictext-panel", "title": "",
        "description": "The detail dialog of a Trades or Rebalances row: "
                       "open it from the row's ⓘ.",
        "datasource": {"type": "datasource", "uid": "-- Mixed --"},
        "timeFrom": "30d", "hideTimeOverride": True,
        "maxDataPoints": 500,
        "targets": [
            *({"refId": ref, "panelId": panel_id, "withTransforms": False,
               "datasource": {"type": "datasource", "uid": "-- Dashboard --"}}
              for ref, panel_id in (("A", trades_id), ("B", transfers_id))),
            # Only the bot logs its events. On the exporter source the
            # script ignores this frame.
            cloudlog(f'{BOT_LOG} jsonPayload.target="liq_event"',
                     ref="bot-events"),
        ],
        "transparent": True,
        "gridPos": {"h": 1, "w": 1, "x": x, "y": y},
        "options": {
            "renderMode": "data",
            "editor": {"format": "auto", "language": "html"},
            "content": "<div></div>",
            "defaultContent": "<div></div>",
            "helpers": "",
            "afterRender": after_render,
            "styles": styles,
            "wrap": False,
        },
    }


def latest_status_table(title, desc, logs, fields, shown, w, h, x, y,
                        body_regex=None, bot_label_regex=None,
                        number_columns=None, time_columns=(), overrides=(),
                        sort_by=None):
    """A Cloud Logging table with one row per entity at its latest status,
    like the SPA's Trade History and Cross-venue Transfers cards (here
    Trades and Rebalances).

    The exporter writes one log entry per status change (insertId
    `<id>:<status>`), so the raw feed holds several rows per trade or
    transfer. The plugin returns entries newest first, so groupBy on
    jsonPayload.id with `first` keeps each entity's entry with the highest
    timestamp. That is usually the latest status, but a status's timestamp
    can be earlier than the one before it: a USDC bridge stamps
    WithdrawalComplete with confirmed_at and the BridgingSubmitting after it
    with the older initiated_at, so such a row can show withdrawing. The
    detail dialog orders a bridge's statuses by its lifecycle instead.
    The bot writes a line per transfer status change and per terminal
    trade status (again on a venue correction), with the same field names,
    so the same grouping applies; its line times follow commit order.

    The id stays as the first column, an ⓘ that opens the row's detail
    dialog (see DETAIL_URL).

    logs: (name, exporter query, bot query); the panel follows `source`
      (pick_source), and the detail panel finds its frames by refId.
    fields: {payload leaf: column name}; the entry's own timestamp is
      always kept, as fields["timestamp"]. A leaf only one source writes
      (the bot's usd) is a column on that source only.
    shown: column names in display order.
    body_regex: an extra extractFields over the entry's JSON body, with
      named groups as new columns. Grafana only treats the pattern as a
      regex when it is wrapped in slashes; without them it silently
      returns the whole body as one "NewField" column. The exporter's body
      is its JSON payload; a bot line's body is its message.
    bot_label_regex: the same columns for the bot's frame, from its labels
      as one JSON string.
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
        # Without a version Grafana runs the table panel's old-version
        # migration on load, which can drop field overrides.
        "pluginVersion": "13.1.0",
        "datasource": CL,
        "timeFrom": "30d",
        # The plugin takes the row limit from maxDataPoints, which Grafana
        # sets to the panel's pixel width when the panel does not set it.
        # 500 newest entries load in about 1.5s and still leave well over
        # the SPA's 100 rows after grouping by id.
        "maxDataPoints": 500,
        "targets": source_targets(*logs),
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "transformations": [
            pick_source(logs[0]),
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
            # The regexp extractor reads only strings, so the bot's labels
            # become their JSON text first. `filter` applies a step to the
            # bot's frame alone.
            *([{"id": step, "filter": {"id": "byRefId",
                                       "options": f"bot-{logs[0]}"},
                "options": options}
               for step, options in (
                   ("convertFieldType", {"conversions": [
                       {"targetField": "labels",
                        "destinationType": "string"}]}),
                   ("extractFields", {"source": "labels", "format": "regexp",
                                      "regExp": f"/{bot_label_regex}/",
                                      "replace": False, "keepTime": False}))]
              if bot_label_regex else []),
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
             "options": {"excludeByName": {},
                         "indexByName": {"Id": 0, **{
                             f"{name} (first)": index + 1
                             for index, name in enumerate(shown)}},
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
                # The ⓘ is the link's title: a data-links cell draws the
                # title in place of the value. A value mapping would do the
                # same, but it would also turn ${__data.fields[...]} into
                # the ⓘ, since data links read a field's display text.
                {"matcher": {"id": "byName", "options": "Id"},
                 "properties": [
                     {"id": "displayName", "value": " "},
                     # Grafana's minimum column width; see native_inventory.
                     {"id": "custom.width", "value": 50},
                     {"id": "custom.align", "value": "center"},
                     {"id": "custom.filterable", "value": False},
                     {"id": "custom.cellOptions", "value": {"type": "data-links"}},
                     {"id": "links", "value": [{
                         "title": "ⓘ", "url": DETAIL_URL,
                         "targetBlank": False, "oneClick": True}]}]},
                *overrides,
            ],
        },
        # Medium rows: small ones read crowded, large ones waste the card.
        "options": {"showHeader": True, "cellHeight": "md"},
    }


def column(name, px, mappings=None, color_text=False):
    # px None: the column stretches over the rest of the card.
    properties = [{"id": "custom.width", "value": px}] if px else []
    if mappings:
        properties.append({"id": "mappings", "value": mappings})
    if color_text:
        properties.append({"id": "custom.cellOptions",
                           "value": {"type": "color-text"}})
    return {"matcher": {"id": "byName", "options": name},
            "properties": properties}


TRANSFER_TYPES = "alpaca_to_base|base_to_alpaca|equity_mint|equity_redemption"


def usd_column(px):
    """The bot's `usd` in dollars, blank when the line has no value."""
    return {"matcher": {"id": "byName", "options": "USD"},
            "properties": [{"id": "custom.width", "value": px},
                           {"id": "unit", "value": "currencyUSD"},
                           {"id": "decimals", "value": 2}]}


panels.append(latest_status_table(
    "Trades",
    "Direct Raindex fills, fills routed through supported adapters such as "
    "Bebop, and the corresponding Alpaca hedge trades placed to offset "
    "exposure. One row per trade at its latest status; its ⓘ "
    "opens the details. Built from the newest 500 status entries, so a "
    "busy period can push older rows out of the 30-day window. USD, "
    "shares times the fill price, is on the bot source only. There, Last "
    "updated is when the bot logged the terminal status, which is later "
    "than the fill for a fill it caught up on; the dialog shows the fill "
    "time.",
    ("trades", 'logName="projects/$env/logs/liquidity-trades"',
     f'{BOT_LOG} jsonPayload.target="liq_trade"'),
    # One timestamp, first: when the row last changed status.
    fields={"timestamp": "Last updated", "symbol": "Asset", "venue": "Venue",
            "direction": "Side", "shares": "Size", "usd": "USD",
            "status": "Status"},
    shown=["Last updated", "Asset", "Venue", "Side", "Size", "USD", "Status"],
    w=11, h=TRADES_H, x=13, y=1,
    number_columns={"Size": 3, "USD": 2},
    overrides=[
        column("Last updated", 185), column("Asset", 85),
        column("Venue", 115, VENUE_MAPPINGS, color_text=True),
        column("Side", 75, SIDE_MAPPINGS, color_text=True),
        column("Size", 100), usd_column(110),
        column("Status", None, STATUS_CAP_MAPPINGS, color_text=True),
    ],
))

panels.append(latest_status_table(
    "Rebalances",
    "Asset movements between venues to rebalance inventory: equity mints "
    "(Alpaca to onchain), redemptions (onchain to Alpaca), and USDC bridges "
    "(Base/Ethereum via CCTP). One row per transfer at its latest status; "
    "its ⓘ opens the details. "
    "A USDC bridge can show the previous status for a while: the exporter "
    "stamps each status with the transfer's updatedAt, which is not always "
    "later than the one before (bridging starts from initiated_at). "
    "Built from the newest 500 status entries, so a busy period can push "
    "older rows out of the 30-day window. USD, an equity transfer at its "
    "mark when the status changed or a bridge's amount, is on the bot "
    "source only.",
    ("transfers", 'logName="projects/$env/logs/liquidity-transfers"',
     f'{BOT_LOG} jsonPayload.target="liq_transfer"'),
    # One timestamp, first: when the row last changed status. The start
    # time is in the row's detail dialog.
    fields={"timestamp": "Last updated", "symbol": "Asset",
            "amount": "Amount", "usd": "USD", "status": "Status"},
    shown=["Last updated", "Type", "Asset", "Amount", "USD", "Status"],
    # Type reads the direction for a USDC bridge and the kind for an equity
    # transfer, like the SPA's transferTypeLabel. A bridge's kind is
    # usdc_bridge and an equity transfer's direction is "", so each row can
    # match only one alternative, whatever the key order.
    body_regex='"(?:direction|kind)":"(?<Type>' + TRANSFER_TYPES + ')"',
    # The same in the bot's labels. Their JSON text escapes the quotes if
    # the plugin ships them as a string, hence the optional backslashes.
    bot_label_regex=(r'"jsonPayload\.(?:direction|kind)\\?":\\?"(?<Type>'
                     + TRANSFER_TYPES + ')'),
    w=11, h=TRANSFERS_H, x=13, y=1 + TRADES_H,
    number_columns={"Amount": 3, "USD": 2},
    overrides=[
        column("Last updated", 185), column("Type", 165, TYPE_MAPPINGS),
        # A USDC bridge carries no symbol; the SPA's Asset column reads
        # "USDC" for it.
        column("Asset", 80, [{"type": "special", "options": {
            "match": "empty", "result": {"text": "USDC", "index": 0}}}]),
        column("Amount", 115), usd_column(110),
        column("Status", None, STATUS_DOT_MAPPINGS, color_text=True),
    ],
))
panels.append(detail_panel(
    x=23, y=0,
    trades_id=next(p["id"] for p in panels if p.get("title") == "Trades"),
    transfers_id=next(p["id"] for p in panels if p.get("title") == "Rebalances")))


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
    panels, tab_links("Dashboard"), [ENV_VAR, SOURCE_VAR, DETAIL_VAR]))

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
# (PNL_TABLE_USD_SIGNED further down).
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
# override rather than joining unit_overrides' PNL_TABLE_USD_SIGNED sweep below. The
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
    "The bot's structured log lines: on the exporter source as the "
    "exporter re-emits them, on the bot source the bot's own JSON lines. "
    "Filter with the level/category/target/search variables above. ⚠️ "
    "Only the newest matching entries, about the panel's pixel width of "
    "them. Narrow the filters, or use the SPA for deep digs. DEBUG/TRACE "
    "are not shown.",
    'logName="projects/$env/logs/liquidity-botlogs" '
    'labels.level=~"^(${level:pipe})$" labels.target=~"${target}" '
    'labels.target=~"^(${category:pipe})$" '
    'jsonPayload.message=~"${search}"',
    # The message is the log-lines frame's `body`; level/target are
    # leaves in `labels` like every other payload field. The bot's lines
    # have the same names, so one set of columns serves both sources.
    columns=[("jsonPayload.level", "Level"), ("jsonPayload.target", "Target"),
             ("body", "Message")],
    # The level filter also keeps out the bot's DEBUG and TRACE lines,
    # which its stdout holds and the exporter never shipped.
    source=("logs",
            f'{BOT_LOG} jsonPayload.level=~"^(${{level:pipe}})$" '
            'jsonPayload.target=~"${target}" '
            'jsonPayload.target=~"^(${category:pipe})$" '
            'jsonPayload.message=~"${search}"'),
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


def board_panels(node):
    """Every panel in a board, rows' collapsed panels included."""
    for panel in node.get("panels", []):
        yield panel
        yield from board_panels(panel)


def log_source_problems(panel):
    """Why a panel's Cloud Logging queries do not follow `source`, if they
    do not. Allowed: a pair exporter-<name> and bot-<name> under
    pick_source(<name>); the detail script's bot-events, which it reads on
    the bot source only; and the exporter-only Orders log."""
    targets = [target for target in panel.get("targets", [])
               if target.get("datasource", panel.get("datasource")) == CL]
    names = {}
    problems = []
    for target in targets:
        source, _, name = target.get("refId", "").partition("-")
        query = target.get("queryText", "")
        if source not in SOURCES:
            if f"logs/{EXPORTER_ONLY_LOG}" not in query:
                problems.append(f"refId {target.get('refId')} is not "
                                "exporter-<name> or bot-<name>")
            continue
        names.setdefault(name, set()).add(source)
        reads_bot = query.startswith(BOT_LOG)
        reads_exporter = any(f"logs/{log}\"" in query
                             for log in EXPORTER_LOGS)
        if (source == "bot") != reads_bot or (source == "bot") == reads_exporter:
            problems.append(f"{target['refId']} reads the wrong log: {query}")
    for name, sources in names.items():
        if sources == set(SOURCES):
            if (panel.get("transformations") or [None])[0] != pick_source(name):
                problems.append(f"{name} does not start with pick_source")
        elif not (sources == {"bot"} and name == "events"
                  and panel["type"] == "marcusolsson-dynamictext-panel"):
            problems.append(f"{name} has only {sorted(sources)}")
    return problems


def check_log_sources(boards):
    failures = [(board["uid"], panel.get("title"), problem)
                for board in boards
                for panel in board_panels(board)
                for problem in log_source_problems(panel)]
    for uid, title, problem in failures:
        print(f"{uid}: panel {title!r}: {problem}", file=sys.stderr)
    if failures:
        raise SystemExit(f"{len(failures)} log queries do not follow source")


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


def bar_cell_shows(cell, value):
    """(fill, percent) that a bar cell's first matching regex mapping draws
    for `value` as Grafana passes it, the number's string form."""
    mappings = next(o["value"] for o in cell if o["id"] == "mappings")
    for mapping in mappings:
        match = re.fullmatch(mapping["options"]["pattern"], value)
        if match:
            html = match.expand(mapping["options"]["result"]["text"]
                                .replace("$1", r"\1"))
            fill = re.search(r"90deg, (rgba\([^)]*\))", html)
            text = re.search(r">([^<]*)%</div>", html)
            return (fill.group(1) if fill else None,
                    text.group(1) if text else None)
    return None


def check_bar_cells():
    """The banded and unjudged encodings draw the colour and percent they
    mean, for values with and without float noise. CI runs this on --check."""
    green, red = "rgba(34,197,94,0.6)", "rgba(239,68,68,0.6)"
    cell = bar_cell(unjudged=True)
    cases = {
        "45.5001": (green, "45.5"), "100.0001": (green, "100"),
        "0.0001": (green, "0"), "45.5000999999999": (green, "45.5"),
        "-45.5001": (red, "45.5"), "-0.0001": (red, "0"),
        "-100.0001": (red, "100"),
        "1045.5001": (GREY_FILL, "45.5"), "1000.0001": (GREY_FILL, "0"),
        "1100.0001": (GREY_FILL, "100"), "1005.0001": (GREY_FILL, "5"),
        "1010.0001": (GREY_FILL, "10"), "1000.5001": (GREY_FILL, "0.5"),
        "1045.5000999999999": (GREY_FILL, "45.5"),
    }
    wrong = {value: (bar_cell_shows(cell, value), want)
             for value, want in cases.items()
             if bar_cell_shows(cell, value) != want}
    # Cells without the unjudged mapping keep their two colours.
    if bar_cell_shows(bar_cell(), "100.0001") != (green, "100"):
        wrong["banded 100.0001"] = bar_cell_shows(bar_cell(), "100.0001")
    if wrong:
        raise SystemExit(f"bar cell mappings draw the wrong bar: {wrong}")


check_bar_cells()
check_liq_pinned(dashboards)
check_log_sources(dashboards)
if sys.argv[1:] == ["--check"]:
    check_committed(dashboards)
elif sys.argv[1:]:
    raise SystemExit(f"usage: {sys.argv[0]} [--check]")
else:
    write_boards(HERE, dashboards)
