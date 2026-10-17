#!/usr/bin/env python3
"""Generator for dashboards/liquidity/t0-liquidity.json, the Liquidity bot
board: the bot's inventory, trades, rebalances, orders, PnL, performance
and logs in Grafana.

Regenerate after edits:

    python3 observability/gen-t0-liquidity.py

Check that the committed JSON matches the generator (CI runs this):

    python3 observability/gen-t0-liquidity.py --check

Data comes from the bot itself:

  cloudmon  its liq_* gauges on /metrics, via the Ops Agent scrape into
            managed prometheus
  cloudlog  its own log, liquidity-bot (its JSON stdout), and the exporter
            sidecar's liquidity-orders log for Orders, which the bot does
            not log

Layout, in tab order: header strip,
Dashboard (inventory tables | trades over rebalances, a 13/11 split;
checked with no sideways scroll on a 1934px-wide viewport), Orders, PnL,
Performance, Logs, then the pre-exporter native-metric rows
(t0-liquidity-native-rows.json), collapsed.

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

import base64
import filecmp
import json
import os
import re
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
NATIVE_ROWS = os.path.join(HERE, "t0-liquidity-native-rows.json")

# The exporter sidecar can still serve liq_* beside the bot, so an unpinned
# liq_ selector could sum the two. Every liq_ selector on these boards reads
# the bot's scrape job, which is named after the project `env` picks
# (t0-liquidity, t0-liquidity-staging).
LIQ_JOB_MATCHER = 'job="$env"'
# The header's Bot light: the bot's scrape up. pin_liq leaves it alone, since
# it already names its job.
LIQ_UP = 'max(up{job="$env"})'
LIQ_NAME = re.compile(r"(?<![A-Za-z0-9_:])liq_[a-z0-9_]+")
LIQ_PREFIX = re.compile(r"(?<![A-Za-z0-9_:])liq_")

CM = {"type": "stackdriver", "uid": "cloudmon"}
CL = {"type": "googlecloud-logging-datasource", "uid": "cloudlog"}

# The log panels read the bot's log, its stdout, with every field of a JSON
# line at jsonPayload.<field>. Each query is refId bot-<name>, which the
# detail panel finds the Trades and Rebalances results by; check_log_sources
# fails on a log query that is not.
BOT_LOG_NAME = "liquidity-bot"
BOT_LOG = f'logName="projects/$env/logs/{BOT_LOG_NAME}"'
BOT_EVENTS_FILTER = f'{BOT_LOG} jsonPayload.target="liq_event"'
# The bot writes no order lines, so Orders reads the exporter's log.
EXPORTER_ONLY_LOG = "liquidity-orders"

_id = [0]


def nid():
    _id[0] += 1
    return _id[0]


# --------------------------------------------------------------------------
# Target + panel helpers
# --------------------------------------------------------------------------

# Spans of a PromQL expression where a liq_ name is not a selector: string
# literals ("...", '...', `...`) and the label lists of by, without, on,
# ignoring, group_left and group_right.
PROMQL_STRING = re.compile(r'"(?:[^"\\]|\\.)*"|\'(?:[^\'\\]|\\.)*\'|`[^`]*`')
PROMQL_LABEL_LIST = re.compile(
    r"(?<![A-Za-z0-9_:])(?:by|without|on|ignoring|group_left|group_right)"
    r"\s*(\([^()]*\))")


def _not_selector_spans(expr):
    strings = [found.span() for found in PROMQL_STRING.finditer(expr)]
    in_string = lambda pos: any(start <= pos < end for start, end in strings)
    lists = [found.span(1) for found in PROMQL_LABEL_LIST.finditer(expr)
             if not in_string(found.start())]
    return strings + lists


def _liq_names(pattern, expr):
    """`pattern`'s matches in `expr` outside the spans where a liq_ name is
    not a selector."""
    spans = _not_selector_spans(expr)
    return [found for found in pattern.finditer(expr)
            if not any(start <= found.start() < end for start, end in spans)]


def _matchers_close(expr, open_pos):
    """The index of the `}` closing the label matchers that open at
    `open_pos`, skipping string literals, or -1."""
    pos = open_pos + 1
    while pos < len(expr):
        literal = PROMQL_STRING.match(expr, pos)
        if literal:
            pos = literal.end()
        elif expr[pos] == "}":
            return pos
        else:
            pos += 1
    return -1


def pin_liq(expr):
    """Adds the `source` job matcher to every liq_ selector in `expr`.

    Handles bare names (`liq_x` -> `liq_x{job="..."}`) and names that already
    carry matchers (`liq_x{a="b"}` -> `liq_x{job="...",a="b"}`). A selector
    that already names a job is left for `unpinned_liq_selectors` to judge.
    A liq_ name inside a string literal or a by/on/... label list is not a
    selector and stays as it is.
    """
    out = []
    pos = 0
    for match in _liq_names(LIQ_NAME, expr):
        if match.start() < pos:
            continue
        out.append(expr[pos:match.end()])
        pos = match.end()
        if not expr.startswith("{", pos):
            out.append("{" + LIQ_JOB_MATCHER + "}")
            continue
        close = _matchers_close(expr, pos)
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
    exactly LIQ_JOB_MATCHER, outside string literals and label lists. A
    templated name such as `liq_$col` is never a valid selector, so it is
    reported too: the check fails closed. So is a nameless selector whose
    `__name__` matcher can pick a liq_ series (see _name_matcher_selectors),
    which pin_liq does not pin."""
    pinned = {match.start() for match in _liq_names(LIQ_NAME, expr)
              if _selector_is_pinned(expr, match.end())}
    return [expr[found.start():found.start() + 40]
            for found in _liq_names(LIQ_PREFIX, expr)
            if found.start() not in pinned] + _name_matcher_selectors(expr)


PROMQL_MATCHER = re.compile(
    r"([A-Za-z_][A-Za-z0-9_]*)\s*(=~|!~|!=|=)\s*"
    r"(\"(?:[^\"\\]|\\.)*\"|'(?:[^'\\]|\\.)*'|`[^`]*`)")


def _name_matcher_selectors(expr):
    """Every nameless selector in `expr`, `{__name__=...}` or `{"name"}`,
    that can read a liq_ series and is not pinned to exactly
    LIQ_JOB_MATCHER. Its metric name is a string literal, which pin_liq
    never touches. An equality matcher counts when its value names a liq_
    metric; a regex or negative matcher always counts, since it can match a
    liq_ name. A bare quoted string as the first item is Prometheus's
    quoted metric name, `{__name__="name"}`, and counts when it names a
    liq_ metric."""
    strings = [found.span() for found in PROMQL_STRING.finditer(expr)]
    found = []
    for pos, char in enumerate(expr):
        if char != "{" or any(start <= pos < end for start, end in strings):
            continue
        if pos and re.match(r"[A-Za-z0-9_:]", expr[pos - 1]):
            continue
        close = _matchers_close(expr, pos)
        if close == -1:
            continue
        inner = expr[pos + 1:close]
        matchers = PROMQL_MATCHER.findall(inner)
        quoted_name = re.match(r"\s*(" + PROMQL_STRING.pattern + r")\s*(?:,|$)",
                               inner)
        reads_liq = any(
            label == "__name__"
            and (op != "=" or LIQ_PREFIX.match(value[1:-1]))
            for label, op, value in matchers) or bool(
                quoted_name and LIQ_PREFIX.match(quoted_name.group(1)[1:-1]))
        jobs = [f"{label}{op}{value}" for label, op, value in matchers
                if label == "job"]
        if reads_liq and jobs != [LIQ_JOB_MATCHER]:
            found.append(expr[pos:pos + 40])
    return found


def _selector_is_pinned(expr, name_end):
    if not expr.startswith("{", name_end):
        return False
    close = _matchers_close(expr, name_end)
    if close == -1:
        return False
    matchers = [part.strip() for part in expr[name_end + 1:close].split(",")]
    jobs = [part for part in matchers if re.match(r"job\s*(=|!=|=~|!~)", part)]
    return jobs == [LIQ_JOB_MATCHER]


def promql(expr, instant=False, ref="A", step="60s"):
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
    # NOTE: no legendFormat. Grafana 13.1.0's Cloud Monitoring query has no
    # such field, and its PromQL path returns the frames with no alias or
    # legend applied, so a legendFormat names nothing. A panel names its
    # series with series_names instead (check_series_names).
    return target


def series_names(names, colors=None):
    """Field overrides that name each PromQL target's series: `names` maps
    a refId to its display name, which may template a series label as
    `${__field.labels.<label>}`. `colors` maps a refId to a fixed colour.
    The colour goes in the same override: a byName matcher sees the
    series' name from before the displayName override, not the new one."""
    colors = colors or {}
    return [{"matcher": {"id": "byFrameRefID", "options": ref},
             "properties": [{"id": "displayName", "value": name}] + (
                 [{"id": "color", "value": {"mode": "fixed",
                                            "fixedColor": colors[ref]}}]
                 if ref in colors else [])}
            for ref, name in names.items()]


def cloudlog(query, ref="A"):
    return {"refId": ref, "datasource": CL, "projectId": "$env",
            "queryText": query, "queryType": "logs"}


def stat(title, desc, expr, display=None, unit="short", decimals=None,
         steps=None, mappings=None, w=3, h=3, x=0, y=0,
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
        "targets": [promql(expr, instant=True)],
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


def bargauge(title, desc, expr, name, unit="ms", w=8, h=8, x=0, y=0,
             steps=None, decimals=0):
    """`name` names each bar, see series_names."""
    return {
        "id": nid(), "type": "bargauge", "title": title, "description": desc,
        "datasource": CM,
        "targets": [promql(expr, instant=True)],
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "fieldConfig": {
            "defaults": {
                "unit": unit, "decimals": decimals,
                "thresholds": {"mode": "absolute",
                               "steps": steps or [{"color": "blue", "value": None}]},
                "mappings": [],
            },
            "overrides": series_names({"A": name}),
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
    """The PnL bar charts: one bar per calendar day, split by symbol or
    by PnL stream.

    x is the `day` LABEL, kept a string on purpose. The bot's day buckets are
    sparse (staging jumps Jul 8 -> Jul 22), and the chart draws them as
    adjacent categories with no gap; converting `day` to a time field would
    reopen those two weeks as dead axis. So: barchart with a
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
                         # A symbol with no fill that day is a zero bar, not
                         # a hole.
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
            # The target bar size is `min(34px, slot * 0.68)`. Grafana's
            # barchart takes only a slot FRACTION, no pixel cap (checked in
            # the 13.1.0 panel editor: Bar width and Bar radius, nothing
            # else), so no single number reproduces that at every bucket
            # count. 0.35 lands on ~34px at this panel width for a normal
            # range; a range with only two buckets still draws two wide
            # bars where the target is two narrow ones. Cosmetic, and
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
                   hide_time=False, dedup_by=None, name=None):
    """columns: list of (raw_field, display_name).

    query: a Cloud Logging filter on the bot's log, read as refId
      bot-<name>, or `name` None for a query on another log (refId A).

    number_columns: {display_name: decimals} — the payload ships decimal
      STRINGS; convertFieldType turns them numeric so decimals apply
      (shown as 0.049, not 0.0493843...).
    time_columns: display names formatted in the "Jul 31, 13:31:50"
      style (override with time_unit, e.g. TIME_FMT for the "... [UTC]"
      variant); string RFC3339 columns (e.g. Started) are also
      convertFieldType'd to real time fields.
    hide_time: drop the Cloud Logging entry's own ingestion timestamp
      (the field normally renamed to time_name) from the rendered table,
      for panels where a business timestamp already in time_columns is
      the only one shown (Orders' "Created" versus the
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
        "targets": [cloudlog(query, ref=f"bot-{name}" if name else "A")],
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
            # groupBy appends so the headers read cleanly. The plugin
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
# Value mappings shared by the row tables: they capitalize and color-code
# the venue and status values.
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
        # A USDC bridge row reads its direction.
        "alpaca_to_base": {"text": "Alpaca → Raindex", "color": "text", "index": 3},
        "base_to_alpaca": {"text": "Raindex → Alpaca", "color": "text", "index": 4},
    }},
]
# D13: transfer status carries a leading dot ("● Completed"); trade
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
# Grafana ignores a panel's relative override on an absolute range. The
# panels hide its "Last 5 minutes" badge: the header pills have no room for
# it, and on the inventory tables it cut off the title's total on a laptop.
# The board description says they show the live state.
LATEST_ONLY_HIDDEN = {"timeFrom": "5m", "hideTimeOverride": True}


# The timestamp style: "Jul 31, 13:31:50 UTC" (D7).
TIME_FMT = [{"id": "unit", "value": "time:MMM D, HH:mm:ss [UTC]"}]

# ==========================================================================
# The tab suite: five dashboards, one per tab, cross-linked with a button
# row that works as the tab bar. Every link carries theme=light so the
# suite renders in the light look regardless of the viewer's Grafana theme
# preference.
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
            # The Dashboard tab fits the window itself (board-layout.js).
            "url": f"/d/{uid}/?theme=light",
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
    line is for the tests in observability/tests/; afterRender is not a
    module."""
    with open(os.path.join(HERE, "liquidity-panels", name)) as f:
        lines = f.readlines()
    # Only a one-line export is dropped whole; the rest of a longer one
    # would stay in the script and break it.
    split = [line for line in lines
             if line.startswith("export ") and not line.rstrip().endswith(";")]
    if split:
        raise SystemExit(f"{name}: keep its export on one line: {split[0]!r}")
    return "".join(line for line in lines if not line.startswith("export "))


def pills(y, w=24, fit_window=False, locked_sort=(), fit_columns=None,
          header_tips=None, tree_sorts=None, table_totals=None,
          scale_tables=(), row_tips=None, full_entries=None, unit_tags=None):
    """The header and settings bar as one Business Text row, repeated on
    every tab: the settings
    pills (broker, Equity and USDC targets with their bands, Trigger,
    Reserve) and a Config button on the left; the CLI recovery guide
    button, a UTC clock, the commit, uptime, and the connection badge on
    the right. Config and the recovery guide open dialogs.

    Every series carries a `k` label naming it; liquidity-panels/header.js
    reads the rows by `k` (liquidity-panels/header-rows.js). The commit and
    info series are their sample timestamps, so the script can keep the
    newest label set when a deploy or restart leaves the previous one in the
    lookback window. The recovery guide is static data in
    liquidity-panels/recovery-guide.json, the source of truth for the
    recovery commands, and prepended to
    the script with client-env.js, which retargets its commands at the
    board's environment.

    w: the Dashboard tab narrows it to 23 to fit the detail panel beside
    it (see detail_panel()). fit_window: the board fits the window itself
    (board-layout.js), so the script takes Grafana's autofitpanels off a
    link that has it: autofit would squeeze a short window's board.
    locked_sort: titles of the tab's tables that must not sort, because a
    sort breaks their tree rows (liquidity-panels/board-tables.js).
    fit_columns: {"fit": [titles], "fill": title, "split": [titles]}: a
    left column whose `fit` tables take the height their rows need and
    `fill` the rest, and a right column the `split` panels share equally,
    both ending level (liquidity-panels/board-layout.js). header_tips:
    {panel title: {header: hover text}} for headers that need explaining
    (liquidity-panels/board-tables.js). tree_sorts: {panel title:
    {"rowField": the row key, "keys": {header: hidden key}}} for tree tables
    whose headers sort by a key every row of a group shares
    (liquidity-panels/board-tables.js). row_tips: {panel title: config}
    for tables whose first column explains its row on hover (ROW_TIPS,
    liquidity-panels/board-tables.js). table_totals: {panel title:
    [column] or {"columns", "stamp", "staleAfterSeconds"}}, each panel's
    total of those columns in USD after its title, and how old its prices
    are once stale (liquidity-panels/board-totals.js). scale_tables: groups
    of tables, [{"id", "panels", "fixedPx", "ratioMinPx", "stretch",
    "stretchMinPx", "cellFontPx"}], whose fixed columns (fixedPx) shrink
    together on a window too narrow for them and the stretching column's
    ratioMinPx (liquidity-panels/board-scale.js). full_entries: {panel
    title: entries}, log tables that first load a screen's entries and then
    their whole history (liquidity-panels/board-progressive.js). unit_tags:
    {panel title: {"match", "column", "tags": [{"values", "text", "tip"}]}},
    a unit after a cell by its row's match column
    (liquidity-panels/board-tables.js).
    """
    def named(expr, name):
        return f'label_replace({expr}, "k", "{name}", "", "")'

    expr = " or ".join([
        named(LIQ_UP, "up"),
        named("time() - max(liq_bot_start_timestamp_seconds)", "uptime"),
        named("max by (git_commit) (timestamp(liq_bot_info))", "commit"),
        named("max by (broker, log_level, wallet_kind, wallet_address, orderbook, "
              "turnkey_organization, server_port) (timestamp(liq_settings_info))",
              "info"),
        named("max(liq_settings_equity_target)", "equity_target"),
        named("max(liq_settings_equity_deviation)", "equity_deviation"),
        named("max(liq_settings_usdc_target)", "usdc_target"),
        named("max(liq_settings_usdc_deviation)", "usdc_deviation"),
        named("max(liq_settings_execution_threshold_usd)", "trigger"),
        named("max(liq_settings_cash_reserved)", "cash_reserved"),
        named("max(liq_settings_order_polling_seconds)", "order_polling"),
        named("max(liq_settings_inventory_poll_seconds)", "inventory_polling"),
        named("max(liq_settings_deployment_block)", "deployment_block"),
        # For the trade sizes' hover (UNIT_TAGS): wt in t and t in wt.
        named("max by (symbol, chain) (liq_equity_chain_wrapper_ratio)",
              "wrapper_ratio"),
    ])
    with open(os.path.join(HERE, "liquidity-panels", "recovery-guide.json")) as f:
        guide = json.load(f)
    after_render = (f"const FIT_WINDOW = {json.dumps(fit_window)};\n"
                    f"const LOCKED_SORT_PANELS = {json.dumps(list(locked_sort))};\n"
                    f"const FIT_COLUMNS = {json.dumps(fit_columns)};\n"
                    f"const ROW_TINTS = {json.dumps(MARK_ROW_TINTS)};\n"
                    f"const HEADER_TIPS = {json.dumps(header_tips or {})};\n"
                    f"const TREE_SORTS = {json.dumps(tree_sorts or {})};\n"
                    f"const ROW_TIPS = {json.dumps(row_tips or {})};\n"
                    f"const UNIT_TAGS = {json.dumps(unit_tags or {})};\n"
                    f"const TABLE_TOTALS = {json.dumps(table_totals or {})};\n"
                    f"const SCALE_TABLES = {json.dumps(list(scale_tables))};\n"
                    f"const FULL_ENTRIES = {json.dumps(full_entries or {})};\n"
                    f"const RECOVERY_GUIDE = {json.dumps(guide)};\n\n"
                    + panel_module("client-env.js") + "\n"
                    + panel_module("header-rows.js") + "\n"
                    + panel_module("copy-command.js") + "\n"
                    + panel_module("board-tables.js") + "\n"
                    + panel_module("board-layout.js") + "\n"
                    + panel_module("board-totals.js") + "\n"
                    + panel_module("board-scale.js") + "\n"
                    + panel_module("board-progressive.js") + "\n"
                    + panel_module("header.js"))
    with open(os.path.join(HERE, "liquidity-panels", "header.css")) as f:
        styles = f.read()
    return [{
        "id": nid(), "type": "marcusolsson-dynamictext-panel", "title": "",
        "description": "The header: settings pills, Config, the CLI "
                       "recovery guide, the deployed commit, uptime, and "
                       "whether the bot is up: its scrape's up.",
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


# The PnL range buttons (1W / 1M / YTD / 1Y / All). PnL is a windowed
# server-side computation, so the exporter runs one /pnl query per window
# and tags every series with window=<key>; this variable picks which set the
# board shows. Default 1w.
# Windows are anchored on the last date WITH DATA, not today (see the
# exporter's collect_pnl docstring).
WINDOW_VAR = {
    "name": "window", "type": "custom", "label": "PnL range",
    "description": "The PnL range: 1D, 1W, 1M, YTD, 1Y or All. Windows are "
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


# The Dashboard tab has no time series to mark, so Grafana's built-in
# "Annotations & Alerts" query, which every board runs at load unless told
# not to, is off there: one request fewer while the board loads.
NO_ANNOTATIONS = {"list": [{
    "builtIn": 1, "datasource": {"type": "grafana", "uid": "-- Grafana --"},
    "enable": False, "hide": True, "iconColor": "rgba(0, 211, 255, 1)",
    "name": "Annotations & Alerts", "type": "dashboard"}]}


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
        **({"annotations": NO_ANNOTATIONS} if uid == "t0-liquidity" else {}),
    }


dashboards = []

# The Equities table: an asset row with its chain rows under it, so the
# header locks its sort (pills(locked_sort=...)).
EQUITIES_TITLE = "Equities"
# Equities sorts by a column without breaking its tree: each sortable column
# has a hidden key that a chain row takes from its asset, so a sort on the key
# and then the row keeps each asset's chain rows under it. board-tables.js
# swaps the panel's sort transformations when a header is clicked; Asset
# restores the default order, the largest exposure either way first
# (EQUITY_DEFAULT_SORT_KEY). Exposure sorts by that size too, not by its
# sign. {header: hidden key column}.
EQUITY_DEFAULT_SORT_KEY = "sort_exposure_size"
EQUITY_SORT_KEYS = {
    "Total": "sort_total",
    "Total USD": "sort_total_usd", "Exposure": EQUITY_DEFAULT_SORT_KEY,
    "tStock": "sort_unwrapped", "wtStock": "sort_wrapped",
    "Ratio": "sort_ratio",
}
# Every hidden key column, once.
EQUITY_SORT_COLUMNS = list(dict.fromkeys(
    [*EQUITY_SORT_KEYS.values(), EQUITY_DEFAULT_SORT_KEY]))
EQUITY_ROW_FIELD = "row\\col"

# What each Dashboard table header shows, on hover (board-tables.js).
HEADER_TIPS = {
    "USD · Brokers": {
        "Cash": "Alpaca's cash, the reserve included",
        "USDC": "USDC held at Alpaca",
        "Reserve": "Cash the bot always keeps at Alpaca and never moves",
        "Rebalanceable": "Withdrawable cash above the reserve: what can move "
                         "onchain now",
        "In flight": "Cash on its way to or from Alpaca",
        "Share of total": "Alpaca's cash against Alpaca, Base's vault and all "
                          "cash in flight. Green while every chain is inside "
                          "its band; the ticks are where a chain leaves it",
    },
    "USD · Onchain": {
        "Vault": "USDC in the chain's vault",
        "In flight": "USDC in flight from or to the vault",
        "Wallet": "USDC in the bot's wallet on the chain, outside the vault",
        "Target": "The chain's target ratio",
        "±": "How far the ratio may drift from the target before the bot "
             "rebalances",
        "Ratio": "vault / (vault + Alpaca cash). Green inside the band, red "
                 "outside; the ticks are the band",
    },
    "Equities": {
        "Asset": "The asset, and under it Alpaca and each chain it has a "
                 "vault on. Hover a symbol or a chain for its flags. Click to "
                 "restore the default order: the largest exposure first",
        "Amount": "Shares held: wrapped shares in a chain's vault, or "
                  "shares at Alpaca",
        "Inflight": "Shares in flight in a mint or redemption. On an "
                    "asset row, every chain's and Alpaca's",
        "Total": "Shares at Alpaca and every chain, in underlying shares, with "
                 "those in flight. Click to sort",
        "Total USD": "Total at the last price. Click to sort",
        "Exposure": "Unhedged net position at the live price, else at "
                    "the last price. Red beyond ±$5. Click to sort",
        "tStock": "Unwrapped tokens (tAAPL) in the bot's wallet on the "
                  "chain, not in a vault. Click to sort",
        "wtStock": "Wrapped tokens (wtAAPL) in the bot's wallet on the "
                   "chain, not in a vault. Click to sort",
        "Ratio": "A chain's or Alpaca's share of the asset; the ticks are "
                 "its band (Alpaca's is what the chains' targets leave). "
                 "Green inside, red outside. Click to sort the assets by "
                 "their onchain share",
    },
    "Trades": {
        "Last updated": "When the bot logged the trade's latest status",
        "Venue": "Where it filled: an onchain venue, or Alpaca for a hedge",
        "Side": "Buy or sell, from the bot's side",
        "Size": "Shares filled",
        "USD": "Shares × fill price",
        "Status": "The trade's latest status",
    },
    "Rebalances": {
        "Last updated": "When the bot logged the transfer's latest status",
        "Type": "Mint, redeem, or the USDC bridge's direction",
        "Amount": "Shares, or USDC for a bridge",
        "USD": "An equity transfer at its mark, or a bridge's amount",
        "Status": "The transfer's latest status; the dialog's timeline can be "
                  "newer",
    },
}
# Display names for the chains the bot knows (config keys). A chain not
# listed here still gets its row and column, under its raw key.
CHAIN_NAMES = {"base": "Base", "robinhood": "Robinhood", "hyperevm": "HyperEVM",
               "ethereum": "Ethereum"}

# The venues with a mark: Alpaca and the chains.
MARK_NAMES = {"alpaca": "Alpaca", **CHAIN_NAMES}

GREEN, RED, AMBER, GREY = "#22c55e", "#ef4444", "#f59e0b", "#6b7280"

# A venue row's background, a wash of its mark's colour, keyed by the
# mark's name (its alt, MARK_NAMES): board-tables.js tints every row holding
# the mark. Bright marks (Robinhood's neon, Alpaca's yellow) get a thinner
# wash, so every tint reads about as strong as Base's blue. The header row
# that installs it comes before the marks, so the names are written out;
# check_row_tints holds them to MARK_NAMES.
MARK_ROW_TINTS = {
    "Alpaca": "rgba(252,212,0,0.08)",
    "Base": "rgba(0,82,255,0.20)",
    "Robinhood": "rgba(204,255,0,0.06)",
    "Ethereum": "rgba(98,126,234,0.20)",
    "HyperEVM": "rgba(151,252,228,0.15)",
}
# A symbol's flags on hover (board-tables.js): the first column of an
# Equities row is an <abbr title="row:<key>">, and the tip lists the flags
# of that row's chains from the panel's hidden flags field
# (CHAIN_FLAGS_EXPR): every chain row of an asset, or the one chain. Each
# column reads one digit of the packed number, a state per digit value:
# [text, colour, light].
ROW_TIP_PREFIX = "row:"
ROW_TIPS = {
    EQUITIES_TITLE: {
        "rowFields": [EQUITY_ROW_FIELD, "Asset"],
        "flagFields": ["flags", "Flags"],
        "chains": CHAIN_NAMES,
        "empty": "No chain flags published",
        "columns": [
            {"name": "Counter trading",
             "states": {"2": ["on", GREEN, "●"], "1": ["off", RED, "●"]}},
            {"name": "Rebalancing",
             "states": {"2": ["enabled", GREEN, "●"],
                        "4": ["paused", AMBER, "●"],
                        "1": ["disabled", RED, "●"]}},
            {"name": "Extended hours",
             "states": {"2": ["on", GREEN, "●"], "1": ["off", RED, "●"],
                        "3": ["n/a, no counter trading", GREY, "○"]}},
        ],
    },
}
# The USD tables above it, sized to their rows, and the log tables beside
# them, half the column each (pills(fit_columns=...)).
USD_BROKERS_TITLE = "USD · Brokers"
USD_ONCHAIN_TITLE = "USD · Onchain"
TRADES_TITLE = "Trades"
# The left column's three tables end in their ratio bar, which stretches to
# the card's edge. Their other columns add up to RATIO_LEFT_PX, so the bars
# line up one above the other (check_ratio_columns). On a window too narrow
# for those columns and RATIO_MIN_PX of bar, board-scale.js shrinks the
# columns and their text together; a 1080p window needs none.
RATIO_LEFT_PX = 790
RATIO_MIN_PX = 180
# Trades and Rebalances scale too, each by its own factor: their fixed
# columns (the ⓘ, Last updated, and the rest) add up to these, and Status
# takes the rest, at least STATUS_MIN_PX (check_scale_groups).
TRADES_FIXED_PX = 50 + 150 + 65 + 80 + 55 + 100 + 80
REBALANCES_FIXED_PX = 50 + 150 + 135 + 65 + 88 + 80
STATUS_MIN_PX = 110
# The inventory tables' numbers, larger than Grafana's 14px so they fill
# their 42px rows; scaled down with the columns on a narrow window.
INVENTORY_CELL_FONT_PX = 16
# Room the factor leaves for what the widths do not count: each cell's 1px
# border and each scaled width rounded up.
TABLE_SLACK_PX = 24
# Each inventory table's total in USD after its title: the balances held,
# in flight left out (a bridge's USDC can sit in a wallet while it is in
# flight, so both would count it twice). {panel title: [column]}.
TABLE_TOTALS = {
    USD_BROKERS_TITLE: ["cash", "usdc"],
    USD_ONCHAIN_TITLE: ["vault", "wallet"],
    # The prices are the last observed, live or not: the title says how old
    # the oldest is once it is older than PRICE_STALE_SECONDS.
    # Then the onchain and broker parts of it: each row's shares and those
    # in flight, a chain row's (symbol/chain) and Alpaca's (symbol/alpaca).
    EQUITIES_TITLE: {"columns": ["total_usd"], "stamp": "price_ts",
                     "staleAfterSeconds": 600, "rowField": EQUITY_ROW_FIELD,
                     "parts": [
                         {"label": "onchain",
                          "columns": ["vault_usd", "inflight_usd"],
                          "rows": "^[^/]+/(?!alpaca$)"},
                         {"label": "brokers",
                          "columns": ["vault_usd", "inflight_usd"],
                          "rows": "/alpaca$"},
                     ]},
}
REBALANCES_TITLE = "Rebalances"
# An onchain trade's size is in wrapped shares (Raindex settles them), a
# broker hedge's in underlying shares; at a wrapper ratio other than 1 the
# two differ for the same exposure, so every size names its unit: wt
# onchain, t at Alpaca (board-tables.js). Venue holds its mapped text.
# On hover each size shows itself in the other unit, through the symbol's
# wrapper ratio on each chain (the header reads them).
UNIT_TAGS = {TRADES_TITLE: {
    "match": "Venue", "column": "Size", "symbol": "Asset",
    "chains": CHAIN_NAMES,
    "tags": [
        {"values": ["Raindex", "Bebop", "Uniswap v4", "Unknown Onchain"],
         "text": "wt", "convert": {"unit": "t", "times": True},
         "tip": "Wrapped shares; the broker hedge is in underlying shares."},
        {"values": ["Alpaca"], "text": "t",
         "convert": {"unit": "wt", "times": False},
         "tip": "Underlying shares at Alpaca."},
    ]}}
# Trades and Rebalances load their newest LOG_FIRST_ENTRIES log entries,
# enough rows for a tall screen once grouped by id, then their newest
# LOG_FULL_ENTRIES (board-progressive.js).
LOG_FIRST_ENTRIES = 100
LOG_FULL_ENTRIES = 500

# ==========================================================================
# Tab 1: Dashboard: the inventory as Grafana tables (USD at
# Alpaca, USD on each chain, equities) on the left, Trades / Rebalances
# stacked right.
# ==========================================================================
# The tables that shrink on a narrow window. The left column's share one
# factor, so their ratio bars stay lined up (RATIO_LEFT_PX).
# On a phone, or a window too narrow for every column (board-scale.js), each
# table is compact: it hides its secondary columns and fits the rest:
# compactFixedPx is the widest table's remaining fixed columns
# (check_scale_groups), with a narrower stretching column. Trades and
# Rebalances also drop the seconds and "UTC" from the time, so a 402px
# iPhone fits them at about 12px text.
COMPACT_TIME_COLUMNS = {
    "Last updated": {"unit": "time:MMM D HH:mm", "custom.width": 100},
}
COMPACT_STATUS_MIN_PX = 90
SCALE_GROUPS = [
    {"id": "inventory",
     "panels": [USD_BROKERS_TITLE, USD_ONCHAIN_TITLE, EQUITIES_TITLE],
     "fixedPx": RATIO_LEFT_PX, "ratioMinPx": RATIO_MIN_PX,
     "cellFontPx": INVENTORY_CELL_FONT_PX,
     "compactHide": {
         USD_BROKERS_TITLE: ["Reserve", "Rebalanceable", "In flight"],
         USD_ONCHAIN_TITLE: ["In flight", "Target", "±"],
         EQUITIES_TITLE: ["Inflight", "Total", "Total USD", "tStock",
                          "wtStock"],
     },
     "compactFixedPx": 378, "compactRatioMinPx": 120},
    {"id": "trades", "panels": [TRADES_TITLE],
     "fixedPx": TRADES_FIXED_PX, "ratioMinPx": STATUS_MIN_PX + TABLE_SLACK_PX,
     "stretch": "Status", "stretchMinPx": STATUS_MIN_PX,
     "compactHide": {TRADES_TITLE: ["Venue", "Side", "Size"]},
     "compactColumns": {**COMPACT_TIME_COLUMNS, "Asset": {"custom.width": 56},
                      "USD": {"custom.width": 68}},
     "compactFixedPx": 50 + 100 + 56 + 68,
     "compactRatioMinPx": COMPACT_STATUS_MIN_PX + TABLE_SLACK_PX},
    {"id": "rebalances", "panels": [REBALANCES_TITLE],
     "fixedPx": REBALANCES_FIXED_PX,
     "ratioMinPx": STATUS_MIN_PX + TABLE_SLACK_PX,
     "stretch": "Status", "stretchMinPx": STATUS_MIN_PX,
     "compactHide": {REBALANCES_TITLE: ["Asset", "Amount", "USD"]},
     "compactColumns": COMPACT_TIME_COLUMNS,
     "compactFixedPx": 50 + 100 + 135,
     "compactRatioMinPx": COMPACT_STATUS_MIN_PX + TABLE_SLACK_PX},
]
panels = []
panels += pills(0, w=23, fit_window=True, locked_sort=(EQUITIES_TITLE,),
                fit_columns={"fit": [USD_BROKERS_TITLE, USD_ONCHAIN_TITLE],
                             "fill": EQUITIES_TITLE,
                             "split": [TRADES_TITLE, REBALANCES_TITLE]},
                header_tips=HEADER_TIPS,
                tree_sorts={EQUITIES_TITLE: {"rowField": EQUITY_ROW_FIELD,
                                             "keys": EQUITY_SORT_KEYS}},
                table_totals=TABLE_TOTALS,
                row_tips=ROW_TIPS,
                scale_tables=SCALE_GROUPS,
                full_entries={TRADES_TITLE: LOG_FULL_ENTRIES,
                              REBALANCES_TITLE: LOG_FULL_ENTRIES},
                unit_tags=UNIT_TAGS)

def native_table(title, desc, expr, row_label, columns, w, h, x, y,
                 first_col, sort_by=(), widths=None, bars=(),
                 align="right", stretch=None, first_col_mappings=None,
                 first_col_html=False, first_col_align="left",
                 presort=(), drop=(), no_value=None):
    """A Grafana table from one or-chain query pivoted on (row_label, col),
    like matrix_table. columns: [(col, display name, [field override
    properties])]; bars: columns drawn as HTML bars (see bar_cell);
    sort_by: the default sort, a list of {displayName, desc}, first key
    first. presort: the row order before display, [(col, desc)], first key
    first, which can use a `drop` column: Grafana 13.1.0's table draws a
    column it is told to hide, so a sort key the card does not show is
    sorted on here and then dropped. no_value: the text of an empty cell.
    """
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
            # sortBy sorts stably, so the last key applied leads.
            *[{"id": "sortBy", "options": {"fields": {}, "sort": [
                {"field": matrix_key if col == row_label else col,
                 "desc": desc}]}}
              for col, desc in reversed(presort)],
            {"id": "organize", "options": {
                "excludeByName": {col: True for col in drop},
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
                         **({"noValue": no_value} if no_value else {}),
                         "custom": {"align": align, "filterable": True,
                                    "cellOptions": {"type": "auto"}},
                         "thresholds": {"mode": "absolute", "steps": [
                             {"color": "red", "value": None}]},
                         "mappings": []},
            "overrides": [
                {"matcher": {"id": "byName", "options": first_col},
                 "properties": [{"id": "custom.align", "value": first_col_align},
                                {"id": "custom.width",
                                 "value": widths.get(row_label, 90)},
                                *([{"id": "mappings",
                                    "value": first_col_mappings}]
                                  if first_col_mappings else []),
                                *([{"id": "custom.cellOptions",
                                    "value": {"type": "markdown"}}]
                                  if first_col_html else [])]},
                *overrides,
                *stretch_props,
            ],
        },
        "options": {"showHeader": True, "cellHeight": "md",
                    **({"sortBy": list(sort_by)} if sort_by else {})},
        **LATEST_ONLY_HIDDEN,
    }



# A ratio outside [0, 1], from a negative balance say, is no share a bar can
# draw: every bar encoding sends it to OUT_OF_RANGE instead. That value
# matches none of the bar cell's patterns, so the cell writes "out of range"
# for it (see bar_cell).
OUT_OF_RANGE = 2000


def in_range(ratio, encoded):
    """`encoded` where `ratio` is in [0, 1], else OUT_OF_RANGE. A NaN ratio
    stays NaN, which the bar cell leaves empty. The sentinel is built with
    clamp, not `* 0`: clamp maps +Inf and -Inf (a share of a zero total) to
    0 and keeps NaN, where `Inf * 0` is NaN and would show as no vault."""
    bounded = f"((({ratio}) >= 0) <= 1)"
    return (f"((({encoded}) and {bounded}) "
            f"or ((clamp(({ratio}), 0, 0) + {OUT_OF_RANGE}) "
            f"unless {bounded}))")


# A table cell can only style its own value, so a judged bar's value carries
# everything the bar draws: the percent and its band's two edges, each in
# tenths of a percent as four digits, behind a leading digit for the bar's
# kind that also keeps their zeros (K PPPP LLLL HHHH), and the verdict as
# the sign, negative outside the band. Every term is a whole number under
# 2^53, so PromQL keeps each digit. An edge past 0% or 100% draws its tick
# at the bar's end. A bar with no single band (an asset on several chains)
# writes NO_EDGE for both edges and draws no ticks.
BAND_BAR_BASE = 10 ** 12
NO_EDGE = 9999
# The bar's kind: a ratio of its own (a USD row, or an asset on one chain),
# the sum of the chain rows under it (an asset on several chains), or one
# chain's part of its asset (a chain row).
BAR_WHOLE, BAR_SUM, BAR_PART = 1, 2, 3


def band_bar(ratio, low, high, inside, kind=BAR_WHOLE):
    """`ratio` for a bar cell, with its band [low, high], or no ticks when
    both are None, `inside`, a 0/1 PromQL bool, as the verdict, and the
    bar's kind. All the expressions carry the same labels."""
    tenths = lambda fraction: (NO_EDGE if fraction is None else
                               f"round(1000 * clamp(({fraction}), 0, 1))")
    return in_range(ratio, f"({kind * BAND_BAR_BASE} + {tenths(ratio)} * 1e8 + "
                           f"{tenths(low)} * 1e4 + {tenths(high)}) "
                           f"* (2 * ({inside}) - 1)")


# Added to a percent the board has no band for, so the bar cell draws it
# grey. A percent is at most 100, so the value always has four integer digits.
UNJUDGED_OFFSET = 1000


def unjudged_pct(expr):
    """A percent with no band: a grey bar."""
    return in_range(expr, f"round(100 * ({expr}), 0.1) + {UNJUDGED_OFFSET}")


# A bar's percent sits on a dark backing, so a band tick behind it never
# runs through the digits on a narrow bar.
PCT_OPEN = ('<span style="background:rgba(17,18,23,0.6);padding:0 3px;'
            'border-radius:3px">')


def bar_html(fill, text, track="rgba(128,128,128,0.15)"):
    # One div: Grafana's sanitizer drops position, top, left and
    # line-height, so the fill is a hard-stop gradient behind the text.
    return ('<div style="height:20px;border-radius:4px;font-size:10px;'
            f'font-weight:600;text-align:center;background:linear-gradient('
            f'90deg, {fill} '
            f'{text}%, {track} {text}%)">{PCT_OPEN}{text}%</span></div>')


def band_bar_html(fill, track, pct, low=None, high=None, kind=BAR_WHOLE):
    """A judged bar: the fill as wide as the percent, the percent in the
    middle, and a tick at each band edge. The edges are tenths of a percent
    (band_bar), so a tick sits at a tenth of that in percent. The track is
    tinted too, so a ratio near 0% still reads red or green. A sum writes
    "Σ" before its percent; a part is a slimmer bar, its ticks where its
    asset's are."""
    tick = lambda at: (f"transparent calc({at}% / 10 - 1px), currentColor "
                       f"calc({at}% / 10 - 1px), currentColor "
                       f"calc({at}% / 10 + 1px), transparent "
                       f"calc({at}% / 10 + 1px)")
    ticks = ("" if low is None else
             f"linear-gradient(90deg, {tick(low)}, {tick(high)}), ")
    height = 13 if kind == BAR_PART else 20
    sign = "Σ " if kind == BAR_SUM else ""
    return (f'<div style="height:{height}px;border-radius:4px;font-size:10px;'
            f'font-weight:600;text-align:center;background:{ticks}'
            'linear-gradient(90deg, '
            f'{fill} {pct}%, {track} {pct}%)">{PCT_OPEN}{sign}{pct}%</span></div>')


# The fills are opaque, the colour the old 60% fill showed on the dark
# panel, so a venue row's tint (MARK_ROW_TINTS) shows only through the
# translucent track and never changes the verdict's red or green.
RED_FILL, RED_TRACK = "rgb(153,52,53)", "rgba(239,68,68,0.22)"
GREEN_FILL, GREEN_TRACK = "rgb(30,129,69)", "rgba(34,197,94,0.22)"


def pct_field(whole):
    """The pattern for band_bar's four-digit percent, in tenths of a
    percent, and the percent it writes from groups 1 to 3: no leading
    zeros, and no ".0" when `whole`. A percent over 100 matches neither."""
    if whole:
        return r"(?:00(\d)|0([1-9]\d)|(100))0", "$1$2$3"
    return r"(?:00(\d)|0([1-9]\d))([1-9])", "$1$2.$3"


def band_bar_mappings(start):
    """One regex mapping per kind, verdict, percent written whole or with a
    decimal (a mapping writes text, it cannot leave out a decimal), and bar
    with or without ticks. Each edge is 0000 to 1000, its tenths of a
    percent as groups 4 and 5, or NO_EDGE."""
    edge = r"(0\d{3}|1000)"
    mappings = []
    for kind in (BAR_WHOLE, BAR_SUM, BAR_PART):
        for sign, fill, track in (("-", RED_FILL, RED_TRACK),
                                  ("", GREEN_FILL, GREEN_TRACK)):
            for whole in (True, False):
                pct_pattern, pct = pct_field(whole)
                for edges, html in (
                        (edge * 2,
                         band_bar_html(fill, track, pct, "$4", "$5", kind)),
                        (f"{NO_EDGE}" * 2,
                         band_bar_html(fill, track, pct, kind=kind))):
                    mappings.append({"type": "regex", "options": {
                        "pattern": f"^{sign}{kind}{pct_pattern}{edges}$",
                        "result": {"text": html,
                                   "index": start + len(mappings)}}})
    return mappings


# A percent drawn as the custom card's bar: the fill as wide as the percent
# and the percent written inside it. Grafana's gauge cell puts the number
# beside its bar, so this is a Markdown + HTML cell: the value turns into a
# string and a regex mapping writes the HTML from its capture groups. A
# band_bar value is red or green with its band's ticks; an unjudged_pct value
# (1000 to 1100) is grey, its percent read after the offset. The patterns
# take percents from 0 to 100 only, so OUT_OF_RANGE, or any value a broken
# encoding makes, is written "out of range", never as a coloured bar.
GREY_FILL = "rgb(80,88,100)"
UNJUDGED_PATTERN = r"^(?=1(?:0\d\d|100)(?!\d))10*(\d+(?:\.[1-9])?).*$"
OUT_OF_RANGE_HTML = ('<div style="height:20px;border-radius:4px;'
                     f'text-align:center;background:{GREY_FILL}">'
                     'out of range</div>')


def bar_cell():
    # No value (a chain without a vault) shows nothing.
    mappings = [{"type": "regex", "options": {"pattern": "^(null|NaN)?$",
                 "result": {"text": " ", "index": 0}}}]
    mappings.append({"type": "regex", "options": {
        "pattern": f"^{OUT_OF_RANGE}$",
        "result": {"text": OUT_OF_RANGE_HTML, "index": len(mappings)}}})
    mappings.append({"type": "regex", "options": {
        "pattern": UNJUDGED_PATTERN, "result": {
            "text": bar_html(GREY_FILL, "$1"), "index": len(mappings)}}})
    mappings += band_bar_mappings(len(mappings))
    mappings.append({"type": "regex", "options": {"pattern": "^.*$", "result": {
        "text": OUT_OF_RANGE_HTML, "index": len(mappings)}}})
    # The cell is an encoded string, so sorting or filtering it would order
    # the encodings, not the percents.
    return [{"id": "mappings", "value": mappings},
            {"id": "custom.cellOptions", "value": {"type": "markdown"}},
            {"id": "custom.align", "value": "center"},
            {"id": "custom.filterable", "value": False}]


# A chain row's counter trading, rebalancing and extended hours, packed into
# one hidden number, CT x 100 + Rebal x 10 + Ext, each digit never 0 so the
# number keeps its leading digits. The symbol's hover reads it (ROW_TIPS).
# Trading and rebalancing are the chain's own config; extended hours is the
# asset's, and applies only where the chain counter trades.
CHAIN_TRADING = "max by (symbol, chain) (liq_asset_chain_trading)"
CHAIN_REBALANCING = "max by (symbol, chain) (liq_asset_chain_rebalancing)"
CHAIN_FLAGS_EXPR = (
    # CT: 2 on, 1 off.
    f"((({CHAIN_TRADING}) > bool 0) + 1) * 100 + "
    # Rebal: 2 enabled, 4 paused, 1 disabled.
    f"(2 * (({CHAIN_REBALANCING}) == bool 1) "
    f"+ 4 * (({CHAIN_REBALANCING}) == bool 0.5) "
    f"+ (({CHAIN_REBALANCING}) == bool 0)) * 10 + "
    # Ext: 2 on, 1 off, 3 not counter trading.
    f"(3 * (({CHAIN_TRADING}) == bool 0) + (({CHAIN_TRADING}) == bool 1) "
    f"+ 2 * (({CHAIN_TRADING}) == bool 2))")


# An empty cell of a USD table: the bot tracks nothing there (no wallet read
# on the chain, no target, no vault), which a 0 would misstate.
NOT_TRACKED = "—"

# A field kept in the frame but not drawn as a column, so a cell tooltip or
# a sort can read it. Overrides only apply option paths the table registers:
# custom.hideFrom.viz, not the custom.hideFrom object.
HIDDEN_FIELD = {"id": "custom.hideFrom.viz", "value": True}


def usd_tooltip(field):
    """Shows `field`, a USD amount, when the pointer is on the cell."""
    return {"id": "custom.tooltip.field", "value": field}


# Unpriced symbols arrive as NaN (see native_inventory) and read as a dash;
# a chain row has no value (null) and stays empty.
NO_PRICE = {"id": "mappings", "value": [{"type": "special", "options": {
    "match": "nan", "result": {"text": "—", "color": "#6b7280",
                               "index": 0}}}]}

# Each venue as a small mark in its brand colour, its name on hover (the
# cell sanitizer keeps title on img; board-tables.js shows it at once). An
# inline SVG data URI, so the board loads nothing from elsewhere. A venue
# without a mark keeps its name.
MARK_SVGS = {
    # Alpaca's logo: a white alpaca, head left, on a yellow circle, its neck
    # running off the circle's edge (clipped to it).
    "alpaca": ('<defs><clipPath id="c"><circle cx="8" cy="8" r="8"/></clipPath>'
               '</defs><circle cx="8" cy="8" r="8" fill="#FCD400"/>'
               '<g clip-path="url(#c)"><path d="M6.31 16.2V7.66C5.9 7.7 5.4 7.6 '
               '5 7.35 4.6 7.05 4.4 6.6 4.38 6.1L4.55 5.95C4.95 5.6 5.35 5.05 5.75 '
               '4.55 6.1 4.15 6.6 3.95 7.15 3.85L7.05 2.8C7.4 2.8 7.75 3.1 8 3.6 '
               '8.05 3.2 8.08 2.85 8.15 2.55 8.55 2.6 8.9 2.85 9.05 3.25L9.2 4.05'
               'C9.95 4.45 10.45 5.25 10.56 6.2L10.8 15.6V16.2Z" fill="#fff"/>'
               '<path d="M7.08 2.85C7.3 3 7.45 3.3 7.45 3.7 7.25 3.5 7.1 3.2 7.08 '
               '2.85Z" fill="#d0d0d0"/><path d="M8.18 2.6C8.3 2.9 8.32 3.3 8.22 '
               '3.7 8.1 3.35 8.1 2.95 8.18 2.6Z" fill="#d0d0d0"/><path d="M6.35 '
               '5.55H6.95C6.85 5.7 6.65 5.78 6.4 5.78 6.3 5.78 6.3 5.62 6.35 5.55Z" '
               'fill="#c8c8c8"/></g>'),
    "base": ('<circle cx="8" cy="8" r="8" fill="#0052FF"/>'
             '<rect x="0" y="6.8" width="9.4" height="2.4" fill="#fff"/>'),
    # Robinhood's three-part feather, drawn on a 1932px square and scaled
    # to the mark's 16.
    "robinhood": ('<circle cx="8" cy="8" r="8" fill="#CCFF00"/>'
                  '<g transform="scale(0.0082816)" fill="#000">'
                  '<path d="M927 597H1174Q1208 597 1208 632V962Q1208 975 1216 965'
                  'L1375 708Q1405 668 1407 580Q1409 470 1366 432Q1335 405 1240 403'
                  'Q1120 403 1072 443Q1000 505 923 587Q917 597 927 597Z"/>'
                  '<path d="M879 648H1103Q1112 650 1106 658C880 905 715 1170 606 1484'
                  'Q602 1491 593 1491H568Q561 1491 564 1483Q600 1330 672 1154V950'
                  'Q672 890 703 853L860 657Q868 648 879 648Z"/>'
                  '<path d="M1147 705Q1156 698 1156 710V983Q1156 994 1150 1003'
                  'L1052 1162Q1035 1188 1002 1198L772 1270Q762 1272 765 1262'
                  'C880 1040 1000 870 1147 705Z"/></g>'),
    "ethereum": ('<circle cx="8" cy="8" r="8" fill="#627EEA"/>'
                 '<path d="M8 2.2 4.6 8.1 8 10.1l3.4-2z" fill="#fff"/>'
                 '<path d="M8 10.8 4.6 8.8 8 13.8l3.4-5z" fill="#fff" '
                 'fill-opacity=".75"/>'),
    "hyperevm": ('<circle cx="8" cy="8" r="8" fill="#97FCE4"/>'
                 '<path d="M4 8c1.3-2.6 2.7-2.6 4 0s2.7 2.6 4 0" '
                 'stroke="#072723" stroke-width="1.6" fill="none" '
                 'stroke-linecap="round"/>'),
}


# A mark's size in the USD tables, one venue a row, and beside an Equities
# chain row's indent.
USD_MARK_PX, EQUITY_MARK_PX = 24, 18


def mark_icon(key, title, size=EQUITY_MARK_PX, tip=True):
    svg = MARK_SVGS.get(key)
    if svg is None:
        return title
    data = base64.b64encode(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16">{svg}</svg>'
        .encode()).decode()
    title_attr = f'title="{title}" ' if tip else ""
    return (f'<img src="data:image/svg+xml;base64,{data}" width="{size}" '
            f'height="{size}" {title_attr}alt="{title}" '
            'style="vertical-align:middle">')

def row_tip(key, html):
    """`html` with its row's flags on hover: board-tables.js reads the
    row:<key> title (ROW_TIPS). The sanitizer keeps title on abbr."""
    return (f'<abbr title="{ROW_TIP_PREFIX}{key}" '
            'style="cursor:help;text-decoration:none;border:0">'
            f'{html}</abbr>')


# RATIO_LEFT_PX leaves Ratio a few pixels over its 60px minimum on the
# reference 1934px viewport, where the card is about 854px wide inside, so
# the number columns have room for big amounts.
RATIO_LEFT_PX_MAX = 854 - 60
# A balance column filtered by its distinct values filters nothing useful,
# and its filter icon costs the header about 20px.
NOT_FILTERABLE = {"id": "custom.filterable", "value": False}
# A column that is zero nearly always (shares or USDC in flight, wallet
# tokens) shows its zeros as a middle dot, quieter than a dash, so the few
# real amounts stand out. A range, so a float's -0 or dust left by a sum
# reads as zero too.
ZERO_DASH = {"id": "mappings", "value": [{"type": "range", "options": {
    "from": -1e-9, "to": 1e-9, "result": {"text": "\u00b7", "index": 0}}}]}


# Exposure in whole cents (see native_inventory), drawn as dollars with its
# sign in front, bold and larger, and red beyond EXPOSURE_ALERT_CENTS either
# way. A value mapping can only read a string and write text, so a regex
# mapping per magnitude splits the cents into dollars, thousands and cents,
# and the magnitude decides the colour: 501 cents and up is red. An unpriced
# symbol (NaN) reads as a dash; a chain row (null) stays empty. The dollar
# sign is &#36;: before $1, a literal one would be the replace's $$ escape.
EXPOSURE_ALERT_CENTS = 500
EXPOSURE_RED = "#f2495c"


def exposure_html(sign, amount, red):
    mark = {"": "+", "-": "\u2212"}[sign]
    colour = f"color:{EXPOSURE_RED};" if red else ""
    # A flex box, so the sign never wraps away from its amount (the
    # sanitizer drops white-space).
    return (f'<span style="display:inline-flex;align-items:baseline;'
            f'{colour}font-weight:600">'
            f'<b style="font-size:14px">{mark}</b>&#36;{amount}</span>')


def exposure_mappings():
    # (pattern for the cents after the sign, amount text, red). Three digits
    # are one dollar digit and two cent digits, where 501 starts the red.
    magnitudes = [
        (r"(\d)", "0.0$1", False),
        (r"([1-9]\d)", "0.$1", False),
        (r"([1-4])(\d\d)", "$1.$2", False),
        (r"(5)(00)", "$1.$2", False),
        (r"(5)(0[1-9]|[1-9]\d)", "$1.$2", True),
        (r"([6-9])(\d\d)", "$1.$2", True),
        (r"([1-9]\d{1,2})(\d\d)", "$1.$2", True),
        (r"([1-9]\d{0,2})(\d{3})(\d\d)", "$1,$2.$3", True),
        (r"([1-9]\d{0,2})(\d{3})(\d{3})(\d\d)", "$1,$2,$3.$4", True),
    ]
    mappings = [
        {"type": "regex", "options": {"pattern": "^null$",
                                      "result": {"text": " "}}},
        {"type": "regex", "options": {"pattern": "^NaN$", "result": {
            "text": '<span style="color:#6b7280">\u2014</span>'}}},
        {"type": "regex", "options": {"pattern": "^-?0$",
                                      "result": {"text": "&#36;0.00"}}},
    ]
    for sign in ("", "-"):
        for pattern, amount, red in magnitudes:
            mappings.append({"type": "regex", "options": {
                "pattern": f"^{sign}{pattern}$",
                "result": {"text": exposure_html(sign, amount, red)}}})
    for index, mapping in enumerate(mappings):
        mapping["options"]["result"]["index"] = index
    return mappings


EXPOSURE_CELL = [{"id": "mappings", "value": exposure_mappings()},
                 {"id": "custom.cellOptions", "value": {"type": "markdown"}},
                 {"id": "custom.align", "value": "right"}, NOT_FILTERABLE]


def native_inventory(w, x, y, heights):
    """The Inventory card as three Grafana tables: USD at Alpaca, USD on each
    chain, and the equities."""
    alpaca_h, chains_h, equity_h = heights
    usd = lambda metric, row: (f'label_replace({metric}, "venue", "{row}", '
                               '"", "")')
    # One row per chain from the bot's per-chain series, else (a bot build
    # from before them) the single Base row from the unlabelled ones. The
    # fallback is for the whole table, not per row: while the bot publishes
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
    low, high = f"(({target}) - ({deviation}))", f"(({target}) + ({deviation}))"
    inside = (f"(abs(({ratio}) - ({target})) <= bool ({deviation}))")
    # Every chain with a band is judged against it, whether or not USDC
    # rebalancing runs on its corridor: the band is the range the chain is
    # meant to stay in. A chain without a corridor has no band and no ratio.
    chain_ratio = band_bar(ratio, low, high, inside)
    # Alpaca's share of the USD total has no band of its own: each chain's
    # ratio is vault / (vault + Alpaca cash). It is green while every chain
    # is inside its band. Its ticks are the shares at which Alpaca's cash,
    # with the chain vaults where they are, would put a chain outside its
    # band: a chain leaves through its top edge once Alpaca's cash falls to
    # vault x (1 - high) / high, and through its bottom edge once it rises
    # to vault x (1 - low) / low. The tightest chain sets each tick. With no
    # band on any chain, the bar is grey.
    vault = per_chain("liq_usdc_chain_available", "liq_usdc_onchain_available")
    cash_floor = (f"max(({vault}) * (1 - ({high})) "
                  f"/ clamp_min(({high}), 0.000001))")
    cash_ceiling = (f"min(({vault}) * (1 - ({low})) "
                    f"/ clamp_min(({low}), 0.000001))")
    others = "(max(liq_usdc_total) - max(liq_usdc_alpaca_total))"
    alpaca_share = "(max(liq_usdc_alpaca_total) / max(liq_usdc_total))"
    alpaca_bar = (f"({band_bar(alpaca_share, f'({cash_floor}) / (({cash_floor}) + {others})', f'({cash_ceiling}) / (({cash_ceiling}) + {others})', f'min({inside})')}) "
                  f"or ({unjudged_pct(alpaca_share)})")
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
        (usd(alpaca_bar, "Alpaca"), "share"),
    ], row_label="venue")
    chains = or_chain([
        (per_chain("liq_usdc_chain_available", "liq_usdc_onchain_available"),
         "vault"),
        (per_chain("liq_usdc_chain_inflight", "liq_usdc_onchain_inflight"),
         "inflight"),
        ('max by (venue) (label_replace(liq_usdc_inflight_base_wallet, '
         '"venue", "base", "", "")) or max by (venue) (label_replace('
         'liq_usdc_inflight_ethereum_wallet, "venue", "ethereum", "", ""))',
         "wallet"),
        (chain_ratio, "ratio"),
        (target, "target"),
        (deviation, "deviation"),
    ], row_label="venue")
    # One row per asset (row = the symbol) and, under it, a row for Alpaca
    # and one per chain the asset has a vault on (row = symbol/alpaca,
    # symbol/chain). Rows sort by that key, so a chain row follows its asset.
    # Everything comes from the inventory snapshots, so the rows are there
    # whether or not the rebalancer can size the asset. The bot values each
    # chain's vault in underlying shares (its wrapped shares through the
    # wrapper ratio, plus in flight), so the chains and Alpaca add up, and
    # publishes each venue's share of the asset (liq_equity_*_held_*).
    asset = lambda expr: (f'label_replace({expr}, "row", "$1", "symbol", '
                          '"(.*)")')
    chain = lambda expr: f'label_join({expr}, "row", "/", "symbol", "chain")'
    # Alpaca's row under each asset, beside its chains: key symbol/alpaca.
    broker = lambda expr: (f'label_replace({expr}, "row", "$1/alpaca", '
                           '"symbol", "(.*)")')
    chain_available = "max by (symbol, chain) (liq_equity_chain_available)"
    chain_inflight = "max by (symbol, chain) (liq_equity_chain_inflight)"
    wrapper_ratio = ("(max by (symbol, chain) (liq_equity_chain_wrapper_ratio) "
                     f"or {chain_available} * 0 + 1)")
    broker_shares = "max by (symbol) (liq_equity_offchain_available)"
    # Alpaca's shares in flight, 0 until the bot lists them apart.
    broker_inflight = ("(max by (symbol) (liq_equity_offchain_inflight) "
                       f"or {broker_shares} * 0)")
    # The asset's every chain and Alpaca, in underlying shares; the bot's own
    # total until a vault is read.
    asset_total = ("(max by (symbol) (liq_equity_held_total) "
                   "or max by (symbol) (liq_equity_total))")
    chain_share = "max by (symbol, chain) (liq_equity_chain_held_share)"
    # Every chain and Alpaca row, whichever column made it (the sort keys).
    chain_rows = ("group by (symbol, chain) (liq_equity_chain_available or "
                  "liq_equity_chain_inflight or liq_equity_chain_unwrapped or "
                  "liq_equity_chain_wrapped or liq_equity_chain_held_share)")
    broker_rows = ("group by (symbol) (liq_equity_offchain_available or "
                   "liq_equity_offchain_inflight or liq_equity_offchain_held_share)")
    broker_share = "max by (symbol) (liq_equity_offchain_held_share)"
    # Each chain's band from the config: its target share and the one
    # deviation. A chain with no target there has no band, so its bar is
    # grey. Alpaca holds what the chains' targets leave, so its band is the
    # rest of them and the same deviation (for one chain, that chain's band
    # mirrored), and its bar is judged by it like a chain's.
    chain_target = "max by (symbol, chain) (liq_settings_equity_chain_target)"
    deviation = "scalar(max(liq_settings_equity_deviation))"
    chain_inside = f"(abs({chain_share} - {chain_target}) <= bool {deviation})"
    chain_bar = (f"({band_bar(chain_share, f'{chain_target} - {deviation}', f'{chain_target} + {deviation}', chain_inside, BAR_PART)}) "
                 f"or ({unjudged_pct(chain_share)})")
    broker_target = f"(1 - sum by (symbol) ({chain_target}))"
    broker_inside = f"(abs({broker_share} - {broker_target}) <= bool {deviation})"
    broker_bar = (f"({band_bar(broker_share, f'{broker_target} - {deviation}', f'{broker_target} + {deviation}', broker_inside, BAR_PART)}) "
                  f"or ({unjudged_pct(broker_share)})")
    # Underlying shares at the underlying share's price, and a wrapped token
    # at its chain's ratio of that. Both are the last prices observed, live
    # or not; their age is in the title (TABLE_TOTALS). Before the bot
    # published the last prices, the live wrapped price stands in.
    wrapped_price = ("(max by (symbol) (liq_equity_last_price_usd) "
                     "or max by (symbol) (liq_position_last_price_usd))")
    underlying_price = ("(max by (symbol) (liq_equity_underlying_price_usd) "
                        f"or {wrapped_price})")
    # group_left () before a parenthesised price, which PromQL would read as
    # group_left's label list.
    priced = lambda expr: f"({expr}) * on (symbol) group_left () {underlying_price}"
    priced_wrapped = lambda expr: f"({expr}) * on (symbol) group_left () {wrapped_price}"
    priced_chain_wrapped = lambda expr: priced(f"({expr}) * on (symbol, chain) {wrapper_ratio}")
    # An asset's shares in flight: every chain's and Alpaca's.
    asset_inflight = f"(sum by (symbol) ({chain_inflight}) + on (symbol) {broker_inflight})"
    exposure = ("(max by (symbol) (liq_equity_exposure_usd) "
                "or max by (symbol) (liq_equity_last_exposure_usd))")
    # The wallet tokens of an asset whose chains the bot does not list yet:
    # the Base wallet on the asset row.
    wallet_fallback = lambda form: (
        f"max by (symbol) (liq_equity_{form}) unless on (symbol) "
        f"count by (symbol) (liq_equity_chain_{form})")
    # The asset-level values the sort keys hold, one per symbol.
    sort_values = {
        "sort_total": asset_total,
        "sort_total_usd": f"({priced(asset_total)})",
        EQUITY_DEFAULT_SORT_KEY: f"abs({exposure})",
        "sort_unwrapped": "(sum by (symbol) (liq_equity_chain_unwrapped) or "
                          "max by (symbol) (liq_equity_unwrapped))",
        "sort_wrapped": "(sum by (symbol) (liq_equity_chain_wrapped) or "
                          "max by (symbol) (liq_equity_wrapped))",
        "sort_ratio": f"(1 - {broker_share})",
    }
    equity = or_chain([
        # Hidden, for the symbol's hover (ROW_TIPS): only on the chain rows
        # a vault makes, so a chain configured without a vault read adds no
        # row with no sort keys.
        (chain(f"({CHAIN_FLAGS_EXPR}) and on (symbol, chain) {chain_available}"),
         "flags"),
        # A chain's wrapped vault shares, or Alpaca's shares.
        (f'{chain(chain_available)} or {broker(broker_shares)}', "vault"),
        (f'{asset(asset_inflight)} or {chain(chain_inflight)} or '
         f'{broker(broker_inflight)}',
         "inflight"),
        (asset(asset_total), "total"),
        # Priced symbols only. The NaN fallback keeps the column on the
        # board for unpriced symbols (a dash, see NO_PRICE), where Grafana
        # would drop a column that has no values at all.
        (asset(f"{priced(asset_total)} or {asset_total} * NaN"), "total_usd"),
        # When the price was quoted, for the title's age (TABLE_TOTALS).
        (asset("max by (symbol) (liq_equity_price_observed_ts_seconds)"),
         "price_ts"),
        # The NaN fallback by symbol only, like the exposure: with the
        # total's other labels, `or` keeps both and their sum is NaN.
        (asset(f"round(100 * {exposure}) or (max by (symbol) (liq_equity_total) * NaN)"),
         "exposure"),
        (f'{chain("max by (symbol, chain) (liq_equity_chain_unwrapped)")} or '
         f'{asset(wallet_fallback("unwrapped"))}',
         "unwrapped"),
        (f'{chain("max by (symbol, chain) (liq_equity_chain_wrapped)")} or '
         f'{asset(wallet_fallback("wrapped"))}',
         "wrapped"),
        # The USD each share amount is worth, shown as its cell's tooltip.
        (f'{chain(priced_chain_wrapped(chain_available))} or '
         f'{broker(priced(broker_shares))}',
         "vault_usd"),
        (f'{asset(priced(asset_inflight))} or {chain(priced(chain_inflight))} or '
         f'{broker(priced(broker_inflight))}',
         "inflight_usd"),
        (f'{chain(priced("max by (symbol, chain) (liq_equity_chain_unwrapped)"))} or '
         f'{asset(priced(wallet_fallback("unwrapped")))}',
         "unwrapped_usd"),
        (f'{chain(priced_chain_wrapped("max by (symbol, chain) (liq_equity_chain_wrapped)"))} or '
         f'{asset(priced_wrapped(wallet_fallback("wrapped")))}',
         "wrapped_usd"),
        (f'{chain(chain_bar)} or {broker(broker_bar)}', "ratio"),
        # Each sort key (EQUITY_SORT_KEYS and the default's): the asset's
        # value, which its chain and Alpaca rows take too, so they stay
        # under it. Every row takes it, whichever series made the row: a
        # chain with tokens in flight or in the wallet and nothing in its
        # vault is a row too, and one without the key sorts away from its
        # asset.
        *((f'{asset(sort_values[key])} or {chain(chain_rows + " * 0 + on (symbol) group_left () (" + sort_values[key] + ")")} or '
           f'{broker(f"{broker_rows} * 0 + ({sort_values[key]})")}',
           key) for key in EQUITY_SORT_COLUMNS),
    ], row_label="row")
    pct = [{"id": "unit", "value": "percentunit"}, {"id": "decimals", "value": 0}]
    return [
        native_table(
            USD_BROKERS_TITLE,
            "**USD at the brokers** (Alpaca) and its share of the USD the bot "
            "balances against Base. The title's total is the brokers' cash "
            "and USDC; cash in flight is not in it.\n"
            "\n"
            "- **Share of total**: Alpaca's cash against Alpaca's, Base's "
            "vault, and all cash in flight. Robinhood's vault is not in it.\n"
            "- **Rebalanceable**: max(0, withdrawable − reserve).\n"
            "- **Colour**: green while every chain's USD ratio is inside its "
            "band, red when any is outside. Alpaca has no band of its own.\n"
            "- **Ticks**: the shares at which Alpaca's cash would push a chain "
            "out of its band, with the chain vaults where they are. Grey when "
            "no chain has a band.",
            alpaca, "venue",
            [("cash", "Cash", None), ("usdc", "USDC", None),
             ("reserve", "Reserve", None),
             ("rebalanceable", "Rebalanceable", None),
             ("inflight", "In flight", [ZERO_DASH]),
             ("share", "Share of total", bar_cell())],
            w, alpaca_h, x, y, first_col="Venue", bars=("share",),
            first_col_mappings=[{"type": "value", "options": {
                "Alpaca": {"text": mark_icon("alpaca", "Alpaca", USD_MARK_PX),
                           "index": 0}}}],
            first_col_html=True, first_col_align="center", stretch="share",
            no_value=NOT_TRACKED,
            widths={"venue": 82, "cash": 150, "usdc": 118, "reserve": 110,
                    "rebalanceable": 170, "inflight": 160}),
        native_table(
            USD_ONCHAIN_TITLE,
            "**Each chain's USDC vault** and the ratio the bot rebalances on. "
            "The title's total is the vaults and wallets; USDC in flight is "
            "not in it.\n"
            "\n"
            "- **Ratio**: vault / (vault + Alpaca cash). Not a share of all "
            "USD: other chains are not in it.\n"
            "- **Ticks**: the chain's band, target ± deviation. Green inside, "
            "red outside, whether or not USDC rebalancing runs on that "
            "corridor.\n"
            "- **Ethereum**: the hub wallet Alpaca deposits to and withdraws "
            "from, with no band.\n"
            "- Hover a mark for the chain's name.", chains, "venue",
            [("vault", "Vault", None), ("inflight", "In flight", [ZERO_DASH]),
             ("wallet", "Wallet", None),
             ("target", "Target", pct), ("deviation", "±", pct),
             ("ratio", "Ratio", bar_cell())],
            w, chains_h, x, y + alpaca_h, first_col="Chain",
            first_col_mappings=[{"type": "value", "options": {
                key: {"text": (mark_icon(key, name, USD_MARK_PX)
                               if key != "ethereum" else
                               mark_icon(key, "Ethereum (hub)", USD_MARK_PX)),
                      "index": index}
                for index, (key, name) in enumerate(CHAIN_NAMES.items())}}],
            first_col_html=True, first_col_align="center",
            bars=("ratio",), stretch="ratio", no_value=NOT_TRACKED,
            widths={"venue": 82, "vault": 156, "inflight": 146, "wallet": 140,
                    "target": 136, "deviation": 130},
            ),
        native_table(
            EQUITIES_TITLE,
            "**One row per asset**, and under it a row for Alpaca and one per "
            "chain the asset has a vault on (the slimmer bars). The largest "
            "exposure, long or "
            "short, comes first; click a header to sort by it. The title's "
            "total is Total USD over every asset.\n"
            "\n"
            "**Columns**\n"
            "\n"
            "- **Asset**: hover a symbol for its chains' flags (counter "
            "trading, rebalancing, and extended-hours counter trading), or a "
            "chain's mark for that chain's.\n"
            "- **Amount**: on a chain row, the wrapped shares in that chain's "
            "vault, so chains are not added together; on Alpaca's, its "
            "shares.\n"
            "- **Total, Total USD**: Alpaca and every chain in underlying "
            "shares, with the shares in flight. For an asset the rebalancer "
            "does not size, Alpaca and Base only.\n"
            "- **Exposure**: net × live price, or × the last price when none "
            "is live (the title says how old it is). Red beyond ±$5. Empty "
            "without any price.\n"
            "- **tStock, wtStock**: on a chain row, the unwrapped and wrapped "
            "tokens in the bot's wallet on that chain, outside the vault. "
            "Hover the header.\n"
            "- Hover a share amount for its USD value.\n"
            "\n"
            "**Ratio**\n"
            "\n"
            "Every bar comes from the inventory snapshots: what each venue "
            "holds, a chain's wrapped shares through its wrapper ratio (as the "
            "bot last read it, else 1) and its shares in flight, against the "
            "asset's total in underlying shares.\n"
            "\n"
            "- **Chain row**: its share of the asset. The ticks are its band "
            "from the config (its target ± the deviation): green inside, red "
            "outside, grey without a target.\n"
            "- **Alpaca row**: Alpaca's share. Its ticks are what the chains' "
            "targets leave, ± the deviation: green inside, red outside, grey "
            "when no chain has a target.\n"
            "- **Asset row**: no bar; Ratio sorts the assets by their onchain "
            "share.\n"
            "- **Total USD, Exposure**: at the last observed prices; the title "
            "says when they are older than 10 minutes.", equity, "row",
            [("flags", "Flags", [HIDDEN_FIELD]),
             ("price_ts", "Price time", [HIDDEN_FIELD]),
             ("vault", "Amount", [NOT_FILTERABLE, usd_tooltip("Amount USD")]),
             ("inflight", "Inflight", [NOT_FILTERABLE, ZERO_DASH,
                                       usd_tooltip("Inflight USD")]),
             ("total", "Total", [NOT_FILTERABLE, usd_tooltip("Total USD")]),
             ("total_usd", "Total USD", [
                 {"id": "unit", "value": "currencyUSD"},
                 {"id": "decimals", "value": 0}, NO_PRICE, NOT_FILTERABLE]),
             ("exposure", "Exposure", EXPOSURE_CELL),
             # Left-aligned, except the wallet
             # columns: the tokens in the bot's Base wallet, the asset's
             # unwrapped tStock (tAAPL) and wrapped wtStock (wtAAPL). Their
             # headers explain them on hover (HEADER_TIPS).
             ("unwrapped", "tStock", [
                 {"id": "custom.align", "value": "right"}, NOT_FILTERABLE,
                 ZERO_DASH, usd_tooltip("tStock USD")]),
             ("wrapped", "wtStock", [
                 {"id": "custom.align", "value": "right"}, NOT_FILTERABLE,
                 ZERO_DASH, usd_tooltip("wtStock USD")]),
             # Last, like every ratio on the tab (see RATIO_LEFT_PX).
             ("ratio", "Ratio", bar_cell()),
             *[(f"{col}_usd", f"{name} USD", [{"id": "unit", "value": "currencyUSD"}, {"id": "decimals", "value": 2},
               HIDDEN_FIELD])
               for col, name in (("vault", "Amount"), ("inflight", "Inflight"),
                                 ("unwrapped", "tStock"),
                                 ("wrapped", "wtStock"))],
             *((key, key, None) for key in
               EQUITY_SORT_COLUMNS)],
            w, equity_h, x, y + alpaca_h + chains_h, first_col="Asset",
            # A chain row's key is symbol/chain: shown indented under its
            # asset as the chain's mark. The symbol and each chain's mark
            # show their flags on hover (ROW_TIPS); Alpaca's mark its name.
            first_col_mappings=[
                *({"type": "regex", "options": {
                    "pattern": f"^([^/]+)/{key}$",
                    "result": {"text": "\u00a0\u00a0\u21b3 " + (
                        mark_icon(key, name) if key not in CHAIN_NAMES
                        else row_tip("$1/" + key, mark_icon(key, name, tip=False))),
                               "index": index}}}
                  for index, (key, name) in enumerate(MARK_NAMES.items())),
                {"type": "regex", "options": {
                    "pattern": "^([^/]+)$",
                    "result": {"text": row_tip("$1", "$1"),
                               "index": len(MARK_NAMES)}}}],
            first_col_html=True,
            # Asset's width fits its header's text plus the filter icon
            # (measured). The balance columns have no filter icon, so each
            # fits its header's text (the measured width less the icon) or
            # a balance, Total's 74px, whichever is wider. A column under
            # Grafana's 50px minimum renders at the minimum and would leave
            # the stretching Ratio column too wide by the difference.
            widths={"row": 110, "vault": 116,
                    "inflight": 76, "total": 116, "total_usd": 92,
                    "ratio": 100, "exposure": 112, "unwrapped": 82,
                    "wrapped": 86},
            bars=("ratio", "exposure"), align="left", stretch="ratio",
            presort=[(EQUITY_DEFAULT_SORT_KEY, True), ("row", False)],
            drop=tuple(EQUITY_SORT_COLUMNS)),
    ]


# The inventory tables (5 + 7 + 13 rows) beside Trades stacked on
# Rebalances (15 + 10). The header sizes the USD tables to their rows, gives
# Equities the rest of the window, halves the right column and levels both
# (board-layout.js); without the dashboard scene the board keeps these.
# Change them only after checking a new set on a preview board
# (observability/README.md says why).
TRADES_H, TRANSFERS_H = 15, 10
panels += native_inventory(w=13, x=0, y=1, heights=(5, 7, 13))

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
    """The trade and transfer detail dialogs, opened from a row of the
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
    It loads no event lines: a 30-day Cloud Logging read took as long as
    the slowest table's, every time the board loaded, for a dialog most
    loads never open. A row fetches its own events when its dialog opens
    (row-events.js). The queries do not use
    `$detail`, so a click does not re-run them; the script reads `${detail}`,
    which makes Grafana redraw the panel when the variable changes.
    liquidity-panels/detail.js builds the dialog. renderMode "data" hands
    the script both frames, where "allRows" would draw a frame picker.
    MODE_LABELS (how each recovery command runs) is prepended from
    recovery-guide.json, like the header's guide, the client prefix from
    client-env.js, the per-row command builders from recovery-commands.js,
    the status-history order from status-history.js, the bot-line
    helpers from log-lines.js, and the commands' Copy buttons from
    copy-command.js.
    """
    with open(os.path.join(HERE, "liquidity-panels", "recovery-guide.json")) as f:
        mode_labels = json.load(f)["modeLabels"]
    commands = "".join(panel_module(module) + "\n" for module in (
        "client-env.js", "recovery-commands.js", "status-history.js",
        "log-lines.js", "row-events.js", "copy-command.js"))
    # A row fetches its own events (row-events.js) with the board's event
    # filter.
    row_events = {"datasource": CL, "filter": BOT_EVENTS_FILTER}
    with open(os.path.join(HERE, "liquidity-panels", "detail.js")) as f:
        after_render = (f"const MODE_LABELS = {json.dumps(mode_labels)};\n"
                        f"const ROW_EVENTS = {json.dumps(row_events)};\n\n"
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
    (here Trades and Rebalances).

    The bot writes a line per transfer status change and per terminal
    trade status (again on a venue correction), so the raw feed holds
    several rows per trade or transfer. The plugin returns entries newest
    first, and the line times follow commit order, so groupBy on
    jsonPayload.id with `first` keeps each entity's latest status.

    The id stays as the first column, an ⓘ that opens the row's detail
    dialog (see DETAIL_URL).

    logs: (name, query) on the bot's log, read as refId bot-<name>; the
      detail panel finds its frames by that refId.
    fields: {payload leaf: column name}; the entry's own timestamp is
      always kept, as fields["timestamp"].
    shown: column names in display order.
    body_regex: an extra extractFields over the entry's JSON body, with
      named groups as new columns. Grafana only treats the pattern as a
      regex when it is wrapped in slashes; without them it silently
      returns the whole body as one "NewField" column. A bot line's body
      is its message.
    bot_label_regex: the same columns from the line's labels, as one JSON
      string.
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
        # The board loads a screen's entries first, then the whole history
        # (LOG_FULL_ENTRIES, board-progressive.js): 500 took 0.5s more to
        # draw, and nobody sees the rows past the first screen at once.
        "maxDataPoints": LOG_FIRST_ENTRIES,
        "targets": [cloudlog(logs[1], ref=f"bot-{logs[0]}")],
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
    # px None: the column stretches over the rest of the card, down to
    # STATUS_MIN_PX, not Grafana's 150px default, so a scaled table fits.
    properties = ([{"id": "custom.width", "value": px}] if px
                  else [{"id": "custom.minWidth", "value": STATUS_MIN_PX}])
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
    TRADES_TITLE,
    "**Raindex fills** (direct, or routed through adapters such as "
    "Bebop) and the **Alpaca hedges** that offset them. One row per "
    "trade at its latest status. Click ⓘ for details.\n"
    "\n"
    "- **USD**: shares × fill price.\n"
    "- **Last updated**: when the bot logged the final status. For a "
    "fill it caught up on, that is later than the fill; the dialog "
    "shows the fill time.\n"
    "- Built from the newest 500 status lines, so a busy period can "
    "push older trades out of the 30-day window.",
    ("trades", f'{BOT_LOG} jsonPayload.target="liq_trade"'),
    # One timestamp, first: when the row last changed status.
    fields={"timestamp": "Last updated", "symbol": "Asset", "venue": "Venue",
            "direction": "Side", "shares": "Size", "usd": "USD",
            "status": "Status"},
    shown=["Last updated", "Asset", "Venue", "Side", "Size", "USD", "Status"],
    w=11, h=TRADES_H, x=13, y=1,
    number_columns={"Size": 3, "USD": 2},
    overrides=[
        column("Last updated", 150), column("Asset", 65),
        column("Venue", 80, VENUE_MAPPINGS, color_text=True),
        column("Side", 55, SIDE_MAPPINGS, color_text=True),
        column("Size", 100), usd_column(80),
        column("Status", None, STATUS_CAP_MAPPINGS, color_text=True),
    ],
))

panels.append(latest_status_table(
    REBALANCES_TITLE,
    "**Moves between venues** to rebalance inventory: equity mints "
    "(Alpaca → onchain), redemptions (onchain → Alpaca), and USDC "
    "bridges (CCTP). One row per transfer at its latest status. Click ⓘ"
    " for the full event timeline and the recovery commands.\n"
    "\n"
    "- **USD**: an equity transfer at its mark when the status changed,"
    " or a bridge's amount.\n"
    "- **Status** comes from the bot's status lines, which can lag its "
    "events: the dialog's timeline is the newest.\n"
    "- Built from the newest 500 status lines, so a busy period can "
    "push older transfers out of the 30-day window.",
    ("transfers", f'{BOT_LOG} jsonPayload.target="liq_transfer"'),
    # One timestamp, first: when the row last changed status. The start
    # time is in the row's detail dialog.
    fields={"timestamp": "Last updated", "symbol": "Asset",
            "amount": "Amount", "usd": "USD", "status": "Status"},
    shown=["Last updated", "Type", "Asset", "Amount", "USD", "Status"],
    # Type reads the direction for a USDC bridge and the kind for an equity
    # transfer. A bridge's kind is
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
        column("Last updated", 150), column("Type", 135, TYPE_MAPPINGS),
        # A USDC bridge carries no symbol; the Asset column reads
        # "USDC" for it.
        column("Asset", 65, [{"type": "special", "options": {
            "match": "empty", "result": {"text": "USDC", "index": 0}}}]),
        column("Amount", 88), usd_column(80),
        column("Status", None, STATUS_DOT_MAPPINGS, color_text=True),
    ],
))
panels.append(detail_panel(
    x=23, y=0,
    trades_id=next(p["id"] for p in panels if p.get("title") == "Trades"),
    transfers_id=next(p["id"] for p in panels if p.get("title") == "Rebalances")))


dashboards.append(make_dashboard(
    "t0-liquidity", "Liquidity bot",
    "The liquidity bot's inventory, trades, rebalances, orders, PnL, "
    "performance and logs, fed by the bot's metrics and log. Five linked "
    "boards act as tabs: use the Dashboard/Orders/PnL/Performance/Logs "
    "links above (they carry theme=light so the suite renders in the light "
    "look). "
    "Refresh is 1m polling, not WebSocket-live. On a relative range (the "
    "default now-24h) the header and the inventory show the live state, "
    "the last 5 minutes, and trades and transfers show the last 30 days. "
    "An absolute range shows that range's data in every panel. The board "
    "fills the window height, and scrolls on a short window "
    "or a phone. Full history and drilldowns: the operations client, "
    "`st0x-liquidity-client --env <production|staging> read resource <name>`.",
    panels, tab_links("Dashboard"), [ENV_VAR, DETAIL_VAR]))

# ==========================================================================
# Tab 2: Orders: the Raindex Orders table (+ ops extras).
# ==========================================================================
panels = []
panels += pills(0)
panels.append(cloudlog_table(
    "Raindex Orders",
    "The bot's Raindex orders, one row per order (Output | Input | Vault "
    "Balance | IO Ratio | Order Hash | Created). Shipped by the exporter's "
    "collect_orders() as one Cloud Logging row per order per poll cycle "
    "(logName liquidity-orders); insertId buckets by poll-hour so a "
    "still-active order dedupes to ~1 stored row/hour rather than one per "
    "60s poll. With a 2h window, a couple of rows per order is "
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
    "The bot's Raindex orders and the pending-order and upstream-API "
    "status tiles.",
    panels, tab_links("Orders"), [ENV_VAR]))

# ==========================================================================
# Tab 3: PnL: tiles, per-asset table, the four charts.
# ==========================================================================
# P2: "currency:financial:$" is Grafana's non-abbreviating currency format
# (Intl.NumberFormat under the hood, grouped thousands, forced decimals,
# literal "$" prefix — added in grafana/grafana#106604, present since
# Grafana 13.x). Unlike "currencyUSD" it never switches to K/M/B, so it
# gives tiles like "$-25.77" and "$130,861.46". The
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
        "", f"{desc} (Computed by the bot's replay, refreshed every 5 "
        "minutes.)",
        f'max(liq_pnl_summary_usd{{stream="{stream}",window="$window"}})',
        display=label.upper(), unit=PNL_TILE_USD, decimals=2,
        steps=([{"color": "red", "value": None},
                {"color": "text", "value": -0.01},
                {"color": "green", "value": 0.01}] if signed
               else [{"color": "text", "value": None}]),
        w=4, h=3, x=(index % 5) * 4, y=2 + (index // 5) * 3,
        value_size=22))
panels.append(stat(
    "", "Open replay inventory, in shares.",
    'max(liq_pnl_summary_shares{kind="inventory_drift",window="$window"})',
    display="OPEN INVENTORY (SH)", decimals=3, color_mode="none",
    w=4, h=3, x=20, y=2))
panels.append(stat(
    "", "Total fills in the PnL database sample.",
    'max(liq_pnl_sample_total_fills{window="$window"})', display="TOTAL FILLS", decimals=0,
    color_mode="none", w=4, h=3, x=20, y=5))
panels.append(stat(
    "", "Average deployed capital from daily snapshots, computed by the "
    "bot.",
    'max(liq_pnl_capital_avg_deployed_usd{window="$window"})', display="AVG DEPLOYED CAPITAL",
    unit="currencyUSD", decimals=0, color_mode="none", w=8, h=3, x=0, y=8))
panels.append(stat(
    "", "Annualized return on that capital, computed by the bot.",
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
# P5: the Volume column, in shares rather than USD, so it gets its own
# override rather than joining unit_overrides' PNL_TABLE_USD_SIGNED sweep below. It
# sits right after Net; decimals=4 shows share counts like "20.0723". It counts a matched lot on both legs, so the exporter serves
# 2 x matchedShares (verified against the entries page: their shares sum
# to matchedShares exactly). A USD value for the volume is not shown: it
# needs per-entry open/close prices from the full entries page.
VOLUME_SHARES = [{"id": "unit", "value": "locale"}, {"id": "decimals", "value": 4}]
panels.append(matrix_table(
    "Per Asset PnL",
    "PnL per asset from the closed-lot replay per symbol. Lots is the "
    "matched lot count. Volume is both legs of the matched shares, in "
    "shares.",
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
# P4: fixed per-stream colors: counter_trade sky, onchain_netting green,
# directional_inventory_baseline amber, directional_imbalance_excess rose.
# The map has exactly these four entries because the per-stream charts plot
# only these four components. "directional_exposure" (baseline_drift +
# directional_excess) and "total" (every stream summed) are tile-only
# aggregates, never their own chart line. The exporter's day series
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
# Chart order: the daily bar pair first, the cumulative pair below it. All
# four read the bot's own day buckets (/pnl `windows[]`), so they carry the
# bot's full history for the selected range from the first scrape, and they
# move with the range pill.
panels.append(day_barchart(
    "PnL by Asset per Time Unit",
    "One bar per calendar day, split by asset and stacked, straight from "
    "the bot's daily PnL buckets. A day with no closed lot for an asset is "
    "a zero, not a gap.",
    "liq_pnl_day_usd", "symbol", w=12, h=7, x=0, y=19))
panels.append(day_barchart(
    "PnL by PnL-Stream per Time Unit",
    "The same days split by PnL stream, the four component streams "
    "only, since directional_exposure and total are tile-only aggregates "
    "and stacking them beside their own components double-counts.",
    "liq_pnl_day_stream_usd", "stream", w=12, h=7, x=12, y=19,
    overrides=PNL_STREAM_OVERRIDES))
panels.append(day_barchart(
    "Cumulative PnL by Asset",
    "Running total per asset through the selected range. Unstacked: "
    "each asset is its own independent series, not a stack. The last "
    "day's bars sum to the Total PnL tile above.",
    "liq_pnl_day_cum_usd", "symbol", w=12, h=7, x=0, y=26, stack=False,
    legend_mode="table", legend_placement="right"))
panels.append(day_barchart(
    "Cumulative PnL by PnL-Stream",
    "Running total per stream, unstacked. Grafana has no "
    "categorical-axis line chart, so these are bars over the same day "
    "buckets.",
    "liq_pnl_day_cum_stream_usd", "stream", w=12, h=7, x=12, y=26,
    stack=False, overrides=PNL_STREAM_OVERRIDES,
    legend_mode="table", legend_placement="right"))
dashboards.append(make_dashboard(
    "t0-liquidity-pnl", "Liquidity bot: PnL",
    "PnL tiles, table and charts, all driven by the range pill. For "
    "closed lots and the cost ledger, run "
    "`st0x-liquidity-client --env <production|staging> read resource pnl` (the /pnl report).",
    panels, tab_links("PnL"), [ENV_VAR, WINDOW_VAR]))

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
    "'—' = nothing unhedged.",
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

# Sentinels of the "sampled ago" stat: distinct negative values, since real
# ages are clamped at 0. The order of the query's `or` branches sets which
# state shows, not the values.
LAG_PAUSED = -2
LAG_NEVER_SAMPLED = -1

# F1: the secondary context line per SLO card (for example "p50 118ms · 42
# fills") as a thin strip of small stats directly under the card row —
# chosen over hover descriptions because the pill row above already
# establishes "small transparent value_and_name stats in a compact strip"
# as this dashboard's visual language for secondary context, so this reads
# as an extension of an existing pattern rather than a new one. h=1, one
# extra grid row (y=6..7); every card gets a strip, kept consistent (all
# eleven sub-stats use the same title="" + displayName + value_and_name
# shape — no panel breaks the pattern).
panels.append(stat(
    "", "p50 detection latency.",
    'max(liq_hedge_latency_ms{stage="detection",quantile="p50"})',
    display="p50", unit="ms", decimals=0, color_mode="none",
    w=2, h=1, x=0, y=6, value_size=10))
panels.append(stat(
    "", "Fill sample count backing the detection "
    "latency percentiles.",
    'max(liq_hedge_latency_ms_samples{stage="detection"}) or vector(0)',
    display="fills (24h)", decimals=0, color_mode="none",
    w=3, h=1, x=2, y=6, value_size=10))
panels.append(stat(
    "", "p50 exposure-window latency.",
    'max(liq_hedge_latency_ms{stage="exposure_window",quantile="p50"})',
    display="p50", unit="ms", decimals=0, color_mode="none",
    w=2, h=1, x=5, y=6, value_size=10))
panels.append(stat(
    "", "Hedge sample count backing the exposure-"
    "window percentiles.",
    'max(liq_hedge_latency_ms_samples{stage="exposure_window"}) or vector(0)',
    display="hedges (24h)", decimals=0, color_mode="none",
    w=3, h=1, x=7, y=6, value_size=10))
panels.append(stat(
    "", "WARN-level log lines in 24h.",
    'max(liq_reliability_log_count_24h{level="warning"}) or vector(0)',
    display="warnings (24h)", decimals=0, color_mode="none",
    w=2, h=1, x=10, y=6, value_size=10))
panels.append(stat(
    "", "Money-at-risk lifecycle failure events in "
    "24h (the 'Errors & warnings by module' table breaks these out by "
    "event type).",
    'sum(liq_failure_event_count_24h) or vector(0)',
    display="lifecycle fails", decimals=0, color_mode="none",
    w=2, h=1, x=12, y=6, value_size=10))
panels.append(stat(
    "", "Apalis job queues with a failed or killed "
    "job right now (instantaneous, not windowed). Killed counts as "
    "failed.",
    'count(sum by (job_type) (liq_job_queue{state=~"failed|killed"}) > 0) '
    'or vector(0)',
    display="bad queues", decimals=0, color_mode="none",
    steps=[{"color": "text", "value": None}, {"color": "red", "value": 1}],
    w=1, h=1, x=14, y=6, value_size=10))
panels.append(stat(
    "", "On-chain fills still uncovered by a hedge, "
    "summed across symbols.",
    'sum(liq_open_exposure_fill_count) or vector(0)',
    display="uncovered fills", decimals=0, color_mode="none",
    w=2, h=1, x=15, y=6, value_size=10))
panels.append(stat(
    "", "Symbols with an open (unhedged) exposure. 0 means all fills are "
    "hedged.",
    'count(liq_open_exposure_fill_count > 0) or vector(0)',
    display="symbols exposed", decimals=0, color_mode="none",
    mappings=[{"type": "value", "options": {
        "0": {"text": "all hedged", "color": "green", "index": 0}}}],
    w=3, h=1, x=17, y=6, value_size=10))
panels.append(stat(
    "", "Age of the oldest hedged chain's block-lag "
    "sample, so one stale chain shows behind a fresh one. A chain whose "
    "latest sample has a time but no lag has no cutoff block, and the "
    "stat reads \"ingestion paused\". A chain with "
    "neither series has no checkpointed sample yet and reads \"no "
    "checkpoint yet\". Paused "
    "wins over an age, and an age wins over no checkpoint, so a stale "
    "chain still shows next to a new one. An age over 300 s is orange "
    "(a stale monitor).",
    "min(0 * (liq_block_lag_sampled_ts_seconds "
    f"unless on(chain) liq_block_lag_blocks) - {-LAG_PAUSED}) "
    "or max(clamp_min(time() - (liq_block_lag_sampled_ts_seconds "
    "and on(chain) liq_block_lag_blocks), 0)) "
    "or min(0 * (liq_poll_cycles_24h "
    "unless on(chain) liq_block_lag_sampled_ts_seconds) "
    f"- {-LAG_NEVER_SAMPLED})",
    display="sampled ago", unit="s", decimals=0,
    steps=[{"color": "text", "value": None}, {"color": "orange", "value": 300}],
    mappings=[{"type": "value", "options": {
        str(LAG_PAUSED): {"text": "ingestion paused", "color": "orange",
                          "index": 0},
        str(LAG_NEVER_SAMPLED): {"text": "no checkpoint yet",
                                 "color": "text", "index": 1}}}],
    w=2, h=1, x=20, y=6, value_size=10))
panels.append(stat(
    "", "Order-fill monitor poll ticks skipped "
    "(overrun) in 24h.",
    'max(liq_poll_skipped_ticks_24h) or vector(0)',
    display="skipped ticks", decimals=0, color_mode="none",
    w=2, h=1, x=22, y=6, value_size=10))

panels.append(bargauge(
    "Hedge cycle stages (p95, 24h)",
    "Aggregate hedge cycle waterfall: p95 per stage. For per-cycle "
    "hedge waterfalls, run `st0x-liquidity-client --env <production|staging> read resource "
    "latencies`.",
    'max by (stage) (liq_hedge_latency_ms{quantile="p95"})', "${__field.labels.stage}",
    w=8, h=8, x=0, y=7,
    steps=[{"color": "green", "value": None}, {"color": "yellow", "value": 30000},
           {"color": "red", "value": 120000}]))
panels.append(timeseries(
    "Latency percentiles over time — $stage",
    "p50/p90/p99 of the selected hedge stage, from the exporter's rolling "
    "24h summary scraped once a minute.",
    [promql('max(liq_hedge_latency_ms{stage="$stage",quantile="p50"})', ref="A"),
     promql('max(liq_hedge_latency_ms{stage="$stage",quantile="p90"})', ref="B"),
     promql('max(liq_hedge_latency_ms{stage="$stage",quantile="p99"})', ref="C")],
    w=8, h=8, x=8, y=7, unit="ms",
    overrides=series_names({"A": "p50", "B": "p90", "C": "p99"},
                           colors={"A": "green", "B": "yellow", "C": "red"})))
errors_by_module = matrix_table(
    "Errors & warnings by module (24h)",
    "Error and warning counts per module. The Logs tab has the "
    "lines themselves. Click a module to jump to the Logs board "
    "pre-filtered on it; sparkline cells are "
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
# current time range, environment and source. Added by mutating the returned panel, not by
# changing the shared matrix_table() helper.
errors_by_module["fieldConfig"]["overrides"].append({
    "matcher": {"id": "byName", "options": "Module"},
    "properties": [{"id": "links", "value": [{
        "title": "View in Logs tab",
        "url": "/d/t0-liquidity-logs/?${__url_time_range}"
               "&var-env=${env:percentencode}"
               "&var-target=${__value.text}&theme=light",
        "targetBlank": False,
    }]}],
})
panels.append(errors_by_module)

panels.append(bargauge(
    "USDC rebalance stages (p95, 30d)",
    "Stage timings for USDC rebalances. From the server's stageSummary; "
    "for per-operation rebalance waterfalls, run `st0x-liquidity-client --env <production|staging> "
    "read resource rebalances`.",
    'max by (stage) (liq_rebalance_stage_ms{kind="usdc",quantile="p95"})', "${__field.labels.stage}",
    w=8, h=7, x=0, y=15))
panels.append(bargauge(
    "Equity rebalance stages (p95, 30d)",
    "Stage timings for equity mints/redemptions. For per-operation "
    "rebalance waterfalls, run `st0x-liquidity-client --env <production|staging> read resource "
    "equity-rebalances`.",
    'max by (stage) (liq_rebalance_stage_ms{kind="equity",quantile="p95"})', "${__field.labels.stage}",
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

# F5: the CCTP attestation-time chart under the USDC rebalance
# section. liq_attestation_last_ms is a gauge of the single most-recent
# attestation duration per kind (the exporter only gets the last entry of
# the bot's attestationTrend each poll) — there is no bucketed history to
# backfill, so the trend only accrues going forward from whenever this
# metric started being scraped. Documented in the panel description.
panels.append(timeseries(
    "CCTP attestation time",
    "Duration of the most recent CCTP attestation per rebalance kind, "
    "sampled once per scrape. The bot keeps the full attestationTrend "
    "history; this gauge only has 'last value at "
    "scrape time', so the line only accrues from whenever this panel "
    "started being scraped forward. A short/flat history here does not "
    "mean attestations were fast, it means the metric is young.",
    [promql('max(liq_attestation_last_ms{kind="usdc"})', ref="A"),
     promql('max(liq_attestation_last_ms{kind="equity"})', ref="B")],
    w=24, h=6, x=0, y=22, unit="ms",
    overrides=series_names({"A": "usdc", "B": "equity"},
                           colors={"A": "blue", "B": "purple"})))

panels.append(timeseries(
    "Block lag over time",
    "The ingestion-health chart: how far fill detection trails the chain.",
    [promql("max(liq_block_lag_blocks)")],
    w=8, h=6, x=0, y=28, decimals=0, fill=15,
    overrides=series_names({"A": "blocks behind"})))
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
    "max by (quantile) (liq_poll_duration_ms)", "${__field.labels.quantile}",
    w=7, h=6, x=17, y=28))
panels.append(matrix_table(
    "Job queues",
    "The bot's apalis job queues (instantaneous whole-table counts, not "
    "windowed). Killed or failed above zero is critical.",
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
    "SLO cards, stage timings and health tables, plus the pre-exporter "
    "native bot metrics as collapsed rows at the bottom.",
    panels, tab_links("Performance"), [ENV_VAR, STAGE_VAR]))

# ==========================================================================
# Tab 5: Logs.
# ==========================================================================
panels = []
panels += pills(0)
panels.append(cloudlog_table(
    "Log History",
    "The bot's structured log lines. "
    "Filter with the level/category/target/search variables above. ⚠️ "
    "Only the newest matching entries, about the panel's pixel width of "
    "them. Narrow the filters, or for deep digs run "
    "`st0x-liquidity-client --env <production|staging> read resource logs` or use the Grafana Logs "
    "export. DEBUG/TRACE are not shown.",
    # The level filter also keeps out the bot's DEBUG and TRACE lines.
    f'{BOT_LOG} jsonPayload.level=~"^(${{level:pipe}})$" '
    'jsonPayload.target=~"${target}" '
    'jsonPayload.target=~"^(${category:pipe})$" '
    'jsonPayload.message=~"${search}"',
    # The message is the log-lines frame's `body`; level/target are
    # leaves in `labels` like every other payload field.
    columns=[("jsonPayload.level", "Level"), ("jsonPayload.target", "Target"),
             ("body", "Message")],
    name="logs",
    w=24, h=19, x=0, y=2,
    widths={"Time": 170, "Level": 70, "Target": 220},
    mappings_by_field={"Level": [
        {"type": "value", "options": {
            "ERROR": {"text": "ERROR", "color": "red", "index": 0},
            "WARN": {"text": "WARN", "color": "yellow", "index": 1},
            "INFO": {"text": "INFO", "color": "blue", "index": 2}}},
    ]},
))
# L2: ms-precision timestamps, like "Aug 18, 13:27:21.965
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
# worse than a dynamic key=value suffix.
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
# L1(a): the Category multi-select. The bot's own backend matches
# these as an EXACT target-field value (`"target":"<cat>"` literal
# substring on the raw log line — see st0x.liquidity src/api.rs), not a
# crate/module prefix, so this is anchored (^...$) rather than a bare
# substring — an unanchored pipe would also swallow crate-path targets
# like "st0x_hedge::portfolio_snapshot::write" (which contains "hedge")
# and diverge from the bot's own filter semantics.
CATEGORY_VAR = {
    "name": "category", "type": "custom", "label": "Category",
    "description": "The log Category filter: exact-match domain "
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
    # vanish from the default view even though the bot's own unfiltered
    # view (categories ∪ crates) still surfaces them. Verified: pipe-of-11 as
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
# L1(b): the free-text search box, matched against jsonPayload.message.
SEARCH_VAR = {
    "name": "search", "type": "textbox", "label": "Message search",
    "description": "The free-text search box: regex against "
                   "jsonPayload.message.",
    "query": ".*", "hide": 0,
    "current": {"selected": True, "text": ".*", "value": ".*"},
    "options": [{"selected": True, "text": ".*", "value": ".*"}],
}
dashboards.append(make_dashboard(
    "t0-liquidity-logs", "Liquidity bot: Logs",
    "Level/category/target/search-filterable "
    "structured log lines.",
    panels, tab_links("Logs"),
    [ENV_VAR, LEVEL_VAR, CATEGORY_VAR, TARGET_VAR, SEARCH_VAR]))

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


# Every log a Cloud Logging query names.
LOG_NAME = re.compile(r'logName\s*[=:]\s*"projects/\$env/logs/([^"]+)"')


def log_source_problems(panel):
    """Why a panel's Cloud Logging queries are not the bot's, if they are
    not. Allowed: refId bot-<name> on the bot's log, and the exporter-only
    Orders log."""
    problems = []
    for target in panel.get("targets", []):
        if target.get("datasource", panel.get("datasource")) != CL:
            continue
        query = target.get("queryText", "")
        logs = set(LOG_NAME.findall(query))
        if logs == {EXPORTER_ONLY_LOG}:
            continue
        if not target.get("refId", "").startswith("bot-"):
            problems.append(f"refId {target.get('refId')} is not bot-<name>")
        if logs != {BOT_LOG_NAME} or not query.startswith(BOT_LOG):
            problems.append(f"{target.get('refId')} reads another log: {query}")
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


def series_name_problems(panel):
    """Why a panel's PromQL series would show unnamed: a PromQL target
    with a legendFormat, which the Cloud Monitoring datasource ignores
    (see promql), or a timeseries or bargauge target whose refId no
    byFrameRefID override names (see series_names)."""
    targets = [target for target in panel.get("targets", [])
               if target.get("queryType") == "promQL"]
    problems = [f"target {target.get('refId')}: legendFormat"
                for target in targets
                if "legendFormat" in target
                or "legendFormat" in target.get("promQLQuery", {})]
    if panel.get("type") in ("timeseries", "bargauge"):
        named = {override["matcher"]["options"]
                 for override in panel.get("fieldConfig", {})
                 .get("overrides", [])
                 if override["matcher"]["id"] == "byFrameRefID"
                 and any(prop["id"] == "displayName"
                         for prop in override["properties"])}
        problems += [f"target {target.get('refId')}: no displayName override"
                     for target in targets if target.get("refId") not in named]
    return problems


def check_series_names(boards):
    """Every PromQL series is named by a field override Grafana applies,
    not by a legendFormat it ignores."""
    failures = [(board["uid"], panel.get("title"), problem)
                for board in boards
                for panel in board_panels(board)
                for problem in series_name_problems(panel)]
    for uid, title, problem in failures:
        print(f"{uid}: panel {title!r} {problem}", file=sys.stderr)
    if failures:
        raise SystemExit(f"{len(failures)} PromQL series are not named by "
                         "a byFrameRefID displayName override")
    stray = series_name_problems({
        "type": "timeseries", "targets": [promql("up", ref="B")],
        "fieldConfig": {"overrides": series_names({"A": "up"})}})
    legend = series_name_problems({"targets": [
        {**promql("up"), "legendFormat": "up"},
        {**promql("up", ref="B"), "promQLQuery": {"legendFormat": "up"}}]})
    if len(stray) != 1 or len(legend) != 2:
        raise SystemExit(f"series_name_problems misses a case: {stray} "
                         f"{legend}")


def panel_link_urls(node):
    """Every data link url in a panel's field config, wherever it is nested."""
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "url" and isinstance(value, str):
                yield value
            else:
                yield from panel_link_urls(value)
    elif isinstance(node, list):
        for value in node:
            yield from panel_link_urls(value)


def check_links_keep_env(boards):
    """A data link to another board keeps the board's env and source, so the
    target board reads the same project and job. A link that copies every
    URL parameter (__url.params) keeps them already."""
    failures = [(board["uid"], panel.get("title"), url)
                for board in boards
                for panel in board_panels(board)
                for url in panel_link_urls(panel.get("fieldConfig", {}))
                if url.startswith("/d/") and "__url.params" not in url
                and "var-env=" not in url]
    for uid, title, url in failures:
        print(f"{uid}: panel {title!r}: link drops env or source: {url}",
              file=sys.stderr)
    if failures:
        raise SystemExit(f"{len(failures)} data links drop env")


def table_columns(panel):
    """The column names a table panel's sortBy can name: organize's ordered
    fields under their new names, and the fields its overrides match by
    name."""
    organized = set()
    for transform in panel.get("transformations", []):
        if transform["id"] == "organize":
            renames = transform["options"].get("renameByName", {})
            organized |= {renames.get(name, name) for name in
                          transform["options"].get("indexByName", {})}
            organized |= set(renames.values())
    matched = {override["matcher"]["options"]
               for override in panel["fieldConfig"]["overrides"]
               if override["matcher"]["id"] == "byName"}
    return organized | matched


def check_table_sorts(boards):
    """Every default sort key of a table names one of its columns, so the
    default order is not silently dropped."""
    failures = [(board["uid"], panel.get("title"), key["displayName"])
                for board in boards
                for panel in board_panels(board) if panel["type"] == "table"
                for key in panel.get("options", {}).get("sortBy", [])
                if key["displayName"] not in table_columns(panel)]
    for uid, title, name in failures:
        print(f"{uid}: panel {title!r} sorts by a missing column {name!r}",
              file=sys.stderr)
    if failures:
        raise SystemExit(f"{len(failures)} tables sort by a missing column")


def check_ratio_columns(boards):
    """Each table in the Dashboard tab's left column ends in its ratio bar,
    every other column has a fixed width, and they add up to RATIO_LEFT_PX,
    so the bars line up. Each table's query also carries its own row label:
    a query that lost it (one overwritten by another, say) draws nothing."""
    titles = {USD_BROKERS_TITLE: "Share of total", USD_ONCHAIN_TITLE: "Ratio",
              EQUITIES_TITLE: "Ratio"}
    if RATIO_LEFT_PX > RATIO_LEFT_PX_MAX:
        raise SystemExit(f"RATIO_LEFT_PX {RATIO_LEFT_PX} leaves the ratio no room")
    problems = {}
    panels = [panel for board in boards for panel in board_panels(board)
              if panel.get("title") in titles]
    missing = titles.keys() - {panel["title"] for panel in panels}
    if missing:
        problems["missing"] = sorted(missing)
    for panel in panels:
        title = panel["title"]
        organize = next(t["options"] for t in panel["transformations"]
                        if t["id"] == "organize")
        matrix = next(t["options"] for t in panel["transformations"]
                      if t["id"] == "groupingToMatrix")
        hidden = {override["matcher"]["options"]
                  for override in panel["fieldConfig"]["overrides"]
                  for prop in override["properties"]
                  if prop["id"] == "custom.hideFrom.viz"}
        order = sorted(organize["indexByName"], key=organize["indexByName"].get)
        shown = [organize["renameByName"].get(name, name) for name in order
                 if name not in organize.get("excludeByName", {})]
        shown = [name for name in shown if name not in hidden]
        widths = {override["matcher"]["options"]: prop["value"]
                  for override in panel["fieldConfig"]["overrides"]
                  for prop in override["properties"]
                  if prop["id"] == "custom.width"}
        if shown[-1] != titles[title]:
            problems[title] = f"ends in {shown[-1]}"
        elif set(shown[:-1]) - widths.keys():
            problems[title] = f"no width: {sorted(set(shown[:-1]) - widths.keys())}"
        elif sum(widths[name] for name in shown[:-1]) != RATIO_LEFT_PX:
            problems[title] = (f"{sum(widths[name] for name in shown[:-1])}px "
                               f"before the ratio")
        elif f'"{matrix["rowField"]}"' not in panel["targets"][0]["promQLQuery"]["expr"]:
            problems[title] = f"query has no {matrix['rowField']} label"
    if problems:
        raise SystemExit(f"left column ratios do not line up: {problems}")


def check_scale_groups(boards):
    """Each scale group's fixedPx is the sum of the fixed column widths of
    every table in it, so the scaling fits the columns it shrinks."""
    by_title = {panel.get("title"): panel for board in boards
                for panel in board_panels(board)}
    problems = []
    for group in SCALE_GROUPS:
        for title in group["panels"]:
            panel = by_title.get(title)
            if panel is None:
                problems.append(f"{group['id']}: no panel {title!r}")
                continue
            fixed = sum(prop["value"]
                        for override in panel["fieldConfig"]["overrides"]
                        for prop in override["properties"]
                        if prop["id"] == "custom.width")
            if fixed != group["fixedPx"]:
                problems.append(f"{title}: fixed widths {fixed}, the "
                                f"{group['id']} group says {group['fixedPx']}")
            hide = group.get("compactHide", {}).get(title, [])
            widths = {override["matcher"]["options"]: prop["value"]
                      for override in panel["fieldConfig"]["overrides"]
                      for prop in override["properties"]
                      if prop["id"] == "custom.width"}
            if set(hide) - widths.keys():
                problems.append(f"{title}: hides unknown columns "
                                f"{sorted(set(hide) - widths.keys())} on a phone")
            phone_widths = {name: values.get("custom.width", widths.get(name))
                            for name, values in group.get("compactColumns", {}).items()}
            if set(phone_widths) - widths.keys():
                problems.append(f"{title}: changes unknown columns "
                                f"{sorted(set(phone_widths) - widths.keys())} on a phone")
            phone = sum(phone_widths.get(name, px) for name, px in widths.items()
                        if name not in hide)
            if phone > group.get("compactFixedPx", group["fixedPx"]):
                problems.append(f"{title}: {phone}px on a phone, over the "
                                f"group's {group.get('compactFixedPx')}")
    if problems:
        raise SystemExit(f"scale groups do not match their tables: {problems}")


def check_after_render_scripts(boards):
    """An afterRender script is not a module, so an export line left in it
    (see panel_module) breaks the whole panel. Its modules share one scope,
    so a name two of them declare at the top level breaks it too."""
    failures = []
    for board in boards:
        for panel in board_panels(board):
            script = panel.get("options", {}).get("afterRender", "")
            if any(line.startswith("export ") for line in script.splitlines()):
                failures.append((board["uid"], panel.get("title"),
                                 "has an export line"))
            names = re.findall(r"^(?:const|let|var|function|class)\s+(\w+)",
                               script, re.M)
            twice = sorted({name for name in names if names.count(name) > 1})
            if twice:
                failures.append((board["uid"], panel.get("title"),
                                 f"declares {', '.join(twice)} twice"))
    for uid, title, problem in failures:
        print(f"{uid}: panel {title!r}: afterRender {problem}",
              file=sys.stderr)
    if failures:
        raise SystemExit(f"{len(failures)} afterRender scripts are broken")


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


def check_pin_liq():
    """pin_liq pins bare and labelled liq_ selectors, and leaves string
    literals and by/on/... label lists alone. CI runs this on --check."""
    pin = "{" + LIQ_JOB_MATCHER
    cases = {
        "liq_a": f"liq_a{pin}}}",
        'liq_a{chain="base"}': f'liq_a{pin},chain="base"}}',
        'liq_a{chain="}liq_b"} + liq_c':
            f'liq_a{pin},chain="}}liq_b"}} + liq_c{pin}}}',
        'label_replace(liq_a, "k", "liq_b", "", "")':
            f'label_replace(liq_a{pin}}}, "k", "liq_b", "", "")',
        "'liq_a' + `liq_b`": "'liq_a' + `liq_b`",
        "max by (liq_x) (liq_a)": f"max by (liq_x) (liq_a{pin}}})",
        "sum without(liq_x)(liq_a)": f"sum without(liq_x)(liq_a{pin}}})",
        "liq_a * on (symbol, liq_x) group_left (liq_y) liq_b":
            f"liq_a{pin}}} * on (symbol, liq_x) group_left (liq_y) liq_b{pin}}}",
        "liq_a / ignoring(liq_x) group_right(liq_y) liq_b":
            f"liq_a{pin}}} / ignoring(liq_x) group_right(liq_y) liq_b{pin}}}",
        'label_replace(liq_a, "k", "by (liq_b)", "", "")':
            f'label_replace(liq_a{pin}}}, "k", "by (liq_b)", "", "")',
    }
    wrong = {expr: (pin_liq(expr), want) for expr, want in cases.items()
             if pin_liq(expr) != want}
    still_unpinned = {expr: unpinned_liq_selectors(pin_liq(expr))
                      for expr in cases
                      if unpinned_liq_selectors(pin_liq(expr))}
    if unpinned_liq_selectors("liq_a") != ["liq_a"]:
        still_unpinned["bare liq_a not reported"] = unpinned_liq_selectors("liq_a")
    # A metric name in a __name__ matcher is a string literal that pin_liq
    # leaves alone, so the check reports the selector instead.
    for expr in ('{__name__="liq_equity_total"}',
                 'count({__name__=~"liq_.*"})',
                 'sum({__name__!="up"})',
                 '{__name__="liq_a",job="other"}',
                 'sum({"liq_equity_total"})', "{'liq_a', chain=\"base\"}",
                 '{ `liq_a` }', '{"liq_a",job="other"}'):
        if not unpinned_liq_selectors(pin_liq(expr)):
            still_unpinned[f"{expr} not reported"] = []
    for expr in ('{__name__="liq_a",' + LIQ_JOB_MATCHER + "}",
                 '{"liq_a",' + LIQ_JOB_MATCHER + "}", '{"up"}',
                 '{__name__="up"}', 'label_replace(up, "k", "{v}", "", "")'):
        if unpinned_liq_selectors(pin_liq(expr)):
            still_unpinned[expr] = unpinned_liq_selectors(pin_liq(expr))
    if wrong or still_unpinned:
        raise SystemExit(f"pin_liq pins the wrong names: {wrong} "
                         f"{still_unpinned}")


def bar_cell_shows(cell, value):
    """What a bar cell's first matching regex mapping draws for `value` as
    Grafana passes it, the number's string form: (fill, percent, low tick,
    high tick), each tick in percent and None on a bar without ticks. A cell
    with no percent gives its text in place of the percent. Grafana fills
    the mapping's text with JavaScript's replace, where a group that did not
    take part writes nothing."""
    mappings = next(o["value"] for o in cell if o["id"] == "mappings")
    for mapping in mappings:
        match = re.fullmatch(mapping["options"]["pattern"], value)
        if match:
            html = re.sub(r"\$(\d)", lambda ref: match.group(int(ref.group(1))) or "",
                          mapping["options"]["result"]["text"])
            fill = re.search(r"90deg, (rgba?\([^)]*\))", html)
            ticks = [f"{int(tenths) / 10:g}" for tenths in re.findall(
                r"currentColor calc\((\d+)% / 10 - 1px\)", html)]
            text = (re.search(r">([^<]*)%</span></div>", html)
                    or re.search(r">([^<]*)</div>", html))
            judged = fill and fill.group(1) in (GREEN_FILL, RED_FILL)
            height = re.search(r"height:(\d+)px", html)
            return (fill.group(1) if fill else None,
                    text.group(1) if text else None,
                    *(ticks if ticks else (None, None)),
                    *((int(height.group(1)),) if judged else ()))
    return None


def check_bar_cells():
    """band_bar and unjudged_pct values draw the colour, percent and ticks
    they mean, and anything outside them reads "out of range". CI runs this
    on --check."""
    out_of_range = (None, "out of range", None, None)
    grey = lambda pct: (GREY_FILL, pct, None, None)
    green = lambda *texts, height=20: (GREEN_FILL, *texts, height)
    red = lambda *texts, height=20: (RED_FILL, *texts, height)
    cell = bar_cell()
    no_edges = f"{NO_EDGE}" * 2
    cases = {
        "1062805500650": green("62.8", "55", "65"),
        "-1062805500650": red("62.8", "55", "65"),
        "1050004500550": green("50", "45", "55"),
        "-1000504751000": red("0.5", "47.5", "100"),
        "1100000000000": green("100", "0", "0"),
        "1000000000000": green("0", "0", "0"),
        "-1000900100025": red("0.9", "1", "2.5"),
        f"10747{no_edges}": green("74.7", None, None),
        f"-10831{no_edges}": red("83.1", None, None),
        # A sum writes Σ before its percent; a part is the slim bar.
        f"20747{no_edges}": green("Σ 74.7", None, None),
        f"-20831{no_edges}": red("Σ 83.1", None, None),
        "3038103500450": green("38.1", "35", "45", height=13),
        "-3028003500450": red("28", "35", "45", height=13),
        "4062805500650": out_of_range,
        "1045.5": grey("45.5"), "1000": grey("0"), "1100": grey("100"),
        "1005": grey("5"), "1010": grey("10"), "1000.5": grey("0.5"),
        "1045.5000000000002": grey("45.5"),
        # A negative or above-one ratio: the encodings send it to
        # OUT_OF_RANGE, and a value no encoding makes never draws a bar.
        str(OUT_OF_RANGE): out_of_range, "990": out_of_range,
        "1200": out_of_range, "1990.5": out_of_range,
        "1100100000000": out_of_range, "1050010010000": out_of_range,
        "1050099990550": out_of_range, "5062805500650": out_of_range,
        "106280550065": out_of_range, "62.8": out_of_range,
        "-62.8": out_of_range,
    }
    wrong = {value: (bar_cell_shows(cell, value), want)
             for value, want in cases.items()
             if bar_cell_shows(cell, value) != want}
    # Every encoding guards its ratio with in_range, so a ratio outside
    # [0, 1] reaches the cell as OUT_OF_RANGE.
    bounded = "(((ratio) >= 0) <= 1)"
    guarded = (f") and {bounded}) or ((clamp((ratio), 0, 0) + "
               f"{OUT_OF_RANGE}) unless {bounded}))")
    for name, encoded in {
            "band_bar": band_bar("ratio", "low", "high", "inside"),
            "band_bar without ticks": band_bar("ratio", None, None, "inside"),
            "unjudged_pct": unjudged_pct("ratio")}.items():
        if not encoded.endswith(guarded):
            wrong[f"{name} out of range"] = encoded
    if wrong:
        raise SystemExit(f"bar cell mappings draw the wrong bar: {wrong}")


def check_exposure_cell():
    """Exposure cents draw as a signed dollar amount, red beyond
    EXPOSURE_ALERT_CENTS either way. CI runs this on --check."""
    def shows(value):
        mappings = next(o["value"] for o in EXPOSURE_CELL if o["id"] == "mappings")
        for mapping in mappings:
            match = re.fullmatch(mapping["options"]["pattern"], value)
            if match:
                html = re.sub(r"\$(\d)", lambda ref: match.group(int(ref.group(1))) or "",
                              mapping["options"]["result"]["text"])
                text = re.sub(r"<[^>]*>", "", html).replace("&#36;", "$")
                return text, EXPOSURE_RED in html
        return None
    cases = {
        "0": ("$0.00", False), "-0": ("$0.00", False),
        "5": ("+$0.05", False), "-42": ("\u2212$0.42", False),
        "450": ("+$4.50", False), "-500": ("\u2212$5.00", False),
        "500": ("+$5.00", False), "501": ("+$5.01", True),
        "-501": ("\u2212$5.01", True), "-599": ("\u2212$5.99", True),
        "600": ("+$6.00", True), "1000": ("+$10.00", True),
        "-123456": ("\u2212$1,234.56", True),
        "123456789": ("+$1,234,567.89", True),
        "NaN": ("\u2014", False), "null": (" ", False),
    }
    wrong = {value: (shows(value), want) for value, want in cases.items()
             if shows(value) != want}
    if wrong:
        raise SystemExit(f"exposure cell draws the wrong amount: {wrong}")


def check_row_tints():
    """Every venue with a mark has a row tint, under the mark's name."""
    if set(MARK_ROW_TINTS) != set(MARK_NAMES.values()):
        raise SystemExit(f"MARK_ROW_TINTS {sorted(MARK_ROW_TINTS)} do not name "
                         f"the venues {sorted(MARK_NAMES.values())}")


def check_no_custom_hidden(boards):
    """No table saved as 13.1.0 hides a column with custom.hidden, which
    that version draws (see native_table's presort and drop)."""
    failures = [(board["uid"], panel.get("title"))
                for board in boards
                for panel in board_panels(board)
                if panel["type"] == "table"
                and panel.get("pluginVersion") == "13.1.0"
                and any(prop["id"] == "custom.hidden"
                        for override in panel.get("fieldConfig", {})
                        .get("overrides", [])
                        for prop in override["properties"])]
    for uid, title in failures:
        print(f"{uid}: panel {title!r} hides a column with custom.hidden; "
              "use custom.hideFrom", file=sys.stderr)
    if failures:
        raise SystemExit(f"{len(failures)} tables use custom.hidden")


def check_lag_age_states(boards):
    """The "sampled ago" stat maps each sentinel its expression can give to
    its state and builds the paused branch from a sample time without a
    lag. Paused wins over an age and an age wins over no checkpoint, so a
    stale chain still shows next to a new one. The ages sit under `max`, so
    with two valid chains (say 30s and 600s old) the stat shows 600s, and
    an age over 300 s is orange.
    CI runs this on --check."""
    panels = [panel for board in boards for panel in board_panels(board)
              if panel.get("fieldConfig", {}).get("defaults", {})
              .get("displayName") == "sampled ago"]
    if len(panels) != 1:
        raise SystemExit(f"expected one 'sampled ago' stat, found {len(panels)}")
    panel = panels[0]
    expr = panel["targets"][0]["promQLQuery"]["expr"]
    texts = {key: result["text"]
             for mapping in panel["fieldConfig"]["defaults"]["mappings"]
             if mapping["type"] == "value"
             for key, result in mapping["options"].items()}
    want = {str(LAG_PAUSED): "ingestion paused",
            str(LAG_NEVER_SAMPLED): "no checkpoint yet"}
    problems = [f"{key} maps to {texts.get(key)!r}, not {text!r}"
                for key, text in want.items() if texts.get(key) != text]
    paused, sep, rest = expr.partition(") or max(")
    ages, sep2, no_checkpoint = rest.partition(") or min(")
    if not sep or not sep2 or not paused.startswith("min("):
        problems.append("want min(<paused>) or max(<ages>) or min(<no checkpoint>)")
    for part, branch in (
            (paused, f"liq_block_lag_blocks{{{LIQ_JOB_MATCHER}}}) - {-LAG_PAUSED}"),
            (paused, "unless on(chain) liq_block_lag_blocks"),
            (no_checkpoint, f"liq_block_lag_sampled_ts_seconds{{{LIQ_JOB_MATCHER}}}) "
                            f"- {-LAG_NEVER_SAMPLED}"),
            (no_checkpoint, "unless on(chain) liq_block_lag_sampled_ts_seconds"),
            (ages, "clamp_min("), (ages, "and on(chain) liq_block_lag_blocks")):
        if branch not in part:
            problems.append(f"branch {branch!r} is not where it belongs")
    if "clamp_min(" in paused or "clamp_min(" in no_checkpoint:
        problems.append("ages must not sit under a sentinel's min")
    steps = panel["fieldConfig"]["defaults"]["thresholds"]["steps"]
    if {"color": "orange", "value": 300} not in steps:
        problems.append("an age over 300 s must turn orange")
    if LAG_PAUSED >= LAG_NEVER_SAMPLED or LAG_NEVER_SAMPLED >= 0:
        problems.append("paused must sort below no checkpoint, below 0")
    if problems:
        raise SystemExit(f"'sampled ago' stat: {problems} in {expr}")

check_pin_liq()
check_bar_cells()
check_exposure_cell()
check_row_tints()
check_lag_age_states(dashboards)
check_liq_pinned(dashboards)
check_log_sources(dashboards)
check_series_names(dashboards)
check_links_keep_env(dashboards)
check_table_sorts(dashboards)
check_ratio_columns(dashboards)
check_no_custom_hidden(dashboards)
check_scale_groups(dashboards)
check_after_render_scripts(dashboards)
if sys.argv[1:] == ["--check"]:
    check_committed(dashboards)
elif sys.argv[1:]:
    raise SystemExit(f"usage: {sys.argv[0]} [--check]")
else:
    write_boards(HERE, dashboards)
