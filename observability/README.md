# Observability for the liquidity bot

The Grafana dashboards and alert rules about the liquidity bot, and nothing
else. They ship from here: a merge to `master` runs
`.github/workflows/observability.yml`, a ten-line caller of the shared flow in
`ST0x-Technology/.github`, which copies them into the bucket the T0
observability box syncs every minute. Dashboards reload within about 30 seconds;
a rules change restarts Grafana. Open them at https://grafana.t0trade.com.

The box itself, its datasources, who gets paged and on which Zulip channel, and
the platform boards (Health, Deployments, Authentication) live in
`T0Trade/t0.grafana` and `T0Trade/t0.devops`, and devops owns those. Nobody here
needs to touch them to change an alert or a board.

`dashboards/liquidity/` is the Grafana folder `liquidity`: a directory here is a
folder there, nested directories included, and a board moves folders by moving
its file. Boards are read-only in the Grafana UI because this directory is the
source. To draft one, build it in the `previews-and-tests` folder in Grafana,
export the JSON, drop it here and open a PR; then delete the scratch copy. Keep
the `t0` tag so the board shows in every board's "Related" dropdown, keep
`timezone: utc`, and never change a `uid` (links and the alert annotations point
at it).

The Liquidity bot board and its four tab boards are generated: edit
`gen-t0-liquidity.py` (or `t0-liquidity-native-rows.json`) and run
`python3 observability/gen-t0-liquidity.py`, which rewrites all five JSON files.
Do not edit those files by hand or replace them with a Grafana export; the next
run of the generator overwrites them.

The liquidity boards read `liq_*` from one source at a time, picked by the
`Source` variable: the exporter sidecar (`job="t0-liquidity-exporter"`, the
default) or the bot's own `/metrics` (`t0-liquidity` in production,
`t0-liquidity-staging` in staging). The generator writes `job=~"$source"` into
every `liq_` selector and fails on one without it, because an unpinned selector
adds both sources together. Stage 4 of the migration makes `bot` the default
(`SOURCE_VAR` in the generator). See `docs/observability.md`, "Board source".

The header row is a Business Text panel (`marcusolsson-dynamictext-panel`): its
JavaScript and CSS live in `liquidity-panels/`, and the generator inlines them
into the board JSON. The plugin is installed on the box by `T0Trade/t0.devops`
(`GF_INSTALL_PLUGINS`); the PR check's throwaway Grafana does not have it, so it
loads these panels as "Panel plugin not found" and still passes.

The recovery commands in the header's guide (`recovery-guide.json`) are a copy
of the SPA's (`dashboard/src/lib/transfer.ts`), in the operations client form
for the environment the board shows. `dashboard/src/lib/transfer-board.test.ts`
fails when the copy drifts, so change the SPA and the copy together. It runs in
the dashboard build of CI.

`alerting/liquidity.rules.yml` is the alert rules, in the `Alerts` folder. Each
rule's `uid` is permanent and must be unique across every repo's file. Removing
a rule from the file does not remove it from Grafana: add its `uid` to
`deleteRules:` as well, or it keeps firing. `deleteRules` retires one of this
file's rules for good; never list a rule that merely moved to another file,
because Grafana applies `deleteRules` by uid whatever file provisions the rule
and would delete it after every restart. Routing is by label:
`service: liquidity` picks the Zulip channel, each rule gets its own topic named
`<service> / <rule>`, and `severity: critical` also pages PagerDuty in US
extended hours. Datasource uids are fixed by the box: `victoriametrics` (PromQL
over the probes' gauges) and `cloudmon` (Cloud Monitoring: the service's own
metrics and log-based metrics).

The check job provisions these files into a throwaway Grafana on every PR,
because a rules file Grafana refuses would take the real one down when it
restarts. It compares the counts of rules and boards it finds against the files,
so a board that fails to load fails the check. It also keeps the file in scope:
rule groups and `deleteRules` only (no contact points, policies or mute timings,
those are devops's), every group in the `Alerts` folder, every rule labelled
`service: liquidity`, and no per-rule routing override.
