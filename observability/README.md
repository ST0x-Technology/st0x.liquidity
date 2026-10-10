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
adds both sources together. The log panels (Trades, Rebalances, the detail
dialog, Logs) follow `Source` too: the exporter's log names, or the bot's own
log, `liquidity-bot`, which holds rows once the bot logs JSON. Each such panel
queries both logs and keeps the picked source's frame, and the generator fails
on one that does not. Stage 4 of the migration makes `bot` the default
(`SOURCE_VAR` in the generator). See `docs/observability.md`, "Board source".

The header row is a Business Text panel (`marcusolsson-dynamictext-panel`): its
JavaScript and CSS live in `liquidity-panels/`, and the generator inlines them
into the board JSON. The plugin is installed on the box by `T0Trade/t0.devops`
(`GF_INSTALL_PLUGINS`); the PR check's throwaway Grafana does not have it, so it
loads these panels as "Panel plugin not found" and still passes. The header
reads its query rows through `header-rows.js`, which
`dashboard/src/lib/board-header.test.ts` tests.

The recovery commands in the header's guide (`recovery-guide.json`) and in the
row dialog (`recovery-commands.js`) are copies of the SPA's
(`dashboard/src/lib/transfer.ts`), in the operations client form for the
environment the board shows (`client-env.js`).
`dashboard/src/lib/transfer-board.test.ts` fails when a copy drifts, for every
status the DTOs carry, so change the SPA and the copies together. The same test
covers the dialog's `status-history.js` and `log-lines.js`, and writes every
shown client command to `crates/liquidity-client/testdata/shown-commands.txt`,
which the client's `every_shown_recovery_command_parses` test parses with the
real CLI, and every shown offline `stox` command to
`crates/cli/testdata/shown-stox-commands.txt`, which `crates/cli` parses and
classifies. Run `bunx vitest run -u` in `dashboard/` after a command changes.
The test runs in the dashboard build of CI.

The Dashboard tab's inventory is three Grafana tables, for their column filters:
USD at Alpaca, USD on each chain, and Equities. Per-chain rows (USD · Onchain)
and columns (Equities) come from the bot's `chain`-labelled series
(`liq_usdc_chain_*`, `liq_usdc_corridor_*`, `liq_equity_chain_available`; the
board does not read `liq_equity_chain_inflight` yet). The exporter does not
publish them, so with `Source` on `exporter` the tables show the single Base row
and column from the unlabelled series. The fallback is per source: while the bot
publishes per-chain series, a chain without a labelled value shows none. The
Equities Ratio is coloured only where it is the bot's own band verdict (a symbol
rebalanced on Base, with a balance, with no vault on another chain, while the
default Base target is set), so it is grey on the `exporter` source, which
cannot see the other chains. A ratio outside 0 to 100% reads "out of range".
Counter-trading assets come first, like the SPA, through a hidden sort column.
The Equities column widths fit the card on a 1934px-wide window with at most two
chain columns showing (Base and RH). A third, such as HyperEVM, makes the table
scroll sideways; the generator's `check_equity_widths` holds only that
two-column budget.

Trades and Rebalances are Grafana tables too. A row's ⓘ sets the hidden `detail`
variable to the row's id; the `detail` panel, a second Business Text panel in
the header row's last column, then opens that row's dialog. It reads the two
tables' results through the Dashboard datasource, so a click runs no new query
and the dialog opens at once. The dialog shows the row's status history
(`status-history.js`), or on the bot source the row's event timeline from the
bot's `liq_event` lines (`log-lines.js`). Closing the dialog clears the
variable.

Keep the Dashboard tab's panel heights (`TRADES_H`, `TRANSFERS_H` and the
`native_inventory` heights in the generator) unless you check a new set on a
preview board: reload it at a few window heights and confirm both columns end
level and every table shows its rows. The tab link opens the board with
`autofitpanels`, which scales and rounds every height, keeps the result, and
fits it again on the next render. Whenever one column rounds a row taller than
the window, the next pass shrinks everything again and the other column loses a
row per pass. Most height sets do that on some window size; the current ones do
not, for windows of 18 to 45 rows.

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
