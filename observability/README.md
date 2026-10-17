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

The liquidity boards read the bot only: its `liq_*` gauges from its own
`/metrics` (scrape job `t0-liquidity` in production, `t0-liquidity-staging` in
staging, the same as the project `Environment` picks) and its own log,
`liquidity-bot`. The generator writes `job="$env"` into every `liq_` selector
and fails on one without it, because the exporter sidecar can still serve the
same names. Every log panel reads the bot's log as refId `bot-<name>`, except
Orders, which reads the exporter's `liquidity-orders` log because the bot writes
no order lines; the generator fails on any other log query.

The header row is a Business Text panel (`marcusolsson-dynamictext-panel`): its
JavaScript and CSS live in `liquidity-panels/`, and the generator inlines them
into the board JSON. The plugin is installed on the box by `T0Trade/t0.devops`
(`GF_INSTALL_PLUGINS`); the PR check's throwaway Grafana does not have it, so it
loads these panels as "Panel plugin not found" and still passes. The header
reads its query rows through `header-rows.js`, which
`observability/tests/board-header.test.ts` tests. `board-header-render.test.ts`
and `board-detail.test.ts` in the same directory run the header's and the row
dialog's scripts as deployed, from the generated board JSON, in happy-dom, so
regenerate the boards before you run them.

The tests live in `observability/tests` (vitest). Run them there with
`bun install && bun run test`. CI's `dashboard tests` job and the flake's `ci`
app run them too.

The recovery commands in the header's guide (`recovery-guide.json`) and in the
row dialog (`recovery-commands.js`) are the source of truth for the commands
operators see, in the operations client form for the environment the board shows
(`client-env.js`). `recovery.test.ts` checks them for every status the DTOs
carry. The same test covers the dialog's `status-history.js` and `log-lines.js`,
and writes every shown client command to
`crates/liquidity-client/testdata/shown-commands.txt`, which the client's
`every_shown_recovery_command_parses` test parses with the real CLI, and every
shown offline `stox` command to `crates/cli/testdata/shown-stox-commands.txt`,
which `crates/cli` parses and classifies. After a command changes, run
`bunx vitest run -u` in `observability/tests` to regenerate both files.

The Dashboard tab's inventory is three Grafana tables: USD at the brokers
(Alpaca), USD on each chain, and Equities, each with its total in USD after its
title (`board-totals.js`, the balances held, in flight left out). USD · Onchain
has a row per chain from the bot's `chain`-labelled series (`liq_usdc_chain_*`,
`liq_usdc_corridor_*`). Equities has a row per asset and, under it, a row for
Alpaca and one per chain the asset has a vault on (`liq_equity_chain_*`), keyed
`symbol/alpaca` and `symbol/chain` so the rows sort into a tree. Grafana's own
sort would break the tree, so the header sorts Equities itself
(`board-tables.js`): each sortable column has a hidden key that a chain row
takes from its asset, and a click sorts by it and then by row. The default order
is the largest exposure first, long or short.

Each table ends in a ratio bar, green inside its band and red outside, with a
tick at each band edge. A table cell can only style its own value, so the query
packs the percent, both edges and the verdict into one number (`band_bar` in the
generator) and regex value mappings draw it. The other columns of the three
tables add up to `RATIO_LEFT_PX`, so the bars line up one above the other. Venue
and chain names are small brand marks (`MARK_SVGS`), and each venue's rows take
a tint of its colour.

Trades and Rebalances are Grafana tables too. A row's ⓘ sets the hidden `detail`
variable to the row's id; the `detail` panel, a second Business Text panel in
the header row's last column, then opens that row's dialog. It reads the two
tables' results through the Dashboard datasource, so a click runs no new query
and the dialog opens at once. The dialog shows the row's event timeline from the
bot's `liq_event` lines (`log-lines.js`). The board loads none of those, a slow
read at every load for a dialog most loads never open: each row fetches its own
when its dialog opens (`row-events.js`), over its own span. Closing the dialog
clears the variable.

The Dashboard tab fits the window itself (`board-layout.js`): the USD tables
take the rows they draw, Equities the rest, and Trades and Rebalances half the
right column each, both columns ending level at the window's bottom. A window
too short to give Equities a few rows (a small laptop) gets them anyway, and the
page scrolls. Grafana's `autofitpanels` would squeeze every panel into one
screen however short, so the header takes it off a link that has it. On a phone
the panels stack, inventory first, and a table too narrow for every column shows
its compact set (`board-scale.js`). It reaches the layout through
`window.__grafanaSceneContext`, which Grafana exposes for debugging; without it
the board keeps the generator's heights. Keep those (`TRADES_H`, `TRANSFERS_H`
and the `native_inventory` heights) unless you check a new set on a preview
board at a few window sizes.

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
