// Business Text "afterRender" code for the Liquidity bot's detail panel: the
// SPA's trade and transfer detail dialogs (trade-history-panel.svelte,
// transfer-panel.svelte in ST0x-Technology/st0x.liquidity) for the row whose
// ⓘ was clicked in the Trades or Rebalances table.
//
// The click sets the hidden `detail` variable to the row's id. A and B are
// the Trades and Rebalances tables' own results through the Dashboard
// datasource: each table's two frames, the exporter's and the bot's (the
// newest 500 status entries of each log), and the script reads the frames
// of the `source` picked. On the bot source, bot-events is the bot's event
// timeline (the newest 500 liq_event lines); the exporter has no event
// log, so there the dialog shows the status history. Closing the dialog
// clears the variable.
//
// The panel is one empty column of the header row; only the dialog shows.
//
// MODE_LABELS, tradeCommands, transferCommands, latest, lineEntries,
// eventTimeline, timelineIncomplete, queryState, commandLine and
// wireCopyButtons are not defined here: the generator prepends them from
// recovery-guide.json, recovery-commands.js, status-history.js, log-lines.js
// and copy-command.js (see detail_panel()).

const theme = context.grafana.theme;
const root = context.element;
root.style.setProperty('--det-muted', theme.colors.text.secondary);
root.style.setProperty('--det-border', theme.colors.border.weak);
root.style.setProperty('--det-stripe', theme.colors.background.secondary);
root.style.setProperty('--det-card', theme.colors.background.primary);
root.style.setProperty('--det-text', theme.colors.text.primary);
// Light theme swaps the status colours for darker shades (detail.css).
root.classList.toggle('det-light', !theme.isDark);

// Alternate rows in the board's Trades and Rebalances
// tables, like the SPA's. Grafana's table has no striping option, but its
// grid marks every row rdg-row-even or rdg-row-odd. This panel lives only on
// the Dashboard tab, so the rule goes into the page while the panel is
// mounted and comes out when the board is left. Grafana can mount the panel
// on a new element (tab switch, view mode), so each render takes the rule
// over for its own element and restarts the watcher, and a watcher removes
// the rule only while its element still owns it.
let stripes = document.getElementById('liquidity-table-stripes');
if (!stripes) {
  stripes = document.createElement('style');
  stripes.id = 'liquidity-table-stripes';
  document.head.appendChild(stripes);
}
stripes.textContent = `.rdg-row-even { background: ${theme.isDark ? 'rgba(255, 255, 255, 0.03)' : 'rgba(0, 0, 0, 0.03)'}; }`;
root.__stripesOwner = root.__stripesOwner || Math.random().toString(36).slice(2);
stripes.dataset.owner = root.__stripesOwner;
clearInterval(root.__stripesWatch);
root.__stripesWatch = setInterval(() => {
  if (root.isConnected) return;
  clearInterval(root.__stripesWatch);
  if (stripes.dataset.owner === root.__stripesOwner) stripes.remove();
}, 2000);

const escapeHtml = (text) =>
  String(text ?? '').replace(/[&<>"']/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char]);

// The id the tables asked for. Entries of any other id are a previous
// query's data, still shown while the new one loads.
const wanted = context.grafana.replaceVariables('${detail}');

// The board's environment is its project, and `source` names the frames.
const env = context.grafana.replaceVariables('${env}');
const SOURCE = context.grafana.replaceVariables('${source:text}') === 'bot' ? 'bot' : 'exporter';

// The wanted id's entries in one frame, from the board's project only (see
// lineEntries).
const entriesOf = (refId) =>
  lineEntries(
    (context.panelData?.series || []).find((series) => series.refId === refId),
    wanted,
    env
  );

// The SPA's formatUtc: "Oct 6, 12:24:39 UTC".
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const utc = (value) => {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '—';
  const pad = (number) => String(number).padStart(2, '0');
  return `${MONTHS[date.getUTCMonth()]} ${date.getUTCDate()}, ${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())}:${pad(date.getUTCSeconds())} UTC`;
};

// The SPA's formatDecimal(value, 3): fixed decimals, comma groups, and the
// raw value when it does not parse.
const decimal = (value) => {
  const number = Number(value);
  if (value === '' || value === null || value === undefined || Number.isNaN(number)) return escapeHtml(value || '—');
  return number.toLocaleString('en-US', { minimumFractionDigits: 3, maximumFractionDigits: 3 });
};

const VENUES = {
  raindex: ['Raindex', 'det-blue', true],
  bebop: ['Bebop', 'det-purple', true],
  uniswap_v4: ['Uniswap v4', 'det-pink', true],
  unknown_onchain: ['Unknown Onchain', '', true],
  alpaca: ['Alpaca', 'det-orange', false],
  dry_run: ['DryRun', '', false],
};
const venue = (key) => VENUES[key] || [key || '—', '', false];

const TYPES = {
  equity_mint: 'Mint',
  equity_redemption: 'Redeem',
  alpaca_to_base: 'Alpaca → Raindex',
  base_to_alpaca: 'Raindex → Alpaca',
  usdc_bridge: 'USDC Bridge',
};
// A USDC bridge is labelled by its direction, or as a bridge when it has none.
const typeOf = (transfer) => (transfer.kind === 'usdc_bridge' ? transfer.direction || transfer.kind : transfer.kind);
const typeLabel = (transfer) => TYPES[typeOf(transfer)] || transfer.kind || '—';
const assetOf = (transfer) => (transfer.kind === 'usdc_bridge' || !transfer.symbol ? 'USDC' : transfer.symbol);

// The SPA's statusStyle colours.
const statusClass = (status) => {
  const lower = String(status || '').toLowerCase();
  if (lower.includes('completed') || lower.includes('filled') || lower.includes('deposited') || lower.includes('confirmed')) return 'det-green';
  if (lower.includes('reconciled')) return 'det-amber';
  if (lower.includes('failed')) return 'det-red';
  if (lower.includes('cancelled')) return 'det-orange';
  return 'det-blue';
};
const statusLabel = (status) => {
  const text = String(status || '—').replace(/_/g, ' ');
  return text.charAt(0).toUpperCase() + text.slice(1);
};

// --------------------------------------------------------------------------
// Recovery commands (recovery-commands.js, prepended): the operations client,
// which signs in with Google and calls the running bot's ops API, so no SSH
// is needed and every command needs the bot.
// --------------------------------------------------------------------------

// The environment the board shows (client-env.js, prepended).
const ENV = boardEnv(context.grafana.replaceVariables('${env:text}'));
const CLIENT = clientFor(ENV);

const modeClass = (mode) => (mode === 'requires-bot' ? 'det-amber' : 'det-red');
const commandBlock = (commands, note) =>
  commands.length === 0 && !note
    ? ''
    : `<div class="det-cli"><div class="det-cli-title">CLI commands</div>${note ? `<p class="det-muted">${escapeHtml(note)}</p>` : ''}${commands
        .map(
          (entry) => `
        <div class="det-command">
          ${commandLine(escapeHtml(entry.command))}
          <div>${escapeHtml(entry.description)}</div>
          <span class="det-mode ${modeClass(entry.mode)}">${escapeHtml(MODE_LABELS[entry.mode] || entry.mode)}</span>
        </div>`
        )
        .join('')}</div>`;

// --------------------------------------------------------------------------
// The dialog
// --------------------------------------------------------------------------

// Block explorers by the bot's chain wire name. A chain without one, or an
// unknown chain, shows the hash unlinked: a link to the wrong explorer would
// read as "this tx does not exist".
const EXPLORERS = {
  base: 'https://basescan.org',
  ethereum: 'https://etherscan.io',
  hyperevm: 'https://hyperevmscan.io',
};
const shortHash = (hash) => `${escapeHtml(hash.slice(0, 10))}…${escapeHtml(hash.slice(-8))}`;
const txLink = (hash, chain) =>
  EXPLORERS[chain]
    ? `<a href="${EXPLORERS[chain]}/tx/${escapeHtml(hash)}" target="_blank" rel="noopener noreferrer">${shortHash(hash)} ↗</a>`
    : `<span class="det-mono" title="${escapeHtml(hash)}">${shortHash(hash)}</span>`;

// The bot's event timeline: each step with its time and fields, like the
// SPA's dialog. On the exporter source, the status history instead: one
// step per status change.
const eventLines = SOURCE === 'bot' ? entriesOf('bot-events') : [];
// Only an error of a query this source reads fails the dialog (see
// queryState). An Error state that lists no error fails it too, and one that
// lists only other errors marks the row as possibly out of date.
const queries = queryState(context.panelData, SOURCE);
const eventsFailed = queries.eventsFailed;
const listedError = (errors) => errors.map((error) => error.message).filter(Boolean).join('; ') || 'unknown error';
// Grafana can drop a failed table query's error when another query also
// failed, so a row shown after a failed refresh says it can be out of date.
const staleNotice = () =>
  queries.unsure
    ? `<p class="det-red">A query failed on the last refresh (${escapeHtml(listedError(queries.listed))}). Grafana may not list every query that failed, so this row and its CLI commands can be out of date. Refresh the board and check the row again before you run a command.</p>`
    : '';
const humanize = (step) => String(step || '').replace(/([a-z0-9])([A-Z])/g, '$1 $2');
const isTxHash = (value) => typeof value === 'string' && /^0x[0-9a-fA-F]{64}$/.test(value);
const eventValue = (key, value, chain) => {
  if (key === 'error') return `<span class="det-red">${escapeHtml(value)}</span>`;
  if (isTxHash(value)) return txLink(value, chain);
  if (value !== null && typeof value === 'object') return `<span class="det-muted">${escapeHtml(JSON.stringify(value))}</span>`;
  return escapeHtml(value);
};
// A payload's own `chain` names where its hashes live; else the row's chain
// (a trade's), else none, which leaves the hashes unlinked.
const eventFields = (payload, rowChain) => {
  const fields = payload && typeof payload === 'object' ? payload : {};
  const chain = typeof fields.chain === 'string' ? fields.chain : rowChain;
  return Object.entries(fields)
    .filter(([key]) => !/(_at|At|timestamp)$/.test(key))
    .map(([key, value]) => `<div class="det-field det-mono"><span class="det-muted">${escapeHtml(key.replace(/_/g, ' '))}</span><span class="det-break">${eventValue(key, value, chain)}</span></div>`)
    .join('');
};

// The events scan and the table scans run at different moments, so a row's
// newest status can also be ahead of its events until the next refresh.
const timeline = (events, history, chain, rowEventId) =>
  events.length > 0
    ? `
  <div class="det-section-title">Event timeline</div>
  ${eventsFailed ? '<p class="det-red">The event query failed, so this timeline can be out of date.</p>' : ''}
  ${timelineIncomplete(events, rowEventId) ? '<p class="det-muted">Some events of this row are not here: they are not loaded yet (refresh), outside the newest 500 event lines the board loads, or the bot missed a line. The full timeline is in the SPA at liquidity.t0trade.com.</p>' : ''}
  <div class="det-timeline">${events
    .map(
      (event) => `
    <div class="det-step">
      <span class="det-dot ${statusClass(event.step)}"></span>
      <div class="${statusClass(event.step)}">${escapeHtml(humanize(event.step))}</div>
      <div class="det-muted det-mono">${utc(event.time)}</div>
      ${eventFields(event.payload, chain)}
    </div>`
    )
    .join('')}</div>`
    : `
  <div class="det-section-title">Status history</div>
  <div class="det-timeline">${history
    .map(
      (entry) => `
    <div class="det-step">
      <span class="det-dot ${statusClass(entry.status)}"></span>
      <div class="${statusClass(entry.status)}">${escapeHtml(statusLabel(entry.status))}</div>
      <div class="det-muted det-mono">${utc(entry.time)}</div>
      ${entry.error ? `<div class="det-red det-mono det-break">${escapeHtml(entry.error)}</div>` : ''}
    </div>`
    )
    .join('')}</div>
  <p class="det-muted">${SOURCE === 'exporter' ? "From the exporter's status log." : eventsFailed ? "From the bot's status lines: the event query failed." : "From the bot's status lines; its event lines for this row are not in the newest 500."} The full event timeline is in the SPA at liquidity.t0trade.com.</p>`;

// valueHtml is trusted HTML: a caller escapes any data it puts in it.
const field = (name, valueHtml) => `<div class="det-field"><span class="det-muted">${escapeHtml(name)}</span><span class="det-break">${valueHtml}</span></div>`;

const tradeDialog = (trade) => {
  const [venueName, venueClass, onchain] = venue(trade.venue);
  const id = String(trade.id);
  // An onchain trade id is `chain:tx_hash:log_index` (OnChainTradeId), or
  // `tx_hash:log_index` from before the chain was part of it (Base).
  const parts = id.split(':');
  const [chain, hash, log] = parts.length === 3 ? parts : ['base', ...parts];
  const idCell =
    onchain && parts.length >= 2 && isTxHash(hash)
      ? `${txLink(hash, chain)} <span class="det-muted">(${escapeHtml(chain)}, log ${escapeHtml(log)})</span>`
      : `<span class="det-muted">${escapeHtml(id)}</span>`;
  return `
  <div class="det-dialog-head">
    <span class="det-title-line">
      <span class="${trade.direction === 'buy' ? 'det-green' : 'det-red'}">${escapeHtml(trade.direction)}</span>
      <span class="det-mono">${escapeHtml(trade.symbol)}</span>
      <span class="det-mono det-muted">${decimal(trade.shares)} ${onchain ? 'wrapped shares' : 'shares'}</span>
      <span class="${venueClass}">${escapeHtml(venueName)}</span>
    </span>
    <button class="det-close" data-close aria-label="Close">&times;</button>
  </div>
  <div class="det-dialog-body">
    ${staleNotice()}
    <div class="det-fields det-mono">
      ${field('ID', idCell)}
      ${field('Occurred At', utc(trade.occurred_at || trade.first))}
      ${field('Status', `<span class="${statusClass(trade.status)}">${escapeHtml(statusLabel(trade.status))}</span>${trade.error ? `<div class="det-red">${escapeHtml(trade.error)}</div>` : ''}`)}
    </div>
    ${timeline(eventTimeline(eventLines, 'trade'), trade.history, onchain ? chain : null, trade.event_id)}
    ${commandBlock(tradeCommands(CLIENT, trade.symbol))}
  </div>`;
};

const transferDialog = (transfer) => {
  const usdcFailed = transfer.kind === 'usdc_bridge' && String(transfer.status).toLowerCase() === 'failed';
  return `
  <div class="det-dialog-head">
    <span class="det-title-line">
      <span>${escapeHtml(typeLabel(transfer))}</span>
      <span class="det-mono det-muted">${escapeHtml(assetOf(transfer))}</span>
      <span class="det-mono det-muted">${decimal(transfer.amount)}</span>
    </span>
    <button class="det-close" data-close aria-label="Close">&times;</button>
  </div>
  <div class="det-dialog-body">
    ${staleNotice()}
    <div class="det-fields det-mono">
      ${field('ID', `<span class="det-muted">${escapeHtml(transfer.id)}</span>`)}
      ${field('Started', utc(transfer.started_at || transfer.first))}
      ${field('Updated', utc(transfer.newest))}
      ${field('Status', `<span class="${statusClass(transfer.status)}">${escapeHtml(statusLabel(transfer.status))}</span>`)}
    </div>
    ${timeline(eventTimeline(eventLines, 'transfer', transfer.kind), transfer.history, null, transfer.event_id)}
    ${commandBlock(
      transferCommands(CLIENT, transfer),
      usdcFailed ? usdcFailedNote(transfer.direction) : ''
    )}
  </div>`;
};

// --------------------------------------------------------------------------
// Open, refresh or close the one dialog
// --------------------------------------------------------------------------

// Kept on the element across renders: the plugin re-runs this code on every
// refresh and query, into the same element.
const state = root.__det || (root.__det = { dismissed: null });

// The variable can lag the close by a render (a refresh that was already
// running), so an id the operator just closed is not reopened until the
// variable has moved off it.
if (state.dismissed !== null && state.dismissed !== wanted) state.dismissed = null;

// Switching Environment keeps the tables' old rows on screen until the new
// queries return, and those rows belong to the other project. Hold back
// every row after a switch until a query has run (Grafana showed the panel
// Loading) and then succeeded. Frames alone cannot tell: Grafana replays and
// copies the old ones, and keeps them on an error. A Cloud Logging query
// takes over a second, so the Loading state shows; if it does not, or shows
// only on the switch render, the dialog says Loading… until the next
// refresh, which fails safe. A query cancelled from the refresh picker
// after the switch also ends Done with the old rows and releases the hold,
// but each row carries its project (lineEntries), so a row of the other
// project still does not show. The memory lives on the window, not the
// element, so a remount (tab switch, view mode) keeps it. The first render
// after the page loads is not held back: its rows are for the environment
// it opened on.
const loading = context.panelData?.state === 'Loading';
const failed = queries.failed;
const envState = window.__liqDetailEnv || (window.__liqDetailEnv = { env: undefined, held: false, sawLoading: false });
const switched = envState.env !== undefined && envState.env !== env;
envState.env = env;
if (switched) {
  // A Loading state on this render can be a run that started before the
  // switch, so only a later one counts.
  envState.held = true;
  envState.sawLoading = false;
} else if (envState.held) {
  // A query that failed does not count as the one that ran.
  if (loading) envState.sawLoading = true;
  else if (failed) envState.sawLoading = false;
  else if (envState.sawLoading) envState.held = false;
}

// A failed refresh can keep the old frames, so it shows the error, not a
// row and its commands that may be out of date.
const trade = envState.held || failed ? null : latest(entriesOf(`${SOURCE}-trades`));
const transfer = trade || envState.held || failed ? null : latest(entriesOf(`${SOURCE}-transfers`));
// The dialog opens on the click, before the query returns, and says so when
// the id has no entries in the panel's window.
const message = (text) => `
  <div class="det-dialog-head"><span class="det-title-line det-mono">${escapeHtml(wanted)}</span><button class="det-close" data-close aria-label="Close">&times;</button></div>
  <div class="det-dialog-body"><p class="det-muted">${escapeHtml(text)}</p></div>`;
const queryError = () => listedError(queries.blocking);
const html = trade
  ? tradeDialog(trade)
  : transfer
    ? transferDialog(transfer)
    : wanted === ''
      ? null
      : failed
        ? message(`The Trades or Rebalances query failed (${queryError()}), so this row cannot be shown. Refresh the board to retry.`)
        : loading || envState.held
          ? message('Loading…')
          : message('This id is not in the newest 500 status entries any more. Search the Logs tab for it.');

// Reuse the dialog when it is still in the element, so a refresh with the
// dialog open swaps its content without closing and reopening it.
let dialog = root.querySelector('dialog.det-dialog');
if (!dialog) {
  dialog = document.createElement('dialog');
  dialog.className = 'det-dialog';
  root.appendChild(dialog);
}

// Property handlers, not listeners: each render replaces them instead of
// stacking another copy. The operator's dismissal is handled when it is
// asked for: the close button, a backdrop click, or Escape (cancel). The
// close event is not used: it is queued, so it can run after a later render
// has reopened a row. A close this script makes, because the variable went
// empty (browser Back, say), is no dismissal: Forward or a click on the
// same row opens the row again.
const recordDismissal = () => {
  state.dismissed = dialog.detId || null;
  dialog.detId = '';
  // An empty value, not null: removing the URL parameter leaves a textbox
  // variable at its old value, so the same row could not be reopened.
  if (context.grafana.replaceVariables('${detail}') !== '') {
    context.grafana.locationService.partial({ 'var-detail': '' }, true);
  }
};
const dismiss = () => {
  recordDismissal();
  dialog.close();
};
dialog.oncancel = recordDismissal;
// A click on the backdrop closes it, like the SPA.
dialog.onclick = (event) => {
  if (event.target === dialog) dismiss();
};

if (html === null || state.dismissed === wanted) {
  if (dialog.open) {
    dialog.detId = '';
    dialog.close();
  }
} else {
  if (dialog.detHtml !== html) {
    // A refresh that brings a new status keeps the operator's scroll.
    const body = dialog.querySelector('.det-dialog-body');
    const scroll = dialog.detId === wanted && body ? body.scrollTop : 0;
    dialog.innerHTML = html;
    dialog.detHtml = html;
    const next = dialog.querySelector('.det-dialog-body');
    if (next) next.scrollTop = scroll;
  }
  dialog.detId = wanted;
  dialog.querySelector('[data-close]').onclick = dismiss;
  wireCopyButtons(dialog);
  if (!dialog.open) dialog.showModal();
}
