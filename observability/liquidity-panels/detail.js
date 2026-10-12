// Business Text "afterRender" code for the Liquidity bot's detail panel: the
// SPA's trade and transfer detail dialogs (trade-history-panel.svelte,
// transfer-panel.svelte in ST0x-Technology/st0x.liquidity) for the row whose
// ⓘ was clicked in the Trades or Rebalances table.
//
// The click sets the hidden `detail` variable to the row's id. A and B are
// the Trades and Rebalances tables' own results (the newest 500 status
// entries of each log, through the Dashboard datasource), and the script
// picks the id's entries out of them. C, the bot's event timeline, has no
// query until the bot logs its events, so the dialog shows the status
// history. Closing the dialog clears the variable.
//
// The panel is one empty column of the header row; only the dialog shows.
//
// MODE_LABELS, tradeCommands, transferCommands and latest are not defined
// here: the generator prepends them from recovery-guide.json,
// recovery-commands.js and status-history.js (see detail_panel()).

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

// One entry per log line: its time and the exporter's payload. The plugin
// ships the payload as the JSON body and again as flattened
// `jsonPayload.<key>` labels; the labels are the fallback.
const entriesOf = (refId) => {
  const frame = (context.panelData?.series || []).find((series) => series.refId === refId);
  if (!frame) return [];
  const column = (name) => frame.fields.find((field) => field.name === name);
  const times = column('timestamp');
  const bodies = column('body');
  const labels = column('labels');
  const count = times ? times.values.length : 0;
  // A JSON object, or null for anything else (no column, bad JSON, a string).
  const parseObject = (value) => {
    if (value !== null && typeof value === 'object') return value;
    try {
      const parsed = JSON.parse(value);
      return parsed !== null && typeof parsed === 'object' ? parsed : null;
    } catch (error) {
      return null;
    }
  };
  const fromLabels = (index) => {
    const payload = {};
    const flat = labels ? parseObject(labels.values[index]) : null;
    for (const [key, value] of Object.entries(flat || {})) {
      if (key.startsWith('jsonPayload.')) payload[key.slice('jsonPayload.'.length)] = value;
    }
    return payload;
  };
  const entries = [];
  for (let index = 0; index < count; index++) {
    const payload = (bodies && parseObject(bodies.values[index])) || fromLabels(index);
    if (payload.id && String(payload.id) === wanted) entries.push({ time: Number(times.values[index]), ...payload });
  }
  return entries;
};

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
};
const typeOf = (transfer) => (transfer.kind === 'usdc_bridge' ? transfer.direction : transfer.kind);
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

// The environment the board shows: the client takes the selector's text.
const ENV = context.grafana.replaceVariables('${env:text}') === 'staging' ? 'staging' : 'production';
const CLIENT = `st0x-liquidity-client --env ${ENV}`;

const modeClass = (mode) => (mode === 'requires-bot' ? 'det-amber' : 'det-red');
const commandBlock = (commands, note) =>
  commands.length === 0 && !note
    ? ''
    : `<div class="det-cli"><div class="det-cli-title">CLI commands</div>${note ? `<p class="det-muted">${escapeHtml(note)}</p>` : ''}${commands
        .map(
          (entry) => `
        <div class="det-command">
          <pre>${escapeHtml(entry.command)}</pre>
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

// The bot's event timeline, once the panel queries it as C: each step with
// its time and fields, like the SPA's dialog.
// Without it, the exporter's status log: one step per status change.
const events = entriesOf('C').sort((left, right) => (left.sequence ?? 0) - (right.sequence ?? 0));
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

// The board loads the newest 500 events of all rows, so an older row can have
// only its later events here. Then the timeline starts after the row's first
// status entry, and says so.
const timelineIncomplete = (history) =>
  history.length > 0 &&
  Math.min(...events.map((event) => event.time)) > Math.min(...history.map((entry) => entry.time)) + 120000;

const timeline = (history, chain) =>
  events.length > 0
    ? `
  <div class="det-section-title">Event timeline</div>
  ${timelineIncomplete(history) ? '<p class="det-muted">Older events of this row are outside the newest 500 the board loads; the full timeline is in the SPA at liquidity.t0trade.com.</p>' : ''}
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
  <p class="det-muted">From the exporter's status log. The full event timeline is in the SPA at liquidity.t0trade.com.</p>`;

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
    <div class="det-fields det-mono">
      ${field('ID', idCell)}
      ${field('Occurred At', utc(trade.first))}
      ${field('Status', `<span class="${statusClass(trade.status)}">${escapeHtml(statusLabel(trade.status))}</span>${trade.error ? `<div class="det-red">${escapeHtml(trade.error)}</div>` : ''}`)}
    </div>
    ${timeline(trade.history, onchain ? chain : null)}
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
    <div class="det-fields det-mono">
      ${field('ID', `<span class="det-muted">${escapeHtml(transfer.id)}</span>`)}
      ${field('Started', utc(transfer.started_at || transfer.first))}
      ${field('Updated', utc(transfer.time))}
      ${field('Status', `<span class="${statusClass(transfer.status)}">${escapeHtml(statusLabel(transfer.status))}</span>`)}
    </div>
    ${timeline(transfer.history, null)}
    ${commandBlock(
      transferCommands(CLIENT, transfer),
      usdcFailed
        ? 'Reconcile applies only when the funds left their source venue, which the logs do not show. Check the transfer in the SPA before you reconcile it.'
        : ''
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
// refresh, which fails safe. Known
// limit: a query cancelled from the refresh picker after the switch also
// ends Done with the old rows, and that releases the hold. The memory
// lives on the window, not the element, so a remount (tab switch, view mode)
// keeps it. The first render after the page loads is not held back: its rows
// are for the environment it opened on.
const env = context.grafana.replaceVariables('${env}');
const loading = context.panelData?.state === 'Loading';
const failed = context.panelData?.state === 'Error';
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
const trade = envState.held || failed ? null : latest(entriesOf('A'));
const transfer = trade || envState.held || failed ? null : latest(entriesOf('B'));
// The dialog opens on the click, before the query returns, and says so when
// the id has no entries in the panel's window.
const message = (text) => `
  <div class="det-dialog-head"><span class="det-title-line det-mono">${escapeHtml(wanted)}</span><button class="det-close" data-close aria-label="Close">&times;</button></div>
  <div class="det-dialog-body"><p class="det-muted">${escapeHtml(text)}</p></div>`;
const queryError = () => {
  const errors = context.panelData?.errors || (context.panelData?.error ? [context.panelData.error] : []);
  return errors.map((error) => error.message).filter(Boolean).join('; ') || 'unknown error';
};
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
// stacking another copy.
dialog.onclose = () => {
  state.dismissed = dialog.detId || null;
  dialog.detId = '';
  // An empty value, not null: removing the URL parameter leaves a textbox
  // variable at its old value, so the same row could not be reopened.
  if (context.grafana.replaceVariables('${detail}') !== '') {
    context.grafana.locationService.partial({ 'var-detail': '' }, true);
  }
};
// A click on the backdrop closes it, like the SPA.
dialog.onclick = (event) => {
  if (event.target === dialog) dialog.close();
};

if (html === null || state.dismissed === wanted) {
  if (dialog.open) dialog.close();
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
  dialog.querySelector('[data-close]').onclick = () => dialog.close();
  if (!dialog.open) dialog.showModal();
}
