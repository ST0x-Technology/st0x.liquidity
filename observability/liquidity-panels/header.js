// Business Text "afterRender" code for the Liquidity bot header: the SPA's
// HeaderBar and SettingsBar (header-bar.svelte, settings-bar.svelte,
// recovery-guide.svelte in ST0x-Technology/st0x.liquidity) in one row, with
// the same Config and CLI recovery guide dialogs.
//
// Input: context.data, rows with a `k` column naming the value (up, uptime,
// commit, info, equity_target, ...) plus the string labels some of them
// carry (git_commit on commit; broker, wallet_kind, ... on info).
//
// RECOVERY_GUIDE is not defined here: the generator prepends it from
// recovery-guide.json (see pills() in the generator), because the template output is
// sanitized and cannot carry data into this code.

const rows = Array.isArray(context.data) ? context.data : [];
const value = {};
const labels = {};
// The stackdriver datasource runs a range query even for an instant target,
// so each `k` can arrive at several times; keep the newest.
const seen = {};
for (const row of rows) {
  const time = Number(row.Time) || 0;
  if (row.k !== 'commit') {
    if (row.k in seen && seen[row.k] > time) continue;
    seen[row.k] = time;
  }
  if (row.k === 'commit') {
    // A range query keeps the previous commit's series for a few minutes
    // after a deploy. The commit row's value is its sample timestamp, so the
    // newest one is the running commit.
    if (!labels.commit || row.Value > labels.commit.at) labels.commit = { sha: row.git_commit, at: row.Value };
  } else if (row.k === 'info') {
    labels.info = row;
  } else {
    value[row.k] = row.Value === null || row.Value === undefined || Number.isNaN(Number(row.Value)) ? null : Number(row.Value);
  }
}
const info = labels.info || {};

const theme = context.grafana.theme;
const root = context.element;
// The guide's commands target the environment the board shows: the client
// takes the selector's text, production or staging.
const ENV = context.grafana.replaceVariables('${env:text}') === 'staging' ? 'staging' : 'production';
// The plugin pads each rendered row by 8px, which makes this one-row panel
// scroll; the row has no room for it.
root.style.padding = '0';
root.style.height = '100%';
root.style.overflow = 'hidden';
root.style.setProperty('--hdr-muted', theme.colors.text.secondary);
// Light theme swaps the mode colours for darker shades (header.css).
root.classList.toggle('hdr-light', !theme.isDark);
root.style.setProperty('--hdr-border', theme.colors.border.weak);
root.style.setProperty('--hdr-pill', theme.colors.background.secondary);
root.style.setProperty('--hdr-card', theme.colors.background.primary);
root.style.setProperty('--hdr-text', theme.colors.text.primary);

const escapeHtml = (text) =>
  String(text ?? '').replace(/[&<>"']/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char]);

// The SPA's fmtPct and band strings: "50%" and "45%–55%".
const pct = (fraction) => `${(fraction * 100).toFixed(0)}%`;
const band = (target, deviation) =>
  target === null || target === undefined
    ? ''
    : deviation === null || deviation === undefined
      ? ''
      : ` <span class="hdr-muted">(${pct(target - deviation)}–${pct(target + deviation)})</span>`;

// The SPA's formatUptime.
const uptime = (seconds) => {
  if (seconds === null || seconds === undefined) return '';
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  if (days > 0) return `up ${days}d ${hours}h`;
  if (hours > 0) return `up ${hours}h ${minutes}m`;
  return `up ${minutes}m`;
};

const clock = () => new Date().toISOString().replace('T', ' ').slice(0, 19) + ' UTC';

const pill = (inner) => `<span class="hdr-pill">${inner}</span>`;
// 1 up, 0 down; no value means the source is not reporting, which says
// nothing about the bot.
const connection =
  value.up === 1 ? ['hdr-ok', 'Connected'] : value.up === 0 ? ['hdr-bad', 'Disconnected'] : ['hdr-unknown', 'No data'];

const pills = [
  info.broker ? pill(`<span class="hdr-mono">${escapeHtml(info.broker)}</span>`) : '',
  value.equity_target !== null && value.equity_target !== undefined
    ? pill(`Equity <b class="hdr-mono">${pct(value.equity_target)}</b>${band(value.equity_target, value.equity_deviation)}`)
    : '',
  value.usdc_target !== null && value.usdc_target !== undefined
    ? pill(`USDC <b class="hdr-mono">${pct(value.usdc_target)}</b>${band(value.usdc_target, value.usdc_deviation)}`)
    : '',
  value.trigger !== null && value.trigger !== undefined ? pill(`Trigger <b class="hdr-mono">$${value.trigger}</b>`) : '',
  value.cash_reserved !== null && value.cash_reserved !== undefined
    ? pill(`Reserve <b class="hdr-mono">$${value.cash_reserved}</b>`)
    : '',
  '<button class="hdr-pill hdr-button" data-open="config">Config</button>',
].join('');

const configRow = (name, text) =>
  `<div class="hdr-row"><span class="hdr-muted">${name}</span><span class="hdr-mono hdr-trunc" title="${escapeHtml(text)}">${escapeHtml(text)}</span></div>`;
const seconds = (number) => (number === null || number === undefined ? '' : `${number}s`);

const configDialog = `
<dialog class="hdr-dialog hdr-narrow" data-dialog="config">
  <div class="hdr-dialog-head"><span>Configuration</span><button class="hdr-close" data-close aria-label="Close">&times;</button></div>
  <div class="hdr-dialog-body hdr-mono">
    ${
      info.wallet_kind
        ? configRow('Wallet', info.wallet_kind) +
          configRow('Address', info.wallet_address) +
          (info.wallet_kind === 'turnkey' && info.turnkey_organization ? configRow('Turnkey org', info.turnkey_organization) : '') +
          '<div class="hdr-rule"></div>'
        : ''
    }
    ${configRow('Exported log level', info.log_level)}
    ${configRow('Server port', info.server_port)}
    ${configRow('Deployment block', value.deployment_block ?? '')}
    ${configRow('Order polling', seconds(value.order_polling))}
    ${configRow('Inventory polling', seconds(value.inventory_polling))}
    ${configRow('Cash reserve', value.cash_reserved !== null && value.cash_reserved !== undefined ? `$${value.cash_reserved}` : 'none')}
    ${configRow('Orderbook', info.orderbook)}
  </div>
</dialog>`;

const guide = RECOVERY_GUIDE;
const modeClass = (mode) =>
  mode === 'requires-bot' ? 'hdr-amber' : 'hdr-red';
const guideDialog = `
<dialog class="hdr-dialog hdr-wide" data-dialog="guide">
  <div class="hdr-dialog-head"><span>CLI recovery guide</span><button class="hdr-close" data-close aria-label="Close">&times;</button></div>
  <div class="hdr-dialog-body">
    <p class="hdr-muted hdr-intro">Every recovery command, grouped by object. They use <span class="hdr-mono">st0x-liquidity-client</span>, which signs in with your Google account and calls the running bot's API, so no SSH is needed and the bot must be running. The one exception is failing a Base to Alpaca USDC bridge after its vault withdrawal confirmed: that needs the offline <span class="hdr-mono">stox fail-usdc-transfer</span> with the bot stopped.</p>
    ${guide.groups
      .map(
        (group) => `
      <section class="hdr-group">
        <h3 class="hdr-mono">${escapeHtml(group.object)}</h3>
        ${group.commands
          .map(
            (entry) => `
          <div class="hdr-command">
            <pre class="hdr-mono">${escapeHtml(entry.command.replace('--env production', `--env ${ENV}`))}</pre>
            <div class="hdr-grid">
              <span class="hdr-muted">What</span><span>${escapeHtml(entry.description)}</span>
              <span class="hdr-muted">When</span><span>${escapeHtml(entry.whenToUse)}</span>
              <span class="hdr-muted">Applies to</span><span>${escapeHtml(entry.appliesTo)}</span>
              <span class="hdr-muted">Mode</span><span class="${modeClass(entry.mode)}">${escapeHtml(guide.modeLabels[entry.mode] || entry.mode)}</span>
            </div>
          </div>`
          )
          .join('')}
      </section>`
      )
      .join('')}
  </div>
</dialog>`;

// The board refreshes every minute and each render replaces the markup, which
// would close a dialog the operator is reading. Remember it to reopen it at
// the same scroll position.
const openDialog = root.querySelector('dialog[open]');
const reopen = openDialog && {
  name: openDialog.dataset.dialog,
  scroll: openDialog.querySelector('.hdr-dialog-body')?.scrollTop || 0,
};

root.innerHTML = `
<div class="hdr">
  <div class="hdr-left">${pills}</div>
  <div class="hdr-right">
    <button class="hdr-guide" data-open="guide">CLI recovery guide</button>
    <span class="hdr-mono hdr-muted" data-clock>${clock()}</span>
    ${labels.commit && labels.commit.sha ? `<span class="hdr-mono hdr-muted" title="Deployed commit">${escapeHtml(labels.commit.sha.slice(0, 7))}</span>` : ''}
    <span class="hdr-muted" title="Bot uptime">${uptime(value.uptime)}</span>
    <span class="hdr-badge ${connection[0]}">${connection[1]}</span>
  </div>
</div>
${configDialog}
${guideDialog}`;

root.querySelectorAll('[data-open]').forEach((button) => {
  button.addEventListener('click', () => root.querySelector(`[data-dialog="${button.dataset.open}"]`).showModal());
});
root.querySelectorAll('dialog').forEach((dialog) => {
  dialog.querySelector('[data-close]').addEventListener('click', () => dialog.close());
  // A click on the backdrop closes it, like the SPA.
  dialog.addEventListener('click', (event) => {
    if (event.target === dialog) dialog.close();
  });
});
if (reopen) {
  const dialog = root.querySelector(`[data-dialog="${reopen.name}"]`);
  if (dialog) {
    dialog.showModal();
    const body = dialog.querySelector('.hdr-dialog-body');
    if (body) body.scrollTop = reopen.scroll;
  }
}

// The SPA's ticking UTC clock. One interval per element, replaced on every
// render so a refresh never stacks them. Grafana can also mount a new
// element (tab switch, view mode), so an interval whose element left the
// page stops itself.
clearInterval(root.__hdrClock);
root.__hdrClock = setInterval(() => {
  if (!root.isConnected) {
    clearInterval(root.__hdrClock);
    return;
  }
  const node = root.querySelector('[data-clock]');
  if (node) node.textContent = clock();
}, 1000);
